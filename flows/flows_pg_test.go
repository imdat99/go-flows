package flows_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/testdb"
)

type item struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

var (
	pick = flows.DefineActivity("pgtest.pick", func(_ context.Context, k flows.Key, it item) (string, string, error) {
		return string(k), it.SKU, nil
	}, nil)
	pickAll = flows.DefineWorkflow("pgtest.pick-all", 1, func(c flows.Ctx, items []item) ([]string, error) {
		futures := make([]*flows.Future[string], len(items))
		for i, it := range items {
			futures[i] = pick.Start(c, it) // fan out
		}
		keys := make([]string, len(items))
		for i, f := range futures {
			k, err := f.Get(c)
			if err != nil {
				return nil, err
			}
			keys[i] = k
		}
		if err := flows.Sleep(c, 20*time.Millisecond); err != nil {
			return nil, err
		}
		return keys, nil
	})
)

// End to end on Postgres through the public API, with two engines sharing
// the database.
func TestPostgresEndToEnd(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		e, err := flows.New(pool, flows.Options{InstanceID: "pg-" + strconv.Itoa(i), PollMax: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Stop(ctx) })
	}
	client, err := flows.New(pool, flows.Options{InstanceID: "client"}) // not started: client only
	if err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	handles := make([]*flows.Handle[[]string], 10)
	for i := range handles {
		h, err := pickAll.Start(ctx, client, "pick-"+strconv.Itoa(i), []item{{"a", 1}, {"b", 2}, {"c", 3}})
		if err != nil {
			t.Fatal(err)
		}
		handles[i] = h
	}
	for i, h := range handles {
		keys, err := h.Wait(wctx)
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		want := "pick-" + strconv.Itoa(i) + ":1:pgtest.pick"
		if len(keys) != 3 || keys[0] != want {
			t.Fatalf("run %d keys %v, want first %q", i, keys, want)
		}
	}
}

var (
	undone   sync.Map // instance id -> *[]string (guarded by undoneMu)
	undoneMu sync.Mutex

	reserve = flows.DefineActivity("pgtest.reserve", func(_ context.Context, _ flows.Key, sku string) (string, string, error) {
		return "r-" + sku, sku, nil
	}, func(_ context.Context, k flows.Key, sku string) error {
		id, _, _ := strings.Cut(string(k), ":")
		undoneMu.Lock()
		defer undoneMu.Unlock()
		v, _ := undone.LoadOrStore(id, &[]string{})
		*v.(*[]string) = append(*v.(*[]string), sku)
		return nil
	})
	reserveAll = flows.DefineWorkflow("pgtest.reserve-all", 1, func(c flows.Ctx, skus []string) (int, error) {
		for _, s := range skus {
			if _, err := reserve.Run(c, s); err != nil {
				return 0, err
			}
		}
		return 0, errors.New("payment declined")
	})
)

// Compensation on Postgres, with eager execution and with polled tasks.
func TestPostgresCompensation(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, eager := range []bool{true, false} {
		e, err := flows.New(pool, flows.Options{InstanceID: "pg-comp-" + strconv.FormatBool(eager), PollMax: 50 * time.Millisecond, DisableEager: !eager})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Start(ctx); err != nil {
			t.Fatal(err)
		}
		wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		id := "comp-" + strconv.FormatBool(eager)
		undone.Delete(id) // -count reruns
		_, err = reserveAll.Run(wctx, e, id, []string{"a", "b", "c"})
		cancel()
		var re *flows.RunError
		if !errors.As(err, &re) || re.Status != flows.StatusCompensated || !errors.Is(err, flows.ErrCompensated) {
			t.Fatalf("eager=%v: %v", eager, err)
		}
		undoneMu.Lock()
		v, _ := undone.Load(id)
		got := slices.Clone(*v.(*[]string))
		undoneMu.Unlock()
		if !slices.Equal(got, []string{"c", "b", "a"}) {
			t.Fatalf("eager=%v: undone %v", eager, got)
		}
		_ = e.Stop(ctx)
	}
}

var (
	inside, maxInside atomic.Int32

	critical = flows.DefineActivity("pgtest.critical", func(context.Context, flows.Key, int) (int, struct{}, error) {
		n := inside.Add(1)
		for {
			m := maxInside.Load()
			if n <= m || maxInside.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inside.Add(-1)
		return 0, struct{}{}, nil
	}, nil)
	exclusive = flows.DefineWorkflow("pgtest.exclusive", 1, func(c flows.Ctx, _ int) (int, error) {
		if err := flows.Lock(c, "shared", time.Second, flows.LockWait(flows.RetryPolicy{Attempts: 1000, Initial: 10 * time.Millisecond})); err != nil {
			return 0, err
		}
		return critical.Run(c, 0)
	})
)

// Business locks on Postgres: workflows on two engines contending for one
// lock never run its critical section at the same time.
func TestPostgresLock(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var engines []*flows.Engine
	for i := range 2 {
		e, err := flows.New(pool, flows.Options{InstanceID: "pg-lock-" + strconv.Itoa(i), PollMax: 20 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Stop(ctx) })
		engines = append(engines, e)
	}
	inside.Store(0)
	maxInside.Store(0)
	wctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	var hs []*flows.Handle[int]
	for i := range 6 {
		h, err := exclusive.Start(ctx, engines[i%2], "excl-"+strconv.Itoa(i), 0)
		if err != nil {
			t.Fatal(err)
		}
		hs = append(hs, h)
	}
	for _, h := range hs {
		if _, err := h.Wait(wctx); err != nil {
			t.Fatal(err)
		}
	}
	if m := maxInside.Load(); m != 1 {
		t.Fatalf("%d executions inside the critical section at once", m)
	}
}

// outbox is a RunHook staging one message per ended execution, in the
// ending transaction.
type outbox struct{}

func (outbox) RunEnded(ctx context.Context, tx pgx.Tx, run flows.RunInfo) error {
	_, err := tx.Exec(ctx, `INSERT INTO outbox (instance, status) VALUES ($1, $2)`, run.InstanceID, string(run.Status))
	return err
}

// The run hook writes in the transaction that ends the execution.
func TestPostgresRunHook(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE outbox (instance text PRIMARY KEY, status text)`); err != nil {
		t.Fatal(err)
	}
	e, err := flows.New(pool, flows.Options{InstanceID: "pg-hook", PollMax: 20 * time.Millisecond, Hook: outbox{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Stop(ctx) })
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := pickAll.Run(wctx, e, "hook-ok", []item{{"a", 1}}); err != nil {
		t.Fatal(err)
	}
	undone.Delete("hook-comp")
	if _, err := reserveAll.Run(wctx, e, "hook-comp", []string{"a"}); !errors.Is(err, flows.ErrCompensated) {
		t.Fatal(err)
	}
	rows, err := pool.Query(ctx, `SELECT instance || '=' || status FROM outbox ORDER BY instance`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"hook-comp=compensated", "hook-ok=completed"}) {
		t.Fatalf("outbox %v", got)
	}
}

var (
	handoffCalls atomic.Int32
	handoffStep  = flows.DefineActivity("pgtest.handoff-step", func(context.Context, flows.Key, int) (int, struct{}, error) {
		handoffCalls.Add(1)
		return 0, struct{}{}, nil
	}, nil)
	handoffWF = flows.DefineWorkflow("pgtest.handoff", 1, func(c flows.Ctx, _ int) (int, error) {
		if _, err := handoffStep.Run(c, 0); err != nil {
			return 0, err
		}
		return flows.Await[int](c, "go", time.Hour)
	})
)

// On Postgres, an engine that stops hands its waiting executions over at
// once (Unstick), and completed activities are not run again.
func TestPostgresStopHandOver(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	a, err := flows.New(pool, flows.Options{InstanceID: "pg-a", PollMax: 20 * time.Millisecond, StickyFor: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Stop(ctx) }) // a no-op after the Stop below
	handoffCalls.Store(0)
	h, err := handoffWF.Start(ctx, a, "pg-handoff", 0)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the workflow to reach Await (its timer is recorded): the
	// execution is then released, sticky to a.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM flows.history h JOIN flows.execution e ON e.id = h.execution_id
			WHERE e.instance_id = 'pg-handoff' AND h.type = $1`, int16(history.TimerScheduled)).Scan(&n)
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the workflow did not reach Await")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := a.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := flows.New(pool, flows.Options{InstanceID: "pg-b", PollMax: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop(ctx) })
	if err := b.Signal(ctx, "pg-handoff", "go", 9); err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if v, err := h.Wait(wctx); err != nil || v != 9 {
		t.Fatalf("%d %v", v, err)
	}
	if n := handoffCalls.Load(); n != 1 {
		t.Fatalf("activity ran %d times", n)
	}
}

var (
	reserveKid = flows.DefineWorkflow("pgtest.reserve-kid", 1, func(c flows.Ctx, sku string) (string, error) {
		return reserve.Run(c, sku)
	})
	reserveParent = flows.DefineWorkflow("pgtest.reserve-parent", 1, func(c flows.Ctx, id string) (int, error) {
		if _, err := reserve.Run(c, "p1"); err != nil {
			return 0, err
		}
		if _, err := reserveKid.Child(c, id+"-kid", "k1").Get(c); err != nil {
			return 0, err
		}
		return 0, errors.New("declined after the child")
	})
)

// T6.3 on Postgres: the failing parent compensates its completed child.
func TestPostgresNestedCompensation(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	e, err := flows.New(pool, flows.Options{InstanceID: "pg-nest", PollMax: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Stop(ctx) })
	undone.Delete("pg-nest")
	undone.Delete("pg-nest-kid")
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := reserveParent.Run(wctx, e, "pg-nest", "pg-nest"); !errors.Is(err, flows.ErrCompensated) {
		t.Fatalf("%v", err)
	}
	for id, want := range map[string]string{"pg-nest": "p1", "pg-nest-kid": "k1"} {
		undoneMu.Lock()
		v, ok := undone.Load(id)
		var got []string
		if ok {
			got = slices.Clone(*v.(*[]string))
		}
		undoneMu.Unlock()
		if !slices.Equal(got, []string{want}) {
			t.Fatalf("%s undone %v", id, got)
		}
	}
}

// T8.4 review: a canceled workflow's error is stored like a failure's
// (sanitized, bounded): a NUL byte or invalid UTF-8 in its text must not
// make Postgres refuse the end forever.
var cancelDirty = flows.DefineWorkflow("pgtest.cancel-dirty", 1, func(c flows.Ctx, _ int) (int, error) {
	_, err := flows.Await[int](c, "never", 0)
	return 0, fmt.Errorf("cleanup of a\x00b\xff: %w", err)
})

func TestCanceledErrorTextIsStorable(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	e, err := flows.New(pool, flows.Options{InstanceID: "pg-cancel-dirty", PollMax: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Stop(ctx) })
	h, err := cancelDirty.Start(ctx, e, "dirty", 0)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := e.Cancel(ctx, "dirty"); err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = h.Wait(wctx)
	var re *flows.RunError
	if !errors.As(err, &re) || re.Status != flows.StatusCanceled || !errors.Is(err, flows.ErrCanceled) {
		t.Fatalf("got %v", err)
	}
}

// T9.6: with Options.ListenDSN, the engine listens on a connection of its
// own (here named by application_name), not one of the pool's; a bad DSN
// is refused at New.
func TestListenDSN(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := flows.New(pool, flows.Options{ListenDSN: "postgres://%zz"}); !errors.Is(err, flows.ErrInvalidArgument) {
		t.Fatalf("bad DSN: %v", err)
	}
	cc := pool.Config().ConnConfig
	dsn := (&url.URL{
		Scheme: "postgres", User: url.UserPassword(cc.User, cc.Password),
		Host: net.JoinHostPort(cc.Host, strconv.Itoa(int(cc.Port))), Path: "/" + cc.Database,
		RawQuery: "application_name=flows-listen-test&sslmode=disable",
	}).String()
	e, err := flows.New(pool, flows.Options{InstanceID: "listen", ListenDSN: dsn, PollMax: time.Minute, LeaseTTL: 3 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Stop(ctx) }()
	for deadline := time.Now().Add(5 * time.Second); ; {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND application_name = 'flows-listen-test' AND query LIKE 'LISTEN%'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the engine does not listen on its ListenDSN connection")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Work started elsewhere (a client) is picked up at once, on the hint
	// (the engine's poll backs off to PollMax).
	time.Sleep(200 * time.Millisecond)
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client, err := flows.New(pool, flows.Options{InstanceID: "client"})
	if err != nil {
		t.Fatal(err)
	}
	keys, err := pickAll.Run(cctx, client, "listen-1", []item{{SKU: "a", Qty: 1}})
	if err != nil || len(keys) != 1 {
		t.Fatalf("run: %v %v", keys, err)
	}
}
