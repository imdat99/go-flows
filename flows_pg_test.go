package flows_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	flows "github.com/imdat99/go-flows"
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
