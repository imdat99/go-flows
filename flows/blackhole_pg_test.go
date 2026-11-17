package flows_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/testdb"
)

// T11.1 on Postgres: when the database blackholes under an engine, the
// activity it runs is canceled with ErrLeaseLost once its lease can no
// longer be trusted, before another engine claims the expired lease and
// runs the next attempt, so the attempts do not overlap.
func TestBlackholedDatabaseGivesUpLeases(t *testing.T) {
	const ttl = 6 * time.Second // margin ttl/3 = 2s between giving up and the lease expiring
	viaProxy, proxy := testdb.NewProxied(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, viaProxy); err != nil {
		t.Fatal(err)
	}
	// B reaches the same database directly: the freeze does not affect it.
	pool, err := pgxpool.NewWithConfig(ctx, proxy.DirectConfig(viaProxy.Config()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	type attempt struct {
		start, end time.Time
		cause      error
	}
	var (
		mu       sync.Mutex
		attempts []attempt
	)
	first := make(chan struct{})
	act := flows.DefineActivity(testdb.UniqueName(t, "act"), func(actx context.Context, _ flows.Key, in int) (int, struct{}, error) {
		mu.Lock()
		n := len(attempts)
		attempts = append(attempts, attempt{start: time.Now()})
		mu.Unlock()
		if n > 0 {
			return in, struct{}{}, nil // a later attempt: done at once
		}
		close(first)
		<-actx.Done()
		mu.Lock()
		attempts[0].end, attempts[0].cause = time.Now(), context.Cause(actx)
		mu.Unlock()
		return 0, struct{}{}, actx.Err()
	}, nil)
	wf := flows.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c flows.Ctx, in int) (int, error) {
		return act.Run(c, in)
	})

	start := func(p *pgxpool.Pool, id string) *flows.Engine {
		e, err := flows.New(p, flows.Options{InstanceID: id, LeaseTTL: ttl, PollMax: 100 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			proxy.Thaw() // let A stop
			sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			_ = e.Stop(sctx)
		})
		return e
	}
	a := start(viaProxy, "a")
	if _, err := wf.Start(ctx, a, "i1", 7); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first:
	case <-time.After(30 * time.Second):
		t.Fatal("the first attempt never started")
	}
	proxy.Freeze()
	start(pool, "b")

	wctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	client, err := flows.New(pool, flows.Options{InstanceID: "client"}) // not started: client only
	if err != nil {
		t.Fatal(err)
	}
	out, err := wf.Handle(client, "i1").Wait(wctx)
	if err != nil || out != 7 {
		t.Fatalf("result %d %v", out, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) < 2 {
		t.Fatalf("%d attempts", len(attempts))
	}
	first0, second := attempts[0], attempts[1]
	if first0.end.IsZero() {
		t.Fatal("the first attempt was never canceled")
	}
	if !errors.Is(first0.cause, flows.ErrLeaseLost) {
		t.Fatalf("first attempt canceled by %v, want ErrLeaseLost", first0.cause)
	}
	if !first0.end.Before(second.start) {
		t.Fatalf("the attempts overlapped: first canceled at +%v, second started at +%v",
			first0.end.Sub(first0.start), second.start.Sub(first0.start))
	}
}
