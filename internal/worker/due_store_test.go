package worker_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/testdb"
	"github.com/imdat99/go-flows/internal/worker"
)

// T9.1: work another worker commits with a delay (a timer, a retry delay, a
// hand-back) while this worker is idle (its polls backed off to a long
// PollMax) is claimed when it falls due: the commit notifies, the claim
// that follows tells when the work falls due.
func TestDelayedWorkOfAnotherWorkerIsOnTime(t *testing.T) {
	const (
		delay   = 300 * time.Millisecond
		pollMax = 3 * time.Second
		limit   = 1500 * time.Millisecond // well under pollMax
	)
	pool := testdb.New(t)
	ctx := context.Background()
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	st := pg.New(pool)
	var mu sync.Mutex
	marks := map[string][]time.Time{}
	mark := func(k string) {
		mu.Lock()
		marks[k] = append(marks[k], time.Now())
		mu.Unlock()
	}
	gate := make(chan struct{}) // B's first activity waits for it
	register := func(isB bool) *registry { return delayedRegistry(isB, delay, gate, mark) }
	newWorker := func(owner string, reg *registry, pollMin, pollMax time.Duration) *worker.Worker {
		w, err := worker.New(st, reg, reg, worker.Options{
			Owner: owner, Queues: []string{q}, Eager: true, LeaseTTL: 5 * time.Second,
			PollMin: pollMin, PollMax: pollMax,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return w
	}
	// "handed" is claimed by a third owner, standing for an engine that will
	// hand it back; B claims sleep and retry and holds them (its first
	// activity waits).
	create(t, st, "handed", "handed", "")
	tasks, err := st.ClaimWorkflowTasks(ctx, "c", []string{q}, 1, time.Minute)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim handed: %v %v", tasks, err)
	}
	handed := tasks[0]
	b := newWorker("b", register(true), 10*time.Millisecond, 50*time.Millisecond)
	for _, wf := range []string{"sleep", "retry"} {
		create(t, st, wf, wf, "")
	}
	waitUntil(t, func() bool {
		n := 0
		for _, id := range []string{"sleep", "retry"} {
			if e, err := st.GetExecution(ctx, id); err == nil && e.Status == store.StatusRunning {
				n++
			}
		}
		return n == 2
	})
	time.Sleep(100 * time.Millisecond) // B holds both (their gate activities run)
	// A starts now (nothing claimable, nothing due) and backs off.
	a := newWorker("a", register(false), pollMax, pollMax)
	t.Cleanup(func() { _ = a.Stop(ctx) })
	time.Sleep(1200 * time.Millisecond)
	// While A is idle: B commits the timer and the retry delay, then stops;
	// the third owner hands "handed" back for delay.
	close(gate)
	// Both first marks (not "exactly one each": under load, one workflow's
	// second mark may come before the other's first).
	waitUntil(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(marks["timer"]) >= 1 && len(marks["retry"]) >= 1 })
	mark("handed")
	if err := st.ReleaseWorkflowTask(ctx, handed.Lease, delay); err != nil {
		t.Fatal(err)
	}
	if err := b.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	for _, wf := range []string{"sleep", "retry", "handed"} {
		if e := waitEnd(t, st, wf); e.Status != store.StatusCompleted {
			t.Fatalf("%s: %s %s", wf, e.Status, e.Err)
		}
	}
	for _, k := range []string{"timer", "retry", "handed"} {
		mu.Lock()
		m := marks[k]
		mu.Unlock()
		if len(m) != 2 {
			t.Fatalf("%s: %d marks, want 2", k, len(m))
		}
		if d := m[1].Sub(m[0]); d < delay || d > limit {
			t.Errorf("%s: a delay of %v took %v", k, delay, d)
		}
	}
}

// countActClaims counts activity claims on a Postgres store.
type countActClaims struct {
	*pg.Store
	n atomic.Int32
}

func (c *countActClaims) ClaimActivityTasksDue(ctx context.Context, owner string, queues []string, n int, ttl time.Duration) ([]store.ActivityTask, time.Duration, error) {
	c.n.Add(1)
	return c.Store.ClaimActivityTasksDue(ctx, owner, queues, n, ttl)
}

// T9.1 review: running activities' lease expiries (their visible_at,
// pushed by heartbeats) are not due times: an idle worker does not wake for
// them.
func TestRunningActivitiesAreNotDue(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	reg := newRegistry()
	reg.addActivity("long", &worker.Activity{Run: func(ctx context.Context, _ *store.ActivityTask) ([]byte, []byte, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil, nil
	}})
	reg.addWorkflow("w", func(ctx coro.Context, _ []byte) ([]byte, error) { return activity(ctx, "long", nil) })
	b, err := worker.New(pg.New(pool), reg, reg, worker.Options{Owner: "b", Queues: []string{q}, Eager: true, LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { close(release); _ = b.Stop(ctx) }()
	for i := range 5 {
		create(t, pg.New(pool), "w"+string(rune('0'+i)), "w", "")
	}
	time.Sleep(500 * time.Millisecond) // B runs all five (heartbeats every ~330 ms)
	st := &countActClaims{Store: pg.New(pool)}
	a, err := worker.New(st, reg, reg, worker.Options{
		Owner: "a", Queues: []string{q}, LeaseTTL: 5 * time.Second,
		PollMin: 3 * time.Second, PollMax: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Stop(ctx) }()
	time.Sleep(4 * time.Second)
	// The claims on start and at the first timed poll (3 s): a wake-up per
	// lease expiry (~1 s) would make 4 or more.
	if n := st.n.Load(); n > 3 {
		t.Fatalf("%d activity claims in 4 s", n)
	}
}

// delayedRegistry registers the workflows of
// TestDelayedWorkOfAnotherWorkerIsOnTime; isB's gate activity waits for
// gate.
func delayedRegistry(isB bool, delay time.Duration, gate chan struct{}, mark func(string)) *registry {
	reg := newRegistry()
	reg.addActivity("gate", &worker.Activity{Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) {
		if isB {
			<-gate
		}
		return nil, nil, nil
	}})
	reg.addActivity("mark", &worker.Activity{Run: func(_ context.Context, t *store.ActivityTask) ([]byte, []byte, error) {
		mark(string(t.Input))
		return nil, nil, nil
	}})
	reg.addActivity("flaky", &worker.Activity{
		Retry: worker.RetryPolicy{Attempts: 2, Initial: delay},
		Run: func(_ context.Context, t *store.ActivityTask) ([]byte, []byte, error) {
			mark("retry")
			if t.Attempt < 2 {
				return nil, nil, errors.New("transient")
			}
			return nil, nil, nil
		},
	})
	reg.addWorkflow("sleep", func(ctx coro.Context, _ []byte) ([]byte, error) {
		if _, err := activity(ctx, "gate", nil); err != nil {
			return nil, err
		}
		if _, err := activity(ctx, "mark", []byte("timer")); err != nil {
			return nil, err
		}
		if _, err := executor.FromContext(ctx).NewTimer(ctx, delay, "t").Get(ctx); err != nil {
			return nil, err
		}
		return activity(ctx, "mark", []byte("timer"))
	})
	reg.addWorkflow("retry", func(ctx coro.Context, _ []byte) ([]byte, error) {
		if _, err := activity(ctx, "gate", nil); err != nil {
			return nil, err
		}
		return activity(ctx, "flaky", nil)
	})
	reg.addWorkflow("handed", func(ctx coro.Context, _ []byte) ([]byte, error) {
		return activity(ctx, "mark", []byte("handed"))
	})
	return reg
}

// waitUntil polls cond until it holds, failing the test after 10 s.
func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// dueWfClaims counts the workflow claims on a Postgres store that report
// a due time.
type dueWfClaims struct {
	*pg.Store
	claims, due atomic.Int32
}

func (c *dueWfClaims) ClaimWorkflowTasksDue(ctx context.Context, owner string, queues []string, n int, ttl time.Duration) ([]store.WorkflowTask, time.Duration, error) {
	tasks, next, err := c.Store.ClaimWorkflowTasksDue(ctx, owner, queues, n, ttl)
	c.claims.Add(1)
	if next > 0 {
		c.due.Add(1)
	}
	return tasks, next, err
}

// T9.1 review: executions an engine keeps leased between eager tasks have
// their lease expiry as ready_at (for recovery): not a due time, an idle
// engine does not wake for it.
func TestKeptExecutionsAreNotDue(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	reg := newRegistry()
	reg.addWorkflow("chain", chainOf(1_000_000)) // runs for the whole test
	reg.addActivity("double", &worker.Activity{Run: func(_ context.Context, t *store.ActivityTask) ([]byte, []byte, error) {
		return t.Input, nil, nil
	}})
	// A long eager window: B never hands a chain over (a hand-over notifies,
	// rightly, and would wake A for reasons this test is not about).
	b, err := worker.New(pg.New(pool), reg, reg, worker.Options{
		Owner: "b", Queues: []string{q}, Eager: true, LeaseTTL: 600 * time.Millisecond, EagerWindow: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Stop(ctx) }()
	for i := range 3 {
		create(t, pg.New(pool), "c"+string(rune('0'+i)), "chain", "0")
	}
	time.Sleep(500 * time.Millisecond) // B runs the three chains, keeping their leases
	st := &dueWfClaims{Store: pg.New(pool)}
	a, err := worker.New(st, reg, reg, worker.Options{
		Owner: "a", Queues: []string{q}, LeaseTTL: 5 * time.Second,
		PollMin: 3 * time.Second, PollMax: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Stop(ctx) }()
	time.Sleep(4 * time.Second)
	// Nothing here falls due later (no timer, retry or hand-back): no claim
	// reports a due time. (Reporting the kept leases' expiries made A wake
	// for each, 39 claims in these 4 s.)
	if st.claims.Load() == 0 || st.due.Load() != 0 {
		t.Fatalf("%d of %d workflow claims reported a due time", st.due.Load(), st.claims.Load())
	}
}
