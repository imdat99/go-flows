package worker_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
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
	// A's timed polls are pollMax apart: work found by them comes about
	// pollMax late (A has just polled), far above limit, which leaves
	// seconds of slack to a loaded machine.
	const (
		delay   = 300 * time.Millisecond
		pollMax = 20 * time.Second
		limit   = 5 * time.Second // well under pollMax
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
	newWorker := func(owner string, st store.Store, reg *registry, pollMin, pollMax time.Duration) *worker.Worker {
		w, err := worker.New(st, reg, reg, worker.Options{
			Owner: owner, Queues: []string{q}, Eager: true, LeaseTTL: max(5*time.Second, 3*pollMax), // PollMax <= LeaseTTL/3
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
	b := newWorker("b", st, register(true), 10*time.Millisecond, 50*time.Millisecond)
	for _, wf := range []string{"sleep", "retry"} {
		create(t, st, wf, wf, "")
	}
	// B holds both: their gate activities run.
	waitUntil(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(marks["gate"]) == 2 })
	// A starts now (nothing claimable, nothing due) and backs off: once
	// it has claimed both kinds of task, it sleeps until pollMax.
	stA := &claimCounts{Store: st}
	a := newWorker("a", stA, register(false), pollMax, pollMax)
	t.Cleanup(func() { _ = a.Stop(ctx) })
	waitUntil(t, func() bool { return stA.wf.Load() >= 1 && stA.act.Load() >= 1 })
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

// claimCounts counts the claims of each kind on a Postgres store.
type claimCounts struct {
	*pg.Store
	wf, act atomic.Int32
}

func (c *claimCounts) ClaimWorkflowTasksDue(ctx context.Context, owner string, queues []string, n int, ttl time.Duration) ([]store.WorkflowTask, time.Duration, error) {
	tasks, next, err := c.Store.ClaimWorkflowTasksDue(ctx, owner, queues, n, ttl)
	c.wf.Add(1)
	return tasks, next, err
}

func (c *claimCounts) ClaimActivityTasksDue(ctx context.Context, owner string, queues []string, n int, ttl time.Duration) ([]store.ActivityTask, time.Duration, error) {
	tasks, next, err := c.Store.ClaimActivityTasksDue(ctx, owner, queues, n, ttl)
	c.act.Add(1)
	return tasks, next, err
}

// countActClaims counts activity claims on a Postgres store, and the
// tasks they returned.
type countActClaims struct {
	*pg.Store
	n, got atomic.Int32
}

func (c *countActClaims) ClaimActivityTasksDue(ctx context.Context, owner string, queues []string, n int, ttl time.Duration) ([]store.ActivityTask, time.Duration, error) {
	c.n.Add(1)
	tasks, next, err := c.Store.ClaimActivityTasksDue(ctx, owner, queues, n, ttl)
	c.got.Add(int32(len(tasks)))
	return tasks, next, err
}

// T9.1 review: running activities' lease expiries (their visible_at,
// pushed by heartbeats) are not due times: an idle worker does not wake for
// them.
func TestRunningActivitiesAreNotDue(t *testing.T) {
	// B's heartbeat (every third of its lease TTL) must keep its leases;
	// when it did not (a loaded machine: A claimed a task whose lease
	// lapsed, a real due time), the run proves nothing and runs again
	// with a longer TTL.
	for ttl := time.Second; !runningActivitiesAreNotDue(t, ttl); ttl *= 2 {
		if ttl >= 8*time.Second {
			t.Fatalf("B's leases lapsed with a TTL of %v", ttl)
		}
		t.Logf("a lease of B lapsed with a TTL of %v; again with %v", ttl, 2*ttl)
	}
}

// runningActivitiesAreNotDue runs TestRunningActivitiesAreNotDue with B's
// lease TTL; false: a lease of B lapsed.
func runningActivitiesAreNotDue(t *testing.T, ttl time.Duration) bool {
	t.Helper()
	pool := testdb.New(t)
	ctx := context.Background()
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var running atomic.Int32
	regB := newRegistry()
	regB.addActivity("long", &worker.Activity{Run: func(ctx context.Context, _ *store.ActivityTask) ([]byte, []byte, error) {
		running.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil, nil
	}})
	wf := func(ctx coro.Context, _ []byte) ([]byte, error) { return activity(ctx, "long", nil) }
	regB.addWorkflow("w", wf)
	// A runs a task it claims (a lapsed lease of B) at once: nothing of A
	// waits for release.
	regA := newRegistry()
	regA.addActivity("long", &worker.Activity{Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) {
		return nil, nil, nil
	}})
	regA.addWorkflow("w", wf)
	var a, b *worker.Worker
	// release first: B's stop waits for its activities.
	defer func() {
		close(release)
		for _, w := range []*worker.Worker{a, b} {
			if w != nil {
				_ = w.Stop(ctx)
			}
		}
	}()
	b, err := worker.New(pg.New(pool), regB, regB, worker.Options{Owner: "b", Queues: []string{q}, Eager: true, LeaseTTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		create(t, pg.New(pool), "w"+string(rune('0'+i)), "w", "")
	}
	waitUntil(t, func() bool { return running.Load() == 5 }) // B runs all five
	// A's timed polls are far apart (20 TTLs); B's heartbeats move the
	// lease expiries every third of a TTL.
	st := &countActClaims{Store: pg.New(pool)}
	a, err = worker.New(st, regA, regA, worker.Options{
		Owner: "a", Queues: []string{q}, LeaseTTL: 60 * ttl, // PollMax <= LeaseTTL/3
		PollMin: 20 * ttl, PollMax: 20 * ttl,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	window := 6 * ttl
	time.Sleep(window)
	if st.got.Load() > 0 {
		return false
	}
	// The claim on start (and slack for one more): a wake-up per lease
	// expiry (at most a TTL apart) would make 7 or more.
	if n := st.n.Load(); n > 3 {
		t.Fatalf("%d activity claims in %v (B's lease TTL %v)", n, window, ttl)
	}
	return true
}

// delayedRegistry registers the workflows of
// TestDelayedWorkOfAnotherWorkerIsOnTime; isB's gate activity waits for
// gate.
func delayedRegistry(isB bool, delay time.Duration, gate chan struct{}, mark func(string)) *registry {
	reg := newRegistry()
	reg.addActivity("gate", &worker.Activity{Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) {
		if isB {
			mark("gate")
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
	// explained, if set, tells whether a reported due time has a cause other
	// than kept leases (it is then not counted).
	explained func(next time.Duration) bool
}

func (c *dueWfClaims) ClaimWorkflowTasksDue(ctx context.Context, owner string, queues []string, n int, ttl time.Duration) ([]store.WorkflowTask, time.Duration, error) {
	tasks, next, err := c.Store.ClaimWorkflowTasksDue(ctx, owner, queues, n, ttl)
	c.claims.Add(1)
	if next > 0 && (c.explained == nil || !c.explained(next)) {
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
		// 2s: under a loaded test run, a shorter lease of B lapsed now and then
		// (a claimable execution, rightly reported); still well inside the 4s
		// below, so reported expiries would show.
		Owner: "b", Queues: []string{q}, Eager: true, LeaseTTL: 2 * time.Second, EagerWindow: time.Minute,
		Logger: testLogger(t, "b"),
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
	// Under a starved CPU, B may hand a chain over (its lease lapsing, or
	// with nothing to run eagerly): the released execution is ready work
	// another transaction committed after A's claim began, reported due in
	// a moment (right: A's claim skipped it). A due with every execution
	// leased, or not within moments, is a kept lease's expiry, the bug this
	// test is about (T10.18: the CI flake was the former).
	st.explained = func(next time.Duration) bool {
		rows, err := pool.Query(context.Background(), `SELECT instance_id, owner_id, locked_until IS NULL,
			ready_at - now(), not_before - now(), sticky_owner FROM flows.execution ORDER BY id`)
		if err != nil {
			t.Logf("due in %v: dump: %v", next, err)
			return false
		}
		defer rows.Close()
		var b strings.Builder
		released := false
		for rows.Next() {
			var id, own, sticky string
			var unleased bool
			var ready, notBefore *time.Duration
			if err := rows.Scan(&id, &own, &unleased, &ready, &notBefore, &sticky); err != nil {
				t.Logf("due in %v: dump: %v", next, err)
				return false
			}
			released = released || unleased
			fmt.Fprintf(&b, " [%s owner=%q unleased=%v ready_in=%v not_before_in=%v sticky=%q]", id, own, unleased, ready, notBefore, sticky)
		}
		t.Logf("due in %v (handed over: %v):%s", next, released, b.String())
		// A handed-over execution is due within moments; a kept lease's
		// expiry is hundreds of milliseconds to the 2s lease ahead.
		return released && next < 100*time.Millisecond
	}
	a, err := worker.New(st, reg, reg, worker.Options{
		Owner: "a", Queues: []string{q}, LeaseTTL: 10 * time.Second, // PollMax <= LeaseTTL/3
		PollMin: 3 * time.Second, PollMax: 3 * time.Second, Logger: testLogger(t, "a"),
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

// testLogger logs warnings and errors of a worker to t, prefixed.
func testLogger(t *testing.T, name string) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(tWriter{t, name}, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

type tWriter struct {
	t    *testing.T
	name string
}

func (w tWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s: %s", w.name, strings.TrimSpace(string(p)))
	return len(p), nil
}

// T10.3: a timer that falls due while its execution is sticky to the engine
// that committed it is that engine's: an idle engine does not wake for it
// (an empty claim, the sticky engine claiming it).
func TestStickyTimersDoNotWakeOthers(t *testing.T) {
	const delay = 1500 * time.Millisecond
	pool := testdb.New(t)
	ctx := context.Background()
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	reg := newRegistry()
	reg.addWorkflow("sleepy", func(ctx coro.Context, _ []byte) ([]byte, error) {
		_, err := executor.FromContext(ctx).NewTimer(ctx, delay, "t").Get(ctx)
		return nil, err
	})
	b, err := worker.New(pg.New(pool), reg, reg, worker.Options{
		Owner: "b", Queues: []string{q}, LeaseTTL: 30 * time.Second, StickyFor: time.Minute,
		PollMin: 5 * time.Second, PollMax: 5 * time.Second, Logger: testLogger(t, "b"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Stop(ctx) }()
	created := time.Now()
	create(t, pg.New(pool), "s", "sleepy", "")
	waitUntil(t, func() bool { // B committed the timer, sticky to itself
		var sticky bool
		err := pool.QueryRow(ctx, `SELECT ready_at > now() AND sticky_owner = 'b' FROM flows.execution`).Scan(&sticky)
		return err == nil && sticky
	})
	st := &dueWfClaims{Store: pg.New(pool)}
	a, err := worker.New(st, reg, reg, worker.Options{
		Owner: "a", Queues: []string{q}, LeaseTTL: 30 * time.Second,
		PollMin: 5 * time.Second, PollMax: 5 * time.Second, Logger: testLogger(t, "a"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Stop(ctx) }()
	time.Sleep(200 * time.Millisecond) // A's claims on start (the poll, the watch's first hint)
	before := st.claims.Load()
	if e := waitEnd(t, pg.New(pool), "s"); e.Status != store.StatusCompleted {
		t.Fatalf("s: %s %s", e.Status, e.Err)
	}
	// B fires its timer on time: it polls 5s apart, so only its own due
	// time wakes it.
	if d := time.Since(created); d > delay+2*time.Second {
		t.Fatalf("the timer of %v fired after %v", delay, d)
	}
	time.Sleep(300 * time.Millisecond) // a wake-up for the timer would have claimed by now
	if n := st.claims.Load() - before; n != 0 {
		t.Fatalf("A claimed %d times for a timer sticky to B", n)
	}
}

// T10.3 review: an engine that becomes sticky owner of an execution whose
// timer another engine committed fires that timer on time: it learns the
// execution's next timer from its executor, not only from the timers it
// committed (which the store's due report then leaves to it).
func TestStickyOwnerFiresTimersOfOthersOnTime(t *testing.T) {
	const delay = 2 * time.Second
	pool := testdb.New(t)
	ctx := context.Background()
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	reg := newRegistry()
	reg.addWorkflow("w", func(ctx coro.Context, _ []byte) ([]byte, error) {
		ex := executor.FromContext(ctx)
		timer := ex.NewTimer(ctx, delay, "t")
		if _, ok := ex.SignalChannel("go").Receive(ctx); !ok {
			return nil, errors.New("no signal")
		}
		_, err := timer.Get(ctx)
		return nil, err
	})
	start := func(owner string, poll time.Duration) *worker.Worker {
		w, err := worker.New(pg.New(pool), reg, reg, worker.Options{
			Owner: owner, Queues: []string{q}, LeaseTTL: 30 * time.Second, StickyFor: time.Minute,
			PollMin: poll, PollMax: poll, Logger: testLogger(t, owner),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return w
	}
	// A commits the timer, then stops (its stickiness handed over).
	a := start("a", 20*time.Millisecond)
	created := time.Now()
	create(t, pg.New(pool), "w", "w", "")
	waitUntil(t, func() bool {
		var committed bool
		err := pool.QueryRow(ctx, `SELECT ready_at > now() FROM flows.execution`).Scan(&committed)
		return err == nil && committed
	})
	if err := a.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	// B takes the signal's task (sticky to B from then on) and polls 5s
	// apart: only a due time of its own wakes it for the timer.
	if err := pg.New(pool).Signal(ctx, "w", "go", nil); err != nil {
		t.Fatal(err)
	}
	b := start("b", 5*time.Second)
	defer func() { _ = b.Stop(ctx) }()
	if e := waitEnd(t, pg.New(pool), "w"); e.Status != store.StatusCompleted {
		t.Fatalf("w: %s %s", e.Status, e.Err)
	}
	if d := time.Since(created); d > delay+2*time.Second {
		t.Fatalf("the timer of %v fired after %v", delay, d)
	}
}
