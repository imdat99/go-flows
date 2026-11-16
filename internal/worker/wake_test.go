package worker_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/testdb"
	"github.com/imdat99/go-flows/internal/worker"
)

// Work a worker commits with a delay (a timer, a retry delay) gets no store
// hint, since it is not ready at commit. A worker whose polls backed off to
// PollMax must still run it on time, not up to PollMax late.
func TestDelayedWorkIsNotPollMaxLate(t *testing.T) {
	const (
		delay   = 100 * time.Millisecond
		pollMax = 3 * time.Second
		limit   = time.Second // well under pollMax
	)
	stores := map[string]func(t *testing.T) store.Store{
		"mem": func(*testing.T) store.Store { return mem.New(nil) },
		"pg": func(t *testing.T) store.Store {
			t.Helper()
			pool := testdb.New(t)
			if err := pg.Migrate(context.Background(), pool); err != nil {
				t.Fatal(err)
			}
			return pg.New(pool)
		},
	}
	for name, open := range stores {
		t.Run(name, func(t *testing.T) {
			st := open(t)
			var mu sync.Mutex
			marks := map[string][]time.Time{}
			mark := func(k string) {
				mu.Lock()
				marks[k] = append(marks[k], time.Now())
				mu.Unlock()
			}
			reg := newRegistry()
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
				if _, err := activity(ctx, "mark", []byte("timer")); err != nil {
					return nil, err
				}
				if _, err := executor.FromContext(ctx).NewTimer(ctx, delay, "t").Get(ctx); err != nil {
					return nil, err
				}
				return activity(ctx, "mark", []byte("timer"))
			})
			reg.addWorkflow("retry", func(ctx coro.Context, _ []byte) ([]byte, error) {
				return activity(ctx, "flaky", nil)
			})
			// PollMin = PollMax: every timed poll is pollMax apart.
			w, err := worker.New(st, reg, reg, worker.Options{
				Owner: "w1", Queues: []string{q}, Eager: true, LeaseTTL: 5 * time.Second,
				PollMin: pollMax, PollMax: pollMax,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = w.Stop(context.Background()) })
			for _, wf := range []string{"sleep", "retry"} {
				create(t, st, wf, wf, "")
			}
			for _, wf := range []string{"sleep", "retry"} {
				if e := waitEnd(t, st, wf); e.Status != store.StatusCompleted {
					t.Fatalf("%s: %s %s", wf, e.Status, e.Err)
				}
			}
			for _, k := range []string{"timer", "retry"} {
				mu.Lock()
				m := marks[k]
				mu.Unlock()
				if len(m) != 2 {
					t.Fatalf("%s: %d marks, want 2", k, len(m))
				}
				if d := m[1].Sub(m[0]); d < delay || d > limit {
					t.Errorf("%s: delay of %v took %v", k, delay, d)
				}
			}
		})
	}
}

// noHints hides the store's Watcher: work comes without hints, as when
// LISTEN is down.
type noHints struct{ store.Store }

// Without hints, timed polls that find work keep the poll interval short:
// a trickle of work is not found at PollMax intervals.
func TestTimedPollsFindingWorkDoNotBackOff(t *testing.T) {
	st := noHints{mem.New(nil)}
	reg := newRegistry()
	reg.addWorkflow("noop", func(coro.Context, []byte) ([]byte, error) { return nil, nil })
	w, err := worker.New(st, reg, reg, worker.Options{
		Owner: "w1", Queues: []string{q}, LeaseTTL: 5 * time.Second,
		PollMin: 10 * time.Millisecond, PollMax: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop(context.Background()) })
	for i := range 16 {
		id := "i" + strconv.Itoa(i)
		at := time.Now()
		create(t, st, id, "noop", "")
		waitEnd(t, st, id)
		// The interval doubles while polls find nothing: from PollMin,
		// 300 ms of gap leaves it under 320 ms.
		if d := time.Since(at); d > time.Second {
			t.Fatalf("%s found after %v", id, d)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// countClaims counts workflow claims (the mem store's Watcher stays).
type countClaims struct {
	*mem.Store
	n atomic.Int32
}

func (c *countClaims) ClaimWorkflowTasks(ctx context.Context, owner string, queues []string, n int, ttl time.Duration) ([]store.WorkflowTask, error) {
	c.n.Add(1)
	return c.Store.ClaimWorkflowTasks(ctx, owner, queues, n, ttl)
}

// A due time cuts a long sleep short without claiming before it is due:
// a workflow sleeping k times costs about one claim per wake-up.
func TestDueTimesDoNotCauseEmptyClaims(t *testing.T) {
	const sleeps = 8
	st := &countClaims{Store: mem.New(nil)}
	reg := newRegistry()
	reg.addWorkflow("sleepy", func(ctx coro.Context, _ []byte) ([]byte, error) {
		for range sleeps {
			if _, err := executor.FromContext(ctx).NewTimer(ctx, 30*time.Millisecond, "t").Get(ctx); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	w, err := worker.New(st, reg, reg, worker.Options{
		Owner: "w1", Queues: []string{q}, LeaseTTL: 5 * time.Second,
		PollMin: 2 * time.Second, PollMax: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop(context.Background()) })
	time.Sleep(50 * time.Millisecond) // the first poll, on start
	before := st.n.Load()
	at := time.Now()
	create(t, st, "i1", "sleepy", "")
	waitEnd(t, st, "i1")
	if d := time.Since(at); d > time.Second {
		t.Fatalf("took %v", d)
	}
	// The start hint and one per timer; a little slack for a timer that
	// rounds to just before its store time is visible.
	if n := st.n.Load() - before; n > sleeps+3 {
		t.Fatalf("%d claims for %d timers", n, sleeps)
	}
}

// T8.2 review: the executor reuses its completion; once it is back in the
// cache another task of the execution (claimed by another poller
// goroutine) may run it, so the driver must be done with the completion
// first. Signal storms make that hand-over frequent (run with -race).
func TestCompletionNotReadAfterCachePut(t *testing.T) {
	st := mem.New(nil)
	reg := newRegistry()
	const signals = 300
	reg.addWorkflow("storm", func(ctx coro.Context, _ []byte) ([]byte, error) {
		ch := executor.FromContext(ctx).SignalChannel("s")
		for range signals {
			if _, ok := ch.Receive(ctx); !ok {
				return nil, errors.New("signal channel closed")
			}
		}
		return nil, nil
	})
	w, err := worker.New(st, reg, reg, worker.Options{
		Owner: "w1", Queues: []string{q}, LeaseTTL: 5 * time.Second,
		PollMax: 20 * time.Millisecond, MaxWorkflowTasks: 16, Eager: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop(context.Background()) })
	var wg sync.WaitGroup
	for i := range 16 {
		id := "storm" + strconv.Itoa(i)
		create(t, st, id, "storm", "")
		wg.Go(func() {
			for range signals {
				if err := st.Signal(context.Background(), id, "s", nil); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	for i := range 16 {
		if e := waitEnd(t, st, "storm"+strconv.Itoa(i)); e.Status != store.StatusCompleted {
			t.Fatalf("storm%d: %s %s", i, e.Status, e.Err)
		}
	}
}
