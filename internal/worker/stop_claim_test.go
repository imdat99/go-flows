package worker_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/worker"
)

// stopRace stands for a database that commits a claim in flight when Stop
// begins: an armed claim waits until the worker's watch ended (Stop
// canceled its polls), then claims, and returns its context's error, if
// any, without the tasks (as Postgres: a canceled client reads nothing).
type stopRace struct {
	*mem.Store
	wfArmed, actArmed       atomic.Bool
	inClaim                 chan struct{} // closed when an armed claim waits
	watching, watchEnded    chan struct{} // the worker's watch began, ended
	once, inOnce, startOnce sync.Once
	completions             atomic.Int32
}

func newStopRace() *stopRace {
	return &stopRace{Store: mem.New(nil), inClaim: make(chan struct{}), watching: make(chan struct{}), watchEnded: make(chan struct{})}
}

func (s *stopRace) Watch(ctx context.Context, wake func(store.WorkKind, string)) error {
	s.startOnce.Do(func() { close(s.watching) })
	err := s.Store.Watch(ctx, wake)
	if ctx.Err() != nil {
		s.once.Do(func() { close(s.watchEnded) })
	}
	return err
}

// race runs an armed claim (once).
func (s *stopRace) race(ctx context.Context, armed *atomic.Bool, claim func() error) error {
	if !armed.CompareAndSwap(true, false) {
		return claim()
	}
	s.inOnce.Do(func() { close(s.inClaim) })
	<-s.watchEnded
	if err := claim(); err != nil {
		return err
	}
	return ctx.Err()
}

func (s *stopRace) ClaimWorkflowTasksDue(ctx context.Context, owner string, queues []string, n int, ttl time.Duration) (tasks []store.WorkflowTask, next time.Duration, err error) {
	err = s.race(ctx, &s.wfArmed, func() (err error) {
		tasks, next, err = s.Store.ClaimWorkflowTasksDue(context.Background(), owner, queues, n, ttl)
		return err
	})
	if err != nil {
		return nil, 0, err // as Postgres: the committed rows are not read
	}
	return tasks, next, nil
}

func (s *stopRace) ClaimActivityTasksDue(ctx context.Context, owner string, queues []string, n int, ttl time.Duration) (tasks []store.ActivityTask, next time.Duration, err error) {
	err = s.race(ctx, &s.actArmed, func() (err error) {
		tasks, next, err = s.Store.ClaimActivityTasksDue(context.Background(), owner, queues, n, ttl)
		return err
	})
	if err != nil {
		return nil, 0, err // as Postgres: the committed rows are not read
	}
	return tasks, next, nil
}

func (s *stopRace) CompleteWorkflowTask(ctx context.Context, l store.Lease, c *store.Completion) ([]store.Lease, error) {
	leases, err := s.Store.CompleteWorkflowTask(ctx, l, c)
	if err == nil {
		s.completions.Add(1)
	}
	return leases, err
}

// T10.13: a claim in flight when Stop begins may commit in the store
// though the poll's context is canceled meanwhile. Its tasks must not be
// dropped: they stayed leased to the stopped worker for a whole LeaseTTL,
// and other workers found them only at a timed poll (kept leases are not
// due times), PollMax later.
func TestClaimInFlightAtStopIsNotDropped(t *testing.T) {
	for _, kind := range []string{"workflow", "activity"} {
		t.Run(kind, func(t *testing.T) {
			st := newStopRace()
			var ran atomic.Int32
			reg := newRegistry()
			reg.addActivity("a", &worker.Activity{Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) {
				ran.Add(1)
				return nil, nil, nil
			}})
			reg.addWorkflow("w", func(ctx coro.Context, _ []byte) ([]byte, error) { return activity(ctx, "a", nil) })
			// A lease of the stopped worker lasts the whole test.
			w, err := worker.New(st, reg, reg, worker.Options{Owner: "b", Queues: []string{q}, LeaseTTL: time.Hour, PollMax: 50 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			create(t, st, "x", "w", "")
			if kind == "workflow" {
				st.wfArmed.Store(true)
			} else {
				st.actArmed.Store(true)
			}
			if err := w.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			<-st.inClaim
			<-st.watching // Stop ends it
			if kind == "activity" {
				// The activity task exists before the stop (the armed
				// claim, the first, found none; it claims again after).
				waitUntil(t, func() bool { return st.completions.Load() >= 1 })
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := w.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			e, err := st.GetExecution(context.Background(), "x")
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "workflow":
				// Run by the stopping worker (its first task: the activity is
				// scheduled), or left claimable.
				if st.completions.Load() >= 1 {
					return
				}
				if tasks, err := st.ClaimWorkflowTasks(context.Background(), "other", []string{q}, 1, time.Minute); err != nil || len(tasks) != 1 {
					t.Fatalf("workflow task dropped at stop (still leased): %s, %d claimable, %v", e.Status, len(tasks), err)
				}
			case "activity":
				if ran.Load() == 1 {
					return
				}
				if tasks, err := st.ClaimActivityTasks(context.Background(), "other", []string{q}, 1, time.Minute); err != nil || len(tasks) != 1 {
					t.Fatalf("activity task dropped at stop (still leased): %d claimable, %v", len(tasks), err)
				}
			}
		})
	}
}

// stopProbe watches the claims around a Stop: its first workflow claim
// waits until the worker's watch ended (Stop began) and claims nothing,
// or (hang) waits until its context is done; every later claim counts as
// started after Stop began.
type stopProbe struct {
	*mem.Store
	hang                     bool
	first                    atomic.Bool
	late                     atomic.Int32
	inClaim, watching, ended chan struct{}
	inOnce, wOnce, eOnce     sync.Once
}

func newStopProbe(hang bool) *stopProbe {
	return &stopProbe{Store: mem.New(nil), hang: hang, inClaim: make(chan struct{}), watching: make(chan struct{}), ended: make(chan struct{})}
}

func (s *stopProbe) Watch(ctx context.Context, wake func(store.WorkKind, string)) error {
	s.wOnce.Do(func() { close(s.watching) })
	err := s.Store.Watch(ctx, wake)
	if ctx.Err() != nil {
		s.eOnce.Do(func() { close(s.ended) })
	}
	return err
}

func (s *stopProbe) ClaimWorkflowTasksDue(ctx context.Context, owner string, queues []string, n int, ttl time.Duration) ([]store.WorkflowTask, time.Duration, error) {
	if s.first.CompareAndSwap(false, true) {
		s.inOnce.Do(func() { close(s.inClaim) })
		if s.hang {
			<-ctx.Done() // a store that does not answer
			return nil, 0, ctx.Err()
		}
		<-s.ended
		return nil, 0, nil
	}
	s.late.Add(1)
	return s.Store.ClaimWorkflowTasksDue(ctx, owner, queues, n, ttl)
}

// T10.13 review: Stop stops claiming. A claim in flight when it begins
// completes, but no claim starts after: here the poller finds both a hint
// (work created while its claim was in flight) and the stop when that
// claim returns.
func TestNoClaimStartsAfterStop(t *testing.T) {
	reg := newRegistry()
	reg.addWorkflow("w", func(coro.Context, []byte) ([]byte, error) { return nil, nil })
	for i := range 20 {
		st := newStopProbe(false)
		w, err := worker.New(st, reg, reg, worker.Options{
			Owner: "b", Queues: []string{q}, LeaseTTL: time.Hour, PollMax: 50 * time.Millisecond, MaxWorkflowTasks: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		<-st.inClaim
		<-st.watching
		create(t, st, "x", "w", "") // a hint while the claim is in flight
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = w.Stop(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if n := st.late.Load(); n > 0 {
			t.Fatalf("run %d: %d claims started after Stop began", i, n)
		}
	}
}

// T10.13 review: a claim that does not return (a partitioned database, an
// exhausted pool) does not hold up the Stop of an idle worker: it is
// canceled after a short grace, and Stop returns nil (no task was
// interrupted), whatever its context.
func TestIdleStopWithHungClaimIsPrompt(t *testing.T) {
	reg := newRegistry()
	for _, deadline := range []time.Duration{0, 200 * time.Millisecond} {
		t.Run(deadline.String(), func(t *testing.T) {
			st := newStopProbe(true)
			w, err := worker.New(st, reg, reg, worker.Options{Owner: "b", Queues: []string{q}, LeaseTTL: time.Hour, PollMax: 50 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			<-st.inClaim
			ctx := context.Background()
			if deadline > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, deadline)
				defer cancel()
			}
			done := make(chan error, 1)
			go func() { done <- w.Stop(ctx) }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("idle stop: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Stop waits for a claim that does not return")
			}
		})
	}
}
