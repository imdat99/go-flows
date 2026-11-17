package worker_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/testdb"
	"github.com/imdat99/go-flows/internal/worker"
)

var errCrashed = errors.New("engine crashed")

// crashable is a store an engine loses at once when crash is set, as when
// its process dies: every later call fails, so its leases are neither
// renewed nor released. It hides the optional interfaces (hints, due
// times): the crashing engine needs none.
type crashable struct {
	st      store.Store
	crashed atomic.Bool
}

func (c *crashable) dead() bool { return c.crashed.Load() }

func (c *crashable) Now(ctx context.Context) (time.Time, error) {
	if c.dead() {
		return time.Time{}, errCrashed
	}
	return c.st.Now(ctx)
}

func (c *crashable) CreateExecution(ctx context.Context, e store.NewExecution) (store.Execution, bool, error) {
	if c.dead() {
		return store.Execution{}, false, errCrashed
	}
	return c.st.CreateExecution(ctx, e)
}

func (c *crashable) GetExecution(ctx context.Context, id string) (store.Execution, error) {
	if c.dead() {
		return store.Execution{}, errCrashed
	}
	return c.st.GetExecution(ctx, id)
}

func (c *crashable) ClaimWorkflowTasks(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.WorkflowTask, error) {
	if c.dead() {
		return nil, errCrashed
	}
	return c.st.ClaimWorkflowTasks(ctx, owner, queues, limit, ttl)
}

func (c *crashable) LoadHistory(ctx context.Context, id, after int64) ([]history.Event, error) {
	if c.dead() {
		return nil, errCrashed
	}
	return c.st.LoadHistory(ctx, id, after)
}

func (c *crashable) CompleteWorkflowTask(ctx context.Context, l store.Lease, cp *store.Completion) ([]store.Lease, error) {
	if c.dead() {
		return nil, errCrashed
	}
	return c.st.CompleteWorkflowTask(ctx, l, cp)
}

func (c *crashable) ExtendWorkflowTask(ctx context.Context, l store.Lease, ttl time.Duration) error {
	if c.dead() {
		return errCrashed
	}
	return c.st.ExtendWorkflowTask(ctx, l, ttl)
}

func (c *crashable) ReleaseWorkflowTask(ctx context.Context, l store.Lease, d time.Duration) error {
	if c.dead() {
		return errCrashed
	}
	return c.st.ReleaseWorkflowTask(ctx, l, d)
}

func (c *crashable) ResumeWorkflowTask(ctx context.Context, l store.Lease, ttl time.Duration) (store.WorkflowTask, error) {
	if c.dead() {
		return store.WorkflowTask{}, errCrashed
	}
	return c.st.ResumeWorkflowTask(ctx, l, ttl)
}

func (c *crashable) ClaimActivityTasks(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.ActivityTask, error) {
	if c.dead() {
		return nil, errCrashed
	}
	return c.st.ClaimActivityTasks(ctx, owner, queues, limit, ttl)
}

func (c *crashable) ExtendActivityTask(ctx context.Context, l store.Lease, ttl time.Duration) error {
	if c.dead() {
		return errCrashed
	}
	return c.st.ExtendActivityTask(ctx, l, ttl)
}

func (c *crashable) CompleteActivityTask(ctx context.Context, l store.Lease, r store.ActivityResult) (*store.WorkflowTask, error) {
	if c.dead() {
		return nil, errCrashed
	}
	return c.st.CompleteActivityTask(ctx, l, r)
}

func (c *crashable) RetryActivityTask(ctx context.Context, l store.Lease, d time.Duration, uncertain bool, compData []byte) error {
	if c.dead() {
		return errCrashed
	}
	return c.st.RetryActivityTask(ctx, l, d, uncertain, compData)
}

func (c *crashable) ReleaseActivityTask(ctx context.Context, l store.Lease) error {
	if c.dead() {
		return errCrashed
	}
	return c.st.ReleaseActivityTask(ctx, l)
}

func (c *crashable) Unstick(ctx context.Context, owner string) error {
	if c.dead() {
		return errCrashed
	}
	return c.st.Unstick(ctx, owner)
}

func (c *crashable) RenewLeases(ctx context.Context, owner string, wf, act []store.Lease, ttl time.Duration) (store.Renewal, error) {
	if c.dead() {
		return store.Renewal{}, errCrashed
	}
	return c.st.RenewLeases(ctx, owner, wf, act, ttl)
}

func (c *crashable) PurgeEnded(ctx context.Context, older time.Duration, limit int) (int, error) {
	if c.dead() {
		return 0, errCrashed
	}
	return c.st.PurgeEnded(ctx, older, limit)
}

func (c *crashable) ForgetInstance(ctx context.Context, id string) error {
	if c.dead() {
		return errCrashed
	}
	return c.st.ForgetInstance(ctx, id)
}

func (c *crashable) ReopenCompensation(ctx context.Context, id string) error {
	if c.dead() {
		return errCrashed
	}
	return c.st.ReopenCompensation(ctx, id)
}

func (c *crashable) NoteStuck(ctx context.Context, l store.Lease, seq int64) (bool, error) {
	if c.dead() {
		return false, errCrashed
	}
	return c.st.NoteStuck(ctx, l, seq)
}

func (c *crashable) ReopenParked(ctx context.Context, id string) error {
	if c.dead() {
		return errCrashed
	}
	return c.st.ReopenParked(ctx, id)
}

func (c *crashable) Terminate(ctx context.Context, id, reason string) (store.EndInfo, error) {
	if c.dead() {
		return store.EndInfo{}, errCrashed
	}
	return c.st.Terminate(ctx, id, reason)
}

func (c *crashable) Signal(ctx context.Context, id, name string, payload []byte) error {
	if c.dead() {
		return errCrashed
	}
	return c.st.Signal(ctx, id, name, payload)
}

func (c *crashable) RequestCancel(ctx context.Context, id string) error {
	if c.dead() {
		return errCrashed
	}
	return c.st.RequestCancel(ctx, id)
}

// T10.1: the work of a crashed engine (its workflow and activity leases,
// no longer renewed) is taken over by an idle engine within about
// LeaseTTL of the crash, not at the idle engine's next timed poll
// (PollMax, a minute here).
func TestCrashedEngineWorkIsRecoveredWithinLeaseTTL(t *testing.T) {
	const (
		ttl     = 2 * time.Second
		pollMax = time.Minute
		limit   = 8 * time.Second // ttl plus a timed poll, with room for a loaded run; far under pollMax
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
		for _, eager := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/eager=%v", name, eager), func(t *testing.T) {
				ctx := context.Background()
				st := open(t)
				running := make(chan struct{}, 1)
				newReg := func(crashing bool) *registry {
					reg := newRegistry()
					// A lost attempt counts (T11.3): a second one is allowed.
					reg.addActivity("work", &worker.Activity{Retry: worker.RetryPolicy{Attempts: 2}, Run: func(ctx context.Context, _ *store.ActivityTask) ([]byte, []byte, error) {
						if crashing { // runs until the engine is gone
							running <- struct{}{}
							<-ctx.Done()
							return nil, nil, ctx.Err()
						}
						return []byte("done"), nil, nil
					}})
					reg.addWorkflow("w", func(ctx coro.Context, _ []byte) ([]byte, error) { return activity(ctx, "work", nil) })
					return reg
				}
				// The crashed engine's failing calls are not logged.
				logger := func(owner string) *slog.Logger {
					if owner == "b" {
						return slog.New(slog.DiscardHandler)
					}
					return testLogger(t, owner)
				}
				start := func(owner string, st store.Store, reg *registry, poll time.Duration) *worker.Worker {
					w, err := worker.New(st, reg, reg, worker.Options{
						Owner: owner, Queues: []string{q}, Eager: eager, LeaseTTL: ttl, StickyFor: ttl / 2,
						PollMin: poll, PollMax: poll, Logger: logger(owner),
					})
					if err != nil {
						t.Fatal(err)
					}
					if err := w.Start(ctx); err != nil {
						t.Fatal(err)
					}
					return w
				}
				// B runs the workflow and its activity, then crashes. It has no
				// hints (crashable): it polls often.
				bst := &crashable{st: st}
				b := start("b", bst, newReg(true), 20*time.Millisecond)
				t.Cleanup(func() {
					sctx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()
					_ = b.Stop(sctx)
				})
				create(t, st, "w", "w", "")
				select {
				case <-running:
				case <-time.After(10 * time.Second):
					t.Fatal("B did not run the activity")
				}
				// A starts idle: everything is leased by B, alive. Its next
				// timed poll is a minute away.
				a := start("a", st, newReg(false), pollMax)
				t.Cleanup(func() { _ = a.Stop(ctx) })
				time.Sleep(100 * time.Millisecond)
				bst.crashed.Store(true)
				crashed := time.Now()
				e := waitEnd(t, st, "w")
				if e.Status != store.StatusCompleted || string(e.Output) != "done" {
					t.Fatalf("w: %s %q %s", e.Status, e.Output, e.Err)
				}
				if d := time.Since(crashed); d > limit {
					t.Fatalf("recovered %v after the crash (LeaseTTL %v, PollMax %v)", d, ttl, pollMax)
				}
				t.Logf("recovered %v after the crash", time.Since(crashed))
			})
		}
	}
}
