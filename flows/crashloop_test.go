package flows

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
)

// crashStore makes chosen tasks behave as if their process died while
// running them (T11.3): their leases are reported lost and not renewed (so
// they expire and are claimed again), and, for workflow tasks, their
// completions and releases fail. Everything else passes through. Embedding
// the interface hides the optional ones (hints, due times): engines poll.
type crashStore struct {
	store.Store
	activity string      // activity name whose attempts crash
	instance string      // instance id whose workflow tasks crash
	wfCrash  atomic.Bool // the workflow tasks of instance crash
	mu       sync.Mutex
	acts     map[int64]bool // lease ids of crashing activity attempts
	wfs      map[int64]bool // execution ids of instance
}

func newCrashStore(activity, instance string) *crashStore {
	return &crashStore{Store: mem.New(nil), activity: activity, instance: instance, acts: map[int64]bool{}, wfs: map[int64]bool{}}
}

func (s *crashStore) ClaimActivityTasks(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.ActivityTask, error) {
	out, err := s.Store.ClaimActivityTasks(ctx, owner, queues, limit, ttl)
	s.mu.Lock()
	for _, t := range out {
		if t.Name == s.activity && !t.Compensate {
			s.acts[t.Lease.ID] = true
		}
	}
	s.mu.Unlock()
	return out, err
}

func (s *crashStore) ClaimWorkflowTasks(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.WorkflowTask, error) {
	out, err := s.Store.ClaimWorkflowTasks(ctx, owner, queues, limit, ttl)
	s.mu.Lock()
	for _, t := range out {
		if t.Execution.InstanceID == s.instance {
			s.wfs[t.Execution.ID] = true
		}
	}
	s.mu.Unlock()
	return out, err
}

func (s *crashStore) crashing(id int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wfs[id] && s.wfCrash.Load()
}

func (s *crashStore) CompleteWorkflowTask(ctx context.Context, l store.Lease, c *store.Completion) ([]store.Lease, error) {
	if s.crashing(l.ID) && c.Status != store.StatusNeedsAttention { // the park goes through
		return nil, errors.New("crashed")
	}
	leases, err := s.Store.CompleteWorkflowTask(ctx, l, c)
	if err == nil {
		// Eagerly claimed attempts: leases in the order of the activities
		// that asked for one.
		i := 0
		s.mu.Lock()
		for _, a := range c.Activities {
			if a.ClaimFor <= 0 || i >= len(leases) {
				continue
			}
			if a.Name == s.activity && !a.Compensate {
				s.acts[leases[i].ID] = true
			}
			i++
		}
		s.mu.Unlock()
	}
	return leases, err
}

func (s *crashStore) ReleaseWorkflowTask(ctx context.Context, l store.Lease, delay time.Duration) error {
	if s.crashing(l.ID) {
		return errors.New("crashed")
	}
	return s.Store.ReleaseWorkflowTask(ctx, l, delay)
}

func (s *crashStore) RenewLeases(ctx context.Context, owner string, workflows, activities []store.Lease, ttl time.Duration) (store.Renewal, error) {
	var lost store.Renewal
	s.mu.Lock()
	wfs := slices.DeleteFunc(slices.Clone(workflows), func(l store.Lease) bool {
		if s.wfs[l.ID] && s.wfCrash.Load() {
			lost.LostWorkflows = append(lost.LostWorkflows, l.ID)
			return true
		}
		return false
	})
	acts := slices.DeleteFunc(slices.Clone(activities), func(l store.Lease) bool {
		if s.acts[l.ID] {
			lost.LostActivities = append(lost.LostActivities, l.ID)
			return true
		}
		return false
	})
	s.mu.Unlock()
	r, err := s.Store.RenewLeases(ctx, owner, wfs, acts, ttl)
	r.LostWorkflows = append(r.LostWorkflows, lost.LostWorkflows...)
	r.LostActivities = append(r.LostActivities, lost.LostActivities...)
	return r, err
}

var (
	// crashCharge never returns: its process "dies" every time (its lease
	// is lost). Two attempts allowed.
	crashCharge = DefineActivity("crash.charge", func(ctx context.Context, k Key, _ string) (string, string, error) {
		countCall(k)
		<-ctx.Done()
		return "", "", ctx.Err()
	}, func(ctx context.Context, k Key, c string) error {
		comps.add(k, "refund "+c+map[bool]string{true: "?"}[CompensationUncertain(ctx)])
		return nil
	}, WithRetry(RetryPolicy{Attempts: 2, Initial: time.Millisecond}))
	crashChargeWF = DefineWorkflow("crash.charge-wf", 1, func(c Ctx, s string) (string, error) {
		if _, err := compStep.Run(c, s); err != nil {
			return "", err
		}
		return crashCharge.Run(c, s)
	})

	crashLoopWF = DefineWorkflow("crash.loop-wf", 1, func(c Ctx, s string) (string, error) {
		return s, nil
	})
)

// T11.3: an activity whose every attempt crashes its process (here: loses
// its lease without a result) is not run forever: once its attempts reach
// its retry policy's limit it fails for good (ErrAttemptsExhausted, an
// uncertain failure), and the workflow compensates as for any failure.
func TestCrashingActivityExhaustsItsAttempts(t *testing.T) {
	for _, eager := range []bool{false, true} {
		name := map[bool]string{true: "eager", false: "poll"}[eager]
		t.Run(name, func(t *testing.T) {
			st := newCrashStore(crashCharge.Name(), "")
			e := startedEngine(t, st, Options{InstanceID: "crash-" + name, LeaseTTL: 300 * time.Millisecond, DisableEager: !eager})
			id := "crash-activity-" + name
			comps.reset(id)
			resetCalls(id)
			_, err := crashChargeWF.Run(waitCtx(t), e, id, "c1")
			var re *RunError
			if !errors.As(err, &re) || re.Status != StatusCompensated || !errors.Is(err, ErrAttemptsExhausted) {
				t.Fatalf("want compensated with ErrAttemptsExhausted, got %v", err)
			}
			if n := callsOf(id); n != 2 {
				t.Fatalf("crashing activity ran %d times, want 2", n)
			}
			// Uncertain: its compensator runs too (with nothing kept).
			wantJournal(t, id, "refund ?", "undo c1")
		})
	}
}

// T11.3: a workflow task whose engine crashes mid-task every time is not
// claimed forever: past MaxTaskClaims claims in a row without a completion,
// the execution is parked (ErrCrashLoop), never failed; Engine.Resume runs
// it again once the cause is fixed.
func TestCrashingWorkflowTaskParks(t *testing.T) {
	id := "crash-loop"
	st := newCrashStore("", id)
	st.wfCrash.Store(true)
	e := startedEngine(t, st, Options{InstanceID: "crash-loop", LeaseTTL: 300 * time.Millisecond, MaxTaskClaims: 2, CacheSize: -1})
	h, err := crashLoopWF.Start(bg, e, id, "ok")
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(waitCtx(t))
	parkedWith(t, err, ErrCrashLoop)
	if !strings.Contains(err.Error(), "3 times") {
		t.Fatalf("reason %v", err)
	}
	st.wfCrash.Store(false) // fixed
	if err := e.Resume(bg, id); err != nil {
		t.Fatal(err)
	}
	if out, err := crashLoopWF.Handle(e, id).Wait(waitCtx(t)); err != nil || out != "ok" {
		t.Fatalf("resumed: %q, %v", out, err)
	}
}
