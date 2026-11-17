package worker_test

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/worker"
)

// staleTTL is the lease of the stale claim tests: the heartbeat gives a
// lease up when it was not renewed for ttl - ttl/3 = 200ms.
const (
	staleTTL   = 300 * time.Millisecond
	staleDelay = 250 * time.Millisecond // a reply later than 200ms, within the store's lease
)

// lateReplies stands for a database whose reply to one claim (armed: the
// first that returns work of that kind) arrives staleDelay after it
// committed: the claim's lease, counted from just before it was sent, is
// past the point where the heartbeat gives it up when it arrives.
type lateReplies struct {
	*mem.Store
	wfArmed, actArmed, eagerArmed atomic.Bool

	mu        sync.Mutex
	stale     []store.Lease // the leases of the late replies
	completed []store.Lease // workflow leases of the completions stored
	released  []store.Lease // workflow leases released
}

func (s *lateReplies) late(armed *atomic.Bool, leases ...store.Lease) {
	if len(leases) == 0 || !armed.CompareAndSwap(true, false) {
		return
	}
	s.mu.Lock()
	s.stale = append(s.stale, leases...)
	s.mu.Unlock()
	time.Sleep(staleDelay)
}

func (s *lateReplies) isStale(l store.Lease) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Contains(s.stale, l)
}

func (s *lateReplies) ClaimWorkflowTasksDue(ctx context.Context, owner string, queues []string, n int, ttl time.Duration) ([]store.WorkflowTask, time.Duration, error) {
	tasks, next, err := s.Store.ClaimWorkflowTasksDue(ctx, owner, queues, n, ttl)
	var ls []store.Lease
	for _, t := range tasks {
		ls = append(ls, t.Lease)
	}
	s.late(&s.wfArmed, ls...)
	return tasks, next, err
}

func (s *lateReplies) ClaimWorkflowTasks(ctx context.Context, owner string, queues []string, n int, ttl time.Duration) ([]store.WorkflowTask, error) {
	tasks, _, err := s.ClaimWorkflowTasksDue(ctx, owner, queues, n, ttl)
	return tasks, err
}

func (s *lateReplies) ClaimActivityTasksDue(ctx context.Context, owner string, queues []string, n int, ttl time.Duration) ([]store.ActivityTask, time.Duration, error) {
	tasks, next, err := s.Store.ClaimActivityTasksDue(ctx, owner, queues, n, ttl)
	var ls []store.Lease
	for _, t := range tasks {
		ls = append(ls, t.Lease)
	}
	s.late(&s.actArmed, ls...)
	return tasks, next, err
}

func (s *lateReplies) ClaimActivityTasks(ctx context.Context, owner string, queues []string, n int, ttl time.Duration) ([]store.ActivityTask, error) {
	tasks, _, err := s.ClaimActivityTasksDue(ctx, owner, queues, n, ttl)
	return tasks, err
}

func (s *lateReplies) CompleteWorkflowTask(ctx context.Context, l store.Lease, c *store.Completion) ([]store.Lease, error) {
	leases, err := s.Store.CompleteWorkflowTask(ctx, l, c)
	if err == nil {
		s.mu.Lock()
		s.completed = append(s.completed, l)
		s.mu.Unlock()
		s.late(&s.eagerArmed, leases...) // eagerly claimed activity tasks
	}
	return leases, err
}

func (s *lateReplies) ReleaseWorkflowTask(ctx context.Context, l store.Lease, delay time.Duration) error {
	err := s.Store.ReleaseWorkflowTask(ctx, l, delay)
	s.mu.Lock()
	s.released = append(s.released, l)
	s.mu.Unlock()
	return err
}

// attemptSeen is an activity invocation.
type attemptSeen struct {
	lease     store.Lease
	attempt   int32
	uncertain bool
}

// staleSetup runs a workflow of one activity on a worker whose store
// replies late to the armed claim; it returns the activity's invocations
// once the workflow completed.
func staleSetup(t *testing.T, eager bool, arm func(*lateReplies)) (*lateReplies, []attemptSeen) {
	t.Helper()
	st := &lateReplies{Store: mem.New(nil)}
	arm(st)
	var (
		mu   sync.Mutex
		seen []attemptSeen
	)
	reg := newRegistry()
	reg.addActivity("a", &worker.Activity{Run: func(_ context.Context, at *store.ActivityTask) ([]byte, []byte, error) {
		mu.Lock()
		seen = append(seen, attemptSeen{lease: at.Lease, attempt: at.Attempt, uncertain: at.Uncertain})
		mu.Unlock()
		return []byte("ok"), nil, nil
	}, Retry: worker.RetryPolicy{Attempts: 1}})
	reg.addWorkflow("w", func(ctx coro.Context, _ []byte) ([]byte, error) {
		return activity(ctx, "a", nil)
	})
	startWorker(t, st, reg, worker.Options{LeaseTTL: staleTTL, Eager: eager, EagerWindow: time.Second})
	create(t, st, "x", "w", "")
	if e := waitEnd(t, st, "x"); e.Status != store.StatusCompleted {
		t.Fatalf("status %s (%s), want completed", e.Status, e.Err)
	}
	mu.Lock()
	defer mu.Unlock()
	return st, append([]attemptSeen(nil), seen...)
}

// checkOneFreshAttempt checks that the activity ran once, not on a stale
// lease, as its first attempt (the stale claim was given back, its
// attempt undone: with a single attempt allowed, a lapsed claim would have
// ended it as exhausted without running it).
func checkOneFreshAttempt(t *testing.T, st *lateReplies, seen []attemptSeen) {
	t.Helper()
	if len(st.stale) == 0 {
		t.Fatal("no late reply: the test did not arm")
	}
	for _, s := range seen {
		if st.isStale(s.lease) {
			t.Fatalf("the activity ran on a claim whose reply came %v after it was sent (past the heartbeat's give-up point): %+v", staleDelay, seen)
		}
	}
	if len(seen) != 1 || seen[0].attempt != 1 || seen[0].uncertain {
		t.Fatalf("attempts %+v, want one: attempt 1, certain (the stale claim given back)", seen)
	}
}

// T11.11: a polled activity task whose claim reply arrives after the point
// where the heartbeat gives its lease up is not run (another engine may
// hold it by then), and is given back without using an attempt.
func TestStaleActivityClaimIsNotRun(t *testing.T) {
	st, seen := staleSetup(t, false, func(s *lateReplies) { s.actArmed.Store(true) })
	checkOneFreshAttempt(t, st, seen)
}

// T11.11: the same for an eagerly claimed activity task, whose lease comes
// with a workflow task completion that replies late.
func TestStaleEagerClaimIsNotRun(t *testing.T) {
	st, seen := staleSetup(t, true, func(s *lateReplies) { s.eagerArmed.Store(true) })
	checkOneFreshAttempt(t, st, seen)
}

// T11.11: a workflow task whose claim reply arrives late is not run (its
// code, the tracer, and eager activities it would start), but released:
// its claim does not count toward MaxTaskClaims.
func TestStaleWorkflowClaimIsNotRun(t *testing.T) {
	st, seen := staleSetup(t, true, func(s *lateReplies) { s.wfArmed.Store(true) })
	if len(st.stale) != 1 {
		t.Fatalf("late replies %v, want 1", st.stale)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, l := range st.completed {
		if l == st.stale[0] {
			t.Fatalf("a workflow task ran on a claim whose reply came %v after it was sent", staleDelay)
		}
	}
	released := false
	for _, l := range st.released {
		released = released || l == st.stale[0]
	}
	if !released {
		t.Fatalf("the stale workflow claim was not released (releases %v)", st.released)
	}
	if len(seen) != 1 {
		t.Fatalf("activity attempts %+v, want 1", seen)
	}
}
