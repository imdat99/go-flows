package worker_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/worker"
)

// turns makes two engines claim workflow tasks in turn: crasher first,
// then the other, and so on.
type turns struct {
	mu             sync.Mutex
	crasher, other int
}

// turnStore is one engine's view of a shared store. Its claims wait for its
// turn. The crasher's tasks behave as if its process died in each: their
// completions (but a park) and releases fail, and their leases are
// reported lost and not renewed. Embedding the interface hides the
// optional ones (hints, due times): engines poll.
type turnStore struct {
	store.Store
	turns   *turns
	crasher bool
}

func (s *turnStore) ClaimWorkflowTasks(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.WorkflowTask, error) {
	s.turns.mu.Lock()
	defer s.turns.mu.Unlock()
	if s.crasher && s.turns.crasher != s.turns.other || !s.crasher && s.turns.other >= s.turns.crasher {
		return nil, nil
	}
	out, err := s.Store.ClaimWorkflowTasks(ctx, owner, queues, limit, ttl)
	if len(out) > 0 {
		if s.crasher {
			s.turns.crasher++
		} else {
			s.turns.other++
		}
	}
	return out, err
}

var errCrashedTask = errors.New("crashed mid-task")

func (s *turnStore) CompleteWorkflowTask(ctx context.Context, l store.Lease, c *store.Completion) ([]store.Lease, error) {
	if s.crasher && c.Status != store.StatusNeedsAttention {
		return nil, errCrashedTask
	}
	return s.Store.CompleteWorkflowTask(ctx, l, c)
}

func (s *turnStore) ReleaseWorkflowTask(ctx context.Context, l store.Lease, delay time.Duration) error {
	if s.crasher {
		return errCrashedTask
	}
	return s.Store.ReleaseWorkflowTask(ctx, l, delay)
}

func (s *turnStore) RenewLeases(ctx context.Context, owner string, workflows, activities []store.Lease, ttl time.Duration) (store.Renewal, error) {
	if !s.crasher {
		return s.Store.RenewLeases(ctx, owner, workflows, activities, ttl)
	}
	r, err := s.Store.RenewLeases(ctx, owner, nil, activities, ttl)
	for _, l := range workflows {
		r.LostWorkflows = append(r.LostWorkflows, l.ID)
	}
	return r, err
}

// T11.3 review: in a rolling deploy, a workflow version whose replay
// crashes the new engines (the only ones that define it) alternates with
// old engines that do not know it and hand it back. The hand-backs must
// not hide the lost claims: the execution still ends parked as a crash
// loop.
func TestCrashLoopThroughHandBacks(t *testing.T) {
	st := mem.New(nil)
	tn := &turns{}
	reg := newRegistry()
	reg.addWorkflow("w", func(coro.Context, []byte) ([]byte, error) { return []byte("ok"), nil })
	opts := func(owner string) worker.Options {
		return worker.Options{Owner: owner, LeaseTTL: 300 * time.Millisecond, HandBack: 20 * time.Millisecond, MaxTaskClaims: 2, CacheSize: -1}
	}
	startWorker(t, &turnStore{Store: st, turns: tn, crasher: true}, reg, opts("new"))
	startWorker(t, &turnStore{Store: st, turns: tn}, newRegistry(), opts("old")) // does not define "w"
	create(t, st, "x", "w", "")
	e := waitEnd(t, st, "x")
	if e.Status != store.StatusNeedsAttention || !slices.Contains(strings.Split(e.ErrKinds, ","), store.KindCrashLoop) {
		t.Fatalf("x: %s %q %q, want parked as a crash loop", e.Status, e.Err, e.ErrKinds)
	}
	tn.mu.Lock()
	defer tn.mu.Unlock()
	if tn.other < 1 || tn.crasher < 2 {
		t.Fatalf("claims: %d crashing, %d handed back; want them interleaved", tn.crasher, tn.other)
	}
}
