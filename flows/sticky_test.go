package flows

import (
	"context"
	"sync"
	"testing"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
)

// stickyStore records the stickiness asked by completions, before and
// once their execution compensates.
type stickyStore struct {
	store.Store
	mu           sync.Mutex
	compensating map[int64]bool // execution id: compensating
	forward      int            // sticky completions before compensating
	whileComp    []store.Completion
	compTasks    int // completions once compensating
}

func (s *stickyStore) CompleteWorkflowTask(ctx context.Context, l store.Lease, c *store.Completion) ([]store.Lease, error) {
	s.mu.Lock()
	switch {
	case s.compensating[l.ID] || c.Status == store.StatusCompensating:
		s.compensating[l.ID] = true
		s.compTasks++
		if c.StickyFor > 0 {
			s.whileComp = append(s.whileComp, *c)
		}
	case c.StickyFor > 0:
		s.forward++
	}
	s.mu.Unlock()
	return s.Store.CompleteWorkflowTask(ctx, l, c)
}

// T10.14 (from T5.6): a compensating execution runs no workflow code, so
// its executor is not cached and its completions ask for no stickiness.
func TestNoStickyWhileCompensating(t *testing.T) {
	st := &stickyStore{Store: mem.New(nil), compensating: map[int64]bool{}}
	// Polled activities: completions release the lease, sticky while the
	// workflow runs.
	e := startedEngine(t, st, Options{InstanceID: "sticky-comp", DisableEager: true})
	const id = "sticky-saga"
	comps.reset(id)
	if _, err := sagaWF.Run(waitCtx(t), e, id, []string{"a", "b", "fail"}); err == nil {
		t.Fatal("saga succeeded")
	}
	wantJournal(t, id, "undo b", "undo a")
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.forward == 0 {
		t.Fatal("no sticky completion before compensating: the test does not exercise stickiness")
	}
	if st.compTasks < 2 {
		t.Fatalf("%d completions while compensating", st.compTasks)
	}
	for _, c := range st.whileComp {
		t.Errorf("sticky for %v while compensating (status %q)", c.StickyFor, c.Status)
	}
}
