package worker_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/worker"
)

// countResumes counts ResumeWorkflowTask calls.
type countResumes struct {
	store.Store
	n atomic.Int32
}

func (c *countResumes) ResumeWorkflowTask(ctx context.Context, l store.Lease, ttl time.Duration) (store.WorkflowTask, error) {
	c.n.Add(1)
	return c.Store.ResumeWorkflowTask(ctx, l, ttl)
}

// T9.7: a workflow taking locks runs on with their answers returned by the
// completion that asked: no ResumeWorkflowTask (a round trip each).
func TestLockAnswerComesWithTheCompletion(t *testing.T) {
	st := &countResumes{Store: mem.New(nil)}
	reg := newRegistry()
	reg.addWorkflow("locks", func(ctx coro.Context, _ []byte) ([]byte, error) {
		for _, k := range []string{"a", "b", "c"} {
			if err := executor.FromContext(ctx).Lock(ctx, k, time.Minute, 0); err != nil {
				return nil, err
			}
		}
		return []byte(`"ok"`), nil
	})
	startWorker(t, st, reg, worker.Options{})
	create(t, st, "locks", "locks", "")
	if e := waitEnd(t, st, "locks"); e.Status != store.StatusCompleted {
		t.Fatalf("%s %s", e.Status, e.Err)
	}
	if n := st.n.Load(); n != 0 {
		t.Fatalf("%d ResumeWorkflowTask calls", n)
	}
}
