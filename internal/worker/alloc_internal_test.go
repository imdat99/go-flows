package worker

import (
	"context"
	"testing"

	"github.com/imdat99/go-flows/internal/store"
)

// T10.14: with no observer and no tracer, observing costs no allocation:
// an activity attempt (with an activity that allocates nothing) and a
// reported end.
func TestNothingConfiguredAllocatesNothing(t *testing.T) {
	w, err := New(store.Store(nil), nil, nil, Options{Owner: "o", Queues: []string{"q"}})
	if err != nil {
		t.Fatal(err)
	}
	if w.opts.Observer != nil || w.opts.Tracer != nil {
		t.Fatal("defaults configured an observer or a tracer")
	}
	out := []byte(`1`)
	a := &Activity{Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) { return out, nil, nil }}
	task := &store.ActivityTask{Name: "a", Attempt: 1}
	ctx := context.Background()
	if n := testing.AllocsPerRun(100, func() {
		if o := w.attempt(ctx, a, task); o.ev.Payload == nil {
			t.Fatal("attempt failed")
		}
	}); n != 0 {
		t.Errorf("activity attempt: %v allocations", n)
	}
	ex := &store.Execution{ID: 1, InstanceID: "i", Workflow: "wf", Version: 1}
	if n := testing.AllocsPerRun(100, func() { w.ended(ex, store.StatusCompleted, "", "") }); n != 0 {
		t.Errorf("end: %v allocations", n)
	}
}
