package worker

import (
	"context"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/history"
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

// completeStub accepts every completion and does nothing else.
type completeStub struct{ store.Store }

func (completeStub) CompleteWorkflowTask(context.Context, store.Lease, *store.Completion) ([]store.Lease, error) {
	return nil, nil
}

// T10.14: with no tracer, a workflow task (driver.step) costs what running
// and completing it does: the tracer's gate allocates nothing. The
// executor of a workflow receiving signals in a loop is cached between the
// tasks, as on a worker.
func TestStepWithoutTracerAllocatesNoMore(t *testing.T) {
	w, err := New(completeStub{}, nil, nil, Options{Owner: "o", Queues: []string{"q"}})
	if err != nil {
		t.Fatal(err)
	}
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		ch := executor.FromContext(ctx).SignalChannel("s")
		for {
			if _, ok := ch.Receive(ctx); !ok {
				return nil, nil
			}
		}
	}}
	const id = 1
	ex := executor.New(wf, id, w.exOpts)
	task := &store.WorkflowTask{
		Lease:     store.Lease{ID: id, Owner: "o", Version: 1},
		Execution: store.Execution{ID: id, InstanceID: "i", Workflow: "wf", Version: 1, Queue: "q", Status: store.StatusRunning},
		Now:       time.Unix(1000, 0),
		Pending:   []store.PendingEvent{{ID: 1, Event: history.Event{Type: history.ExecutionStarted, Name: "wf", Version: 1, Queue: "q"}}},
	}
	if _, err := ex.Execute(task); err != nil {
		t.Fatal(err)
	}
	w.cache.put(ex)
	d := &driver{w: w, ctx: context.Background(), lease: task.Lease}
	t.Cleanup(func() { d.evict() })
	ctx := context.Background()
	signal := history.Event{Type: history.SignalReceived, Name: "s"}
	next := func() {
		task.LastSeq = ex.LastSeq()
		task.Now = task.Now.Add(time.Second)
		task.Pending[0].ID++
		task.Pending[0].Event = signal
		d.held = true
	}
	parts := testing.AllocsPerRun(200, func() {
		next()
		c := d.execute(task)
		if c == nil {
			t.Fatal("task not executed")
		}
		d.complete(ctx, task, c)
	})
	step := testing.AllocsPerRun(200, func() {
		next()
		d.step(ctx, task)
	})
	if d.taskErr != nil {
		t.Fatal(d.taskErr)
	}
	if step != parts {
		t.Errorf("step: %v allocations, execute and complete: %v", step, parts)
	}
}
