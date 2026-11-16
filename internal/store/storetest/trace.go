package storetest

import (
	"bytes"
	"testing"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// claimTraced claims i1 (with trace context tr) and plain (without one)
// and returns i1's task.
func claimTraced(t *testing.T, s store.Store, h Harness, tr []byte) store.WorkflowTask {
	t.Helper()
	var task store.WorkflowTask
	for _, x := range claim(t, s, owner, h.TTL) {
		switch x.Execution.InstanceID {
		case "i1":
			task = x
		case "plain":
			if x.Execution.Trace != nil {
				t.Fatalf("plain execution's trace %x", x.Execution.Trace)
			}
		}
	}
	if !bytes.Equal(task.Execution.Trace, tr) {
		t.Fatalf("claimed %x", task.Execution.Trace)
	}
	return task
}

// T10.10: an execution's trace context (opaque bytes) is stored at its
// creation and returned with everything that runs it: GetExecution, the
// workflow claim, the resumed task, activity claims (polled tasks; eager
// ones are built by the worker from the workflow task). A child gets its
// own; continue-as-new gets the one given. Without one it stays nil.
func testTraceContext(t *testing.T, h Harness) {
	s := h.New(t)
	tr := []byte{0, 1, 2, 3, 0xff}
	ne := newExec("i1")
	ne.Trace = tr
	if e, _, err := s.CreateExecution(ctx, ne); err != nil || !bytes.Equal(e.Trace, tr) {
		t.Fatalf("created %+v %v", e, err)
	}
	ne.Trace[0] = 9 // the store keeps its own copy
	tr = []byte{0, 1, 2, 3, 0xff}
	mustCreate(t, s, "plain")
	if e, _ := s.GetExecution(ctx, "i1"); !bytes.Equal(e.Trace, tr) {
		t.Fatalf("get %x", e.Trace)
	}
	task := claimTraced(t, s, h, tr)
	child := []byte("child-trace")
	c := &store.Completion{
		History:    []history.Event{{Seq: 1, Type: history.ExecutionStarted}},
		Consumed:   pendingIDs(task),
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: queue}},
		Children: []store.NewExecution{
			{InstanceID: "kid", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 2, Trace: child, Taken: true}, // stale: reset
			{InstanceID: "plain", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 3, Trace: child},            // in use
		},
		KeepFor: h.TTL,
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
	// T10.10 review: the store reports which children it did not create
	// (Taken is output: reset on every completion).
	if c.Children[0].Taken || !c.Children[1].Taken {
		t.Fatalf("taken: kid %v, plain %v", c.Children[0].Taken, c.Children[1].Taken)
	}
	r, err := s.ResumeWorkflowTask(ctx, task.Lease, h.TTL)
	if err != nil || !bytes.Equal(r.Execution.Trace, tr) {
		t.Fatalf("resumed %+v %v", r, err)
	}
	acts := claimActivity(t, s, owner, h.TTL)
	if len(acts) != 1 || !bytes.Equal(acts[0].Trace, tr) {
		t.Fatalf("activity %+v", acts)
	}
	// A retried attempt carries it too.
	if err := s.RetryActivityTask(ctx, acts[0].Lease, 0, false, nil); err != nil {
		t.Fatal(err)
	}
	acts = claimActivity(t, s, owner, h.TTL)
	if len(acts) != 1 || acts[0].Attempt != 2 || !bytes.Equal(acts[0].Trace, tr) {
		t.Fatalf("retried activity %+v", acts)
	}
	kid := claimOne(t, s, owner, h.TTL)
	if kid.Execution.InstanceID != "kid" || !bytes.Equal(kid.Execution.Trace, child) {
		t.Fatalf("child %s %x", kid.Execution.InstanceID, kid.Execution.Trace)
	}
	continueTraced(t, s, task, acts[0].Lease)
}

// continueTraced completes activity al, then continues task's execution
// as new.
func continueTraced(t *testing.T, s store.Store, task store.WorkflowTask, al store.Lease) {
	t.Helper()
	// Continue-as-new starts a new trace: the next execution gets the one
	// the completion gives (none here: nil), never the old one.
	if _, err := s.CompleteActivityTask(ctx, al, store.ActivityResult{Event: history.Event{Type: history.ActivityCompleted}}); err != nil {
		t.Fatal(err)
	}
	next := []byte("next-trace")
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History:       []history.Event{{Seq: 2, Type: history.ExecutionContinuedAsNew}},
		Status:        store.StatusContinued,
		ContinueAsNew: &store.NewExecution{Workflow: "wf", Version: 1, Queue: queue, Trace: next},
	}); err != nil {
		t.Fatal(err)
	}
	e, err := s.GetExecution(ctx, "i1")
	if err != nil || e.ID == task.Execution.ID || !bytes.Equal(e.Trace, next) {
		t.Fatalf("continued %+v %v", e, err)
	}
}

// T10.10 review: compensation tasks carry the execution's trace context,
// polled and eager (the worker builds eager tasks from the workflow task).
func testTraceOnCompensation(t *testing.T, h Harness) {
	s := h.New(t)
	ne := newExec("i1")
	ne.Trace = []byte("comp-trace")
	if _, _, err := s.CreateExecution(ctx, ne); err != nil {
		t.Fatal(err)
	}
	task := claimOne(t, s, owner, h.TTL)
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History: []history.Event{
			{Seq: 1, Type: history.ExecutionStarted},
			{Seq: 2, Type: history.ExecutionFailed, Err: "boom"},
			{Seq: 3, Type: history.CompensationScheduled, ScheduleID: 1, Name: "charge"},
		},
		Consumed:   pendingIDs(task),
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "charge", Queue: queue, Compensate: true}},
		Status:     store.StatusCompensating,
		Err:        "boom",
	}); err != nil {
		t.Fatal(err)
	}
	acts := claimActivity(t, s, owner, h.TTL)
	if len(acts) != 1 || !acts[0].Compensate || string(acts[0].Trace) != "comp-trace" {
		t.Fatalf("compensation task %+v", acts)
	}
}
