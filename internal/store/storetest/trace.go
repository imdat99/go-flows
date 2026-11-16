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
// own; continue-as-new keeps it. Without one it stays nil.
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
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History:    []history.Event{{Seq: 1, Type: history.ExecutionStarted}},
		Consumed:   pendingIDs(task),
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: queue}},
		Children:   []store.NewExecution{{InstanceID: "kid", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 2, Trace: child}},
		KeepFor:    h.TTL,
	}); err != nil {
		t.Fatal(err)
	}
	r, err := s.ResumeWorkflowTask(ctx, task.Lease, h.TTL)
	if err != nil || !bytes.Equal(r.Execution.Trace, tr) {
		t.Fatalf("resumed %+v %v", r, err)
	}
	acts := claimActivity(t, s, owner, h.TTL)
	if len(acts) != 1 || !bytes.Equal(acts[0].Trace, tr) {
		t.Fatalf("activity %+v", acts)
	}
	kid := claimOne(t, s, owner, h.TTL)
	if kid.Execution.InstanceID != "kid" || !bytes.Equal(kid.Execution.Trace, child) {
		t.Fatalf("child %s %x", kid.Execution.InstanceID, kid.Execution.Trace)
	}
	// Continue-as-new keeps the trace.
	if _, err := s.CompleteActivityTask(ctx, acts[0].Lease, store.ActivityResult{Event: history.Event{Type: history.ActivityCompleted}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History:       []history.Event{{Seq: 2, Type: history.ExecutionContinuedAsNew}},
		Status:        store.StatusContinued,
		ContinueAsNew: &store.NewExecution{Workflow: "wf", Version: 1, Queue: queue},
	}); err != nil {
		t.Fatal(err)
	}
	next, err := s.GetExecution(ctx, "i1")
	if err != nil || next.ID == task.Execution.ID || !bytes.Equal(next.Trace, tr) {
		t.Fatalf("continued %+v %v", next, err)
	}
}
