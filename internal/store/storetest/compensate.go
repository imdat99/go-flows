package storetest

import (
	"errors"
	"testing"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// compensating starts i1, records its failure and leaves it compensating
// with one compensation task (schedule id 1, uncertain), unclaimed; it
// returns the workflow lease (released).
func compensating(t *testing.T, s store.Store, h Harness, eager bool) (store.WorkflowTask, []store.Lease) {
	t.Helper()
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	claimFor := h.TTL
	if !eager {
		claimFor = 0
	}
	leases, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History: []history.Event{
			{Seq: 1, Type: history.ExecutionStarted},
			{Seq: 2, Type: history.ExecutionFailed, Err: "boom"},
			{Seq: 3, Type: history.CompensationScheduled, ScheduleID: 1, Name: "charge", Payload: []byte(`{"c":1}`), Uncertain: true},
		},
		Consumed:   pendingIDs(task),
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "charge", Queue: queue, Input: []byte(`{"c":1}`), Compensate: true, Uncertain: true, ClaimFor: claimFor}},
		Status:     store.StatusCompensating,
		Err:        "boom",
	})
	if err != nil {
		t.Fatal(err)
	}
	return task, leases
}

// A compensation task carries Uncertain (eager and polled), and its
// outcome is delivered as a compensation event; the execution stays
// compensating and claimable.
func testCompensationTask(t *testing.T, h Harness) {
	for _, eager := range []bool{false, true} {
		s := h.New(t)
		_, leases := compensating(t, s, h, eager)
		e, _ := s.GetExecution(ctx, "i1")
		if e.Status != store.StatusCompensating || !e.Status.Active() || e.Err != "boom" || !e.EndedAt.IsZero() {
			t.Fatalf("compensating execution %+v", e)
		}
		var lease store.Lease
		if eager {
			if len(leases) != 1 {
				t.Fatalf("eager leases %v", leases)
			}
			lease = leases[0]
		} else {
			acts := claimActivity(t, s, owner, h.TTL)
			if len(acts) != 1 || !acts[0].Compensate || !acts[0].Uncertain || acts[0].Name != "charge" || string(acts[0].Input) != `{"c":1}` {
				t.Fatalf("claimed %+v", acts)
			}
			lease = acts[0].Lease
		}
		if _, err := s.CompleteActivityTask(ctx, lease, store.ActivityResult{Event: history.Event{Type: history.CompensationFailed, Err: "no"}}); err != nil {
			t.Fatal(err)
		}
		task := claimOne(t, s, owner, h.TTL)
		if len(task.Pending) != 1 || task.Pending[0].Event.Type != history.CompensationFailed ||
			task.Pending[0].Event.ScheduleID != 1 || task.Pending[0].Event.Err != "no" || task.Execution.Status != store.StatusCompensating {
			t.Fatalf("delivered %+v (status %s)", task.Pending, task.Execution.Status)
		}
	}
}

// An activity result of an unknown type is rejected.
func testActivityResultTypes(t *testing.T, h Harness) {
	s := h.New(t)
	_, leases := compensating(t, s, h, true)
	if _, err := s.CompleteActivityTask(ctx, leases[0], store.ActivityResult{Event: history.Event{Type: history.SignalReceived}}); !errors.Is(err, store.ErrInvalidCompletion) {
		t.Fatalf("signal as a result: %v", err)
	}
}

// ReopenCompensation resumes only an execution parked for a failed
// compensation, once.
func testReopenCompensation(t *testing.T, h Harness) {
	s := h.New(t)
	if err := s.ReopenCompensation(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown: %v", err)
	}
	_, leases := compensating(t, s, h, true)
	if err := s.ReopenCompensation(ctx, "i1"); !errors.Is(err, store.ErrNotActive) {
		t.Fatalf("reopen while compensating: %v", err)
	}
	if _, err := s.CompleteActivityTask(ctx, leases[0], store.ActivityResult{Event: history.Event{Type: history.CompensationFailed, Err: "no"}}); err != nil {
		t.Fatal(err)
	}
	task := claimOne(t, s, owner, h.TTL)
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History:  []history.Event{{Seq: 4, Type: history.CompensationFailed, ScheduleID: 1, Err: "no"}},
		Consumed: pendingIDs(task),
		Status:   store.StatusNeedsAttention,
		Err:      "compensation of charge failed: no",
		ErrKinds: store.KindCompensationFailed,
	}); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, owner, h.TTL); len(got) != 0 {
		t.Fatalf("parked execution claimed: %+v", got)
	}
	if err := s.ReopenCompensation(ctx, "i1"); err != nil {
		t.Fatal(err)
	}
	if err := s.ReopenCompensation(ctx, "i1"); !errors.Is(err, store.ErrNotActive) {
		t.Fatalf("reopen twice: %v", err)
	}
	task = claimOne(t, s, owner, h.TTL)
	if task.Execution.Status != store.StatusCompensating || len(task.Pending) != 1 ||
		task.Pending[0].Event.Type != history.CompensationRetryRequested || task.LastSeq != 4 {
		t.Fatalf("reopened task %+v", task)
	}
	// Ending the compensation records the workflow's error.
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History:  []history.Event{{Seq: 5, Type: history.CompensationRetryRequested}},
		Consumed: pendingIDs(task),
		Status:   store.StatusCompensated,
		Err:      "boom",
	}); err != nil {
		t.Fatal(err)
	}
	e, _ := s.GetExecution(ctx, "i1")
	if e.Status != store.StatusCompensated || e.Err != "boom" || e.ErrKinds != "" {
		t.Fatalf("ended %+v", e)
	}

	// A non-determinism park is not reopened.
	mustCreate(t, s, "nd")
	task = claimOne(t, s, owner, h.TTL)
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{Status: store.StatusNeedsAttention, Err: "nd"}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReopenCompensation(ctx, "nd"); !errors.Is(err, store.ErrNotActive) {
		t.Fatalf("reopen of a non-determinism park: %v", err)
	}
}

// T8.4: ReopenParked resumes only an execution parked for wrong code
// (non-determinism or code that did not yield), once; it is claimable at
// once, even with nothing pending (the park consumed what it delivered, or
// was the resume of a kept lease).
func testReopenParked(t *testing.T, h Harness) {
	s := h.New(t)
	if err := s.ReopenParked(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown: %v", err)
	}
	for _, kind := range []string{store.KindNonDeterministic, "x," + store.KindStuck} {
		id := "i-" + kind
		mustCreate(t, s, id)
		task := claimOne(t, s, owner, h.TTL)
		if err := s.ReopenParked(ctx, id); !errors.Is(err, store.ErrNotActive) {
			t.Fatalf("reopen while running: %v", err)
		}
		started(t, s, task, &store.Completion{Status: store.StatusNeedsAttention, Err: "wrong code", ErrKinds: kind})
		if got := claim(t, s, owner, h.TTL); len(got) != 0 {
			t.Fatalf("parked execution claimed: %+v", got)
		}
		if err := s.ReopenParked(ctx, id); err != nil {
			t.Fatal(err)
		}
		if err := s.ReopenParked(ctx, id); !errors.Is(err, store.ErrNotActive) {
			t.Fatalf("reopen twice: %v", err)
		}
		e, _ := s.GetExecution(ctx, id)
		if e.Status != store.StatusRunning || e.Err != "" || e.ErrKinds != "" {
			t.Fatalf("reopened %+v", e)
		}
		task = claimOne(t, s, owner, h.TTL)
		if task.Execution.InstanceID != id || len(task.Pending) != 0 {
			t.Fatalf("reopened task %+v", task)
		}
		if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
			History: []history.Event{{Seq: task.LastSeq + 1, Type: history.ExecutionCompleted}}, Status: store.StatusCompleted,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// A failed compensation is not reopened this way.
	mustCreate(t, s, "comp")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{Status: store.StatusNeedsAttention, Err: "x", ErrKinds: store.KindCompensationFailed})
	if err := s.ReopenParked(ctx, "comp"); !errors.Is(err, store.ErrNotActive) {
		t.Fatalf("reopen of a compensation park: %v", err)
	}
}

// T6.3: a compensating parent reopens its completed child for
// compensation; the child's end answers the parent with
// CompensationCompleted. A child that did not complete answers at once.
func testCompensateChildren(t *testing.T, h Harness) {
	s := h.New(t)
	n := compensatingParent(t, s, h)
	if e, _ := s.GetExecution(ctx, "ok"); e.Status != store.StatusCompensating || !e.EndedAt.IsZero() {
		t.Fatalf("reopened child %+v", e)
	}
	if e, _ := s.GetExecution(ctx, "bad"); e.Status != store.StatusFailed {
		t.Fatalf("failed child %+v", e)
	}
	checkChildCompensation(t, s, h, n)
}

// compensatingParent creates p with children ok (completed) and bad
// (failed), and records p compensating both; it returns p's last seq.
func compensatingParent(t *testing.T, s store.Store, h Harness) int64 {
	t.Helper()
	mustCreate(t, s, "p")
	pt := claimOne(t, s, owner, h.TTL)
	started(t, s, pt, &store.Completion{Children: []store.NewExecution{
		{InstanceID: "ok", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 1},
		{InstanceID: "bad", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 2},
	}})
	for _, ct := range claim(t, s, owner, h.TTL) {
		status, typ := store.StatusCompleted, history.ExecutionCompleted
		if ct.Execution.InstanceID == "bad" {
			status, typ = store.StatusFailed, history.ExecutionFailed
		}
		if _, err := s.CompleteWorkflowTask(ctx, ct.Lease, &store.Completion{
			History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}, {Seq: 2, Type: typ}},
			Consumed: pendingIDs(ct), Status: status,
		}); err != nil {
			t.Fatal(err)
		}
	}
	pt = claimOne(t, s, owner, h.TTL) // both outcomes
	c := &store.Completion{Consumed: pendingIDs(pt), Status: store.StatusCompensating, Err: "boom", CompensateChildren: []int32{1, 2}}
	for i, p := range pt.Pending {
		ev := p.Event
		ev.Seq = int64(2 + i)
		c.History = append(c.History, ev)
	}
	n := int64(len(c.History))
	c.History = append(c.History,
		history.Event{Seq: n + 2, Type: history.ExecutionFailed, Err: "boom"},
		history.Event{Seq: n + 3, Type: history.CompensationScheduled, ScheduleID: 1, Ref: "ok"},
		history.Event{Seq: n + 4, Type: history.CompensationScheduled, ScheduleID: 2, Ref: "bad"},
	)
	if _, err := s.CompleteWorkflowTask(ctx, pt.Lease, c); err != nil {
		t.Fatal(err)
	}
	return n + 4
}

// checkChildCompensation runs the reopened child's compensation and checks
// the parent's answers.
func checkChildCompensation(t *testing.T, s store.Store, h Harness, last int64) {
	t.Helper()
	var parent, child store.WorkflowTask
	for _, task := range claim(t, s, owner, h.TTL) {
		switch task.Execution.InstanceID {
		case "p":
			parent = task
		case "ok":
			child = task
		}
	}
	if len(parent.Pending) != 1 || parent.Pending[0].Event.Type != history.CompensationCompleted || parent.Pending[0].Event.ScheduleID != 2 {
		t.Fatalf("parent: immediate answer for the failed child %+v", parent.Pending)
	}
	if child.Execution.Status != store.StatusCompensating || len(child.Pending) != 1 ||
		child.Pending[0].Event.Type != history.ExecutionCompensationRequested || child.LastSeq != 2 {
		t.Fatalf("child task %+v", child)
	}
	if _, err := s.CompleteWorkflowTask(ctx, parent.Lease, &store.Completion{
		History:  []history.Event{{Seq: last + 1, Type: history.CompensationCompleted, ScheduleID: 2}},
		Consumed: pendingIDs(parent),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteWorkflowTask(ctx, child.Lease, &store.Completion{
		History:  []history.Event{{Seq: 3, Type: history.ExecutionCompensationRequested}},
		Consumed: pendingIDs(child), Status: store.StatusCompensated,
	}); err != nil {
		t.Fatal(err)
	}
	parent = claimOne(t, s, owner, h.TTL)
	if len(parent.Pending) != 1 || parent.Pending[0].Event.Type != history.CompensationCompleted || parent.Pending[0].Event.ScheduleID != 1 {
		t.Fatalf("parent: answer at the child's end %+v", parent.Pending)
	}
	// Children to compensate in an ending completion are invalid.
	if _, err := s.CompleteWorkflowTask(ctx, parent.Lease, &store.Completion{Status: store.StatusCompensated, CompensateChildren: []int32{1}}); !errors.Is(err, store.ErrInvalidCompletion) {
		t.Fatalf("ending with children to compensate: %v", err)
	}
}
