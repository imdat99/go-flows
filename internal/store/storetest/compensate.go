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
