package storetest

import (
	"errors"
	"slices"
	"testing"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

func testRenewLeases(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	mustCreate(t, s, "i2")
	tasks := claim(t, s, owner, h.TTL)
	if len(tasks) != 2 {
		t.Fatalf("claimed %d", len(tasks))
	}
	keep, drop := tasks[0], tasks[1]
	// i1 gets an activity.
	leases, err := s.CompleteWorkflowTask(ctx, keep.Lease, &store.Completion{
		History:    []history.Event{{Seq: 1, Type: history.ExecutionStarted}},
		Consumed:   pendingIDs(keep),
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: queue, ClaimFor: h.TTL}},
		KeepFor:    h.TTL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Signal(ctx, keep.Execution.InstanceID, "x", nil); err != nil {
		t.Fatal(err)
	}
	// Renew twice past the original expiry: nobody else can claim.
	for range 2 {
		h.Advance(t, s, h.TTL*3/4)
		r, err := s.RenewLeases(ctx, owner, []store.Lease{keep.Lease, drop.Lease}, leases, h.TTL)
		if err != nil {
			t.Fatal(err)
		}
		if len(r.LostWorkflows)+len(r.LostActivities)+len(r.BusyActivities)+len(r.BusyWorkflows) != 0 {
			t.Fatalf("renewal %+v", r)
		}
	}
	if got := claim(t, s, other, h.TTL); len(got) != 0 {
		t.Fatalf("renewed executions claimed: %d", len(got))
	}
	if got := claimActivity(t, s, other, h.TTL); len(got) != 0 {
		t.Fatal("renewed activity claimed")
	}
	// Leases released or granted again are reported lost.
	if err := s.ReleaseWorkflowTask(ctx, drop.Lease, 0); err != nil {
		t.Fatal(err)
	}
	h.Advance(t, s, h.TTL+h.TTL/2)
	claim(t, s, other, h.TTL)
	claimActivity(t, s, other, h.TTL)
	r, err := s.RenewLeases(ctx, owner, []store.Lease{keep.Lease, drop.Lease}, leases, h.TTL)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(r.LostWorkflows)
	want := []int64{keep.Lease.ID, drop.Lease.ID}
	slices.Sort(want)
	if !slices.Equal(r.LostWorkflows, want) || !slices.Equal(r.LostActivities, []int64{leases[0].ID}) {
		t.Fatalf("lost %+v", r)
	}
	if r, err := s.RenewLeases(ctx, owner, nil, nil, h.TTL); err != nil || len(r.LostWorkflows) != 0 {
		t.Fatalf("empty renewal: %+v %v", r, err)
	}
}

// GW-4: purging ended executions removes everything they own and frees
// their ids; active executions are untouched.
func testPurgeEnded(t *testing.T, h Harness) {
	s := h.New(t)
	for _, id := range []string{"old1", "old2", "old3"} {
		mustCreate(t, s, id)
	}
	for _, tk := range claim(t, s, owner, h.TTL) {
		started(t, s, tk, &store.Completion{Status: store.StatusCompleted})
	}
	mustCreate(t, s, "active")
	h.Advance(t, s, h.TTL)
	n, err := s.PurgeEnded(ctx, h.TTL/2, 2)
	if err != nil || n != 2 {
		t.Fatalf("purge limit: %d %v", n, err)
	}
	n, err = s.PurgeEnded(ctx, h.TTL/2, 10)
	if err != nil || n != 1 {
		t.Fatalf("purge rest: %d %v", n, err)
	}
	if n, _ := s.PurgeEnded(ctx, h.TTL/2, 10); n != 0 {
		t.Fatalf("purged %d active executions", n)
	}
	for _, id := range []string{"old1", "old2", "old3"} {
		if _, err := s.GetExecution(ctx, id); err == nil {
			t.Fatalf("%s still there", id)
		}
	}
	if _, created, err := s.CreateExecution(ctx, newExec("old1")); err != nil || !created {
		t.Fatalf("purged id not reusable: %v %v", created, err)
	}
	if _, err := s.GetExecution(ctx, "active"); err != nil {
		t.Fatal(err)
	}
}

// The holder of a kept lease dies after its eager activity completed: once
// the lease expires, another owner claims the execution with the result.
func testKeptLeaseHolderDies(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	leases, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History:    []history.Event{{Seq: 1, Type: history.ExecutionStarted}},
		Consumed:   pendingIDs(task),
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: queue, ClaimFor: h.TTL}},
		KeepFor:    h.TTL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteActivityTask(ctx, leases[0], store.ActivityResult{Event: history.Event{Type: history.ActivityCompleted}}); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, other, h.TTL); len(got) != 0 {
		t.Fatal("claimed while the kept lease is live")
	}
	h.Advance(t, s, h.TTL+h.TTL/2)
	got := claimOne(t, s, other, h.TTL)
	if !slices.Equal(pendingTypes(got), []history.Type{history.ActivityCompleted}) {
		t.Fatalf("pending %v", pendingTypes(got))
	}
}

// GW-3: a terminal completion of a resumed task that delivered nothing
// (Consumed nil) is still rejected while a signal is pending.
func testTerminalWithNilConsumed(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{KeepFor: h.TTL})
	if err := s.Signal(ctx, "i1", "late", nil); err != nil {
		t.Fatal(err)
	}
	c := store.Completion{History: []history.Event{{Seq: 2, Type: history.ExecutionCompleted}}, Status: store.StatusCompleted}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &c); !errors.Is(err, store.ErrPendingEvents) {
		t.Fatalf("terminal completion with nil Consumed and a pending signal: %v", err)
	}
}

// Terminate ends an execution by force, even while a worker holds it: the
// holder's writes fail, tasks and pending events are dropped, and the
// parent learns of it.
func testTerminate(t *testing.T, h Harness) {
	s := h.New(t)
	if err := s.Terminate(ctx, "missing", "x"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown: %v", err)
	}
	mustCreate(t, s, "parent")
	pt := claimOne(t, s, owner, h.TTL)
	started(t, s, pt, &store.Completion{Children: []store.NewExecution{{InstanceID: "child", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 1}}})
	ct := claimOne(t, s, owner, h.TTL)
	leases, err := s.CompleteWorkflowTask(ctx, ct.Lease, &store.Completion{
		History:    []history.Event{{Seq: 1, Type: history.ExecutionStarted}},
		Consumed:   pendingIDs(ct),
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: queue, ClaimFor: h.TTL}},
		KeepFor:    h.TTL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Terminate(ctx, "child", "operator said so"); err != nil {
		t.Fatal(err)
	}
	e, _ := s.GetExecution(ctx, "child")
	if e.Status != store.StatusTerminated || e.Err != "operator said so" || e.EndedAt.IsZero() {
		t.Fatalf("terminated execution %+v", e)
	}
	hist, _ := s.LoadHistory(ctx, e.ID, 0)
	if n := len(hist); n != 2 || hist[1].Type != history.ExecutionTerminated || hist[1].Seq != 2 {
		t.Fatalf("history %+v", hist)
	}
	if _, err := s.ResumeWorkflowTask(ctx, ct.Lease, h.TTL); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("holder's lease survived: %v", err)
	}
	if _, err := s.CompleteActivityTask(ctx, leases[0], store.ActivityResult{Event: history.Event{Type: history.ActivityCompleted}}); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("activity of a terminated execution: %v", err)
	}
	if err := s.Terminate(ctx, "child", "again"); !errors.Is(err, store.ErrNotActive) {
		t.Fatalf("terminate twice: %v", err)
	}
	ptask := claimOne(t, s, owner, h.TTL)
	ev := ptask.Pending[0].Event
	if ev.Type != history.ChildFailed || ev.ErrKinds != store.KindTerminated || ev.ScheduleID != 1 {
		t.Fatalf("parent notification %+v", ev)
	}
}

// GW-13: an activity the workflow canceled is reported by RenewLeases so
// its worker cancels it; its outcome is still recorded.
func testActivityCancel(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	leases, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History:    []history.Event{{Seq: 1, Type: history.ExecutionStarted}},
		Consumed:   pendingIDs(task),
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: queue, ClaimFor: h.TTL}, {ScheduleID: 2, Name: "a", Queue: queue, ClaimFor: h.TTL}},
		KeepFor:    h.TTL,
	})
	if err != nil {
		t.Fatal(err)
	}
	r, _ := s.RenewLeases(ctx, owner, nil, leases, h.TTL)
	if len(r.CanceledActivities) != 0 {
		t.Fatalf("canceled before any request: %+v", r)
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History:            []history.Event{{Seq: 2, Type: history.ActivityCancelRequested, ScheduleID: 2}},
		CanceledActivities: []int32{2},
		KeepFor:            h.TTL,
	}); err != nil {
		t.Fatal(err)
	}
	r, err = s.RenewLeases(ctx, owner, nil, leases, h.TTL)
	if err != nil || len(r.CanceledActivities) != 1 || r.CanceledActivities[0] != leases[1].ID || len(r.LostActivities) != 0 {
		t.Fatalf("renewal after cancel %+v %v", r, err)
	}
	res := store.ActivityResult{Event: history.Event{Type: history.ActivityFailed, Err: "canceled", Uncertain: true}, Resume: &task.Lease, ResumeFor: h.TTL}
	next, err := s.CompleteActivityTask(ctx, leases[1], res)
	if err != nil || next == nil || len(next.Pending) != 1 || !next.Pending[0].Event.Uncertain {
		t.Fatalf("outcome of a canceled activity: %+v %v", next, err)
	}
}

// A task canceled while it waits for a retry is claimed with
// CancelRequested, so its worker does not run it again.
func testCancelBeforeRetry(t *testing.T, h Harness) {
	s := h.New(t)
	scheduleActivity(t, s, h, "i1", store.NewActivityTask{ScheduleID: 1, Name: "a", Queue: queue})
	a := claimActivity(t, s, owner, h.TTL)[0]
	if a.CancelRequested {
		t.Fatal("fresh task claimed as canceled")
	}
	if err := s.RetryActivityTask(ctx, a.Lease, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.Signal(ctx, "i1", "x", nil); err != nil {
		t.Fatal(err)
	}
	tk := claimOne(t, s, owner, h.TTL)
	complete(t, s, tk, store.Completion{
		History:            []history.Event{{Type: history.ActivityCancelRequested, ScheduleID: 1}},
		Consumed:           pendingIDs(tk),
		CanceledActivities: []int32{1},
	})
	again := claimActivity(t, s, other, h.TTL)
	if len(again) != 1 || !again[0].CancelRequested {
		t.Fatalf("claim after cancel: %+v", again)
	}
}
