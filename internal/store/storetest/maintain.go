package storetest

import (
	"errors"
	"slices"
	"testing"
	"time"

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

// T8.4 review: a kept lease that expires with nothing pending may be
// claimed as a task without events (Postgres: events delivered under a
// kept lease leave its readiness alone, so its expiry makes it ready) or
// not at all (memory); either way, the next event makes it claimable with
// that event.
func testKeptLeaseExpiresWithoutEvents(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}},
		Consumed: pendingIDs(task),
		KeepFor:  h.TTL,
	}); err != nil {
		t.Fatal(err)
	}
	h.Advance(t, s, h.TTL+h.TTL/2)
	got := claim(t, s, other, h.TTL)
	switch {
	case len(got) > 1, len(got) == 1 && len(got[0].Pending) != 0:
		t.Fatalf("claimed %+v", got)
	case len(got) == 1: // release the empty task as a worker would
		if _, err := s.CompleteWorkflowTask(ctx, got[0].Lease, &store.Completion{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Signal(ctx, "i1", "s", nil); err != nil {
		t.Fatal(err)
	}
	if got := claimOne(t, s, other, h.TTL); len(got.Pending) != 1 {
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
	// A long retry delay: the cancel makes the task claimable at once, so
	// the workflow learns of it without waiting out the backoff.
	if err := s.RetryActivityTask(ctx, a.Lease, time.Hour, false); err != nil {
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

// Unstick hands an owner's sticky executions over at once; another owner's
// stickiness stays.
func testUnstick(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{StickyFor: time.Hour})
	if err := s.Signal(ctx, "i1", "x", nil); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, other, h.TTL); len(got) != 0 {
		t.Fatalf("sticky execution claimed by another owner: %+v", got)
	}
	if err := s.Unstick(ctx, other); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, other, h.TTL); len(got) != 0 {
		t.Fatal("another owner unstuck it")
	}
	if err := s.Unstick(ctx, owner); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, other, h.TTL); len(got) != 1 {
		t.Fatalf("after unstick: %+v", got)
	}
}

// Uncertainty survives retries: an attempt that may have had its effect
// (retried with uncertain, or whose holder died) makes later attempts
// uncertain; a certain retry does not; compensation tasks keep their own
// meaning.
func testUncertainAcrossAttempts(t *testing.T, h Harness) {
	s := h.New(t)
	scheduleActivity(t, s, h, "i1", store.NewActivityTask{ScheduleID: 1, Name: "a", Queue: queue})
	a := claimActivity(t, s, owner, h.TTL)[0]
	if a.Uncertain {
		t.Fatal("first attempt uncertain")
	}
	if err := s.RetryActivityTask(ctx, a.Lease, 0, false); err != nil {
		t.Fatal(err)
	}
	a = claimActivity(t, s, owner, h.TTL)[0]
	if a.Uncertain {
		t.Fatal("uncertain after a certain retry")
	}
	if err := s.RetryActivityTask(ctx, a.Lease, 0, true); err != nil {
		t.Fatal(err)
	}
	a = claimActivity(t, s, owner, h.TTL)[0]
	if !a.Uncertain || a.Attempt != 3 {
		t.Fatalf("after an uncertain retry: %+v", a)
	}
	// Its holder dies: the next claim is uncertain as well.
	scheduleActivity(t, s, h, "i2", store.NewActivityTask{ScheduleID: 1, Name: "b", Queue: queue})
	var b store.ActivityTask
	for _, x := range claimActivity(t, s, owner, h.TTL) {
		if x.InstanceID == "i2" {
			b = x
		}
	}
	if b.Uncertain {
		t.Fatal("fresh task uncertain")
	}
	h.Advance(t, s, h.TTL+h.TTL/2)
	for _, x := range claimActivity(t, s, other, h.TTL) {
		if x.InstanceID == "i2" && !x.Uncertain {
			t.Fatalf("claim after a dead holder: %+v", x)
		}
	}
}

// T6.1: an execution that ends (not by continue-as-new) or is terminated
// asks its active children to cancel; ended children are left alone.
func testEndCancelsChildren(t *testing.T, h Harness) {
	s := h.New(t)
	for _, parent := range []string{"p-end", "p-term"} {
		mustCreate(t, s, parent)
		task := claimOne(t, s, owner, h.TTL)
		started(t, s, task, &store.Completion{Children: []store.NewExecution{
			{InstanceID: parent + "-c1", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 1},
			{InstanceID: parent + "-c2", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 2},
		}})
		// Both children start; c2 ends at once.
		kids := claim(t, s, owner, h.TTL)
		if len(kids) != 2 {
			t.Fatalf("children %+v", kids)
		}
		for _, ct := range kids {
			c := &store.Completion{
				History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}},
				Consumed: pendingIDs(ct),
			}
			if ct.Execution.InstanceID == parent+"-c2" {
				c.History = append(c.History, history.Event{Seq: 2, Type: history.ExecutionCompleted})
				c.Status = store.StatusCompleted
			}
			if _, err := s.CompleteWorkflowTask(ctx, ct.Lease, c); err != nil {
				t.Fatal(err)
			}
		}
		pt := claimOne(t, s, owner, h.TTL) // c2's outcome
		if parent == "p-end" {
			if _, err := s.CompleteWorkflowTask(ctx, pt.Lease, &store.Completion{
				History:  []history.Event{{Seq: 2, Type: pt.Pending[0].Event.Type, ScheduleID: 2}, {Seq: 3, Type: history.ExecutionCompleted}},
				Consumed: pendingIDs(pt), Status: store.StatusCompleted,
			}); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err := s.CompleteWorkflowTask(ctx, pt.Lease, &store.Completion{
				History:  []history.Event{{Seq: 2, Type: pt.Pending[0].Event.Type, ScheduleID: 2}},
				Consumed: pendingIDs(pt),
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.Terminate(ctx, parent, "x"); err != nil {
				t.Fatal(err)
			}
		}
		ct := claimOne(t, s, owner, h.TTL)
		if ct.Execution.InstanceID != parent+"-c1" || len(ct.Pending) != 1 || ct.Pending[0].Event.Type != history.ExecutionCancelRequested {
			t.Fatalf("%s: child after the parent ended: %+v", parent, ct)
		}
		if _, err := s.CompleteWorkflowTask(ctx, ct.Lease, &store.Completion{
			History:  []history.Event{{Seq: 2, Type: history.ExecutionCancelRequested}, {Seq: 3, Type: history.ExecutionCanceled}},
			Consumed: pendingIDs(ct), Status: store.StatusCanceled,
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// GW-3 (T6.2): signals the workflow received but had not read go to the
// next execution first, then the undelivered ones, in order.
func testContinueAsNewCarriesUnread(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	if err := s.Signal(ctx, "i1", "s", []byte("3")); err != nil { // undelivered
		t.Fatal(err)
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History: []history.Event{
			{Seq: 1, Type: history.ExecutionStarted}, {Seq: 2, Type: history.ExecutionContinuedAsNew},
		},
		Consumed: pendingIDs(task), Status: store.StatusContinued,
		ContinueAsNew: &store.NewExecution{Workflow: "wf", Version: 1, Queue: queue},
		Carry: []history.Event{
			{Type: history.SignalReceived, Name: "s", Payload: []byte("1")},
			{Type: history.SignalReceived, Name: "t", Payload: []byte("2")},
			{Type: history.ExecutionCancelRequested},
		},
	}); err != nil {
		t.Fatal(err)
	}
	next := claimOne(t, s, owner, h.TTL)
	var got []string
	for _, p := range next.Pending[1:] {
		got = append(got, p.Event.Type.String()+":"+p.Event.Name+string(p.Event.Payload))
	}
	if next.Pending[0].Event.Type != history.ExecutionStarted || !slices.Equal(got, []string{
		"SignalReceived:s1", "SignalReceived:t2", "ExecutionCancelRequested:", "SignalReceived:s3",
	}) {
		t.Fatalf("next execution got %v", got)
	}
	// Carry without continue-as-new is invalid.
	if _, err := s.CompleteWorkflowTask(ctx, next.Lease, &store.Completion{Carry: []history.Event{{Type: history.SignalReceived}}}); !errors.Is(err, store.ErrInvalidCompletion) {
		t.Fatalf("carry alone: %v", err)
	}
}

// T6.2: continue-as-new leaves the children running (detached).
func testContinueAsNewLeavesChildren(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "p")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{Children: []store.NewExecution{{InstanceID: "kid", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 1}}})
	kid := claimOne(t, s, owner, h.TTL)
	started(t, s, kid, nil)
	if err := s.Signal(ctx, "p", "x", nil); err != nil {
		t.Fatal(err)
	}
	pt := claimOne(t, s, owner, h.TTL)
	if _, err := s.CompleteWorkflowTask(ctx, pt.Lease, &store.Completion{
		History:  []history.Event{{Seq: 2, Type: history.SignalReceived, Name: "x"}, {Seq: 3, Type: history.ExecutionContinuedAsNew}},
		Consumed: pendingIDs(pt), Status: store.StatusContinued,
		ContinueAsNew: &store.NewExecution{Workflow: "wf", Version: 1, Queue: queue},
	}); err != nil {
		t.Fatal(err)
	}
	for _, task := range claim(t, s, owner, h.TTL) {
		if task.Execution.InstanceID == "kid" {
			t.Fatalf("the kid got %+v", task.Pending)
		}
	}
	if e, _ := s.GetExecution(ctx, "kid"); e.Status != store.StatusRunning {
		t.Fatalf("kid %s", e.Status)
	}
}

// An execution that compensates or is parked is still active (its workflow
// function returned, but it is not over): purging it would lose its
// compensation, or the record of what needs attention.
func testPurgeKeepsActiveExecutions(t *testing.T, h Harness) {
	s := h.New(t)
	for _, id := range []string{"parked", "compensating"} {
		mustCreate(t, s, id)
	}
	for _, tk := range claim(t, s, owner, h.TTL) {
		st := store.StatusNeedsAttention
		if tk.Execution.InstanceID == "compensating" {
			st = store.StatusCompensating
		}
		started(t, s, tk, &store.Completion{Status: st, Err: "x"})
	}
	h.Advance(t, s, h.TTL)
	if n, err := s.PurgeEnded(ctx, 0, 10); err != nil || n != 0 {
		t.Fatalf("purged %d active executions: %v", n, err)
	}
	for _, id := range []string{"parked", "compensating"} {
		if _, err := s.GetExecution(ctx, id); err != nil {
			t.Fatalf("%s purged: %v", id, err)
		}
	}
}

// T8.4 review: compensation reaches grandchildren (a reopened child
// compensates its own children), so a grandchild is kept while any ancestor
// is active, even though its own parent ended.
func testPurgeKeepsDescendantsOfActiveExecutions(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "g")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{Children: []store.NewExecution{{InstanceID: "p", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 1}}})
	p := claimOne(t, s, owner, h.TTL)
	started(t, s, p, &store.Completion{Children: []store.NewExecution{{InstanceID: "c", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 1}}})
	c := claimOne(t, s, owner, h.TTL)
	if c.Execution.InstanceID != "c" {
		t.Fatalf("claimed %s", c.Execution.InstanceID)
	}
	if _, err := s.CompleteWorkflowTask(ctx, c.Lease, &store.Completion{
		History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}, {Seq: 2, Type: history.ExecutionCompleted}},
		Consumed: pendingIDs(c), Status: store.StatusCompleted,
	}); err != nil {
		t.Fatal(err)
	}
	p = claimOne(t, s, owner, h.TTL) // its child's result
	if _, err := s.CompleteWorkflowTask(ctx, p.Lease, &store.Completion{
		History:  []history.Event{{Seq: p.LastSeq + 1, Type: history.ExecutionCompleted}},
		Consumed: pendingIDs(p), Status: store.StatusCompleted,
	}); err != nil {
		t.Fatal(err)
	}
	h.Advance(t, s, h.TTL)
	if n, err := s.PurgeEnded(ctx, 0, 10); err != nil || n != 0 {
		t.Fatalf("purged %d %v", n, err)
	}
	for _, id := range []string{"p", "c"} {
		if _, err := s.GetExecution(ctx, id); err != nil {
			t.Fatalf("%s purged while g is active: %v", id, err)
		}
	}
}

// Review of T6.3 (M1): a child of an active parent is kept from purge (its
// parent may still have it compensated).
func testPurgeKeepsChildrenOfActiveParents(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "p")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{Children: []store.NewExecution{{InstanceID: "kid", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 1}}})
	kid := claimOne(t, s, owner, h.TTL)
	if _, err := s.CompleteWorkflowTask(ctx, kid.Lease, &store.Completion{
		History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}, {Seq: 2, Type: history.ExecutionCompleted}},
		Consumed: pendingIDs(kid), Status: store.StatusCompleted,
	}); err != nil {
		t.Fatal(err)
	}
	h.Advance(t, s, h.TTL)
	if n, err := s.PurgeEnded(ctx, 0, 10); err != nil || n != 0 {
		t.Fatalf("purged %d %v", n, err)
	}
	if _, err := s.GetExecution(ctx, "kid"); err != nil {
		t.Fatalf("child of an active parent purged: %v", err)
	}
}

// T9.1: a DueClaimer reports when the next work of the queues becomes
// claimable: a timer, a retry delay, a hand-back; nothing when none.
func testDueClaims(t *testing.T, h Harness) {
	s := h.New(t)
	dc, ok := s.(store.DueClaimer)
	if !ok {
		t.Skip("store does not implement store.DueClaimer")
	}
	near := func(what string, got, want time.Duration) {
		t.Helper()
		if got <= 0 || got > want+want/2+50*time.Millisecond {
			t.Fatalf("%s: next in %v, want about %v", what, got, want)
		}
	}
	if _, next, err := dc.ClaimWorkflowTasksDue(ctx, owner, []string{queue}, 10, h.TTL); err != nil || next != 0 {
		t.Fatalf("empty store: %v %v", next, err)
	}
	d := h.TTL // a delay the harness's clock can wait for
	// A timer.
	mustCreate(t, s, "timer")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{Timers: []history.Event{{ScheduleID: 1, At: task.Now.Add(d)}}})
	tasks, next, err := dc.ClaimWorkflowTasksDue(ctx, owner, []string{queue, "other"}, 10, h.TTL)
	if err != nil || len(tasks) != 0 {
		t.Fatalf("claimed %v %v", tasks, err)
	}
	near("timer", next, d)
	// A hand-back, sooner.
	mustCreate(t, s, "handed")
	task = claimOne(t, s, owner, h.TTL)
	if err := s.ReleaseWorkflowTask(ctx, task.Lease, d/2); err != nil {
		t.Fatal(err)
	}
	_, next, err = dc.ClaimWorkflowTasksDue(ctx, owner, []string{queue}, 10, h.TTL)
	if err != nil {
		t.Fatal(err)
	}
	near("hand-back", next, d/2)
	// Another queue's work is not reported.
	if _, next, err := dc.ClaimWorkflowTasksDue(ctx, owner, []string{"other"}, 10, h.TTL); err != nil || next != 0 {
		t.Fatalf("other queue: %v %v", next, err)
	}
	// An activity retry delay.
	mustCreate(t, s, "retry")
	task = claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: queue}}})
	acts, _, err := dc.ClaimActivityTasksDue(ctx, owner, []string{queue}, 10, h.TTL)
	if err != nil || len(acts) != 1 {
		t.Fatalf("activity claim: %v %v", acts, err)
	}
	if err := s.RetryActivityTask(ctx, acts[0].Lease, d/4, false); err != nil {
		t.Fatal(err)
	}
	_, next, err = dc.ClaimActivityTasksDue(ctx, owner, []string{queue}, 10, h.TTL)
	if err != nil {
		t.Fatal(err)
	}
	near("retry delay", next, d/4)
	// A running (leased) activity task is not due: its visible_at is its
	// lease expiry, recovery's business.
	mustCreate(t, s, "running")
	task = claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: "leased"}}})
	if acts, _, err := dc.ClaimActivityTasksDue(ctx, owner, []string{"leased"}, 10, h.TTL); err != nil || len(acts) != 1 {
		t.Fatalf("claim leased: %v %v", acts, err)
	}
	if _, next, err := dc.ClaimActivityTasksDue(ctx, owner, []string{"leased"}, 10, h.TTL); err != nil || next != 0 {
		t.Fatalf("a running task reported due in %v (%v)", next, err)
	}
}
