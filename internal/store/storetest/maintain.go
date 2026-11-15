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
