package storetest

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// T9.7 review: Completion.Resume returns what ResumeWorkflowTask would
// (lease, LastSeq, pending events in order: answers held and acquired, a
// signal; not a future timer), into a buffer with stale content.
func testResumeParity(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "holder")
	ht := claimOne(t, s, owner, h.TTL)
	hc := &store.Completion{
		History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}, {Seq: 2, Type: history.LockRequested, ScheduleID: 1, Name: "k"}},
		Consumed: pendingIDs(ht),
		Locks:    []store.LockRequest{{ScheduleID: 1, Key: "k", TTL: time.Hour}}, KeepFor: time.Hour,
	}
	if _, err := s.CompleteWorkflowTask(ctx, ht.Lease, hc); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, s, "i1")
	task := claimOne(t, s, "o2", h.TTL)
	if task.Execution.InstanceID != "i1" {
		t.Fatalf("claimed %s", task.Execution.InstanceID)
	}
	if err := s.Signal(ctx, "i1", "sig", []byte("x")); err != nil {
		t.Fatal(err)
	}
	now, _ := s.Now(ctx)
	// A buffer with stale content from another instance (capacity reused).
	next := store.WorkflowTask{Execution: store.Execution{InstanceID: "other"}, Pending: make([]store.PendingEvent, 5, 8), LastSeq: 99}
	c := &store.Completion{
		History: []history.Event{
			{Seq: 1, Type: history.ExecutionStarted},
			{Seq: 2, Type: history.LockRequested, ScheduleID: 1, Name: "k"},
			{Seq: 3, Type: history.LockRequested, ScheduleID: 2, Name: "k2"},
			{Seq: 4, Type: history.TimerScheduled, ScheduleID: 3},
		},
		Consumed: pendingIDs(task),
		Locks:    []store.LockRequest{{ScheduleID: 2, Key: "k2", TTL: h.TTL}, {ScheduleID: 1, Key: "k", TTL: h.TTL}},
		Timers:   []history.Event{{ScheduleID: 3, At: now.Add(time.Hour)}},
		KeepFor:  h.TTL, Resume: &next,
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
	if !c.Resumed {
		t.Fatal("not resumed")
	}
	got := next
	ref, err := s.ResumeWorkflowTask(ctx, task.Lease, h.TTL)
	if err != nil {
		t.Fatal(err)
	}
	got.Now, ref.Now = time.Time{}, time.Time{}
	utcTimes(&got)
	utcTimes(&ref) // the decoders differ in Location only (pgx: Local)
	if !reflect.DeepEqual(got, ref) {
		t.Fatalf("resumed\n%#v\nwant\n%#v", got, ref)
	}
	types := map[history.Type]int{}
	for _, p := range got.Pending {
		types[p.Event.Type]++
	}
	if len(got.Pending) != 3 || types[history.LockHeld] != 1 || types[history.LockAcquired] != 1 || types[history.SignalReceived] != 1 {
		t.Fatalf("pending %+v", got.Pending)
	}
}

// A compensating execution keeping its lease resumes too.
func testResumeCompensating(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	var next store.WorkflowTask
	c := &store.Completion{
		History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}, {Seq: 2, Type: history.LockRequested, ScheduleID: 1, Name: "k"}},
		Consumed: pendingIDs(task),
		Status:   store.StatusCompensating, Err: "boom",
		Locks:   []store.LockRequest{{ScheduleID: 1, Key: "k", TTL: h.TTL}},
		KeepFor: h.TTL, Resume: &next,
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
	if !c.Resumed || next.Execution.Status != store.StatusCompensating {
		t.Fatalf("resumed %v %+v", c.Resumed, next)
	}
}

// A park releases the lease: nothing resumed, no error.
func testResumeParked(t *testing.T, h Harness) {
	s := h.New(t)
	if h.SetHook != nil {
		h.SetHook(s, func(context.Context, store.EndInfo) error { return nil })
	}
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	next := store.WorkflowTask{LastSeq: 77}
	c := &store.Completion{
		History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}, {Seq: 2, Type: history.LockRequested, ScheduleID: 1, Name: "k"}},
		Consumed: pendingIDs(task),
		Status:   store.StatusNeedsAttention, Err: "park",
		Locks:   []store.LockRequest{{ScheduleID: 1, Key: "k", TTL: h.TTL}},
		KeepFor: h.TTL, Resume: &next,
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
	if c.Resumed {
		t.Fatalf("parked yet resumed %+v", next)
	}
}

// Eager activity leases and the resumed task come back from one batch.
func testResumeWithEager(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	var next store.WorkflowTask
	c := &store.Completion{
		History: []history.Event{
			{Seq: 1, Type: history.ExecutionStarted},
			{Seq: 2, Type: history.ActivityScheduled, ScheduleID: 1, Name: "a"},
			{Seq: 3, Type: history.LockRequested, ScheduleID: 2, Name: "k"},
		},
		Consumed:   pendingIDs(task),
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: queue, ClaimFor: h.TTL}},
		Locks:      []store.LockRequest{{ScheduleID: 2, Key: "k", TTL: h.TTL}},
		KeepFor:    h.TTL, Resume: &next,
	}
	leases, err := s.CompleteWorkflowTask(ctx, task.Lease, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 1 || leases[0].ID == 0 || !c.Resumed || len(next.Pending) != 1 || next.Pending[0].Event.Type != history.LockAcquired || next.LastSeq != 3 {
		t.Fatalf("leases %+v resumed %v next %+v", leases, c.Resumed, next)
	}
}

// utcTimes sets the times of t in UTC.
func utcTimes(t *store.WorkflowTask) {
	t.Execution.CreatedAt, t.Execution.EndedAt = t.Execution.CreatedAt.UTC(), t.Execution.EndedAt.UTC()
	for i := range t.Pending {
		t.Pending[i].Event.At = t.Pending[i].Event.At.UTC()
	}
}
