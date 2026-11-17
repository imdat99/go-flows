package storetest

import (
	"errors"
	"testing"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// T11.11: a claim whose attempt never started (its reply came too late) is
// given back with ReleaseActivityTask: claimable at once, the claim undone
// (its attempt count back, no lost attempt, not uncertain). It is fenced:
// a stale lease (another owner claimed since) is ErrLeaseLost. A lost
// attempt before the released claim keeps the task uncertain.
func testReleaseActivityTask(t *testing.T, h Harness) {
	s := h.New(t)
	scheduleActivity(t, s, h, "i1", store.NewActivityTask{ScheduleID: 1, Name: "a", Queue: queue, MaxAttempts: 1})
	a := claimActivity(t, s, owner, h.TTL)[0]
	if err := s.ReleaseActivityTask(ctx, a.Lease); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseActivityTask(ctx, a.Lease); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("released twice: %v, want ErrLeaseLost", err)
	}
	reclaim(t, s, h, "after a release", 1, false)
	if err := s.ReleaseActivityTask(ctx, a.Lease); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("a stale lease: %v, want ErrLeaseLost", err)
	}

	// Eagerly claimed: back to unclaimed.
	s = h.New(t)
	leases := scheduleActivity(t, s, h, "i2", store.NewActivityTask{ScheduleID: 1, Name: "a", Queue: queue, ClaimFor: h.TTL, MaxAttempts: 1})
	if len(leases) != 1 {
		t.Fatalf("eager leases %+v", leases)
	}
	if err := s.ReleaseActivityTask(ctx, leases[0]); err != nil {
		t.Fatal(err)
	}
	reclaim(t, s, h, "eager, after a release", 1, false)

	// A lost attempt before: still uncertain, the attempt count back.
	s = h.New(t)
	scheduleActivity(t, s, h, "i3", store.NewActivityTask{ScheduleID: 1, Name: "a", Queue: queue, MaxAttempts: 3})
	lose(t, s, h)
	a = claimActivity(t, s, owner, h.TTL)[0]
	if a.Attempt != 2 || !a.Uncertain {
		t.Fatalf("after a lost attempt: %+v", a)
	}
	if err := s.ReleaseActivityTask(ctx, a.Lease); err != nil {
		t.Fatal(err)
	}
	reclaim(t, s, h, "released after a lost attempt", 2, true)
}

// reclaim claims the single activity task as another owner and checks its
// attempt and uncertainty (never exhausted).
func reclaim(t *testing.T, s store.Store, h Harness, what string, attempt int32, uncertain bool) {
	t.Helper()
	acts := claimActivity(t, s, other, h.TTL)
	if len(acts) != 1 || acts[0].Attempt != attempt || acts[0].Exhausted || acts[0].Uncertain != uncertain {
		t.Fatalf("%s: %+v, want attempt %d, uncertain %v, not exhausted", what, acts, attempt, uncertain)
	}
}

// lose claims the single visible activity task and lets its lease expire
// without a result (its process crashed), returning the claim.
func lose(t *testing.T, s store.Store, h Harness) store.ActivityTask {
	t.Helper()
	acts := claimActivity(t, s, owner, h.TTL)
	if len(acts) != 1 {
		t.Fatalf("claimed %d activities, want 1", len(acts))
	}
	h.Advance(t, s, h.TTL+h.TTL/2)
	return acts[0]
}

// T11.3: an activity whose attempts are lost (its process crashes) ends
// once the claims reach its MaxAttempts: the claim beyond it is reported
// Exhausted (the worker records a final failure instead of running it).
// Attempts that recorded something (a retry, a hand-back) never count as
// lost, and a task without a known limit is never exhausted.
func testActivityLostAttemptsExhaust(t *testing.T, h Harness) {
	s := h.New(t)
	scheduleActivity(t, s, h, "i1", store.NewActivityTask{ScheduleID: 1, Name: "a", Queue: queue, MaxAttempts: 2})
	for want := int32(1); want <= 2; want++ {
		if a := lose(t, s, h); a.Attempt != want || a.Exhausted {
			t.Fatalf("attempt %d: %+v", want, a)
		}
	}
	acts := claimActivity(t, s, other, h.TTL)
	if len(acts) != 1 || acts[0].Attempt != 3 || !acts[0].Exhausted || !acts[0].Uncertain {
		t.Fatalf("beyond the limit after lost attempts: %+v", acts)
	}
	// The failure the worker records reaches the workflow as any other.
	res := store.ActivityResult{Event: history.Event{Type: history.ActivityFailed, Err: "exhausted", Permanent: true, Uncertain: true}}
	if _, err := s.CompleteActivityTask(ctx, acts[0].Lease, res); err != nil {
		t.Fatal(err)
	}
	task := claimOne(t, s, owner, h.TTL)
	if len(task.Pending) != 1 || task.Pending[0].Event.Type != history.ActivityFailed || task.Pending[0].Event.Attempt != 3 {
		t.Fatalf("pending %+v", task.Pending)
	}

	// A recorded retry (or hand-back) is not a lost attempt.
	scheduleActivity(t, s, h, "i2", store.NewActivityTask{ScheduleID: 1, Name: "a", Queue: queue, MaxAttempts: 1})
	a := claimActivity(t, s, owner, h.TTL)[0]
	if err := s.RetryActivityTask(ctx, a.Lease, 0, false, nil); err != nil {
		t.Fatal(err)
	}
	if acts := claimActivity(t, s, owner, h.TTL); len(acts) != 1 || acts[0].Attempt != 2 || acts[0].Exhausted {
		t.Fatalf("after a recorded retry: %+v", acts)
	}
}

// T11.3: no limit (0, rows of earlier releases) never exhausts; an eagerly
// claimed task and a compensation task count their lost attempts too.
func testActivityExhaustEdges(t *testing.T, h Harness) {
	s := h.New(t)
	scheduleActivity(t, s, h, "i1", store.NewActivityTask{ScheduleID: 1, Name: "a", Queue: queue})
	for want := int32(1); want <= 2; want++ {
		if a := lose(t, s, h); a.Attempt != want || a.Exhausted {
			t.Fatalf("unlimited, attempt %d: %+v", want, a)
		}
	}
	if acts := claimActivity(t, s, other, h.TTL); len(acts) != 1 || acts[0].Attempt != 3 || acts[0].Exhausted {
		t.Fatalf("unlimited, attempt 3: %+v", acts)
	}

	s = h.New(t)
	leases := scheduleActivity(t, s, h, "i2", store.NewActivityTask{ScheduleID: 1, Name: "a", Queue: queue, ClaimFor: h.TTL, MaxAttempts: 1})
	if len(leases) != 1 {
		t.Fatalf("eager leases %+v", leases)
	}
	h.Advance(t, s, h.TTL+h.TTL/2) // its holder died
	acts := claimActivity(t, s, other, h.TTL)
	if len(acts) != 1 || acts[0].Attempt != 2 || !acts[0].Exhausted {
		t.Fatalf("eager holder died: %+v", acts)
	}

	s = h.New(t)
	scheduleActivity(t, s, h, "i3", store.NewActivityTask{ScheduleID: 1, Name: "a", Queue: queue, Compensate: true, MaxAttempts: 1})
	if c := lose(t, s, h); c.Exhausted || !c.Compensate {
		t.Fatalf("compensation, first attempt: %+v", c)
	}
	acts = claimActivity(t, s, other, h.TTL)
	if len(acts) != 1 || !acts[0].Compensate || acts[0].Attempt != 2 || !acts[0].Exhausted {
		t.Fatalf("compensation beyond the limit: %+v", acts)
	}
}

// T11.3: a workflow task claimed again and again without completing (its
// engine crashes mid-task) is counted: Claims grows with each claim, a
// release undoes only its own claim (a hand-back, by an engine that cannot
// run the task, must not hide the claims lost before it), and a completion
// starts the count over. A parked crash loop is reopened by ReopenParked.
func testWorkflowClaimsCount(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	if task := claimOne(t, s, owner, h.TTL); task.Claims != 1 {
		t.Fatalf("first claim: Claims %d", task.Claims)
	}
	h.Advance(t, s, h.TTL+h.TTL/2) // lost
	for range 2 {
		task := claimOne(t, s, other, h.TTL)
		if task.Claims != 2 {
			t.Fatalf("after a lost claim: Claims %d, want 2", task.Claims)
		}
		// Handed back: the lost claim before it still counts.
		if err := s.ReleaseWorkflowTask(ctx, task.Lease, 0); err != nil {
			t.Fatal(err)
		}
	}
	task := claimOne(t, s, other, h.TTL)
	if task.Claims != 2 {
		t.Fatalf("after hand-backs: Claims %d, want 2", task.Claims)
	}
	started(t, s, task, nil)
	if err := s.Signal(ctx, "i1", "go", nil); err != nil {
		t.Fatal(err)
	}
	task = claimOne(t, s, owner, h.TTL)
	if task.Claims != 1 {
		t.Fatalf("after a completion: Claims %d", task.Claims)
	}
	if err := s.ReleaseWorkflowTask(ctx, task.Lease, 0); err != nil {
		t.Fatal(err)
	}
	task = claimOne(t, s, owner, h.TTL)
	if task.Claims != 1 {
		t.Fatalf("after a release: Claims %d", task.Claims)
	}
	// Kept and resumed: no claim; releasing the kept lease does not go
	// below zero.
	c := &store.Completion{Consumed: pendingIDs(task), KeepFor: h.TTL}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
	resumed, err := s.ResumeWorkflowTask(ctx, task.Lease, h.TTL)
	if err != nil || resumed.Claims != 0 {
		t.Fatalf("resumed: Claims %d, %v", resumed.Claims, err)
	}
	if err := s.ReleaseWorkflowTask(ctx, task.Lease, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.Signal(ctx, "i1", "again", nil); err != nil {
		t.Fatal(err)
	}
	task = claimOne(t, s, owner, h.TTL)
	if task.Claims != 1 {
		t.Fatalf("after releasing a kept lease: Claims %d", task.Claims)
	}
	park := &store.Completion{Status: store.StatusNeedsAttention, Err: "crash loop", ErrKinds: store.KindCrashLoop}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, park); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, owner, h.TTL); len(got) != 0 {
		t.Fatal("a parked execution was claimed")
	}
	if err := s.ReopenParked(ctx, "i1"); err != nil {
		t.Fatalf("reopen a crash loop: %v", err)
	}
	if task = claimOne(t, s, owner, h.TTL); task.Claims != 1 {
		t.Fatalf("reopened: Claims %d", task.Claims)
	}
}
