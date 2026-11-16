package storetest

import (
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// T9.8 fourth review: a woken waiter that left without the lock (timer
// consumed, no new request) keeps its waiter row; a later release, its
// lock's TTL shorter, still reserves the lock for the waiter it woke.
func testLockWaiterStaleWoken(t *testing.T, h Harness) {
	// i2's turn (i3's lock's TTL, 2*h.TTL) must last while i4 asks.
	realTime(t, h, func(h Harness) bool {
		s := h.New(t)
		seq := lockSeq{}
		for _, id := range []string{"i0", "i1", "i2", "i3", "i4"} {
			mustCreate(t, s, id)
			seq.consume(t, s, claimOne(t, s, owner, lease(h)))
		}
		if ev := seq.queueForTTL(t, s, h, "i0", 1, "k", 0, 0, 20*h.TTL); ev.Type != history.LockAcquired {
			t.Fatalf("free: %+v", ev)
		}
		for _, id := range []string{"i1", "i2"} {
			if ev := seq.queueFor(t, s, h, id, 1, "k", time.Hour, time.Hour); ev.Type != history.LockHeld {
				t.Fatalf("%s: %+v", id, ev)
			}
		}
		seq.end(t, s, h, "i0") // i1 woken
		t1 := claimOne(t, s, owner, lease(h))
		if t1.Execution.InstanceID != "i1" {
			t.Fatalf("woken %s", t1.Execution.InstanceID)
		}
		seq.consume(t, s, t1) // i1 leaves (ctx canceled with the wake): no new request
		if ev := seq.queueForTTL(t, s, h, "i3", 1, "k", 0, 0, 2*h.TTL); ev.Type != history.LockAcquired {
			t.Fatalf("i3 on i1's dead turn: %+v", ev)
		}
		inTime := within(t, s, 2*h.TTL)
		seq.end(t, s, h, "i3") // i2 woken
		t2 := claimOne(t, s, owner, lease(h))
		if t2.Execution.InstanceID != "i2" {
			t.Fatalf("woken %s", t2.Execution.InstanceID)
		}
		if ev := seq.queueFor(t, s, h, "i4", 1, "k", 0, 0); ev.Type != history.LockHeld || ev.Err != "i2" {
			if !inTime() {
				return false
			}
			t.Errorf("i4 in i2's turn: %+v (FIFO broken)", ev)
		}
		if err := s.ReleaseWorkflowTask(ctx, t2.Lease, 0); err != nil {
			t.Fatal(err)
		}
		return true
	})
}

// Terminating the holder wakes the first waiter as its end does.
func testLockWaiterTerminatedHolder(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	for _, id := range []string{"i0", "i1", "i2"} {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, lease(h)))
	}
	if ev := seq.queueFor(t, s, h, "i0", 1, "k", 0, 0); ev.Type != history.LockAcquired {
		t.Fatalf("free: %+v", ev)
	}
	if ev := seq.queueFor(t, s, h, "i1", 1, "k", time.Hour, time.Hour); ev.Type != history.LockHeld {
		t.Fatalf("i1: %+v", ev)
	}
	if _, err := s.Terminate(ctx, "i0", "x"); err != nil {
		t.Fatal(err)
	}
	t1 := claimOne(t, s, owner, lease(h))
	if t1.Execution.InstanceID != "i1" {
		t.Fatalf("woken %s", t1.Execution.InstanceID)
	}
	if ev := seq.queueFor(t, s, h, "i2", 1, "k", 0, 0); ev.Type != history.LockHeld || ev.Err != "i1" {
		t.Errorf("i2 in i1's turn: %+v", ev)
	}
	if err := s.ReleaseWorkflowTask(ctx, t1.Lease, 0); err != nil {
		t.Fatal(err)
	}
	if ev := seq.queueFor(t, s, h, "i1", 2, "k", time.Hour, time.Hour); ev.Type != history.LockAcquired {
		t.Errorf("i1: %+v", ev)
	}
}

// A parked waiter (needs_attention, its timer still pending) is not woken:
// the release wakes the next one.
func testLockWaiterParkedSkipped(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	for _, id := range []string{"i0", "i1", "i2", "i3"} {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, lease(h)))
	}
	if ev := seq.queueFor(t, s, h, "i0", 1, "k", 0, 0); ev.Type != history.LockAcquired {
		t.Fatalf("free: %+v", ev)
	}
	for _, id := range []string{"i1", "i2"} {
		if ev := seq.queueFor(t, s, h, id, 1, "k", time.Hour, time.Hour); ev.Type != history.LockHeld {
			t.Fatalf("%s: %+v", id, ev)
		}
	}
	task := wakeTaskFor(t, s, lease(h), "i1")
	c := &store.Completion{Consumed: pendingIDs(task), Status: store.StatusNeedsAttention, Err: "wrong code", ErrKinds: store.KindNonDeterministic}
	for _, p := range task.Pending {
		seq.add(c, "i1", p.Event)
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
	seq.end(t, s, h, "i0")
	woken := claimOne(t, s, owner, lease(h))
	if woken.Execution.InstanceID != "i2" {
		t.Fatalf("woken %s, want i2 (i1 is parked)", woken.Execution.InstanceID)
	}
	if ev := seq.queueFor(t, s, h, "i3", 1, "k", 0, 0); ev.Type != history.LockHeld || ev.Err != "i2" {
		t.Fatalf("i3 in i2's turn: %+v", ev)
	}
	if err := s.ReleaseWorkflowTask(ctx, woken.Lease, 0); err != nil {
		t.Fatal(err)
	}
}
