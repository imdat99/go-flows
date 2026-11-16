package storetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// T9.8 third review: a request whose acquireLock waits for the lock row
// while the holder's ending transaction (wake, then the release) commits
// must not take the lock in the turn of the waiter that release woke (the
// turn is on the lock row it re-reads).
func testLockWaiterRace(t *testing.T, h Harness) {
	if h.SetHook == nil {
		t.Skip("no hook")
	}
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
	inHook := make(chan struct{})
	release := make(chan struct{})
	h.SetHook(s, func(ctx context.Context, e store.EndInfo) error {
		close(inHook)
		<-release
		return nil
	})
	// i0's end, prepared.
	t0 := wakeTaskFor(t, s, lease(h), "i0")
	c0 := &store.Completion{Consumed: pendingIDs(t0), Status: store.StatusCompleted}
	for _, p := range t0.Pending {
		seq.add(c0, "i0", p.Event)
	}
	seq.add(c0, "i0", history.Event{Type: history.ExecutionCompleted})
	// i2's request (no wait), prepared.
	t2 := wakeTaskFor(t, s, lease(h), "i2")
	c2 := &store.Completion{Consumed: pendingIDs(t2), Locks: []store.LockRequest{{ScheduleID: 5, Key: "k", TTL: waitTTL(h)}}}
	for _, p := range t2.Pending {
		seq.add(c2, "i2", p.Event)
	}
	seq.add(c2, "i2", history.Event{Type: history.LockRequested, ScheduleID: 5, Name: "k"})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, err := s.CompleteWorkflowTask(ctx, t0.Lease, c0); err != nil {
			t.Error(err)
		}
	}()
	<-inHook
	go func() {
		defer wg.Done()
		if _, err := s.CompleteWorkflowTask(ctx, t2.Lease, c2); err != nil {
			t.Error(err)
		}
	}()
	time.Sleep(300 * time.Millisecond) // i2's acquireLock waits for k's row (pg; the memory store runs one call at a time)
	close(release)
	wg.Wait()
	for _, task := range claim(t, s, owner, lease(h)) {
		for _, p := range task.Pending {
			t.Logf("%s: %v %s err=%q", task.Execution.InstanceID, p.Event.Type, p.Event.Name, p.Event.Err)
			if task.Execution.InstanceID == "i2" && p.Event.Type == history.LockAcquired {
				t.Errorf("i2 took k during i1's turn (FIFO broken)")
			}
		}
	}
}
