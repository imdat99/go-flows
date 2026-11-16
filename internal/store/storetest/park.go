package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// park parks the execution id as non-deterministic, consuming its pending
// events (wakeTask's signal).
func (seq lockSeq) park(t *testing.T, s store.Store, h Harness, id string) {
	t.Helper()
	task := wakeTask(t, s, h, id)
	c := &store.Completion{Consumed: pendingIDs(task), Status: store.StatusNeedsAttention, Err: "wrong code", ErrKinds: store.KindNonDeterministic}
	for _, p := range task.Pending {
		seq.add(c, id, p.Event)
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
}

// T10.14: a parked holder (needs_attention) does not run, so its lock is
// kept for its TTL (a park is not an end: nobody is woken) and then lapses
// like an idle holder's: anyone may take it, and the holder is told
// (LockLost) when it is reopened.
func testLockHolderParked(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	for _, id := range []string{"i1", "i2", "i3"} {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, h.TTL))
	}
	if ev := seq.queueForTTL(t, s, h, "i1", 1, "k", 0, 0, h.TTL); ev.Type != history.LockAcquired {
		t.Fatalf("free: %+v", ev)
	}
	granted, err := s.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seq.park(t, s, h, "i1")
	// Kept while the TTL lasts: a waiter is refused, and the park wakes it
	// not (its timer is an hour away). On a machine too loaded to ask
	// within the TTL, the lock may have lapsed already: not asserted then.
	ev := seq.queueForTTL(t, s, h, "i2", 1, "k", time.Hour, time.Hour, h.TTL)
	asked, err := s.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if asked.Sub(granted) < h.TTL && (ev.Type != history.LockHeld || ev.Err != "i1") {
		t.Fatalf("lock of a just parked holder: %+v", ev)
	}
	if ev.Type != history.LockHeld {
		t.Skipf("too slow: %v between the grant and the next request (TTL %v)", asked.Sub(granted), h.TTL)
	}
	if tasks := claim(t, s, owner, h.TTL); len(tasks) != 0 {
		t.Fatalf("the park woke %s", tasks[0].Execution.InstanceID)
	}
	h.Advance(t, s, h.TTL+h.TTL/2)
	// Lapsed: anyone's (a lapse wakes nobody, not even the waiter i2).
	if ev := seq.queueForTTL(t, s, h, "i3", 1, "k", 0, 0, h.TTL); ev.Type != history.LockAcquired {
		t.Fatalf("lapsed lock of a parked holder: %+v", ev)
	}
	// The parked holder is told, but stays parked until reopened.
	if tasks := claim(t, s, owner, h.TTL); len(tasks) != 0 {
		t.Fatalf("claimed %s while its holder is parked", tasks[0].Execution.InstanceID)
	}
	if e, err := s.GetExecution(ctx, "i1"); err != nil || e.Status != store.StatusNeedsAttention {
		t.Fatalf("holder: %+v %v", e, err)
	}
	reopenLost(t, s, h, "i1", 1, "k")
}

// reopenLost reopens the parked execution id and expects it told that it
// lost lock key (taken at schedule id sched).
func reopenLost(t *testing.T, s store.Store, h Harness, id string, sched int32, key string) {
	t.Helper()
	if err := s.ReopenParked(ctx, id); err != nil {
		t.Fatal(err)
	}
	task := claimOne(t, s, owner, h.TTL)
	if task.Execution.InstanceID != id {
		t.Fatalf("claimed %s, want the reopened %s", task.Execution.InstanceID, id)
	}
	var lost bool
	for _, p := range task.Pending {
		lost = lost || p.Event.Type == history.LockLost && p.Event.ScheduleID == sched && p.Event.Name == key
	}
	if !lost {
		t.Fatalf("reopened holder not told of the lost lock: %v", pendingTypes(task))
	}
}

// T10.14: terminating a parked holder releases its lock at once, waking
// the first waiter.
func testLockHolderParkedTerminated(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	for _, id := range []string{"i1", "i2"} {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, h.TTL))
	}
	if ev := seq.queueFor(t, s, h, "i1", 1, "k", 0, 0); ev.Type != history.LockAcquired {
		t.Fatalf("free: %+v", ev)
	}
	seq.park(t, s, h, "i1")
	if ev := seq.queueFor(t, s, h, "i2", 1, "k", time.Hour, time.Hour); ev.Type != history.LockHeld || ev.Err != "i1" {
		t.Fatalf("lock of a parked holder: %+v", ev)
	}
	if err := s.Terminate(ctx, "i1", "operator"); err != nil {
		t.Fatal(err)
	}
	woken := claimOne(t, s, owner, h.TTL)
	if woken.Execution.InstanceID != "i2" {
		t.Fatalf("woken %s, want i2", woken.Execution.InstanceID)
	}
	if err := s.ReleaseWorkflowTask(ctx, woken.Lease, 0); err != nil {
		t.Fatal(err)
	}
	if ev := seq.queueFor(t, s, h, "i2", 2, "k", time.Hour, time.Hour); ev.Type != history.LockAcquired {
		t.Fatalf("waiter after the parked holder's termination: %+v", ev)
	}
}

// T10.14: a completion that would end the execution while events wait
// (ErrPendingEvents) is refused before the end hook: the hook neither
// sees it nor commits anything, and the execution runs on; the re-run's
// end is the hook's only call.
func testEndHookPendingEvents(t *testing.T, h Harness) {
	if h.SetHook == nil {
		t.Skip("no hook support")
	}
	s := h.New(t)
	var (
		mu   sync.Mutex
		seen []store.EndInfo
	)
	h.SetHook(s, func(_ context.Context, e store.EndInfo) error {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, e)
		return nil
	})
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	if err := s.Signal(ctx, "i1", "late", []byte(`1`)); err != nil {
		t.Fatal(err)
	}
	if err := s.RequestCancel(ctx, "i1"); err != nil {
		t.Fatal(err)
	}
	ev := task.Pending[0].Event
	ev.Seq = 1
	end := &store.Completion{
		History:  []history.Event{ev, {Seq: 2, Type: history.ExecutionCompleted}},
		Consumed: pendingIDs(task), Status: store.StatusCompleted,
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, end); !errors.Is(err, store.ErrPendingEvents) {
		t.Fatalf("end with undelivered events: %v", err)
	}
	mu.Lock()
	if len(seen) != 0 {
		t.Fatalf("hook saw a refused end: %+v", seen)
	}
	mu.Unlock()
	if e, _ := s.GetExecution(ctx, "i1"); e.Status != store.StatusRunning {
		t.Fatalf("status after a refused end: %s", e.Status)
	}
	// The worker re-runs with the events (here: under a new claim once the
	// lease is handed back) and ends then.
	if err := s.ReleaseWorkflowTask(ctx, task.Lease, 0); err != nil {
		t.Fatal(err)
	}
	task = claimOne(t, s, owner, h.TTL)
	c := &store.Completion{Consumed: pendingIDs(task), Status: store.StatusCanceled}
	for i, p := range task.Pending {
		ev := p.Event
		ev.Seq = int64(i + 1)
		c.History = append(c.History, ev)
	}
	c.History = append(c.History, history.Event{Seq: int64(len(c.History) + 1), Type: history.ExecutionCanceled})
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatalf("end after the re-run: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0].InstanceID != "i1" || seen[0].Status != store.StatusCanceled {
		t.Fatalf("hook calls %+v", seen)
	}
}
