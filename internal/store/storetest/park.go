package storetest

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// park parks the execution id as non-deterministic, consuming its pending
// events (wakeTask's signal, and any other), which it returns.
func (seq lockSeq) park(t *testing.T, s store.Store, h Harness, id string) []store.PendingEvent {
	t.Helper()
	task := wakeTask(t, s, h, id)
	c := &store.Completion{Consumed: pendingIDs(task), Status: store.StatusNeedsAttention, Err: "wrong code", ErrKinds: store.KindNonDeterministic}
	for _, p := range task.Pending {
		seq.add(c, id, p.Event)
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
	return task.Pending
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
	ttl := 3 * h.TTL // the steps before the lapse take far less on a loaded machine
	// Read before the request: no later than the grant, so the lock lives
	// at least until granted+ttl (claims of i1 only extend it).
	granted := storeNow(t, s)
	if ev := seq.queueForTTL(t, s, h, "i1", 1, "k", 0, 0, ttl); ev.Type != history.LockAcquired {
		t.Fatalf("free: %+v", ev)
	}
	// i2 waits (its timer an hour away), then i1 parks.
	ev := seq.queueForTTL(t, s, h, "i2", 1, "k", time.Hour, time.Hour, ttl)
	taker := ""
	switch {
	case ev.Type == history.LockAcquired: // too slow: lapsed already
		t.Logf("slow machine: the lock lapsed before i2 asked (%v after the grant, TTL %v); i2 is the taker",
			storeNow(t, s).Sub(granted), ttl)
		taker = "i2"
	case ev.Err != "i1":
		t.Fatalf("waiter: %+v", ev)
	}
	parked := seq.park(t, s, h, "i1")
	// The park is not an end: it wakes nobody.
	if tasks := claim(t, s, owner, h.TTL); len(tasks) != 0 {
		t.Fatalf("the park woke %s", tasks[0].Execution.InstanceID)
	}
	if taker == "" {
		// Kept while its TTL lasts: the waiter is still refused. Read
		// after the request (no earlier than its answer): if that is
		// before granted+ttl, the lock was live when it was answered.
		ev = seq.queueForTTL(t, s, h, "i2", 2, "k", time.Hour, time.Hour, ttl)
		after := storeNow(t, s)
		if ev.Type == history.LockAcquired {
			if after.Sub(granted) < ttl {
				t.Fatalf("lock of a parked holder taken within its TTL: %+v", ev)
			}
			t.Logf("slow machine: the lock lapsed around i2's second request (%v after the grant, TTL %v); i2 is the taker",
				after.Sub(granted), ttl)
			taker = "i2"
		} else if ev.Err != "i1" {
			t.Fatalf("waiter after the park: %+v", ev)
		}
	}
	h.Advance(t, s, ttl+ttl/2)
	if taker == "" {
		// Lapsed: anyone's (a lapse wakes nobody, not even the waiter i2).
		if ev := seq.queueForTTL(t, s, h, "i3", 1, "k", 0, 0, ttl); ev.Type != history.LockAcquired {
			t.Fatalf("lapsed lock of a parked holder: %+v", ev)
		}
	}
	// The parked holder is told, but stays parked until reopened.
	if tasks := claim(t, s, owner, h.TTL); len(tasks) != 0 {
		t.Fatalf("claimed %s while its holder is parked", tasks[0].Execution.InstanceID)
	}
	if e, err := s.GetExecution(ctx, "i1"); err != nil || e.Status != store.StatusNeedsAttention {
		t.Fatalf("holder: %+v %v", e, err)
	}
	if slices.ContainsFunc(parked, func(p store.PendingEvent) bool { return p.Event.Type == history.LockLost }) {
		t.Log("slow machine: the holder learned of the lost lock before it parked; reopen not checked")
		return
	}
	reopenLost(t, s, h, "i1", 1, "k")
}

func storeNow(t *testing.T, s store.Store) time.Time {
	t.Helper()
	now, err := s.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return now
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
	if _, err := s.Terminate(ctx, "i1", "operator"); err != nil {
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
// end is the hook's only call. Without hook support, the refusal and the
// re-run are still checked.
func testEndHookPendingEvents(t *testing.T, h Harness) {
	s := h.New(t)
	var (
		mu   sync.Mutex
		seen []store.EndInfo
	)
	if h.SetHook != nil {
		h.SetHook(s, func(_ context.Context, e store.EndInfo) error {
			mu.Lock()
			defer mu.Unlock()
			seen = append(seen, e)
			return nil
		})
	}
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
	if len(task.Pending) != 3 {
		t.Fatalf("re-run pending %v, want the start, the signal and the cancel", pendingTypes(task))
	}
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
	if e, _ := s.GetExecution(ctx, "i1"); e.Status != store.StatusCanceled {
		t.Fatalf("status after the re-run: %s", e.Status)
	}
	if h.SetHook == nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0].InstanceID != "i1" || seen[0].Status != store.StatusCanceled {
		t.Fatalf("hook calls %+v", seen)
	}
}
