package storetest

import (
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// queueFor has the execution id ask for key as flows.Lock does: with
// LockRequest.Wait (retry > 0) its lock-wait timer, due in wake, comes in
// the same completion. It consumes the answer (canceling the timer when
// granted) and returns it.
func (seq lockSeq) queueFor(t *testing.T, s store.Store, h Harness, id string, sched int32, key string, retry, wake time.Duration) history.Event {
	t.Helper()
	return seq.queueForTTL(t, s, h, id, sched, key, retry, wake, waitTTL(h))
}

// waitTTL is the lock TTL of the waiter tests: long enough not to lapse
// between steps on a loaded machine (a lapse is not what they test).
func waitTTL(h Harness) time.Duration { return 20 * h.TTL }

// queueForTTL is queueFor with the lock's TTL.
func (seq lockSeq) queueForTTL(t *testing.T, s store.Store, h Harness, id string, sched int32, key string, retry, wake, ttl time.Duration) history.Event {
	t.Helper()
	ev, task := seq.ask(t, s, h, id, sched, key, retry, wake, ttl)
	seq.answer(t, s, task, sched, ev)
	return ev
}

// ask is the first half of queueFor: the request (and its timer), and the
// claimed task with the answer, not consumed yet.
func (seq lockSeq) ask(t *testing.T, s store.Store, h Harness, id string, sched int32, key string, retry, wake, ttl time.Duration) (history.Event, store.WorkflowTask) {
	t.Helper()
	task := wakeTask(t, s, h, id)
	c := &store.Completion{
		Consumed: pendingIDs(task),
		Locks:    []store.LockRequest{{ScheduleID: sched, Key: key, TTL: ttl, Wait: retry > 0, WaitRetry: retry}},
	}
	for _, p := range task.Pending {
		seq.add(c, id, p.Event)
	}
	seq.add(c, id, history.Event{Type: history.LockRequested, ScheduleID: sched, Name: key})
	if retry > 0 {
		now, err := s.Now(ctx)
		if err != nil {
			t.Fatal(err)
		}
		at := now.Add(wake).Truncate(time.Microsecond)
		seq.add(c, id, history.Event{Type: history.TimerScheduled, ScheduleID: sched + 1000, Name: store.LockWaitTimer(key), At: at})
		c.Timers = []history.Event{{ScheduleID: sched + 1000, Name: store.LockWaitTimer(key), At: at}}
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
	next := claimOne(t, s, owner, h.TTL)
	if next.Execution.InstanceID != id {
		t.Fatalf("claimed %s, want %s", next.Execution.InstanceID, id)
	}
	var answer history.Event
	for _, p := range next.Pending {
		if p.Event.ScheduleID == sched && (p.Event.Type == history.LockAcquired || p.Event.Type == history.LockHeld) {
			answer = p.Event
		}
	}
	return answer, next
}

// answer consumes task's events; a granted waiting request's timer
// (sched+1000, if any) is canceled.
func (seq lockSeq) answer(t *testing.T, s store.Store, task store.WorkflowTask, sched int32, answer history.Event) {
	t.Helper()
	id := task.Execution.InstanceID
	c := &store.Completion{Consumed: pendingIDs(task)}
	for _, p := range task.Pending {
		seq.add(c, id, p.Event)
	}
	if answer.Type == history.LockAcquired {
		seq.add(c, id, history.Event{Type: history.TimerCanceled, ScheduleID: sched + 1000})
		c.CanceledTimers = []int32{sched + 1000}
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
}

func (seq lockSeq) add(c *store.Completion, id string, ev history.Event) {
	seq[id]++
	ev.Seq = seq[id]
	c.History = append(c.History, ev)
}

// end completes the execution id.
func (seq lockSeq) end(t *testing.T, s store.Store, h Harness, id string) {
	t.Helper()
	task := wakeTask(t, s, h, id)
	c := &store.Completion{Consumed: pendingIDs(task), Status: store.StatusCompleted}
	for _, p := range task.Pending {
		seq[id]++
		ev := p.Event
		ev.Seq = seq[id]
		c.History = append(c.History, ev)
	}
	seq[id]++
	c.History = append(c.History, history.Event{Seq: seq[id], Type: history.ExecutionCompleted})
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
}

// T9.8: waiters (LockRequest.Wait) are served in the order they asked: a
// released lock goes to the first one, whose lock-wait timer fires at once
// (no wait for its next attempt); others asking meanwhile are refused.
func testLockWaitersFIFO(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	ids := []string{"i0", "i1", "i2", "i3", "i4"}
	for _, id := range ids {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, h.TTL))
	}
	if ev := seq.queueFor(t, s, h, "i0", 1, "k", 0, 0); ev.Type != history.LockAcquired {
		t.Fatalf("free lock: %+v", ev)
	}
	for _, id := range []string{"i1", "i2", "i3"} {
		if ev := seq.queueFor(t, s, h, id, 1, "k", time.Hour, time.Hour); ev.Type != history.LockHeld || ev.Err != "i0" {
			t.Fatalf("%s: %+v", id, ev)
		}
	}
	// i3 asks again: it keeps its place.
	if ev := seq.queueFor(t, s, h, "i3", 2, "k", time.Hour, time.Hour); ev.Type != history.LockHeld {
		t.Fatalf("i3 again: %+v", ev)
	}
	for _, next := range []string{"i1", "i2", "i3"} {
		holder := map[string]string{"i1": "i0", "i2": "i1", "i3": "i2"}[next]
		seq.end(t, s, h, holder) // releases k: next's lock-wait timer fires now
		task := claimOne(t, s, owner, h.TTL)
		if task.Execution.InstanceID != next || len(task.Pending) == 0 {
			t.Fatalf("after %s ended: %+v", holder, task)
		}
		for _, p := range task.Pending { // i3 asked twice: two timers
			if p.Event.Type != history.TimerFired || p.Event.Name != store.LockWaitTimer("k") {
				t.Fatalf("after %s ended: %+v", holder, p.Event)
			}
		}
		// Someone not queued asks first (next's task still runs): refused,
		// the lock is next's turn.
		if ev := seq.queueFor(t, s, h, "i4", 10, "k", 0, 0); ev.Type != history.LockHeld || ev.Err != next {
			t.Fatalf("i4 before %s: %+v", next, ev)
		}
		// next asks again in the task that sees its timer fired (as Lock).
		if err := s.ReleaseWorkflowTask(ctx, task.Lease, 0); err != nil {
			t.Fatal(err)
		}
		if ev := seq.queueFor(t, s, h, next, 3, "k", time.Hour, time.Hour); ev.Type != history.LockAcquired {
			t.Fatalf("%s: %+v", next, ev)
		}
	}
	seq.end(t, s, h, "i3")
	if ev := seq.queueFor(t, s, h, "i4", 11, "k", 0, 0); ev.Type != history.LockAcquired {
		t.Fatalf("no waiter left: %+v", ev)
	}
}

// A waiter that does not come back (dead, or gone on without the lock)
// loses its turn once its next attempt is overdue by the lock's TTL; one
// asking without Wait leaves the queue.
func testLockWaiterExpires(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	for _, id := range []string{"i0", "i1", "i2", "i3"} {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, h.TTL))
	}
	if ev := seq.queueForTTL(t, s, h, "i0", 1, "k", 0, 0, h.TTL); ev.Type != history.LockAcquired {
		t.Fatalf("free lock: %+v", ev)
	}
	if ev := seq.queueForTTL(t, s, h, "i1", 1, "k", time.Millisecond, time.Hour, h.TTL); ev.Type != history.LockHeld {
		t.Fatalf("i1: %+v", ev)
	}
	if ev := seq.queueFor(t, s, h, "i2", 1, "k", time.Hour, time.Hour); ev.Type != history.LockHeld {
		t.Fatalf("i2: %+v", ev)
	}
	// i2 gives up (its last attempt does not wait): it leaves the queue.
	if ev := seq.queueFor(t, s, h, "i2", 2, "k", 0, 0); ev.Type != history.LockHeld {
		t.Fatalf("i2 last: %+v", ev)
	}
	seq.end(t, s, h, "i0")
	// i1 is woken, but its worker dies with its task (a long lease).
	if dead := claimOne(t, s, "dead", 100*h.TTL); dead.Execution.InstanceID != "i1" {
		t.Fatalf("woken: %s", dead.Execution.InstanceID)
	}
	if ev := seq.queueFor(t, s, h, "i3", 1, "k", 0, 0); ev.Type != history.LockHeld || ev.Err != "i1" {
		t.Fatalf("i1's turn: %+v", ev)
	}
	h.Advance(t, s, h.TTL+h.TTL/2) // i1 never came back
	if ev := seq.queueFor(t, s, h, "i3", 2, "k", 0, 0); ev.Type != history.LockAcquired {
		t.Fatalf("after i1's turn lapsed: %+v", ev)
	}
}

// T9.8 review: a waiter whose wait ended without the lock (its context
// canceled: the timer is canceled) has no turn; one woken before it saw
// its LockHeld answer finds its timer fired with it.
func testLockWaiterEdges(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	for _, id := range []string{"i0", "i1", "i2", "i3"} {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, h.TTL))
	}
	if ev := seq.queueFor(t, s, h, "i0", 1, "k", 0, 0); ev.Type != history.LockAcquired {
		t.Fatalf("free lock: %+v", ev)
	}
	if ev := seq.queueFor(t, s, h, "i1", 1, "k", time.Hour, time.Hour); ev.Type != history.LockHeld {
		t.Fatalf("i1: %+v", ev)
	}
	// i1's wait is canceled: its timer goes.
	task := wakeTask(t, s, h, "i1")
	c := &store.Completion{Consumed: pendingIDs(task), CanceledTimers: []int32{1001}}
	for _, p := range task.Pending {
		seq.add(c, "i1", p.Event)
	}
	seq.add(c, "i1", history.Event{Type: history.TimerCanceled, ScheduleID: 1001})
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
	// i2 asks and is answered only after i0 ended: woken with its answer.
	ev, task := seq.ask(t, s, h, "i2", 1, "k", time.Hour, time.Hour, waitTTL(h))
	if ev.Type != history.LockHeld {
		t.Fatalf("i2: %+v", ev)
	}
	seq.end(t, s, h, "i0") // i2's task still runs
	if err := s.ReleaseWorkflowTask(ctx, task.Lease, 0); err != nil {
		t.Fatal(err)
	}
	got := claim(t, s, owner, h.TTL)
	if len(got) != 1 || got[0].Execution.InstanceID != "i2" {
		for _, g := range got {
			t.Logf("%s: %+v", g.Execution.InstanceID, g.Pending)
		}
		t.Fatalf("after i0 ended: %d tasks", len(got))
	}
	fired := false
	for _, p := range got[0].Pending {
		fired = fired || p.Event.Type == history.TimerFired && p.Event.Name == store.LockWaitTimer("k")
	}
	if !fired {
		t.Fatalf("i2 not woken: %+v", got[0].Pending)
	}
	// i3 asks while it is i2's turn (i1, canceled, has none).
	if ev := seq.queueFor(t, s, h, "i3", 1, "k", 0, 0); ev.Type != history.LockHeld || ev.Err != "i2" {
		t.Fatalf("i3 in i2's turn: %+v", ev)
	}
	if err := s.ReleaseWorkflowTask(ctx, got[0].Lease, 0); err != nil {
		t.Fatal(err)
	}
	if ev := seq.queueFor(t, s, h, "i2", 2, "k", time.Hour, time.Hour); ev.Type != history.LockAcquired {
		t.Fatalf("i2: %+v", ev)
	}
}

// A lapsed lock (not released) wakes nobody and is anyone's: only a
// release gives a waiter its turn.
func testLockWaiterLapsed(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	for _, id := range []string{"i0", "i1", "i2"} {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, h.TTL))
	}
	if ev := seq.queueForTTL(t, s, h, "i0", 1, "k", 0, 0, h.TTL); ev.Type != history.LockAcquired {
		t.Fatalf("free lock: %+v", ev)
	}
	if ev := seq.queueFor(t, s, h, "i1", 1, "k", time.Hour, time.Hour); ev.Type != history.LockHeld || ev.Err != "i0" {
		t.Fatalf("i1: %+v", ev)
	}
	h.Advance(t, s, h.TTL+h.TTL/2) // i0 idles: k lapses
	ev, others := seq.requestLock(t, s, h, wakeTask(t, s, h, "i2"), 1, "k")
	if ev.Type != history.LockAcquired {
		t.Fatalf("lapsed: %+v", ev)
	}
	if len(others) != 1 || others[0].Execution.InstanceID != "i0" || others[0].Pending[0].Event.Type != history.LockLost {
		t.Fatalf("i0 not told: %+v", others)
	}
}

// T9.8 review: the woken waiter's last attempt (no Wait, no new timer, in
// the task that sees its timer fired) gets the lock.
func testLockWaiterLastAttempt(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	for _, id := range []string{"i0", "i1", "i2"} {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, h.TTL))
	}
	if ev := seq.queueFor(t, s, h, "i0", 1, "k", 0, 0); ev.Type != history.LockAcquired {
		t.Fatalf("free lock: %+v", ev)
	}
	for _, id := range []string{"i1", "i2"} {
		if ev := seq.queueFor(t, s, h, id, 1, "k", time.Hour, time.Hour); ev.Type != history.LockHeld {
			t.Fatalf("%s: %+v", id, ev)
		}
	}
	seq.end(t, s, h, "i0")
	if ev := seq.queueFor(t, s, h, "i1", 2, "k", 0, 0); ev.Type != history.LockAcquired {
		t.Fatalf("woken waiter, last attempt: %+v", ev)
	}
}

// T9.8 review: a woken waiter that leaves (terminated) before it asks
// again keeps nobody out of the free lock.
func testLockWaiterHeadTerminated(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	for _, id := range []string{"i0", "i1", "i2", "i3"} {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, h.TTL))
	}
	if ev := seq.queueFor(t, s, h, "i0", 1, "k", 0, 0); ev.Type != history.LockAcquired {
		t.Fatalf("free lock: %+v", ev)
	}
	for _, id := range []string{"i1", "i2"} {
		if ev := seq.queueFor(t, s, h, id, 1, "k", time.Hour, time.Hour); ev.Type != history.LockHeld {
			t.Fatalf("%s: %+v", id, ev)
		}
	}
	seq.end(t, s, h, "i0")
	if err := s.Terminate(ctx, "i1", "gone"); err != nil {
		t.Fatal(err)
	}
	if ev := seq.queueFor(t, s, h, "i3", 1, "k", 0, 0); ev.Type != history.LockAcquired {
		t.Fatalf("free lock, woken waiter gone: %+v", ev)
	}
}
