package storetest

import (
	"errors"
	"testing"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// lockSeq tracks the history length of each execution of a lock test.
type lockSeq map[string]int64

// wakeTask makes instance id ready (a signal) and claims it.
func wakeTask(t *testing.T, s store.Store, h Harness, id string) store.WorkflowTask {
	t.Helper()
	if err := s.Signal(ctx, id, "wake", nil); err != nil {
		t.Fatal(err)
	}
	task := claimOne(t, s, owner, h.TTL)
	if task.Execution.InstanceID != id {
		t.Fatalf("claimed %s, want %s", task.Execution.InstanceID, id)
	}
	return task
}

// requestLock has the execution of task ask for key (schedule id sched)
// and returns the answer, and the tasks of other executions claimed with
// it (still claimed). The execution is left released.
func (seq lockSeq) requestLock(t *testing.T, s store.Store, h Harness, task store.WorkflowTask, sched int32, key string) (history.Event, []store.WorkflowTask) {
	t.Helper()
	id := task.Execution.InstanceID
	c := &store.Completion{Consumed: pendingIDs(task), Locks: []store.LockRequest{{ScheduleID: sched, Key: key, TTL: h.TTL}}}
	add := func(ev history.Event) {
		seq[id]++
		ev.Seq = seq[id]
		c.History = append(c.History, ev)
	}
	for _, p := range task.Pending {
		add(p.Event)
	}
	add(history.Event{Type: history.LockRequested, ScheduleID: sched, Name: key})
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatalf("request %s: %v", key, err)
	}
	var answer history.Event
	var others []store.WorkflowTask
	for _, next := range claim(t, s, owner, h.TTL) {
		if next.Execution.InstanceID != id {
			others = append(others, next)
			continue
		}
		for _, p := range next.Pending {
			if p.Event.ScheduleID == sched && (p.Event.Type == history.LockAcquired || p.Event.Type == history.LockHeld) {
				answer = p.Event
			}
		}
		seq.consume(t, s, next)
	}
	if answer.Type == 0 {
		t.Fatalf("no answer for %s", key)
	}
	return answer, others
}

// consume records a task's pending events and releases it.
func (seq lockSeq) consume(t *testing.T, s store.Store, task store.WorkflowTask) {
	t.Helper()
	c := &store.Completion{Consumed: pendingIDs(task)}
	for _, p := range task.Pending {
		seq[task.Execution.InstanceID]++
		ev := p.Event
		ev.Seq = seq[task.Execution.InstanceID]
		c.History = append(c.History, ev)
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
}

// Business locks: acquisition, contention, reentrance, takeover after
// expiry (the previous holder learns it), release at the end.
func testLocks(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	for _, id := range []string{"i1", "i2", "i3"} {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, h.TTL))
	}
	if ev, _ := seq.requestLock(t, s, h, wakeTask(t, s, h, "i1"), 1, "k"); ev.Type != history.LockAcquired {
		t.Fatalf("free lock: %+v", ev)
	}
	if ev, _ := seq.requestLock(t, s, h, wakeTask(t, s, h, "i2"), 1, "k"); ev.Type != history.LockHeld || ev.Err != "i1" {
		t.Fatalf("held lock: %+v", ev)
	}
	if ev, _ := seq.requestLock(t, s, h, wakeTask(t, s, h, "i1"), 2, "k"); ev.Type != history.LockAcquired {
		t.Fatalf("reentrant: %+v", ev)
	}
	// i1 does not run for longer than the TTL: its lock lapses, i2 takes it,
	// and i1 is told.
	h.Advance(t, s, h.TTL+h.TTL/2)
	ev, others := seq.requestLock(t, s, h, wakeTask(t, s, h, "i2"), 2, "k")
	if ev.Type != history.LockAcquired {
		t.Fatalf("expired lock: %+v", ev)
	}
	if len(others) != 1 {
		t.Fatalf("the previous holder was not woken: %+v", others)
	}
	task := others[0]
	if task.Execution.InstanceID != "i1" || len(task.Pending) != 1 {
		t.Fatalf("lost lock notice %+v", task)
	}
	if ev := task.Pending[0].Event; ev.Type != history.LockLost || ev.ScheduleID != 2 || ev.Name != "k" {
		t.Fatalf("lost lock event %+v", ev)
	}
	seq.consume(t, s, task)
	// The end of i2 releases the lock at once.
	task = wakeTask(t, s, h, "i2")
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History: []history.Event{{Seq: seq["i2"] + 1, Type: history.ExecutionCompleted}}, Consumed: pendingIDs(task),
		Status: store.StatusCompleted,
	}); err != nil {
		t.Fatal(err)
	}
	if ev, _ := seq.requestLock(t, s, h, wakeTask(t, s, h, "i3"), 1, "k"); ev.Type != history.LockAcquired {
		t.Fatalf("after the holder ended: %+v", ev)
	}
	// So does a termination.
	if err := s.Terminate(ctx, "i3", "x"); err != nil {
		t.Fatal(err)
	}
	if ev, _ := seq.requestLock(t, s, h, wakeTask(t, s, h, "i1"), 3, "k"); ev.Type != history.LockAcquired {
		t.Fatalf("after the holder was terminated: %+v", ev)
	}
}

// A running execution keeps its locks: claims and lease renewals extend
// them.
func testLockRenewal(t *testing.T, h Harness) {
	// Real sleeps on Postgres: twice the TTL, in steps leaving 45% of it
	// as slack for a loaded machine (the three steps still outlast one TTL,
	// so only the extensions keep the lock).
	h.TTL *= 2
	s := h.New(t)
	seq := lockSeq{}
	for _, id := range []string{"i1", "i2"} {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, h.TTL))
	}
	if ev, _ := seq.requestLock(t, s, h, wakeTask(t, s, h, "i1"), 1, "k"); ev.Type != history.LockAcquired {
		t.Fatalf("free lock: %+v", ev)
	}
	h.Advance(t, s, h.TTL*55/100)
	held := wakeTask(t, s, h, "i1") // the claim extends the lock
	h.Advance(t, s, h.TTL*55/100)
	if _, err := s.RenewLeases(ctx, owner, []store.Lease{held.Lease}, nil, h.TTL); err != nil {
		t.Fatal(err)
	}
	h.Advance(t, s, h.TTL*55/100)
	if ev, _ := seq.requestLock(t, s, h, wakeTask(t, s, h, "i2"), 1, "k"); ev.Type != history.LockHeld {
		t.Fatalf("lock of a running execution taken: %+v", ev)
	}
}

// Locks in a completion that ends the execution, or without a TTL, are
// invalid.
func testLockValidation(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	for _, c := range []*store.Completion{
		{Status: store.StatusCompleted, Locks: []store.LockRequest{{ScheduleID: 1, Key: "k", TTL: h.TTL}}},
		{Locks: []store.LockRequest{{ScheduleID: 1, Key: "k"}}},
	} {
		if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); !errors.Is(err, store.ErrInvalidCompletion) {
			t.Fatalf("%+v: %v", c, err)
		}
	}
}

// Review of T5.4 (H1): locks live while the execution's activities run,
// though the execution itself does not (its lease released).
func testLockHeldDuringActivity(t *testing.T, h Harness) {
	// Real sleeps on Postgres: twice the TTL, in steps leaving 45% of it
	// as slack for a loaded machine (the three steps still outlast one TTL,
	// so only the extensions keep the lock).
	h.TTL *= 2
	s := h.New(t)
	seq := lockSeq{}
	for _, id := range []string{"i1", "i2"} {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, h.TTL))
	}
	if ev, _ := seq.requestLock(t, s, h, wakeTask(t, s, h, "i1"), 1, "k"); ev.Type != history.LockAcquired {
		t.Fatalf("free lock: %+v", ev)
	}
	task := wakeTask(t, s, h, "i1")
	c := &store.Completion{
		Consumed:   pendingIDs(task),
		Activities: []store.NewActivityTask{{ScheduleID: 2, Name: "a", Queue: queue}},
	}
	for _, p := range task.Pending {
		seq["i1"]++
		ev := p.Event
		ev.Seq = seq["i1"]
		c.History = append(c.History, ev)
	}
	seq["i1"]++
	c.History = append(c.History, history.Event{Seq: seq["i1"], Type: history.ActivityScheduled, ScheduleID: 2, Name: "a"})
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
	h.Advance(t, s, h.TTL*55/100)
	act := claimActivity(t, s, other, h.TTL) // the claim extends the lock
	if len(act) != 1 {
		t.Fatalf("activities %+v", act)
	}
	h.Advance(t, s, h.TTL*55/100)
	if _, err := s.RenewLeases(ctx, other, nil, []store.Lease{act[0].Lease}, h.TTL); err != nil {
		t.Fatal(err)
	}
	h.Advance(t, s, h.TTL*55/100)
	if ev, _ := seq.requestLock(t, s, h, wakeTask(t, s, h, "i2"), 1, "k"); ev.Type != history.LockHeld || ev.Err != "i1" {
		t.Fatalf("lock taken while its holder's activity runs: %+v", ev)
	}
}

// Review of T5.4 (M3): an undelivered LockLost blocks the end, like a
// signal; continue-as-new releases locks.
func testLockLostBlocksEnd(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	for _, id := range []string{"i1", "i2"} {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, h.TTL))
	}
	if ev, _ := seq.requestLock(t, s, h, wakeTask(t, s, h, "i1"), 1, "k"); ev.Type != history.LockAcquired {
		t.Fatalf("free lock: %+v", ev)
	}
	held := wakeTask(t, s, h, "i1")
	seq.consume(t, s, held)
	h.Advance(t, s, h.TTL+h.TTL/2)
	ev, others := seq.requestLock(t, s, h, wakeTask(t, s, h, "i2"), 1, "k")
	if ev.Type != history.LockAcquired || len(others) != 1 {
		t.Fatalf("takeover %+v %+v", ev, others)
	}
	task := others[0] // i1, with LockLost pending: ending without it fails
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History: []history.Event{{Seq: seq["i1"] + 1, Type: history.ExecutionCompleted}},
		Status:  store.StatusCompleted,
	}); !errors.Is(err, store.ErrPendingEvents) {
		t.Fatalf("end with an undelivered LockLost: %v", err)
	}
	seq.consume(t, s, task)
	// i2 continues as new: the lock is free again.
	task = wakeTask(t, s, h, "i2")
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History:  []history.Event{{Seq: seq["i2"] + 1, Type: history.ExecutionContinuedAsNew}},
		Consumed: pendingIDs(task), Status: store.StatusContinued,
		ContinueAsNew: &store.NewExecution{Workflow: "wf", Version: 1, Queue: queue},
	}); err != nil {
		t.Fatal(err)
	}
	next := claimOne(t, s, owner, h.TTL) // the new execution of i2
	seq["i2"] = 0
	if ev, _ := seq.requestLock(t, s, h, next, 1, "k"); ev.Type != history.LockAcquired {
		t.Fatalf("after continue-as-new: %+v", ev)
	}
}

// Concurrent requests for a fresh key: exactly one execution gets it; one
// checkpoint may ask for the same key twice.
func testLockRace(t *testing.T, h Harness) {
	s := h.New(t)
	const n = 6
	var tasks []store.WorkflowTask
	for i := range n {
		mustCreate(t, s, "r"+string(rune('a'+i)))
	}
	tasks = claim(t, s, owner, h.TTL)
	if len(tasks) != n {
		t.Fatalf("claimed %d", len(tasks))
	}
	errs := make(chan error, n)
	for _, task := range tasks {
		go func() {
			_, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
				History: []history.Event{
					{Seq: 1, Type: history.ExecutionStarted},
					{Seq: 2, Type: history.LockRequested, ScheduleID: 1, Name: "k"},
					{Seq: 3, Type: history.LockRequested, ScheduleID: 2, Name: "k"},
				},
				Consumed: pendingIDs(task),
				Locks:    []store.LockRequest{{ScheduleID: 1, Key: "k", TTL: h.TTL}, {ScheduleID: 2, Key: "k", TTL: h.TTL}},
			})
			errs <- err
		}()
	}
	for range n {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	acquired := 0
	for _, task := range claim(t, s, owner, h.TTL) {
		got := 0
		for _, p := range task.Pending {
			if p.Event.Type == history.LockAcquired {
				got++
			}
		}
		if got != 0 && got != 2 {
			t.Fatalf("%s: %d of its 2 requests acquired", task.Execution.InstanceID, got)
		}
		if got == 2 {
			acquired++
		}
	}
	if acquired != 1 {
		t.Fatalf("%d executions hold the lock", acquired)
	}
}

// Review of T5.4 (round 2): an expired lock whose holder runs (a live
// lease) is not taken over.
func testLockNotTakenFromRunningHolder(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	for _, id := range []string{"i1", "i2"} {
		mustCreate(t, s, id)
		seq.consume(t, s, claimOne(t, s, owner, h.TTL))
	}
	if ev, _ := seq.requestLock(t, s, h, wakeTask(t, s, h, "i1"), 1, "k"); ev.Type != history.LockAcquired {
		t.Fatalf("free lock: %+v", ev)
	}
	if err := s.Signal(ctx, "i1", "wake", nil); err != nil {
		t.Fatal(err)
	}
	held := claimOne(t, s, owner, 4*h.TTL) // a long lease; the lock gets TTL
	h.Advance(t, s, h.TTL+h.TTL/2)
	if ev, _ := seq.requestLock(t, s, h, wakeTask(t, s, h, "i2"), 1, "k"); ev.Type != history.LockHeld || ev.Err != "i1" {
		t.Fatalf("lock of a running holder taken: %+v", ev)
	}
	seq.consume(t, s, held)
}
