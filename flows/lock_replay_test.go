package flows

import (
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
)

// T10.14: replay of LockWait. The workflow takes lock "r" (waiting), then
// awaits signal "go".
var lockReplayPolicy = RetryPolicy{Attempts: 3, Initial: 50 * time.Millisecond}

func lockReplayBody(c Ctx, lock func(Ctx) error) (int, error) {
	if err := lock(c); err != nil {
		return 0, err
	}
	return Await[int](c, "go", time.Hour)
}

var lockReplayWF = DefineWorkflow("test.lock-replay", 1, func(c Ctx, _ int) (int, error) {
	return lockReplayBody(c, func(c Ctx) error { return Lock(c, "r", time.Second, LockWait(lockReplayPolicy)) })
})

// oldLockWait is LockWait before T9.8 (git show af75012:lock.go): no
// queued request, a plain Sleep between attempts.
func oldLockWait(c Ctx, key string, ttl time.Duration, p RetryPolicy) error {
	st := c.state()
	for attempt := int32(1); ; attempt++ {
		err := st.Lock(c.c, key, ttl, 0)
		if err == nil || int(attempt) >= p.Attempts {
			return err
		}
		if err := Sleep(c, max(p.Delay(attempt+1), minLockWait)); err != nil {
			return err
		}
	}
}

// oldLockReplayWF is lockReplayWF as the binary before T9.8 ran it (same
// name and version).
var oldLockReplayWF = executor.Workflow{Name: "test.lock-replay", Version: 1, Run: func(cc coro.Context, _ []byte) ([]byte, error) {
	v, err := lockReplayBody(Ctx{cc}, func(c Ctx) error { return oldLockWait(c, "r", time.Second, lockReplayPolicy) })
	if err != nil {
		return nil, err
	}
	return strconv.AppendInt(nil, int64(v), 10), nil
}}

func registeredWorkflow(t *testing.T, name string) executor.Workflow {
	t.Helper()
	registry.RLock()
	defer registry.RUnlock()
	wf, ok := registry.workflows[wfKey{name, 1}]
	if !ok {
		t.Fatalf("%s not registered", name)
	}
	return wf
}

// step gives the pending events of a task from the completions of the
// tasks before it.
type step func(cs []*store.Completion) []history.Event

// latest returns the latest of cs with a lock request (locks) or a timer.
func latest(cs []*store.Completion, locks bool) *store.Completion {
	for _, c := range slices.Backward(cs) {
		if locks && len(c.Locks) > 0 || !locks && len(c.Timers) > 0 {
			return c
		}
	}
	panic("no such completion")
}

// liveRun runs wf live, one task per step; each step gives the pending
// events of the next task from the completion before it. It returns the
// completions and the history.
func liveRun(t *testing.T, wf executor.Workflow, steps ...step) ([]*store.Completion, []history.Event) {
	t.Helper()
	ex := executor.New(wf, 1, executor.Options{})
	defer ex.Close()
	var (
		hist []history.Event
		cs   []*store.Completion
	)
	for i, next := range steps {
		task := lockReplayTask(ex.LastSeq(), i, next(cs))
		c, err := ex.Execute(task)
		if err != nil {
			t.Fatalf("live task %d: %v", i, err)
		}
		c = cloneCompletion(c)
		hist = append(hist, c.History...)
		cs = append(cs, c)
	}
	return cs, hist
}

func lockReplayTask(lastSeq int64, i int, evs []history.Event) *store.WorkflowTask {
	task := &store.WorkflowTask{LastSeq: lastSeq, Now: time.Unix(1000+int64(i), 0)}
	for j, ev := range evs {
		task.Pending = append(task.Pending, store.PendingEvent{ID: int64(100*i + j + 1), Event: ev})
	}
	return task
}

// cloneCompletion copies what the tests read (the executor reuses its
// completion).
func cloneCompletion(c *store.Completion) *store.Completion {
	return &store.Completion{
		History: slices.Clone(c.History), Locks: slices.Clone(c.Locks), Timers: slices.Clone(c.Timers),
		CanceledTimers: slices.Clone(c.CanceledTimers), Status: c.Status, Err: c.Err,
	}
}

func started([]*store.Completion) []history.Event {
	return []history.Event{{Type: history.ExecutionStarted, Name: "test.lock-replay", Version: 1, Queue: DefaultQueue, Payload: []byte("0")}}
}

func lockAnswer(typ history.Type) step {
	return func(cs []*store.Completion) []history.Event {
		c := latest(cs, true)
		return []history.Event{{Type: typ, ScheduleID: c.Locks[0].ScheduleID, Name: c.Locks[0].Key, Err: "other"}}
	}
}

func fireTimer(cs []*store.Completion) []history.Event {
	ev := latest(cs, false).Timers[0]
	ev.Type = history.TimerFired
	return []history.Event{ev}
}

func signalGo([]*store.Completion) []history.Event {
	return []history.Event{{Type: history.SignalReceived, Name: "go", Payload: []byte("7")}}
}

// replayAtEachTask replays every prefix of the run on a fresh executor
// (a cold worker: no cache) and executes the next task: its completion
// must be the live one.
func replayAtEachTask(t *testing.T, wf executor.Workflow, cs []*store.Completion, steps ...step) {
	t.Helper()
	var hist []history.Event
	for i := range cs {
		ex := executor.New(wf, 1, executor.Options{})
		if err := ex.Replay(hist); err != nil {
			ex.Close()
			t.Fatalf("replay of %d tasks: %v", i, err)
		}
		c, err := ex.Execute(lockReplayTask(ex.LastSeq(), i, steps[i](cs[:i])))
		if err != nil {
			ex.Close()
			t.Fatalf("task %d after a replay: %v", i, err)
		}
		if !slices.EqualFunc(c.History, cs[i].History, sameEvent) {
			t.Fatalf("task %d after a replay:\n%+v\nlive:\n%+v", i, c.History, cs[i].History)
		}
		ex.Close()
		hist = append(hist, cs[i].History...)
	}
}

func sameEvent(a, b history.Event) bool {
	return a.Seq == b.Seq && a.Type == b.Type && a.ScheduleID == b.ScheduleID && a.Name == b.Name && a.At.Equal(b.At)
}

// scheduled returns the TimerScheduled events of hist.
func scheduled(hist []history.Event) []history.Event {
	var out []history.Event
	for _, ev := range hist {
		if ev.Type == history.TimerScheduled {
			out = append(out, ev)
		}
	}
	return out
}

// A waiter refused once, woken by its lock-wait timer, then granted:
// replayed cold at every task boundary, it issues what the live run did.
func TestLockWaitReplaysRefusedThenGranted(t *testing.T) {
	wf := registeredWorkflow(t, "test.lock-replay")
	steps := []step{
		started,                          // asks: lock + its timer
		lockAnswer(history.LockHeld),     // refused: sleeps on the timer
		fireTimer,                        // asks again: lock + its timer
		lockAnswer(history.LockAcquired), // granted: timer canceled, Await
		signalGo,
	}
	cs, hist := liveRun(t, wf, steps...)
	if c := cs[0]; len(c.Locks) != 1 || !c.Locks[0].Wait || len(c.Timers) != 1 || c.Timers[0].Name != store.LockWaitTimer("r") ||
		c.Timers[0].ScheduleID != c.Locks[0].ScheduleID+1 {
		t.Fatalf("first attempt: locks %+v timers %+v", c.Locks, c.Timers)
	}
	if c := cs[3]; !slices.Equal(c.CanceledTimers, []int32{cs[2].Timers[0].ScheduleID}) {
		t.Fatalf("granted: canceled %v, want the lock-wait timer %d", c.CanceledTimers, cs[2].Timers[0].ScheduleID)
	}
	if last := cs[len(cs)-1]; last.Status != store.StatusCompleted {
		t.Fatalf("status %s %q", last.Status, last.Err)
	}
	r := executor.New(wf, 1, executor.Options{})
	defer r.Close()
	if err := r.Replay(hist); err != nil {
		t.Fatalf("replay of the whole run: %v", err)
	}
	replayAtEachTask(t, wf, cs, steps...)
}

// A lock granted at once still issued its lock-wait timer (canceled): the
// timer consumed a schedule id, live and on replay.
func TestLockWaitGrantedAtOnceReplays(t *testing.T) {
	wf := registeredWorkflow(t, "test.lock-replay")
	steps := []step{started, lockAnswer(history.LockAcquired), signalGo}
	cs, hist := liveRun(t, wf, steps...)
	lock := cs[0].Locks[0].ScheduleID
	timers := scheduled(hist)
	if len(timers) != 2 || timers[0].Name != store.LockWaitTimer("r") || timers[0].ScheduleID != lock+1 ||
		timers[1].ScheduleID != lock+2 {
		t.Fatalf("lock at %d, timers %+v: the Await timer must follow the lock-wait timer", lock, timers)
	}
	if !slices.Equal(cs[1].CanceledTimers, []int32{lock + 1}) {
		t.Fatalf("canceled %v", cs[1].CanceledTimers)
	}
	r := executor.New(wf, 1, executor.Options{})
	defer r.Close()
	if err := r.Replay(hist); err != nil {
		t.Fatalf("replay: %v", err)
	}
	replayAtEachTask(t, wf, cs, steps...)
}

// A history written by the LockWait before T9.8 (a plain Sleep timer, no
// lock-wait timer) does not replay with the current one: it is
// non-determinism (the upgrade note of CHANGELOG.md), at any point after
// the first lock request was answered. As on a worker, the history is
// replayed, then the next task executed (the executor checks the last
// task of an unfinished history when it executes the next one).
func TestOldLockWaitHistoryIsNonDeterministic(t *testing.T) {
	wf := registeredWorkflow(t, "test.lock-replay")
	for name, steps := range map[string][]step{
		// The run stops before its last step: the next task.
		"refused, sleeping": {started, lockAnswer(history.LockHeld), fireTimer},
		"granted at once":   {started, lockAnswer(history.LockAcquired), signalGo},
		"refused, granted":  {started, lockAnswer(history.LockHeld), fireTimer, lockAnswer(history.LockAcquired), signalGo},
	} {
		cs, _ := liveRun(t, oldLockReplayWF, steps...)
		n := len(steps) - 1
		var hist []history.Event
		for _, c := range cs[:n] {
			hist = append(hist, c.History...)
		}
		r := executor.New(wf, 1, executor.Options{})
		err := r.Replay(hist)
		if err == nil {
			_, err = r.Execute(lockReplayTask(r.LastSeq(), n, steps[n](cs[:n])))
		}
		r.Close()
		if !errors.Is(err, ErrNonDeterministic) {
			t.Errorf("%s: replay of an old LockWait history: %v", name, err)
		}
	}
}

// At engine level, an execution whose history the old LockWait wrote is
// parked (needs_attention) when the current binary replays it, never
// failed; its lock is kept, not released by the park.
func TestOldLockWaitExecutionIsParked(t *testing.T) {
	st := mem.New(nil)
	const id = "old-lock-wait"
	ex, _, err := st.CreateExecution(bg, store.NewExecution{InstanceID: id, Workflow: "test.lock-replay", Version: 1, Queue: DefaultQueue, Input: []byte("0")})
	if err != nil {
		t.Fatal(err)
	}
	// The old binary: the lock is granted at once, then the workflow
	// awaits "go".
	old := executor.New(oldLockReplayWF, ex.ID, executor.Options{})
	defer old.Close()
	for range 2 {
		tasks, err := st.ClaimWorkflowTasks(bg, "old-binary", []string{DefaultQueue}, 1, time.Minute)
		if err != nil || len(tasks) != 1 {
			t.Fatalf("claim: %v %v", tasks, err)
		}
		c, err := old.Execute(&tasks[0])
		if err != nil {
			t.Fatal(err)
		}
		for i := range c.Locks {
			c.Locks[i].TTL = time.Minute // as the worker raises it; no lapse during the test
		}
		if _, err := st.CompleteWorkflowTask(bg, tasks[0].Lease, c); err != nil {
			t.Fatal(err)
		}
	}
	e := startedEngine(t, st, Options{InstanceID: "new-binary"})
	if !historyHas(e, id, "LockAcquired") || !historyHas(e, id, "TimerScheduled") {
		t.Fatalf("old history: %+v", historyEvents(t, e, id))
	}
	if err := e.Signal(bg, id, "go", 1); err != nil {
		t.Fatal(err)
	}
	_, err = lockReplayWF.Handle(e, id).Wait(waitCtx(t))
	parkedWith(t, err, ErrNonDeterministic)
	// Parked, not ended: the lock is still the old execution's.
	_, err = startLock(t, e, "old-lock-contender", lockIn{Key: "r"}).Wait(waitCtx(t))
	if !errors.Is(err, ErrLockHeld) {
		t.Fatalf("contender while the holder is parked: %v", err)
	}
}

// End to end without the executor cache (every task replays): a waiting
// request granted at once, and a waiter refused then woken by the release,
// both complete (no park).
func TestLockWaitWithoutCache(t *testing.T) {
	e := startedEngine(t, mem.New(nil), Options{InstanceID: "lock-cold", CacheSize: -1, LeaseTTL: 2 * time.Second})
	a := startLock(t, e, "cold-a", lockIn{Key: "c", Wait: true, Await: true}) // granted at once
	waitFor(t, func() bool { return historyHas(e, "cold-a", "LockAcquired") })
	b := startLock(t, e, "cold-b", lockIn{Key: "c", Wait: true, Sleep: 20 * time.Millisecond})
	waitFor(t, func() bool { return historyHas(e, "cold-b", "LockHeld") })
	if err := e.Signal(bg, "cold-a", "go", 1); err != nil {
		t.Fatal(err)
	}
	if out, err := a.Wait(waitCtx(t)); err != nil || out != "locked c" {
		t.Fatalf("holder: %q %v", out, err)
	}
	if out, err := b.Wait(waitCtx(t)); err != nil || out != "locked c" {
		t.Fatalf("waiter: %q %v", out, err)
	}
	if !historyHas(e, "cold-b", "TimerCanceled") {
		t.Fatal("the waiter's lock-wait timer was not canceled once granted")
	}
}
