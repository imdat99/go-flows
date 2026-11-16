package executor_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// liveThenReplay runs tasks live on one executor, accumulating the history,
// then replays that history on a fresh executor (found in review of T3.1).
func liveThenReplay(t *testing.T, wf executor.Workflow, tasks [][]history.Event) error {
	t.Helper()
	ex := executor.New(wf, 1, executor.Options{})
	defer ex.Close()
	var hist []history.Event
	now := time.Unix(1000, 0)
	for i, evs := range tasks {
		task := &store.WorkflowTask{LastSeq: ex.LastSeq(), Now: now.Add(time.Duration(i) * time.Second)}
		for j, ev := range evs {
			task.Pending = append(task.Pending, store.PendingEvent{ID: int64(j + 1), Event: ev})
		}
		c, err := ex.Execute(task)
		if err != nil {
			t.Fatalf("live task %d: %v", i, err)
		}
		hist = append(hist, c.History...)
	}
	r := executor.New(wf, 1, executor.Options{})
	defer r.Close()
	return r.Replay(hist)
}

func startEvent() history.Event {
	return history.Event{Type: history.ExecutionStarted, Name: "wf", Version: 1, Queue: "q"}
}

// A side effect returns at once in the live run; on replay it must too, or
// another coroutine takes its schedule ids first.
func TestSideEffectKeepsCommandOrderOnReplay(t *testing.T) {
	twoCoroutines := func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		coro.Go(ctx, func(ctx coro.Context) {
			_, _ = st.SideEffect(ctx, func() []byte { return []byte("x") })
			st.ScheduleActivity(ctx, "A", "q", nil)
		})
		coro.Go(ctx, func(ctx coro.Context) { st.ScheduleActivity(ctx, "B", "q", nil) })
		_, err := st.NewTimer(ctx, time.Hour, "t").Get(ctx)
		return nil, err
	}
	withSignal := func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		coro.Go(ctx, func(ctx coro.Context) {
			_, _ = st.SignalChannel("s").Receive(ctx)
			st.ScheduleActivity(ctx, "B", "q", nil)
		})
		_, _ = st.SideEffect(ctx, func() []byte { return []byte("x") })
		st.ScheduleActivity(ctx, "A", "q", nil)
		_, err := st.NewTimer(ctx, time.Hour, "t").Get(ctx)
		return nil, err
	}
	for name, run := range map[string]executor.Func{"two coroutines": twoCoroutines, "signal in the same task": withSignal} {
		wf := executor.Workflow{Name: "wf", Version: 1, Run: run}
		tasks := [][]history.Event{{startEvent(), {Type: history.SignalReceived, Name: "s"}}}
		if err := liveThenReplay(t, wf, tasks); err != nil {
			t.Errorf("%s: replay of a deterministic workflow failed: %v", name, err)
		}
	}
}

// GW-11: a replay that ends earlier than the recorded run is
// non-determinism.
func TestGW11_EndingEarlierOnReplayIsDetected(t *testing.T) {
	early := false
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		_, _ = st.SignalChannel("s").Receive(ctx)
		if early {
			return []byte("early"), nil
		}
		_, _ = st.SignalChannel("s").Receive(ctx)
		return nil, nil
	}}
	ex := executor.New(wf, 1, executor.Options{})
	defer ex.Close()
	var hist []history.Event
	for i, evs := range [][]history.Event{{startEvent()}, {{Type: history.SignalReceived, Name: "s"}}} {
		task := &store.WorkflowTask{LastSeq: ex.LastSeq(), Now: time.Unix(int64(i), 0)}
		for j, ev := range evs {
			task.Pending = append(task.Pending, store.PendingEvent{ID: int64(j + 1), Event: ev})
		}
		c, err := ex.Execute(task)
		if err != nil {
			t.Fatal(err)
		}
		hist = append(hist, c.History...)
	}
	early = true
	r := executor.New(wf, 1, executor.Options{})
	defer r.Close()
	err := r.Replay(hist)
	if err == nil {
		_, err = r.Execute(&store.WorkflowTask{
			LastSeq: r.LastSeq(), Now: time.Unix(5, 0),
			Pending: []store.PendingEvent{{ID: 9, Event: history.Event{Type: history.SignalReceived, Name: "s"}}},
		})
	}
	if !errors.Is(err, executor.ErrNonDeterministic) {
		t.Fatalf("ending early on replay: %v", err)
	}
}

// After its end is recorded, an executor reports no status again (a store
// must not end the execution twice), and after an error it is unusable.
func TestExecuteAfterEndAndAfterError(t *testing.T) {
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(coro.Context, []byte) ([]byte, error) { return []byte("ok"), nil }}
	ex := executor.New(wf, 1, executor.Options{})
	defer ex.Close()
	c, err := ex.Execute(&store.WorkflowTask{Pending: []store.PendingEvent{{ID: 1, Event: startEvent()}}})
	if err != nil || c.Status != store.StatusCompleted {
		t.Fatalf("first: %v %v", c.Status, err)
	}
	c, err = ex.Execute(&store.WorkflowTask{
		LastSeq: ex.LastSeq(),
		Pending: []store.PendingEvent{{ID: 2, Event: history.Event{Type: history.SignalReceived, Name: "s"}}},
	})
	if err != nil || c.Status != "" || len(c.History) != 0 || len(c.Consumed) != 1 {
		t.Fatalf("after the end: status %q history %d consumed %v err %v", c.Status, len(c.History), c.Consumed, err)
	}
	if _, err := ex.Execute(&store.WorkflowTask{LastSeq: 99}); err == nil {
		t.Fatal("task at the wrong seq accepted")
	}
	if _, err := ex.Execute(&store.WorkflowTask{LastSeq: ex.LastSeq()}); err == nil {
		t.Fatal("an executor that failed must stay unusable")
	}
}

// Canceling a timer and an activity, then a cold replay: no false
// non-determinism.
func TestCancelReplaysCold(t *testing.T) {
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		tctx, cancelTimer := coro.WithCancel(ctx)
		timer := st.NewTimer(tctx, time.Hour, "t")
		actx, cancelAct := coro.WithCancel(ctx)
		act := st.ScheduleActivity(actx, "A", "q", nil)
		_, _ = st.SignalChannel("s").Receive(ctx)
		cancelTimer()
		cancelAct()
		_, terr := timer.Get(ctx)
		r, _ := act.Get(ctx)
		if !errors.Is(terr, executor.ErrCanceled) || !errors.Is(r.Err, executor.ErrCanceled) {
			return nil, errors.New("cancels not observed")
		}
		_, err := st.NewTimer(ctx, time.Minute, "after").Get(ctx)
		return nil, err
	}}
	tasks := [][]history.Event{
		{startEvent()},
		{{Type: history.SignalReceived, Name: "s"}},
		{{Type: history.ActivityCompleted, ScheduleID: 2}}, // the late result
	}
	if err := liveThenReplay(t, wf, tasks); err != nil {
		t.Fatalf("cold replay after cancels: %v", err)
	}
}

// A replay split inside a task (history loaded in pages) keeps side effects
// in order.
func TestReplaySplitInsideATask(t *testing.T) {
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		coro.Go(ctx, func(ctx coro.Context) {
			_, _ = st.SideEffect(ctx, func() []byte { return []byte("x") })
			st.ScheduleActivity(ctx, "A", "q", nil)
		})
		coro.Go(ctx, func(ctx coro.Context) { st.ScheduleActivity(ctx, "B", "q", nil) })
		_, err := st.NewTimer(ctx, time.Hour, "t").Get(ctx)
		return nil, err
	}}
	ex := executor.New(wf, 1, executor.Options{})
	defer ex.Close()
	c, err := ex.Execute(&store.WorkflowTask{Now: time.Unix(1, 0), Pending: []store.PendingEvent{{ID: 1, Event: startEvent()}}})
	if err != nil {
		t.Fatal(err)
	}
	for split := 1; split < len(c.History); split++ {
		r := executor.New(wf, 1, executor.Options{})
		if err := r.Replay(c.History[:split]); err != nil {
			t.Fatalf("split %d, first part: %v", split, err)
		}
		if err := r.Replay(c.History[split:]); err != nil {
			t.Fatalf("split %d, second part: %v", split, err)
		}
		r.Close()
	}
}

// MayCompensate counts live and replayed activities: running ones, and
// outcomes with compensation data.
func TestMayCompensate(t *testing.T) {
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		if _, err := st.ScheduleActivity(ctx, "A", "q", nil).Get(ctx); err != nil {
			return nil, err
		}
		_, err := st.ScheduleActivity(ctx, "B", "q", nil).Get(ctx)
		return nil, err
	}}
	ex := executor.New(wf, 1, executor.Options{})
	defer ex.Close()
	var hist []history.Event
	run := func(evs ...history.Event) {
		t.Helper()
		task := &store.WorkflowTask{LastSeq: ex.LastSeq(), Now: time.Unix(1000, 0)}
		for j, ev := range evs {
			task.Pending = append(task.Pending, store.PendingEvent{ID: int64(j + 1), Event: ev})
		}
		c, err := ex.Execute(task)
		if err != nil {
			t.Fatal(err)
		}
		hist = append(hist, c.History...)
	}
	run(startEvent())
	if !ex.MayCompensate() {
		t.Fatal("A is running")
	}
	run(history.Event{Type: history.ActivityCompleted, ScheduleID: 1})
	if !ex.MayCompensate() {
		t.Fatal("B is running")
	}
	run(history.Event{Type: history.ActivityFailed, ScheduleID: 2, Err: "x"})
	if ex.MayCompensate() {
		t.Fatal("nothing to compensate")
	}
	r := executor.New(wf, 1, executor.Options{})
	defer r.Close()
	for i := range hist {
		if hist[i].Type == history.ActivityCompleted {
			hist[i].CompData = []byte("{}") // A's outcome, with data
		}
	}
	if err := r.Replay(hist); err != nil {
		t.Fatal(err)
	}
	if !r.MayCompensate() {
		t.Fatal("replayed outcome with data")
	}
}

// Lock commands replay deterministically: answers, a held lock's error,
// and a lost lock failing the workflow; the completion carries the request.
func TestLockReplay(t *testing.T) {
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		if err := st.Lock(ctx, "a", time.Second, 0); err != nil {
			return nil, err
		}
		err := st.Lock(ctx, "b", time.Second, 0)
		var held *executor.LockHeldError
		if !errors.As(err, &held) || held.Holder != "other" || !errors.Is(err, executor.ErrLockHeld) {
			return nil, errors.New("unexpected answer")
		}
		_, err = st.NewTimer(ctx, time.Hour, "t").Get(ctx)
		return nil, err
	}}
	ex := executor.New(wf, 1, executor.Options{})
	defer ex.Close()
	c, err := ex.Execute(&store.WorkflowTask{Pending: []store.PendingEvent{{ID: 1, Event: startEvent()}}, Now: time.Unix(1000, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Locks) != 1 || c.Locks[0].Key != "a" || c.Locks[0].TTL != time.Second || c.Locks[0].ScheduleID != 1 {
		t.Fatalf("locks %+v", c.Locks)
	}
	tasks := [][]history.Event{
		{startEvent()},
		{{Type: history.LockAcquired, ScheduleID: 1}},
		{{Type: history.LockHeld, ScheduleID: 2, Err: "other"}},
		{{Type: history.LockLost, ScheduleID: 1, Name: "a"}},
	}
	if err := liveThenReplay(t, wf, tasks); err != nil {
		t.Fatalf("replay: %v", err)
	}
	// The lost lock ended the workflow.
	ex2 := executor.New(wf, 1, executor.Options{})
	defer ex2.Close()
	var last *store.Completion
	for i, evs := range tasks {
		task := &store.WorkflowTask{LastSeq: ex2.LastSeq(), Now: time.Unix(1000+int64(i), 0)}
		for j, ev := range evs {
			task.Pending = append(task.Pending, store.PendingEvent{ID: int64(j + 1), Event: ev})
		}
		if last, err = ex2.Execute(task); err != nil {
			t.Fatal(err)
		}
	}
	if last.Status != store.StatusFailed || !strings.Contains(last.Err, "lock lost") {
		t.Fatalf("after LockLost: %s %q", last.Status, last.Err)
	}
}

// GW-3 (T6.2): continue-as-new carries the signals delivered but not read,
// in arrival order across names, including those delivered in the same task
// after the workflow decided to continue.
func TestContinueAsNewCarriesUnreadSignals(t *testing.T) {
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		ch := st.SignalChannel("n")
		for range 2 {
			ch.Receive(ctx)
		}
		return nil, &executor.ContinueAsNewError{Input: []byte("x")}
	}}
	ex := executor.New(wf, 1, executor.Options{})
	defer ex.Close()
	task := &store.WorkflowTask{Now: time.Unix(1, 0), Pending: []store.PendingEvent{{ID: 1, Event: startEvent()}}}
	for i, name := range []string{"n", "m", "n", "n", "m"} {
		task.Pending = append(task.Pending, store.PendingEvent{ID: int64(i + 2), Event: history.Event{
			Type: history.SignalReceived, Name: name, Payload: []byte{byte('0' + i)},
		}})
	}
	c, err := ex.Execute(task)
	if err != nil || c.Status != store.StatusContinued {
		t.Fatalf("%v %s", err, c.Status)
	}
	var got []string
	for _, ev := range c.Carry {
		got = append(got, ev.Name+string(ev.Payload))
	}
	// n0 and n2 were read; m1, n3, m4 were not.
	if strings.Join(got, ",") != "m1,n3,m4" {
		t.Fatalf("carried %v", got)
	}
}

// GW-3 (review of T6.2): a cancel request the workflow did not act on
// before continuing as new goes on with the unread signals.
func TestContinueAsNewCarriesCancel(t *testing.T) {
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		executor.FromContext(ctx).SignalChannel("n").Receive(ctx)
		return nil, &executor.ContinueAsNewError{Input: []byte("x")}
	}}
	ex := executor.New(wf, 1, executor.Options{})
	defer ex.Close()
	c, err := ex.Execute(&store.WorkflowTask{Now: time.Unix(1, 0), Pending: []store.PendingEvent{
		{ID: 1, Event: startEvent()},
		{ID: 2, Event: history.Event{Type: history.SignalReceived, Name: "n", Payload: []byte("0")}},
		{ID: 3, Event: history.Event{Type: history.SignalReceived, Name: "n", Payload: []byte("1")}},
		{ID: 4, Event: history.Event{Type: history.ExecutionCancelRequested}},
	}})
	if err != nil || c.Status != store.StatusContinued {
		t.Fatalf("%v %s", err, c.Status)
	}
	if len(c.Carry) != 2 || c.Carry[0].Type != history.SignalReceived || string(c.Carry[0].Payload) != "1" ||
		c.Carry[1].Type != history.ExecutionCancelRequested {
		t.Fatalf("carry %+v", c.Carry)
	}
}

// The carry is the same when the executor was rebuilt from history (cold
// replay): the delivery log is rebuilt by replaying the signals.
func TestContinueAsNewCarryAfterReplay(t *testing.T) {
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		st.SignalChannel("go").Receive(ctx)
		st.SignalChannel("n").Receive(ctx)
		return nil, &executor.ContinueAsNewError{Input: []byte("x")}
	}}
	live := executor.New(wf, 1, executor.Options{})
	defer live.Close()
	first := &store.WorkflowTask{Now: time.Unix(1, 0), Pending: []store.PendingEvent{{ID: 1, Event: startEvent()}}}
	for i := range 3 {
		first.Pending = append(first.Pending, store.PendingEvent{ID: int64(i + 2), Event: history.Event{
			Type: history.SignalReceived, Name: "n", Payload: []byte{byte('0' + i)},
		}})
	}
	c, err := live.Execute(first)
	if err != nil || c.Status != "" {
		t.Fatalf("%v %s", err, c.Status)
	}
	cold := executor.New(wf, 1, executor.Options{})
	defer cold.Close()
	if err := cold.Replay(c.History); err != nil {
		t.Fatal(err)
	}
	c, err = cold.Execute(&store.WorkflowTask{LastSeq: cold.LastSeq(), Now: time.Unix(2, 0), Pending: []store.PendingEvent{
		{ID: 9, Event: history.Event{Type: history.SignalReceived, Name: "go"}},
	}})
	if err != nil || c.Status != store.StatusContinued {
		t.Fatalf("%v %s", err, c.Status)
	}
	var got []string
	for _, ev := range c.Carry {
		got = append(got, ev.Name+string(ev.Payload))
	}
	if strings.Join(got, ",") != "n1,n2" {
		t.Fatalf("carried %v", got)
	}
}

// The delivery log is compacted while one name stays unread (review of
// T6.2): the carry is still right after many tasks.
func TestCarryAfterSignalLogCompaction(t *testing.T) {
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		for range 100 {
			st.SignalChannel("n").Receive(ctx)
		}
		return nil, &executor.ContinueAsNewError{Input: []byte("x")}
	}}
	ex := executor.New(wf, 1, executor.Options{})
	defer ex.Close()
	task := &store.WorkflowTask{Now: time.Unix(1, 0), Pending: []store.PendingEvent{
		{ID: 1, Event: startEvent()},
		{ID: 2, Event: history.Event{Type: history.SignalReceived, Name: "debug", Payload: []byte("d")}},
	}}
	var c *store.Completion
	for i := range 100 {
		task.Pending = append(task.Pending, store.PendingEvent{ID: int64(i + 10), Event: history.Event{
			Type: history.SignalReceived, Name: "n", Payload: []byte(strconv.Itoa(i)),
		}})
		var err error
		if c, err = ex.Execute(task); err != nil {
			t.Fatal(err)
		}
		task = &store.WorkflowTask{LastSeq: ex.LastSeq(), Now: time.Unix(int64(i+2), 0)}
	}
	if c.Status != store.StatusContinued || len(c.Carry) != 1 || c.Carry[0].Name != "debug" {
		t.Fatalf("%s carry %+v", c.Status, c.Carry)
	}
}
