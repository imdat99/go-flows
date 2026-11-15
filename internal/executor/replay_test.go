package executor_test

import (
	"errors"
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
