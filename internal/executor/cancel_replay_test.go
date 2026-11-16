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

// replayThenTask replays hist on a fresh executor, then runs one more task
// delivering ev: the error of either.
func replayThenTask(wf executor.Workflow, hist []history.Event, ev history.Event) error {
	r := executor.New(wf, 1, executor.Options{})
	defer r.Close()
	if err := r.Replay(hist); err != nil {
		return err
	}
	_, err := r.Execute(&store.WorkflowTask{
		LastSeq: r.LastSeq(), Now: time.Unix(2000, 0),
		Pending: []store.PendingEvent{{ID: 1, Event: ev}},
	})
	return err
}

// liveHistory runs tasks live and returns the history they wrote.
func liveHistory(t *testing.T, wf executor.Workflow, tasks [][]history.Event) []history.Event {
	t.Helper()
	ex := executor.New(wf, 1, executor.Options{})
	defer ex.Close()
	var hist []history.Event
	for i, evs := range tasks {
		task := &store.WorkflowTask{LastSeq: ex.LastSeq(), Now: time.Unix(int64(1000+i), 0)}
		for j, ev := range evs {
			task.Pending = append(task.Pending, store.PendingEvent{ID: int64(j + 1), Event: ev})
		}
		c, err := ex.Execute(task)
		if err != nil {
			t.Fatalf("live task %d: %v", i, err)
		}
		hist = append(hist, c.History...)
	}
	return hist
}

// GW-11 (T8.4 review): a replay that cancels an activity the recorded run
// did not cancel diverged (it sees ErrCanceled where the run got a
// result): it is non-determinism, whether the activity's outcome is in the
// history or not.
func TestGW11_CancelNotInHistoryIsNonDeterministic(t *testing.T) {
	for _, withOutcome := range []bool{true, false} {
		cancelIt := false
		wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
			st := executor.FromContext(ctx)
			actx, cancel := coro.WithCancel(ctx)
			f := st.ScheduleActivity(actx, "A", "q", nil)
			_, _ = st.SignalChannel("s").Receive(ctx)
			if cancelIt {
				cancel()
			}
			if withOutcome {
				_, _ = f.Get(ctx)
			}
			_, _ = st.SignalChannel("t").Receive(ctx)
			return nil, nil
		}}
		tasks := [][]history.Event{{startEvent()}, {{Type: history.SignalReceived, Name: "s"}}}
		if withOutcome {
			tasks = append(tasks, []history.Event{{Type: history.ActivityCompleted, ScheduleID: 1, Payload: []byte("ok")}})
		}
		hist := liveHistory(t, wf, tasks)
		cancelIt = true
		err := replayThenTask(wf, hist, history.Event{Type: history.SignalReceived, Name: "t"})
		if !errors.Is(err, executor.ErrNonDeterministic) {
			t.Errorf("outcome recorded %v: a cancel missing from the history was not detected: %v", withOutcome, err)
		}
	}
}

// Same for a timer.
func TestGW11_TimerCancelNotInHistoryIsNonDeterministic(t *testing.T) {
	cancelIt := false
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		tctx, cancel := coro.WithCancel(ctx)
		st.NewTimer(tctx, time.Hour, "x")
		_, _ = st.SignalChannel("s").Receive(ctx)
		if cancelIt {
			cancel()
		}
		_, _ = st.SignalChannel("t").Receive(ctx)
		return nil, nil
	}}
	hist := liveHistory(t, wf, [][]history.Event{{startEvent()}, {{Type: history.SignalReceived, Name: "s"}}})
	cancelIt = true
	if err := replayThenTask(wf, hist, history.Event{Type: history.SignalReceived, Name: "t"}); !errors.Is(err, executor.ErrNonDeterministic) {
		t.Fatalf("not detected: %v", err)
	}
}

// A cancel the recorded run did make replays without error (the check
// does not reject genuine cancels, before or after the outcome).
func TestRecordedCancelReplays(t *testing.T) {
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		actx, cancel := coro.WithCancel(ctx)
		f := st.ScheduleActivity(actx, "A", "q", nil)
		tctx, cancelT := coro.WithCancel(ctx)
		st.NewTimer(tctx, time.Hour, "x")
		_, _ = st.SignalChannel("s").Receive(ctx)
		cancel()
		cancelT()
		_, _ = f.Get(ctx)
		_, _ = st.SignalChannel("t").Receive(ctx)
		return nil, nil
	}}
	hist := liveHistory(t, wf, [][]history.Event{
		{startEvent()},
		{{Type: history.SignalReceived, Name: "s"}},
		{{Type: history.ActivityFailed, ScheduleID: 1, Err: "canceled"}},
	})
	if err := replayThenTask(wf, hist, history.Event{Type: history.SignalReceived, Name: "t"}); err != nil {
		t.Fatal(err)
	}
}

// T8.4 review: an event type this binary does not know (a newer binary
// wrote it) is ErrUnknownEvent, for a hand-back, not non-determinism.
func TestUnknownEventTypeIsNotNonDeterminism(t *testing.T) {
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		_, _ = executor.FromContext(ctx).SignalChannel("s").Receive(ctx)
		return nil, nil
	}}
	hist := liveHistory(t, wf, [][]history.Event{{startEvent()}})
	hist = append(hist,
		history.Event{Seq: int64(len(hist) + 1), Type: history.Type(200)},
		history.Event{Seq: int64(len(hist) + 2), Type: history.TaskStarted, At: time.Unix(1005, 0)})
	r := executor.New(wf, 1, executor.Options{})
	defer r.Close()
	err := r.Replay(hist)
	if !errors.Is(err, executor.ErrUnknownEvent) || errors.Is(err, executor.ErrNonDeterministic) {
		t.Fatalf("got %v", err)
	}
}
