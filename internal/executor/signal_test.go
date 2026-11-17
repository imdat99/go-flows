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

// signalWF sends "go" to b (awaited), then to "missing" (awaited, must
// fail with the store's not-found kind), then waits for a timer.
func signalWF(target string, payload []byte) executor.Workflow {
	return executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		if r, err := st.SignalWorkflow(target, "go", payload).Get(ctx); err != nil || r.Err != nil {
			return nil, errors.Join(err, r.Err)
		}
		r, _ := st.SignalWorkflow("missing", "go", nil).Get(ctx)
		re, ok := errors.AsType[*executor.RecordedError](r.Err)
		if !ok || !re.HasKind(store.KindNotFound) {
			return nil, errors.New("unexpected answer")
		}
		_, err := st.NewTimer(ctx, time.Hour, "t").Get(ctx)
		return nil, err
	}}
}

var signalTasks = [][]history.Event{
	{startEvent()},
	{{Type: history.SignalDelivered, ScheduleID: 1, Ref: "b"}},
	{{Type: history.SignalFailed, ScheduleID: 2, Ref: "missing", Err: store.ErrNotFound.Error(), ErrKinds: store.KindNotFound}},
}

// T12.1: a signal to another workflow is a command: recorded (SignalSent),
// carried by the completion, resolved by its answer, replayed from the
// history without being sent again.
func TestSignalWorkflowReplay(t *testing.T) {
	wf := signalWF("b", []byte("1"))
	ex := executor.New(wf, 1, executor.Options{})
	defer ex.Close()
	c, err := ex.Execute(&store.WorkflowTask{Pending: []store.PendingEvent{{ID: 1, Event: signalTasks[0][0]}}, Now: time.Unix(1000, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Signals) != 1 || c.Signals[0].ScheduleID != 1 || c.Signals[0].InstanceID != "b" || c.Signals[0].Name != "go" ||
		string(c.Signals[0].Payload) != "1" {
		t.Fatalf("signals %+v", c.Signals)
	}
	if ev := c.History[len(c.History)-1]; ev.Type != history.SignalSent || ev.Ref != "b" || ev.Name != "go" ||
		ev.ScheduleID != 1 || ev.ArgHash != history.HashArgs([]byte("1")) {
		t.Fatalf("event %+v", ev)
	}
	if err := liveThenReplay(t, wf, signalTasks); err != nil {
		t.Fatalf("replay: %v", err)
	}

	// Replayed through both signals (a cold executor: cache eviction), the
	// next task sends nothing: they are in the history, never sent again.
	live := executor.New(wf, 1, executor.Options{})
	defer live.Close()
	var hist []history.Event
	for i, evs := range signalTasks[:2] {
		task := &store.WorkflowTask{LastSeq: live.LastSeq(), Now: time.Unix(1000+int64(i), 0)}
		for j, ev := range evs {
			task.Pending = append(task.Pending, store.PendingEvent{ID: int64(j + 1), Event: ev})
		}
		c, err := live.Execute(task)
		if err != nil {
			t.Fatal(err)
		}
		hist = append(hist, c.History...)
	}
	r2 := executor.New(wf, 1, executor.Options{})
	defer r2.Close()
	if err := r2.Replay(hist); err != nil {
		t.Fatal(err)
	}
	c, err = r2.Execute(&store.WorkflowTask{
		LastSeq: r2.LastSeq(), Now: time.Unix(1003, 0),
		Pending: []store.PendingEvent{{ID: 9, Event: signalTasks[2][0]}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Signals) != 0 || c.Status != "" {
		t.Fatalf("after replay: signals %+v, status %q", c.Signals, c.Status)
	}
}

// T12.1: replaying a signal with another target, name or payload than
// recorded is non-determinism.
func TestSignalWorkflowMismatch(t *testing.T) {
	rec := executor.New(signalWF("b", []byte("1")), 1, executor.Options{})
	defer rec.Close()
	c, err := rec.Execute(&store.WorkflowTask{Pending: []store.PendingEvent{{ID: 1, Event: startEvent()}}, Now: time.Unix(1000, 0)})
	if err != nil {
		t.Fatal(err)
	}
	hist := append([]history.Event(nil), c.History...)
	hist = append(hist,
		history.Event{Seq: int64(len(hist) + 1), Type: history.TaskStarted, At: time.Unix(1001, 0)},
		history.Event{Seq: int64(len(hist) + 2), Type: history.SignalDelivered, ScheduleID: 1, Ref: "b"})
	for name, wf := range map[string]executor.Workflow{
		"target":  signalWF("c", []byte("1")),
		"payload": signalWF("b", []byte("2")),
		"name": {Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
			st := executor.FromContext(ctx)
			r, err := st.SignalWorkflow("b", "other", []byte("1")).Get(ctx)
			return nil, errors.Join(err, r.Err)
		}},
		"kind": {Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
			st := executor.FromContext(ctx)
			_, err := st.NewTimer(ctx, time.Second, "go").Get(ctx)
			return nil, err
		}},
	} {
		r := executor.New(wf, 1, executor.Options{})
		if err := r.Replay(hist); !errors.Is(err, executor.ErrNonDeterministic) {
			t.Errorf("%s: replay error %v, want non-determinism", name, err)
		}
		r.Close()
	}
	// A recorded signal the replay does not send is non-determinism too.
	none := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		_, err := executor.FromContext(ctx).NewTimer(ctx, time.Hour, "t").Get(ctx)
		return nil, err
	}}
	r := executor.New(none, 1, executor.Options{})
	defer r.Close()
	if err := r.Replay(hist); !errors.Is(err, executor.ErrNonDeterministic) {
		t.Errorf("missing signal: replay error %v, want non-determinism", err)
	}
}

// T12.1: a signal sent in the task that ends the workflow, without
// awaiting it, is still sent (unlike an activity or a child, it starts
// nothing that would be left behind).
func TestSignalWorkflowAtTheEnd(t *testing.T) {
	for name, ret := range map[string]error{"completed": nil, "continued": &executor.ContinueAsNewError{Input: []byte("x")}} {
		wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
			executor.FromContext(ctx).SignalWorkflow("b", "done", []byte("7"))
			return nil, ret
		}}
		ex := executor.New(wf, 1, executor.Options{})
		c, err := ex.Execute(&store.WorkflowTask{Pending: []store.PendingEvent{{ID: 1, Event: startEvent()}}, Now: time.Unix(1000, 0)})
		ex.Close()
		if err != nil {
			t.Fatal(err)
		}
		if c.Status.Active() || len(c.Signals) != 1 || c.Signals[0].InstanceID != "b" || !store.ValidShape(c) {
			t.Fatalf("%s: status %q, signals %+v, valid %v", name, c.Status, c.Signals, store.ValidShape(c))
		}
		n := len(c.History)
		if c.History[n-2].Type != history.SignalSent || !c.History[n-1].Type.IsTerminal() {
			t.Fatalf("%s: history %+v", name, c.History)
		}
		if a, ch := ex.DroppedAtEnd(); a != 0 || ch != 0 {
			t.Fatalf("%s: dropped %d %d", name, a, ch)
		}
	}
}

// T12.1 with T12.2: a signal's future is not a child's: RequestCancelChild
// refuses it, and the signal is sent and answered as usual.
func TestSignalWorkflowNotAChild(t *testing.T) {
	var reqErr error
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		f := st.SignalWorkflow("b", "go", nil)
		reqErr = st.RequestCancelChild(f)
		r, err := f.Get(ctx)
		return nil, errors.Join(err, r.Err)
	}}
	ex := executor.New(wf, 1, executor.Options{})
	defer ex.Close()
	c, err := ex.Execute(&store.WorkflowTask{Pending: []store.PendingEvent{{ID: 1, Event: startEvent()}}, Now: time.Unix(1000, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if reqErr == nil || len(c.Signals) != 1 || len(c.ChildCancels) != 0 {
		t.Fatalf("request %v, signals %+v, child cancels %v", reqErr, c.Signals, c.ChildCancels)
	}
	c, err = ex.Execute(&store.WorkflowTask{
		LastSeq: ex.LastSeq(), Now: time.Unix(1001, 0),
		Pending: []store.PendingEvent{{ID: 2, Event: history.Event{Type: history.SignalDelivered, ScheduleID: 1, Ref: "b"}}},
	})
	if err != nil || c.Status != store.StatusCompleted {
		t.Fatalf("after the answer: %v %q %q", err, c.Status, c.Err)
	}
}
