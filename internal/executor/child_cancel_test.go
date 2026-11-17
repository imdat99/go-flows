package executor_test

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// cancelChildWF starts child "k" and, per its input, requests its cancel:
// "late" after signal "s" (twice when "dup"), "early" in the task that
// starts it, "none" never; then it returns the child's outcome ("out:" or
// "err:" and the error's kinds).
func cancelChildWF(requests *int) executor.Workflow {
	return executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, in []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		f := st.StartChild(ctx, "k", "kid", 1, q, nil)
		mode := string(in)
		request := func() {
			if err := st.RequestCancelChild(f); err != nil {
				panic(err)
			}
			if requests != nil {
				*requests++
			}
		}
		if mode == "early" {
			request()
		} else {
			_, _ = st.SignalChannel("s").Receive(ctx)
		}
		if mode == "late" || mode == "dup" {
			request()
			if mode == "dup" {
				request()
			}
		}
		r, _ := f.Get(ctx) // the outcome is in r, not in the error
		if r.Err != nil {
			if re, ok := errors.AsType[*executor.RecordedError](r.Err); ok {
				return []byte("err:" + strings.Join(re.Kinds, ",")), nil
			}
			return []byte("err:" + r.Err.Error()), nil
		}
		return []byte("out:" + string(r.Output)), nil
	}}
}

func startWith(in string) history.Event {
	ev := startEvent()
	ev.Payload = []byte(in)
	return ev
}

// runTasks executes tasks live on one executor and returns the completions.
func runTasks(t *testing.T, ex *executor.Executor, tasks [][]history.Event) []*store.Completion {
	t.Helper()
	var out []*store.Completion
	for i, evs := range tasks {
		task := &store.WorkflowTask{LastSeq: ex.LastSeq(), Now: time.Unix(int64(1000+i), 0)}
		for j, ev := range evs {
			task.Pending = append(task.Pending, store.PendingEvent{ID: int64(j + 1), Event: ev})
		}
		c, err := ex.Execute(task)
		if err != nil {
			t.Fatalf("task %d: %v", i, err)
		}
		cp := *c
		cp.History = slices.Clone(c.History)
		cp.ChildCancels = slices.Clone(c.ChildCancels)
		cp.Children = slices.Clone(c.Children)
		out = append(out, &cp)
	}
	return out
}

func countType(hist []history.Event, ty history.Type) int {
	n := 0
	for i := range hist {
		if hist[i].Type == ty {
			n++
		}
	}
	return n
}

// T12.2: a cancel request reaches the child (ChildCancelRequested, in the
// same completion as ChildCancels) but does not resolve the future: the
// child's real outcome does, completed or canceled.
func TestRequestCancelChildWaitsForTheOutcome(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome history.Event
		want    string
	}{
		{"completes anyway", history.Event{Type: history.ChildCompleted, ScheduleID: 1, Payload: []byte("done")}, "out:done"},
		{"honours the cancel", history.Event{Type: history.ChildFailed, ScheduleID: 1, Err: "child workflow canceled", ErrKinds: "canceled"}, "err:canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex := executor.New(cancelChildWF(nil), 1, executor.Options{})
			defer ex.Close()
			cs := runTasks(t, ex, [][]history.Event{{startWith("late")}, {{Type: history.SignalReceived, Name: "s"}}})
			if len(cs[0].Children) != 1 {
				t.Fatalf("children %+v", cs[0].Children)
			}
			req := cs[1]
			if !slices.Equal(req.ChildCancels, []string{"k"}) || countType(req.History, history.ChildCancelRequested) != 1 {
				t.Fatalf("request task: cancels %v history %v", req.ChildCancels, types(req.History))
			}
			if req.Status != "" {
				t.Fatalf("the request resolved the future: the workflow ended %s (%s)", req.Status, req.Output)
			}
			end := runTasks(t, ex, [][]history.Event{{tc.outcome}})[0]
			if end.Status != store.StatusCompleted || string(end.Output) != tc.want {
				t.Fatalf("end %s %q, want %q", end.Status, end.Output, tc.want)
			}
		})
	}
}

// T12.2: a request in the task that starts the child drops the child (it is
// never created) and resolves the future with ErrCanceled.
func TestRequestCancelChildBeforeItStarted(t *testing.T) {
	ex := executor.New(cancelChildWF(nil), 1, executor.Options{})
	defer ex.Close()
	c := runTasks(t, ex, [][]history.Event{{startWith("early")}})[0]
	if len(c.Children) != 0 || len(c.ChildCancels) != 0 || countType(c.History, history.ChildScheduled) != 0 {
		t.Fatalf("children %v cancels %v history %v", c.Children, c.ChildCancels, types(c.History))
	}
	if c.Status != store.StatusCompleted || string(c.Output) != "err:"+executor.ErrCanceled.Error() {
		t.Fatalf("end %s %q", c.Status, c.Output)
	}
}

// T12.2: requesting twice records one request.
func TestRequestCancelChildTwice(t *testing.T) {
	n := 0
	ex := executor.New(cancelChildWF(&n), 1, executor.Options{})
	defer ex.Close()
	cs := runTasks(t, ex, [][]history.Event{{startWith("dup")}, {{Type: history.SignalReceived, Name: "s"}}})
	if n != 2 || !slices.Equal(cs[1].ChildCancels, []string{"k"}) || countType(cs[1].History, history.ChildCancelRequested) != 1 {
		t.Fatalf("requests %d: cancels %v history %v", n, cs[1].ChildCancels, types(cs[1].History))
	}
	end := runTasks(t, ex, [][]history.Event{{{Type: history.ChildCompleted, ScheduleID: 1, Payload: []byte("x")}}})[0]
	if string(end.Output) != "out:x" {
		t.Fatalf("end %s %q", end.Status, end.Output)
	}
}

// T12.2: the parent canceled while it waits for a child it asked to cancel:
// the child's future resolves with ErrCanceled, as with WithCancel.
func TestRequestCancelChildThenParentCanceled(t *testing.T) {
	ex := executor.New(cancelChildWF(nil), 1, executor.Options{})
	defer ex.Close()
	cs := runTasks(t, ex, [][]history.Event{
		{startWith("late")},
		{{Type: history.SignalReceived, Name: "s"}},
		{{Type: history.ExecutionCancelRequested}},
	})
	end := cs[2]
	if end.Status != store.StatusCompleted || !strings.Contains(string(end.Output), executor.ErrCanceled.Error()) {
		t.Fatalf("end %s %q", end.Status, end.Output)
	}
}

// T12.2: a cancel request on an activity's future is refused.
func TestRequestCancelChildOfActivityIsRefused(t *testing.T) {
	var got error
	wf := executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		got = st.RequestCancelChild(st.ScheduleActivity(ctx, "A", q, nil))
		return nil, nil
	}}
	ex := executor.New(wf, 1, executor.Options{})
	defer ex.Close()
	runTasks(t, ex, [][]history.Event{{startEvent()}})
	if got == nil {
		t.Fatal("no error")
	}
}

// T12.2: a recorded request replays; one the replay does not make, or makes
// where the history has none, is non-determinism (parks the execution).
func TestRequestCancelChildReplay(t *testing.T) {
	live := func(mode string, outcome bool) []history.Event {
		tasks := [][]history.Event{{startWith(mode)}, {{Type: history.SignalReceived, Name: "s"}}}
		if outcome {
			tasks = append(tasks, []history.Event{{Type: history.ChildCompleted, ScheduleID: 1, Payload: []byte("x")}})
		}
		return liveHistory(t, cancelChildWF(nil), tasks)
	}
	replay := func(mode string, hist []history.Event) error {
		r := executor.New(cancelChildWF(nil), 1, executor.Options{})
		defer r.Close()
		for i := range hist {
			if hist[i].Type == history.ExecutionStarted {
				hist[i].Payload = []byte(mode)
			}
		}
		if err := r.Replay(hist); err != nil {
			return err
		}
		_, err := r.Execute(&store.WorkflowTask{
			LastSeq: r.LastSeq(), Now: time.Unix(2000, 0),
			Pending: []store.PendingEvent{{ID: 1, Event: history.Event{Type: history.SignalReceived, Name: "t"}}},
		})
		return err
	}
	for _, outcome := range []bool{false, true} {
		if err := replay("late", live("late", outcome)); err != nil {
			t.Errorf("outcome %v: recorded request: %v", outcome, err)
		}
		if err := replay("none", live("late", outcome)); !errors.Is(err, executor.ErrNonDeterministic) {
			t.Errorf("outcome %v: request in the history, not on replay: %v", outcome, err)
		}
	}
	if err := replay("late", live("none", false)); !errors.Is(err, executor.ErrNonDeterministic) {
		t.Errorf("request on replay, not in the history: %v", err)
	}
}
