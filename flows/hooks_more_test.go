package flows

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
)

// startedEngine starts an engine on st, stopped when the test ends.
func startedEngine(t *testing.T, st store.Store, opts Options) *Engine {
	t.Helper()
	if opts.PollMax == 0 {
		opts.PollMax = 20 * time.Millisecond
	}
	e, err := newEngine(st, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})
	return e
}

// runsOf returns the entries of list for instance id.
func (r *recorder) runsOf(list func(*recorder) []RunInfo, id string) []RunInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []RunInfo
	for _, run := range list(r) {
		if run.InstanceID == id {
			out = append(out, run)
		}
	}
	return out
}

// historyEvents returns the history of id's latest execution.
func historyEvents(t *testing.T, e *Engine, id string) []history.Event {
	t.Helper()
	ex, err := e.st.GetExecution(bg, id)
	if err != nil {
		t.Fatal(err)
	}
	hist, err := e.st.LoadHistory(bg, ex.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	return hist
}

var (
	// lateSignal, set by the test, runs once when lateWF is about to end
	// (live): a signal arriving after the task was claimed.
	lateSignal atomic.Pointer[func()]
	lateWF     = DefineWorkflow("test.late-signal", 1, func(c Ctx, _ int) (int, error) {
		v, err := Await[int](c, "go", time.Hour)
		if err != nil {
			return 0, err
		}
		if f := lateSignal.Swap(nil); f != nil {
			(*f)()
		}
		return v, nil
	})
)

// T10.14: an end refused because a signal arrived meanwhile
// (store.ErrPendingEvents) is not an end for the run hook nor the
// observer: the task runs again with the signal, and only that end is
// hooked.
func TestRunHookWithPendingEvents(t *testing.T) {
	r := &recorder{}
	e := startedEngine(t, mem.New(nil), Options{InstanceID: "hook-pending", Hook: r, Observer: obs{r}})
	const id = "late-signal"
	send := func() {
		if err := e.Signal(bg, id, "late", 2); err != nil {
			t.Error(err)
		}
	}
	lateSignal.Store(&send)
	t.Cleanup(func() { lateSignal.Store(nil) })
	h, err := lateWF.Start(bg, e, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return historyHas(e, id, "TimerScheduled") })
	if err := e.Signal(bg, id, "go", 1); err != nil {
		t.Fatal(err)
	}
	if v, err := h.Wait(waitCtx(t)); err != nil || v != 1 {
		t.Fatalf("result %v %v", v, err)
	}
	if lateSignal.Load() != nil {
		t.Fatal("the late signal was not sent")
	}
	// The end committed is the one after the late signal: the first one
	// was refused.
	hist := historyEvents(t, e, id)
	late, end := -1, -1
	for i, ev := range hist {
		switch {
		case ev.Type == history.SignalReceived && ev.Name == "late":
			late = i
		case ev.Type == history.ExecutionCompleted:
			end = i
		}
	}
	if late < 0 || end < late {
		t.Fatalf("late signal at %d, end at %d: the end was not refused", late, end)
	}
	hooked := r.runsOf(func(r *recorder) []RunInfo { return r.hooked }, id)
	if len(hooked) != 1 || hooked[0].Status != StatusCompleted {
		t.Fatalf("hook calls %+v", hooked)
	}
	ended := r.runsOf(func(r *recorder) []RunInfo { return r.ended }, id)
	if len(ended) != 1 || ended[0].Status != StatusCompleted {
		t.Fatalf("observed ends %+v", ended)
	}
}

var (
	ndHookChanged atomic.Bool
	ndHookWF      = DefineWorkflow("test.nd-hook", 1, func(c Ctx, _ int) (int, error) {
		if !ndHookChanged.Load() {
			if _, err := compNoUndo.Run(c, "first"); err != nil {
				return 0, err
			}
		}
		return Await[int](c, "go", time.Hour)
	})
)

// T10.14: a park for non-determinism, decided by the worker at replay, is
// an end for the run hook (in its transaction) and the observer, with the
// reason and its kind; resumed with the fixed code, the real end is hooked
// as well.
func TestRunHookSeesNonDeterminismPark(t *testing.T) {
	t.Cleanup(func() { ndHookChanged.Store(false) })
	r := &recorder{}
	// No cache: every task replays the history.
	e := startedEngine(t, mem.New(nil), Options{InstanceID: "hook-nd", CacheSize: -1, Hook: r, Observer: obs{r}})
	const id = "nd-hook"
	h, err := ndHookWF.Start(bg, e, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return historyHas(e, id, "TimerScheduled") })
	ndHookChanged.Store(true) // a deploy that removed the activity call
	if err := e.Signal(bg, id, "go", 3); err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(waitCtx(t))
	parkedWith(t, err, ErrNonDeterministic)
	hooked := func(r *recorder) []RunInfo { return r.hooked }
	ended := func(r *recorder) []RunInfo { return r.ended }
	for name, list := range map[string]func(*recorder) []RunInfo{"hook": hooked, "observer": ended} {
		runs := r.runsOf(list, id)
		if len(runs) != 1 {
			t.Fatalf("%s: %+v", name, runs)
		}
		run := runs[0]
		if run.Status != StatusNeedsAttention || run.Workflow != "test.nd-hook" || run.Version != 1 ||
			!strings.Contains(run.ErrorKinds, store.KindNonDeterministic) || run.Error == "" {
			t.Fatalf("%s: park %+v", name, run)
		}
	}
	ndHookChanged.Store(false) // the fix
	if err := e.Resume(bg, id); err != nil {
		t.Fatal(err)
	}
	if v, err := h.Wait(waitCtx(t)); err != nil || v != 3 {
		t.Fatalf("after the fix: %v %v", v, err)
	}
	for name, list := range map[string]func(*recorder) []RunInfo{"hook": hooked, "observer": ended} {
		runs := r.runsOf(list, id)
		if len(runs) != 2 || runs[1].Status != StatusCompleted {
			t.Fatalf("%s after the fix: %+v", name, runs)
		}
	}
}
