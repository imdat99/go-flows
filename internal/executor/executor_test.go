package executor_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
)

const q = "q"

var bg = context.Background()

// driver runs workflows against the memory store the way a worker will:
// claim, replay or reuse the cached executor, execute, complete; activities
// run synchronously from a map.
type driver struct {
	t     *testing.T
	clock *mem.ManualClock
	s     *mem.Store
	wf    executor.Workflow
	acts  map[string]func(in []byte) ([]byte, error)
	cache map[int64]*executor.Executor
	cold  bool // drop the executor after every task: always replay
	calls []string
	held  []store.ActivityTask // claimed tasks of activities without a function
}

func newDriver(t *testing.T, wf executor.Workflow) *driver {
	t.Helper()
	c := &mem.ManualClock{}
	d := &driver{
		t: t, clock: c, s: mem.New(c), wf: wf, acts: map[string]func([]byte) ([]byte, error){},
		cache: map[int64]*executor.Executor{},
	}
	t.Cleanup(func() {
		for _, e := range d.cache {
			e.Close()
		}
	})
	return d
}

func (d *driver) start(id string, input string) {
	d.t.Helper()
	if _, _, err := d.s.CreateExecution(bg, store.NewExecution{InstanceID: id, Workflow: d.wf.Name, Version: d.wf.Version, Queue: q, Input: []byte(input)}); err != nil {
		d.t.Fatal(err)
	}
}

// step runs every claimable workflow task, then every activity task, and
// reports whether anything ran. The first error of an executor is returned.
func (d *driver) step() (bool, error) {
	d.t.Helper()
	ran := false
	tasks, err := d.s.ClaimWorkflowTasks(bg, "o", []string{q}, 10, time.Minute)
	if err != nil {
		d.t.Fatal(err)
	}
	for i := range tasks {
		ran = true
		if err := d.runTask(&tasks[i]); err != nil {
			_ = d.s.ReleaseWorkflowTask(bg, tasks[i].Lease, 0)
			return ran, err
		}
	}
	acts, err := d.s.ClaimActivityTasks(bg, "o", []string{q}, 10, 24*time.Hour)
	if err != nil {
		d.t.Fatal(err)
	}
	for _, a := range acts {
		ran = true
		fn := d.acts[a.Name]
		if fn == nil { // left to the test
			d.held = append(d.held, a)
			continue
		}
		d.calls = append(d.calls, a.Name+"("+string(a.Input)+")")
		out, aerr := fn(a.Input)
		ev := history.Event{Type: history.ActivityCompleted, Payload: out}
		if aerr != nil {
			ev = history.Event{Type: history.ActivityFailed, Err: aerr.Error(), Permanent: true}
		}
		if _, err := d.s.CompleteActivityTask(bg, a.Lease, store.ActivityResult{Event: ev}); err != nil {
			d.t.Fatal(err)
		}
	}
	return ran, nil
}

func (d *driver) runTask(task *store.WorkflowTask) error {
	ex := d.cache[task.Execution.ID]
	if ex != nil && ex.LastSeq() != task.LastSeq {
		ex.Close()
		ex = nil
	}
	if ex == nil {
		ex = executor.New(d.wf, task.Execution.ID, executor.Options{})
		hist, err := d.s.LoadHistory(bg, task.Execution.ID, 0)
		if err != nil {
			d.t.Fatal(err)
		}
		if err := ex.Replay(hist); err != nil {
			ex.Close()
			return err
		}
	}
	c, err := ex.Execute(task)
	if err != nil {
		ex.Close()
		delete(d.cache, task.Execution.ID)
		return err
	}
	if _, err := d.s.CompleteWorkflowTask(bg, task.Lease, c); err != nil {
		d.t.Fatalf("complete: %v (%+v)", err, c)
	}
	if d.cold || !c.Status.Active() && c.Status != "" {
		ex.Close()
		delete(d.cache, task.Execution.ID)
	} else {
		d.cache[task.Execution.ID] = ex
	}
	return nil
}

// run steps until idle and returns the first executor error.
func (d *driver) run() error {
	d.t.Helper()
	for range 100 {
		ran, err := d.step()
		if err != nil {
			return err
		}
		if !ran {
			return nil
		}
	}
	d.t.Fatal("no quiescence after 100 steps")
	return nil
}

func (d *driver) execution(id string) store.Execution {
	d.t.Helper()
	e, err := d.s.GetExecution(bg, id)
	if err != nil {
		d.t.Fatal(err)
	}
	return e
}

func activity(ctx coro.Context, name string, in []byte) ([]byte, error) {
	r, err := executor.FromContext(ctx).ScheduleActivity(ctx, name, q, in).Get(ctx)
	if err != nil {
		return nil, err
	}
	return r.Output, r.Err
}

func double(in []byte) ([]byte, error) {
	n, _ := strconv.Atoi(string(in))
	return []byte(strconv.Itoa(2 * n)), nil
}

// chain: two activities in sequence, then a parallel pair.
func chain(ctx coro.Context, in []byte) ([]byte, error) {
	a, err := activity(ctx, "double", in)
	if err != nil {
		return nil, err
	}
	b, err := activity(ctx, "double", a)
	if err != nil {
		return nil, err
	}
	st := executor.FromContext(ctx)
	f1 := st.ScheduleActivity(ctx, "double", q, b)
	f2 := st.ScheduleActivity(ctx, "double", q, []byte("1"))
	r1, _ := f1.Get(ctx)
	r2, _ := f2.Get(ctx)
	return []byte(string(r1.Output) + "," + string(r2.Output)), nil
}

func TestActivitiesWarmAndCold(t *testing.T) {
	for _, cold := range []bool{false, true} {
		t.Run(fmt.Sprintf("cold=%v", cold), func(t *testing.T) {
			d := newDriver(t, executor.Workflow{Name: "chain", Version: 1, Run: chain})
			d.cold = cold
			d.acts["double"] = double
			d.start("i1", "3")
			if err := d.run(); err != nil {
				t.Fatal(err)
			}
			e := d.execution("i1")
			if e.Status != store.StatusCompleted || string(e.Output) != "24,2" {
				t.Fatalf("execution %s %q %s", e.Status, e.Output, e.Err)
			}
			want := []string{"double(3)", "double(6)", "double(12)", "double(1)"}
			if !slices.Equal(d.calls, want) {
				t.Fatalf("calls %v, want %v (each activity exactly once)", d.calls, want)
			}
			hist, _ := d.s.LoadHistory(bg, e.ID, 0)
			if hist[0].Type != history.TaskStarted || hist[1].Type != history.ExecutionStarted ||
				hist[len(hist)-1].Type != history.ExecutionCompleted {
				t.Fatalf("history shape %v", types(hist))
			}
		})
	}
}

func types(hist []history.Event) []history.Type {
	out := make([]history.Type, len(hist))
	for i, ev := range hist {
		out[i] = ev.Type
	}
	return out
}

func TestTimerUsesTaskTime(t *testing.T) {
	var times []time.Time
	wf := executor.Workflow{Name: "sleep", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		times = append(times, st.Now())
		if _, err := st.NewTimer(ctx, time.Hour, "nap").Get(ctx); err != nil {
			return nil, err
		}
		times = append(times, st.Now())
		return nil, nil
	}}
	d := newDriver(t, wf)
	d.start("i1", "")
	if err := d.run(); err != nil {
		t.Fatal(err)
	}
	if d.execution("i1").Status != store.StatusRunning {
		t.Fatal("timer fired early")
	}
	d.clock.Advance(time.Hour)
	if err := d.run(); err != nil {
		t.Fatal(err)
	}
	if d.execution("i1").Status != store.StatusCompleted || len(times) != 2 || times[1].Sub(times[0]) != time.Hour {
		t.Fatalf("status %s times %v", d.execution("i1").Status, times)
	}
}

// GW-14: signals are delivered in order, none dropped, including those that
// arrived before the workflow reads the channel.
func TestSignalsInOrder(t *testing.T) {
	wf := executor.Workflow{Name: "sig", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		ch := executor.FromContext(ctx).SignalChannel("s")
		var got []string
		for range 150 {
			v, _ := ch.Receive(ctx)
			got = append(got, string(v))
		}
		return []byte(strings.Join(got, ",")), nil
	}}
	d := newDriver(t, wf)
	d.start("i1", "")
	for i := range 100 {
		_ = d.s.Signal(bg, "i1", "s", []byte(strconv.Itoa(i)))
	}
	if err := d.run(); err != nil {
		t.Fatal(err)
	}
	for i := 100; i < 150; i++ {
		_ = d.s.Signal(bg, "i1", "s", []byte(strconv.Itoa(i)))
	}
	if err := d.run(); err != nil {
		t.Fatal(err)
	}
	want := make([]string, 150)
	for i := range want {
		want[i] = strconv.Itoa(i)
	}
	if e := d.execution("i1"); string(e.Output) != strings.Join(want, ",") {
		t.Fatalf("output %q", e.Output)
	}
}

func TestCancelRequestCancelsTheWorkflow(t *testing.T) {
	wf := executor.Workflow{Name: "c", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		_, err := executor.FromContext(ctx).NewTimer(ctx, time.Hour, "wait").Get(ctx)
		return nil, err
	}}
	d := newDriver(t, wf)
	d.start("i1", "")
	if err := d.run(); err != nil {
		t.Fatal(err)
	}
	_ = d.s.RequestCancel(bg, "i1")
	if err := d.run(); err != nil {
		t.Fatal(err)
	}
	e := d.execution("i1")
	if e.Status != store.StatusCanceled {
		t.Fatalf("status %s (%s)", e.Status, e.Err)
	}
	hist, _ := d.s.LoadHistory(bg, e.ID, 0)
	if !slices.Contains(types(hist), history.TimerCanceled) {
		t.Fatalf("the timer was not canceled: %v", types(hist))
	}
}

// GW-10: a panic in workflow code fails the workflow, with its stack.
func TestGW10_PanicFailsTheWorkflow(t *testing.T) {
	wf := executor.Workflow{Name: "p", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		coro.Go(ctx, func(coro.Context) { panic("boom") })
		_, err := executor.FromContext(ctx).NewTimer(ctx, time.Hour, "").Get(ctx)
		return nil, err
	}}
	d := newDriver(t, wf)
	d.start("i1", "")
	if err := d.run(); err != nil {
		t.Fatal(err)
	}
	if e := d.execution("i1"); e.Status != store.StatusFailed || !strings.Contains(e.Err, "boom") {
		t.Fatalf("status %s err %q", e.Status, e.Err)
	}
}

func TestSideEffectIsRecordedOnce(t *testing.T) {
	n := 0
	wf := executor.Workflow{Name: "se", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		v, err := st.SideEffect(ctx, func() []byte { n++; return []byte(strconv.Itoa(n)) })
		if err != nil {
			return nil, err
		}
		if _, err := activity(ctx, "double", v); err != nil {
			return nil, err
		}
		return v, nil
	}}
	d := newDriver(t, wf)
	d.cold = true // replay every task: the side effect must not run again
	d.acts["double"] = double
	d.start("i1", "")
	if err := d.run(); err != nil {
		t.Fatal(err)
	}
	if e := d.execution("i1"); string(e.Output) != "1" || n != 1 {
		t.Fatalf("output %q, side effect ran %d times", e.Output, n)
	}
}

// GW-11: a replay that diverges from the history is an error for the
// caller to park the execution, never a failure of the workflow.
func TestGW11_NonDeterminismIsDetected(t *testing.T) {
	cases := map[string]func(ctx coro.Context, in []byte) ([]byte, error){
		"activity renamed": func(ctx coro.Context, in []byte) ([]byte, error) {
			return activity(ctx, "triple", in)
		},
		"argument changed": func(ctx coro.Context, _ []byte) ([]byte, error) {
			return activity(ctx, "double", []byte("999"))
		},
		"extra command first": func(ctx coro.Context, in []byte) ([]byte, error) {
			executor.FromContext(ctx).ScheduleActivity(ctx, "double", q, []byte("0"))
			return activity(ctx, "double", in)
		},
		"command removed": func(ctx coro.Context, _ []byte) ([]byte, error) {
			_, err := executor.FromContext(ctx).NewTimer(ctx, time.Hour, "").Get(ctx)
			return nil, err
		},
		"timer instead of activity": func(ctx coro.Context, _ []byte) ([]byte, error) {
			_, err := executor.FromContext(ctx).NewTimer(ctx, time.Minute, "").Get(ctx)
			return nil, err
		},
	}
	original := func(ctx coro.Context, in []byte) ([]byte, error) {
		if _, err := activity(ctx, "double", in); err != nil {
			return nil, err
		}
		_, err := executor.FromContext(ctx).NewTimer(ctx, time.Hour, "").Get(ctx)
		return nil, err
	}
	for name, changed := range cases {
		t.Run(name, func(t *testing.T) {
			d := newDriver(t, executor.Workflow{Name: "w", Version: 1, Run: original})
			d.acts["double"] = double
			d.acts["triple"] = double
			d.start("i1", "3")
			if err := d.run(); err != nil { // waits on the timer
				t.Fatal(err)
			}
			d.wf.Run = changed
			d.cold = true
			for _, e := range d.cache {
				e.Close()
			}
			clear(d.cache)
			d.clock.Advance(time.Hour) // the timer fires: a replay follows
			err := d.run()
			if !errors.Is(err, executor.ErrNonDeterministic) {
				t.Fatalf("replay with %s: %v", name, err)
			}
		})
	}
}

// The workflow ends when its main function returns; an activity started
// and not awaited is not scheduled.
func TestUnawaitedActivityIsDropped(t *testing.T) {
	wf := executor.Workflow{Name: "u", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		executor.FromContext(ctx).ScheduleActivity(ctx, "double", q, []byte("1"))
		return []byte("done"), nil
	}}
	d := newDriver(t, wf)
	d.acts["double"] = double
	d.start("i1", "")
	if err := d.run(); err != nil {
		t.Fatal(err)
	}
	if e := d.execution("i1"); e.Status != store.StatusCompleted || len(d.calls) != 0 {
		t.Fatalf("status %s calls %v", e.Status, d.calls)
	}
}

func TestContinueAsNew(t *testing.T) {
	wf := executor.Workflow{Name: "can", Version: 1, Run: func(ctx coro.Context, in []byte) ([]byte, error) {
		n, _ := strconv.Atoi(string(in))
		if n < 3 {
			return nil, &executor.ContinueAsNewError{Input: []byte(strconv.Itoa(n + 1))}
		}
		return in, nil
	}}
	d := newDriver(t, wf)
	d.start("i1", "0")
	if err := d.run(); err != nil {
		t.Fatal(err)
	}
	if e := d.execution("i1"); e.Status != store.StatusCompleted || string(e.Output) != "3" {
		t.Fatalf("status %s output %q", e.Status, e.Output)
	}
}

func TestActivityFailureCarriesTheRecordedError(t *testing.T) {
	wf := executor.Workflow{Name: "f", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		_, err := activity(ctx, "fail", nil)
		var re *executor.RecordedError
		if !errors.As(err, &re) || !re.Permanent || re.Message != "nope" {
			return nil, fmt.Errorf("unexpected error: %w", err)
		}
		return []byte("ok"), nil
	}}
	d := newDriver(t, wf)
	d.acts["fail"] = func([]byte) ([]byte, error) { return nil, errors.New("nope") }
	d.start("i1", "")
	if err := d.run(); err != nil {
		t.Fatal(err)
	}
	if e := d.execution("i1"); string(e.Output) != "ok" {
		t.Fatalf("status %s err %q", e.Status, e.Err)
	}
}

// Canceling the context of a scheduled activity resolves its future with
// ErrCanceled and records the cancel request; the late result is still
// recorded (the saga layer needs its compensation data).
func TestActivityCancelByContext(t *testing.T) {
	wf := executor.Workflow{Name: "ac", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		actx, cancel := coro.WithCancel(ctx)
		f := st.ScheduleActivity(actx, "slow", q, nil)
		if _, err := st.NewTimer(ctx, time.Minute, "").Get(ctx); err != nil {
			return nil, err
		}
		cancel()
		r, _ := f.Get(ctx)
		if !errors.Is(r.Err, executor.ErrCanceled) {
			return nil, fmt.Errorf("activity result after cancel: %w", r.Err)
		}
		_, err := st.NewTimer(ctx, time.Minute, "").Get(ctx) // let the late result arrive
		return []byte("ok"), err
	}}
	d := newDriver(t, wf)
	d.start("i1", "")
	if _, err := d.step(); err != nil { // schedules the activity and the timer
		t.Fatal(err)
	}
	d.clock.Advance(time.Minute)
	if err := d.run(); err != nil { // the timer fires: the workflow cancels the activity
		t.Fatal(err)
	}
	if len(d.held) != 1 {
		t.Fatalf("held %d activity tasks", len(d.held))
	}
	if _, err := d.s.CompleteActivityTask(bg, d.held[0].Lease, store.ActivityResult{Event: history.Event{Type: history.ActivityCompleted, CompData: []byte("undo")}}); err != nil {
		t.Fatal(err)
	}
	d.clock.Advance(time.Minute)
	if err := d.run(); err != nil {
		t.Fatal(err)
	}
	e := d.execution("i1")
	hist, _ := d.s.LoadHistory(bg, e.ID, 0)
	ty := types(hist)
	if e.Status != store.StatusCompleted || !slices.Contains(ty, history.ActivityCancelRequested) || !slices.Contains(ty, history.ActivityCompleted) {
		t.Fatalf("status %s (%s) history %v", e.Status, e.Err, ty)
	}
}
