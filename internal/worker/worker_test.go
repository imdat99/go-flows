package worker_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/worker"
)

const q = "q"

// registry is a test implementation of worker.Workflows and
// worker.Activities.
type registry struct {
	mu   sync.Mutex
	wfs  map[string]executor.Workflow
	acts map[string]*worker.Activity
}

func newRegistry() *registry {
	return &registry{wfs: map[string]executor.Workflow{}, acts: map[string]*worker.Activity{}}
}

func (r *registry) Workflow(name string, version int32) (executor.Workflow, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	wf, ok := r.wfs[name+"@"+strconv.Itoa(int(version))]
	return wf, ok
}

func (r *registry) Activity(name string, compensate bool) (*worker.Activity, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.acts[name]
	return a, ok && !compensate
}

func (r *registry) addWorkflow(name string, run executor.Func) {
	r.mu.Lock()
	r.wfs[name+"@1"] = executor.Workflow{Name: name, Version: 1, Run: run}
	r.mu.Unlock()
}

func (r *registry) addActivity(name string, a *worker.Activity) {
	r.mu.Lock()
	r.acts[name] = a
	r.mu.Unlock()
}

func activity(ctx coro.Context, name string, in []byte) ([]byte, error) {
	r, err := executor.FromContext(ctx).ScheduleActivity(ctx, name, q, in).Get(ctx)
	if err != nil {
		return nil, err
	}
	return r.Output, r.Err
}

// counting returns an activity doubling its integer input and counting its
// invocations per input.
func counting(calls *sync.Map) *worker.Activity {
	return &worker.Activity{Run: func(_ context.Context, t *store.ActivityTask) ([]byte, []byte, error) {
		n, _ := strconv.Atoi(string(t.Input))
		c, _ := calls.LoadOrStore(t.InstanceID+"/"+strconv.Itoa(int(t.ScheduleID)), new(atomic.Int32))
		c.(*atomic.Int32).Add(1)
		return []byte(strconv.Itoa(2 * n)), nil, nil
	}}
}

// chainOf runs n activities in sequence, doubling the input each time.
func chainOf(n int) executor.Func {
	return func(ctx coro.Context, in []byte) ([]byte, error) {
		for range n {
			out, err := activity(ctx, "double", in)
			if err != nil {
				return nil, err
			}
			in = out
		}
		return in, nil
	}
}

func startWorker(t *testing.T, st store.Store, reg *registry, opts worker.Options) *worker.Worker {
	t.Helper()
	if opts.Owner == "" {
		opts.Owner = "w1"
	}
	opts.Queues = []string{q}
	if opts.LeaseTTL == 0 {
		opts.LeaseTTL = 5 * time.Second
	}
	opts.PollMax = 50 * time.Millisecond
	w, err := worker.New(st, reg, reg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := w.Stop(ctx); err != nil {
			t.Errorf("stop: %v", err)
		}
	})
	return w
}

func create(t *testing.T, st store.Store, id, wf, input string) {
	t.Helper()
	if _, _, err := st.CreateExecution(context.Background(), store.NewExecution{InstanceID: id, Workflow: wf, Version: 1, Queue: q, Input: []byte(input)}); err != nil {
		t.Fatal(err)
	}
}

// waitEnd waits until the instance's latest execution ended.
func waitEnd(t *testing.T, st store.Store, id string) store.Execution {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		e, err := st.GetExecution(context.Background(), id)
		if err == nil && (!e.Status.Active() || e.Status == store.StatusNeedsAttention) {
			return e
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not end: %+v %v", id, e, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestEagerAndPolledChains(t *testing.T) {
	for _, eager := range []bool{true, false} {
		t.Run(fmt.Sprintf("eager=%v", eager), func(t *testing.T) {
			st := mem.New(nil)
			reg := newRegistry()
			var calls sync.Map
			reg.addWorkflow("chain", chainOf(5))
			reg.addActivity("double", counting(&calls))
			startWorker(t, st, reg, worker.Options{Eager: eager})
			for i := range 10 {
				create(t, st, "i"+strconv.Itoa(i), "chain", "1")
			}
			for i := range 10 {
				e := waitEnd(t, st, "i"+strconv.Itoa(i))
				if e.Status != store.StatusCompleted || string(e.Output) != "32" {
					t.Fatalf("i%d: %s %q %s", i, e.Status, e.Output, e.Err)
				}
			}
			calls.Range(func(k, v any) bool {
				if n := v.(*atomic.Int32).Load(); n != 1 {
					t.Errorf("%v ran %d times", k, n)
				}
				return true
			})
		})
	}
}

func TestTwoWorkersShareTheWork(t *testing.T) {
	st := mem.New(nil)
	reg := newRegistry()
	var calls sync.Map
	reg.addWorkflow("chain", chainOf(3))
	reg.addActivity("double", counting(&calls))
	startWorker(t, st, reg, worker.Options{Owner: "a", Eager: true})
	startWorker(t, st, reg, worker.Options{Owner: "b", Eager: true})
	for i := range 30 {
		create(t, st, "i"+strconv.Itoa(i), "chain", "1")
	}
	for i := range 30 {
		if e := waitEnd(t, st, "i"+strconv.Itoa(i)); string(e.Output) != "8" {
			t.Fatalf("i%d: %s %q", i, e.Status, e.Output)
		}
	}
	n := 0
	calls.Range(func(_, v any) bool {
		n++
		if c := v.(*atomic.Int32).Load(); c != 1 {
			t.Errorf("activity ran %d times", c)
		}
		return true
	})
	if n != 90 {
		t.Fatalf("%d activities ran, want 90", n)
	}
}

// GW-15: retries happen on the activity task; only the final outcome is in
// the history.
func TestGW15_EngineSideRetries(t *testing.T) {
	st := mem.New(nil)
	reg := newRegistry()
	var attempts atomic.Int32
	reg.addWorkflow("w", func(ctx coro.Context, _ []byte) ([]byte, error) { return activity(ctx, "flaky", nil) })
	reg.addActivity("flaky", &worker.Activity{
		Retry: worker.RetryPolicy{Attempts: 3, Initial: 10 * time.Millisecond},
		Run: func(_ context.Context, t *store.ActivityTask) ([]byte, []byte, error) {
			attempts.Add(1)
			if t.Attempt < 3 {
				return nil, nil, errors.New("transient")
			}
			return []byte("ok"), nil, nil
		},
	})
	startWorker(t, st, reg, worker.Options{Eager: true})
	create(t, st, "i1", "w", "")
	e := waitEnd(t, st, "i1")
	if e.Status != store.StatusCompleted || string(e.Output) != "ok" || attempts.Load() != 3 {
		t.Fatalf("status %s output %q attempts %d", e.Status, e.Output, attempts.Load())
	}
	hist, _ := st.LoadHistory(context.Background(), e.ID, 0)
	n := 0
	for _, ev := range hist {
		if ev.Type == history.ActivityCompleted || ev.Type == history.ActivityFailed {
			n++
			if ev.Attempt != 3 {
				t.Fatalf("recorded attempt %d", ev.Attempt)
			}
		}
	}
	if n != 1 {
		t.Fatalf("%d outcomes recorded, want 1 (retries are not history)", n)
	}
}

func TestPermanentFailureAndPanicReachTheWorkflow(t *testing.T) {
	st := mem.New(nil)
	reg := newRegistry()
	reg.addWorkflow("w", func(ctx coro.Context, _ []byte) ([]byte, error) {
		_, err1 := activity(ctx, "boom", nil)
		_, err2 := activity(ctx, "panics", nil)
		var r1, r2 *executor.RecordedError
		if !errors.As(err1, &r1) || !errors.As(err2, &r2) {
			return nil, fmt.Errorf("errors %w / %w", err1, err2)
		}
		if r1.Uncertain || !r2.Uncertain || !r2.Permanent {
			return nil, fmt.Errorf("flags: uncertain %v/%v permanent %v", r1.Uncertain, r2.Uncertain, r2.Permanent)
		}
		return []byte("ok"), nil
	})
	reg.addActivity("boom", &worker.Activity{
		Retry: worker.RetryPolicy{Attempts: 1},
		Run:   func(context.Context, *store.ActivityTask) ([]byte, []byte, error) { return nil, nil, errors.New("no") },
	})
	reg.addActivity("panics", &worker.Activity{
		Retry: worker.RetryPolicy{Attempts: 5},
		Run:   func(context.Context, *store.ActivityTask) ([]byte, []byte, error) { panic("kaboom") },
	})
	startWorker(t, st, reg, worker.Options{Eager: true})
	create(t, st, "i1", "w", "")
	if e := waitEnd(t, st, "i1"); e.Status != store.StatusCompleted {
		t.Fatalf("status %s err %s", e.Status, e.Err)
	}
}

// GW-12: a worker without the workflow hands the execution back; a worker
// that has it runs it.
func TestGW12_UnknownWorkflowIsHandedBack(t *testing.T) {
	st := mem.New(nil)
	empty := newRegistry()
	startWorker(t, st, empty, worker.Options{Owner: "old", HandBack: 20 * time.Millisecond})
	create(t, st, "i1", "chain", "1")
	time.Sleep(100 * time.Millisecond)
	if e, _ := st.GetExecution(context.Background(), "i1"); e.Status != store.StatusRunning {
		t.Fatalf("an unknown workflow must not fail the execution: %s %s", e.Status, e.Err)
	}
	reg := newRegistry()
	var calls sync.Map
	reg.addWorkflow("chain", chainOf(1))
	reg.addActivity("double", counting(&calls))
	startWorker(t, st, reg, worker.Options{Owner: "new"})
	if e := waitEnd(t, st, "i1"); e.Status != store.StatusCompleted || string(e.Output) != "2" {
		t.Fatalf("status %s output %q", e.Status, e.Output)
	}
}

// GW-11: non-determinism parks the execution with the reason; it is not
// failed.
func TestGW11_NonDeterminismParks(t *testing.T) {
	st := mem.New(nil)
	reg := newRegistry()
	var changed atomic.Bool
	reg.addWorkflow("w", func(ctx coro.Context, _ []byte) ([]byte, error) {
		name := "a"
		if changed.Load() {
			name = "b"
		}
		if _, err := activity(ctx, name, nil); err != nil {
			return nil, err
		}
		_, err := executor.FromContext(ctx).SignalChannel("go").Receive(ctx)
		_ = err
		return []byte("done"), nil
	})
	ok := &worker.Activity{Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) { return nil, nil, nil }}
	reg.addActivity("a", ok)
	reg.addActivity("b", ok)
	startWorker(t, st, reg, worker.Options{CacheSize: -1}) // no cache: every task replays
	create(t, st, "i1", "w", "")
	deadline := time.Now().Add(5 * time.Second)
	for {
		hist, _ := st.LoadHistory(context.Background(), 1, 0)
		if len(hist) > 4 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	changed.Store(true)
	if err := st.Signal(context.Background(), "i1", "go", nil); err != nil {
		t.Fatal(err)
	}
	e := waitEnd(t, st, "i1")
	if e.Status != store.StatusNeedsAttention || e.Err == "" {
		t.Fatalf("status %s err %q", e.Status, e.Err)
	}
}

// Stop lets in-flight tasks finish and releases kept leases: another
// worker continues at once.
func TestStopHandsOver(t *testing.T) {
	st := mem.New(nil)
	reg := newRegistry()
	release := make(chan struct{})
	var started atomic.Int32
	reg.addWorkflow("w", chainOf(2))
	reg.addActivity("double", &worker.Activity{Run: func(_ context.Context, t *store.ActivityTask) ([]byte, []byte, error) {
		if started.Add(1) == 1 {
			<-release
		}
		return t.Input, nil, nil
	}})
	w1, err := worker.New(st, reg, reg, worker.Options{Owner: "w1", Queues: []string{q}, Eager: true, LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err := w1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	create(t, st, "i1", "w", "7")
	for started.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- w1.Stop(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	close(release) // the in-flight activity finishes during the stop
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	startWorker(t, st, reg, worker.Options{Owner: "w2", Eager: true, LeaseTTL: time.Minute})
	start := time.Now()
	if e := waitEnd(t, st, "i1"); e.Status != store.StatusCompleted || string(e.Output) != "7" {
		t.Fatalf("status %s output %q", e.Status, e.Output)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("handover waited %v: the lease was not released", d)
	}
}

// GW-13: an activity whose context the workflow cancels is canceled where
// it runs, and its outcome is still recorded.
func TestGW13_WorkflowCancelsARunningActivity(t *testing.T) {
	for _, eager := range []bool{true, false} {
		t.Run(fmt.Sprintf("eager=%v", eager), func(t *testing.T) {
			st := mem.New(nil)
			reg := newRegistry()
			reg.addWorkflow("w", func(ctx coro.Context, _ []byte) ([]byte, error) {
				st := executor.FromContext(ctx)
				actx, cancel := coro.WithCancel(ctx)
				f := st.ScheduleActivity(actx, "slow", q, nil)
				_, _ = st.SignalChannel("stop").Receive(ctx)
				cancel()
				r, _ := f.Get(ctx)
				if !errors.Is(r.Err, executor.ErrCanceled) {
					return nil, fmt.Errorf("activity result %w", r.Err)
				}
				_, err := st.NewTimer(ctx, 200*time.Millisecond, "").Get(ctx) // let the outcome land
				return []byte("ok"), err
			})
			sawCancel := make(chan error, 1)
			reg.addActivity("slow", &worker.Activity{Run: func(ctx context.Context, _ *store.ActivityTask) ([]byte, []byte, error) {
				<-ctx.Done()
				sawCancel <- context.Cause(ctx)
				return nil, []byte("undo"), ctx.Err()
			}})
			startWorker(t, st, reg, worker.Options{Eager: eager, LeaseTTL: 300 * time.Millisecond})
			create(t, st, "i1", "w", "")
			time.Sleep(50 * time.Millisecond)
			if err := st.Signal(context.Background(), "i1", "stop", nil); err != nil {
				t.Fatal(err)
			}
			select {
			case cause := <-sawCancel:
				if !errors.Is(cause, worker.ErrActivityCanceled) {
					t.Fatalf("cause %v", cause)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the running activity was never canceled")
			}
			e := waitEnd(t, st, "i1")
			hist, _ := st.LoadHistory(context.Background(), e.ID, 0)
			var outcome *history.Event
			for i := range hist {
				if hist[i].Type == history.ActivityFailed {
					outcome = &hist[i]
				}
			}
			if e.Status != store.StatusCompleted || outcome == nil || !outcome.Uncertain || string(outcome.CompData) != "undo" {
				t.Fatalf("status %s outcome %+v", e.Status, outcome)
			}
		})
	}
}

// A Stop whose deadline passes while an activity runs neither fails the
// activity nor the workflow: the attempt is abandoned and runs again
// elsewhere (found in review of T3.2-T3.6).
func TestStopDeadlineDoesNotFailWork(t *testing.T) {
	for _, eager := range []bool{true, false} {
		t.Run(fmt.Sprintf("eager=%v", eager), func(t *testing.T) {
			st := mem.New(nil)
			reg := newRegistry()
			var calls atomic.Int32
			reg.addWorkflow("w", func(ctx coro.Context, _ []byte) ([]byte, error) { return activity(ctx, "block", nil) })
			reg.addActivity("block", &worker.Activity{Run: func(ctx context.Context, _ *store.ActivityTask) ([]byte, []byte, error) {
				if calls.Add(1) == 1 {
					<-ctx.Done() // the first attempt only ends at the stop deadline
					return nil, nil, ctx.Err()
				}
				return []byte("ok"), nil, nil
			}})
			w1, err := worker.New(st, reg, reg, worker.Options{Owner: "w1", Queues: []string{q}, Eager: eager, LeaseTTL: 300 * time.Millisecond, PollMax: 20 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			if err := w1.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			create(t, st, "i1", "w", "")
			for calls.Load() == 0 {
				time.Sleep(time.Millisecond)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			// A task was interrupted (not an idle worker): the deadline's
			// error.
			if err := w1.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("stop interrupting a task: %v, want the deadline's error", err)
			}
			cancel()
			if e, _ := st.GetExecution(context.Background(), "i1"); e.Status != store.StatusRunning {
				t.Fatalf("stop deadline ended the workflow: %s %s", e.Status, e.Err)
			}
			startWorker(t, st, reg, worker.Options{Owner: "w2", Eager: eager, LeaseTTL: 300 * time.Millisecond})
			if e := waitEnd(t, st, "i1"); e.Status != store.StatusCompleted || string(e.Output) != "ok" {
				t.Fatalf("status %s output %q err %s", e.Status, e.Output, e.Err)
			}
		})
	}
}

// After a hand-over the workflow slot is free: with one workflow slot and
// two executions running long eager activities, both still make progress
// (signals reach the second one).
func TestHandOverFreesTheWorkflowSlot(t *testing.T) {
	st := mem.New(nil)
	reg := newRegistry()
	release := make(chan struct{})
	reg.addWorkflow("w", func(ctx coro.Context, _ []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		f := st.ScheduleActivity(ctx, "long", q, nil)
		if _, ok := st.SignalChannel("go").Receive(ctx); !ok {
			return nil, errors.New("signal channel closed")
		}
		r, err := f.Get(ctx)
		if err != nil {
			return nil, err
		}
		return r.Output, r.Err
	})
	reg.addActivity("long", &worker.Activity{Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) {
		<-release
		return []byte("done"), nil, nil
	}})
	startWorker(t, st, reg, worker.Options{Eager: true, MaxWorkflowTasks: 1, MaxActivities: 4, EagerWindow: 20 * time.Millisecond})
	create(t, st, "a", "w", "")
	create(t, st, "b", "w", "")
	time.Sleep(200 * time.Millisecond) // both handed over, activities still running
	for _, id := range []string{"a", "b"} {
		if err := st.Signal(context.Background(), id, "go", nil); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(200 * time.Millisecond) // the signals are processed with the slot free
	close(release)
	for _, id := range []string{"a", "b"} {
		if e := waitEnd(t, st, id); e.Status != store.StatusCompleted {
			t.Fatalf("%s: %s %s", id, e.Status, e.Err)
		}
	}
}

// An attempt that succeeds after the stop deadline is still recorded: it
// must not run again elsewhere (found in review of T3.2-T3.6).
func TestLateSuccessAfterStopDeadlineIsRecorded(t *testing.T) {
	st := mem.New(nil)
	reg := newRegistry()
	var calls atomic.Int32
	reg.addWorkflow("w", func(ctx coro.Context, _ []byte) ([]byte, error) { return activity(ctx, "slow", nil) })
	reg.addActivity("slow", &worker.Activity{Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) {
		calls.Add(1)
		time.Sleep(150 * time.Millisecond) // ignores its context
		return []byte("ok"), nil, nil
	}})
	w1, err := worker.New(st, reg, reg, worker.Options{Owner: "w1", Queues: []string{q}, Eager: true, LeaseTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := w1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	create(t, st, "i1", "w", "")
	for calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	_ = w1.Stop(ctx)
	cancel()
	startWorker(t, st, reg, worker.Options{Owner: "w2", Eager: true})
	if e := waitEnd(t, st, "i1"); e.Status != store.StatusCompleted || calls.Load() != 1 {
		t.Fatalf("status %s, activity ran %d times", e.Status, calls.Load())
	}
}

// failingResume fails the first CompleteActivityTask that resumes a
// workflow lease with a transient error.
type failingResume struct {
	*mem.Store
	failed atomic.Bool
}

func (s *failingResume) CompleteActivityTask(ctx context.Context, l store.Lease, r store.ActivityResult) (*store.WorkflowTask, error) {
	if r.Resume != nil && s.failed.CompareAndSwap(false, true) {
		return nil, errors.New("transient")
	}
	return s.Store.CompleteActivityTask(ctx, l, r)
}

// A transient error while recording an eager result keeps the workflow
// lease and hands the execution over promptly, instead of waiting out the
// lease (found in review of T3.2-T3.6).
func TestTransientResumeErrorHandsOverPromptly(t *testing.T) {
	st := &failingResume{Store: mem.New(nil)}
	reg := newRegistry()
	var n atomic.Int32
	reg.addWorkflow("w", chainOf(2))
	reg.addActivity("double", &worker.Activity{
		Retry: worker.RetryPolicy{Attempts: 3},
		Run: func(_ context.Context, t *store.ActivityTask) ([]byte, []byte, error) {
			n.Add(1)
			return t.Input, nil, nil
		},
	})
	startWorker(t, st, reg, worker.Options{Eager: true, LeaseTTL: time.Minute})
	create(t, st, "i1", "w", "3")
	start := time.Now()
	if e := waitEnd(t, st, "i1"); e.Status != store.StatusCompleted {
		t.Fatalf("status %s %s", e.Status, e.Err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("took %v: the lease was waited out", d)
	}
}
