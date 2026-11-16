package flows

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
)

type recorder struct {
	mu       sync.Mutex
	hooked   []RunInfo
	ended    []RunInfo
	attempts []ActivityAttempt
}

func (r *recorder) RunEnded(_ context.Context, tx pgx.Tx, run RunInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hooked = append(r.hooked, run)
	return nil
}

type obs struct{ r *recorder }

func (o obs) ActivityAttempt(a ActivityAttempt) {
	o.r.mu.Lock()
	defer o.r.mu.Unlock()
	o.r.attempts = append(o.r.attempts, a)
}

func (o obs) RunEnded(run RunInfo) {
	o.r.mu.Lock()
	defer o.r.mu.Unlock()
	o.r.ended = append(o.r.ended, run)
}

func (r *recorder) find(list func(*recorder) []RunInfo, id string) (RunInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := slices.IndexFunc(list(r), func(x RunInfo) bool { return x.InstanceID == id })
	if i < 0 {
		return RunInfo{}, false
	}
	return list(r)[i], true
}

func TestRunHookAndObserver(t *testing.T) {
	r := &recorder{}
	e, err := newEngine(mem.New(nil), Options{InstanceID: "hooks", PollMax: 20 * time.Millisecond, Hook: r, Observer: obs{r}})
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
	comps.reset("hook-saga")
	if _, err := sagaWF.Run(waitCtx(t), e, "hook-saga", []string{"a"}); err == nil {
		t.Fatal("saga succeeded")
	}
	h, _ := waitForeverWF.Start(bg, e, "hook-term", 0)
	waitFor(t, func() bool { st, _ := h.Status(bg); return st == StatusRunning })
	if err := e.Terminate(bg, "hook-term", "operator"); err != nil {
		t.Fatal(err)
	}
	hooked := func(r *recorder) []RunInfo { return r.hooked }
	ended := func(r *recorder) []RunInfo { return r.ended }
	for _, list := range []func(*recorder) []RunInfo{hooked, ended} {
		run, ok := r.find(list, "hook-saga")
		if !ok || run.Status != StatusCompensated || run.Workflow != "comp.saga" || run.Version != 1 || run.Error == "" {
			t.Fatalf("saga end %+v %v", run, ok)
		}
		run, ok = r.find(list, "hook-term")
		if !ok || run.Status != StatusTerminated || run.Error != "operator" || run.Workflow != "test.forever" {
			t.Fatalf("termination %+v %v", run, ok)
		}
	}
	r.checkAttempts(t)
}

// checkAttempts expects the saga's activity and its compensation among the
// observed attempts.
func (r *recorder) checkAttempts(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	var act, comp bool
	for _, a := range r.attempts {
		if a.InstanceID != "hook-saga" || a.Activity != "comp.step" || a.Outcome != "completed" || a.Attempt != 1 {
			continue
		}
		if a.Compensation {
			comp = true
		} else {
			act = true
		}
	}
	if !act || !comp {
		t.Fatalf("attempts %+v", r.attempts)
	}
}

type panicObserver struct{}

func (panicObserver) ActivityAttempt(ActivityAttempt) { panic("metrics bug") }
func (panicObserver) RunEnded(RunInfo)                { panic("metrics bug") }

// Review of T5.5: observer panics never escape nor lose results.
func TestPanickingObserver(t *testing.T) {
	for _, eager := range []bool{true, false} {
		e, err := newEngine(mem.New(nil), Options{InstanceID: "obs", PollMax: 20 * time.Millisecond, Observer: panicObserver{}, DisableEager: !eager})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Start(bg); err != nil {
			t.Fatal(err)
		}
		comps.reset("obs-saga")
		if _, err := sagaWF.Run(waitCtx(t), e, "obs-saga", []string{"a"}); !errors.Is(err, ErrCompensated) {
			t.Fatalf("eager=%v: %v", eager, err)
		}
		wantJournal(t, "obs-saga", "undo a")
		h, _ := waitForeverWF.Start(bg, e, "obs-term", 0)
		waitFor(t, func() bool { st, _ := h.Status(bg); return st == StatusRunning })
		if err := e.Terminate(bg, "obs-term", "x"); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		_ = e.Stop(ctx)
		cancel()
	}
}

// failOnce is a RunHook failing its first call.
type failOnce struct {
	mu    sync.Mutex
	calls int
	ok    []RunInfo
}

func (f *failOnce) RunEnded(_ context.Context, _ pgx.Tx, run RunInfo) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls == 1 {
		return errors.New("outbox down")
	}
	f.ok = append(f.ok, run)
	return nil
}

// Review of T5.5: after a hook error, the end is retried (the task is
// handed back) and committed once.
func TestRunHookFailureIsRetried(t *testing.T) {
	f := &failOnce{}
	e, err := newEngine(mem.New(nil), Options{InstanceID: "hook-retry", PollMax: 20 * time.Millisecond, Hook: f})
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
	ctx, cancel := context.WithTimeout(bg, 30*time.Second)
	defer cancel()
	if v, err := edgeWF.Run(ctx, e, "hook-retry", 0); err != nil || v != "ok" {
		t.Fatalf("%q %v", v, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != 2 || len(f.ok) != 1 || f.ok[0].Status != StatusCompleted {
		t.Fatalf("calls %d, committed %+v", f.calls, f.ok)
	}
}

type traceKey struct{}

type fakeTracer struct {
	mu       sync.Mutex
	attempts []string
	tasks    []string
	boom     bool
}

func (f *fakeTracer) StartActivity(ctx context.Context, a ActivityInfo) (context.Context, func(error)) {
	if f.boom {
		panic("tracer bug")
	}
	return context.WithValue(ctx, traceKey{}, "span:"+a.Activity), func(err error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.attempts = append(f.attempts, a.Activity+"/"+strconv.FormatBool(a.Compensation)+"/"+strconv.FormatBool(err == nil))
	}
}

func (f *fakeTracer) StartWorkflowTask(ctx context.Context, w WorkflowTaskInfo) (context.Context, func(Status, error)) {
	if f.boom {
		panic("tracer bug")
	}
	return ctx, func(s Status, _ error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.tasks = append(f.tasks, w.Workflow+":"+string(s))
	}
}

func (f *fakeTracer) StartWorkflow(context.Context, StartInfo) ([]byte, func(error)) {
	if f.boom {
		panic("tracer bug")
	}
	return []byte("t"), func(error) {
		if f.boom {
			panic("tracer bug")
		}
	}
}

var (
	sawSpan  atomic.Value
	spanStep = DefineActivity("trace.step", func(ctx context.Context, _ Key, s string) (string, string, error) {
		if v, ok := ctx.Value(traceKey{}).(string); ok {
			sawSpan.Store(v)
		}
		return s, s, nil
	}, func(context.Context, Key, string) error { return nil })
	traced = DefineWorkflow("trace.wf", 1, func(c Ctx, _ int) (int, error) {
		if _, err := spanStep.Run(c, "x"); err != nil {
			return 0, err
		}
		return 0, errNoStock
	})
)

// T7.3: the tracer wraps attempts (its context reaches the activity) and
// workflow tasks; its panics are harmless.
func TestTracer(t *testing.T) {
	for _, boom := range []bool{false, true} {
		f := &fakeTracer{boom: boom}
		e, err := newEngine(mem.New(nil), Options{InstanceID: "trace", PollMax: 20 * time.Millisecond, Tracer: f})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Start(bg); err != nil {
			t.Fatal(err)
		}
		sawSpan.Store("")
		if _, err := traced.Run(waitCtx(t), e, "traced", 0); !errors.Is(err, ErrCompensated) {
			t.Fatalf("boom=%v: %v", boom, err)
		}
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		_ = e.Stop(ctx)
		cancel()
		if boom {
			continue
		}
		if v := sawSpan.Load(); v != "span:trace.step" {
			t.Fatalf("activity context %v", v)
		}
		f.mu.Lock()
		if !slices.Equal(f.attempts, []string{"trace.step/false/true", "trace.step/true/true"}) {
			t.Fatalf("attempts %v", f.attempts)
		}
		if n := len(f.tasks); n < 2 || f.tasks[n-1] != "trace.wf:compensated" {
			t.Fatalf("tasks %v", f.tasks)
		}
		f.mu.Unlock()
	}
}

// balanceTracer counts starts and ends, and records how attempts and tasks
// ended. mode "nilctx" returns a nil context, "nilend" a nil end.
type balanceTracer struct {
	mode         string
	mu           sync.Mutex
	starts, ends int
	attemptErrs  []bool // per ended attempt: had an error
	taskEnds     []string
}

func (b *balanceTracer) StartActivity(ctx context.Context, a ActivityInfo) (context.Context, func(error)) {
	b.mu.Lock()
	b.starts++
	b.mu.Unlock()
	end := func(err error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.ends++
		b.attemptErrs = append(b.attemptErrs, err != nil)
	}
	switch b.mode {
	case "nilctx":
		return nil, end
	case "nilend":
		b.mu.Lock()
		b.ends++ // no end will come
		b.mu.Unlock()
		return ctx, nil
	}
	return ctx, end
}

func (b *balanceTracer) StartWorkflowTask(ctx context.Context, w WorkflowTaskInfo) (context.Context, func(Status, error)) {
	b.mu.Lock()
	b.starts++
	b.mu.Unlock()
	if b.mode == "nilctx" {
		ctx = nil
	}
	return ctx, func(s Status, err error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.ends++
		b.taskEnds = append(b.taskEnds, string(s)+"/"+strconv.FormatBool(err != nil))
	}
}

func (b *balanceTracer) StartWorkflow(context.Context, StartInfo) ([]byte, func(error)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.starts++
	if b.mode == "nilend" {
		b.ends++ // no end will come
		return nil, nil
	}
	return nil, func(error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.ends++
	}
}

func tracedEngine(t *testing.T, tr Tracer, opts Options) *Engine {
	t.Helper()
	opts.InstanceID, opts.PollMax, opts.Tracer = "balance", 20*time.Millisecond, tr
	e, err := newEngine(mem.New(nil), opts)
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

// Review of T7.3: every span started ends once, on every path; attempts
// that failed (retried, panicked) end with an error; a park ends its task
// needs_attention with an error.
func TestTracerSpansBalance(t *testing.T) {
	for _, c := range []struct {
		name string
		opts Options
		mode string
	}{{"eager", Options{}, ""}, {"polled", Options{DisableEager: true}, ""}, {"nilctx", Options{}, "nilctx"}, {"nilend", Options{}, "nilend"}} {
		b := &balanceTracer{mode: c.mode}
		e := tracedEngine(t, b, c.opts)
		id := "balance-" + c.name
		resetCalls(id)
		if out, err := portWF.Run(waitCtx(t), e, id, []string{"flaky"}); err != nil || out != "flaky@3" {
			t.Fatalf("%s: %q %v", c.name, out, err)
		}
		comps.reset(id + "-p")
		if _, err := portWF.Run(waitCtx(t), e, id+"-p", []string{"a", "panic"}); !errors.Is(err, ErrPanic) {
			t.Fatalf("%s: %v", c.name, err)
		}
		waitFor(t, func() bool {
			b.mu.Lock()
			defer b.mu.Unlock()
			return b.starts == b.ends
		})
		if c.mode != "" {
			continue
		}
		b.mu.Lock()
		failed := 0
		for _, e := range b.attemptErrs {
			if e {
				failed++
			}
		}
		// flaky: 2 failed + 1 ok; a: ok; panic (permanent): 1 failed; undo
		// panic, undo a: ok.
		if len(b.attemptErrs) != 7 || failed != 3 {
			t.Fatalf("%s: attempts %v", c.name, b.attemptErrs)
		}
		b.mu.Unlock()
	}
	// A park ends its task needs_attention, with the reason.
	shortReplay.Store(false)
	defer shortReplay.Store(false)
	b := &balanceTracer{}
	e := tracedEngine(t, b, Options{CacheSize: -1})
	h, err := changingWF.Start(bg, e, "balance-park", 0)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return historyHas(e, "balance-park", "TimerScheduled") })
	shortReplay.Store(true)
	if err := e.Signal(bg, "balance-park", "go", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Wait(waitCtx(t)); !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("%v", err)
	}
	waitFor(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.starts == b.ends && slices.Contains(b.taskEnds, "needs_attention/true")
	})
}

// readCounter counts the GetExecution reads made through it.
type readCounter struct {
	store.Store
	reads atomic.Int32
}

func (r *readCounter) GetExecution(ctx context.Context, id string) (store.Execution, error) {
	r.reads.Add(1)
	return r.Store.GetExecution(ctx, id)
}

// T10.9: Terminate tells the observer what it ended from the terminate
// statement itself, without reading the execution again.
func TestTerminateInformsObserverWithoutARead(t *testing.T) {
	r := &recorder{}
	st := &readCounter{Store: mem.New(nil)}
	e, err := newEngine(st, Options{InstanceID: "term-obs", PollMax: 20 * time.Millisecond, Observer: obs{r}})
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
	h, _ := waitForeverWF.Start(bg, e, "term-obs", 0)
	waitFor(t, func() bool { st, _ := h.Status(bg); return st == StatusRunning })
	before := st.reads.Load()
	if err := e.Terminate(bg, "term-obs", "operator"); err != nil {
		t.Fatal(err)
	}
	if n := st.reads.Load() - before; n != 0 {
		t.Fatalf("Terminate read the execution %d times", n)
	}
	run, ok := r.find(func(r *recorder) []RunInfo { return r.ended }, "term-obs")
	if !ok || run.Status != StatusTerminated || run.Error != "operator" || run.Workflow != "test.forever" || run.Version != 1 {
		t.Fatalf("observed %+v %v", run, ok)
	}
}
