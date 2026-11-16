package flows

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
)

// spanLog is a Tracer recording what each start, task and attempt got:
// a start returns "start:<instance><<the caller's span>" as the trace
// context ("cont:<instance><<the previous trace context>" for
// continue-as-new); a task's context carries "task:<instance>".
type spanLog struct {
	mu       sync.Mutex
	starts   []startRec
	attempts []loggedSpan
	tasks    []loggedSpan
}

type startRec struct {
	instance, trace, prev string
	ended, created        bool
	err                   error
}

type loggedSpan struct {
	instance  string
	execution int64
	trace     string
	attempt   int
	err       error
}

type spanKey struct{}

func newSpanLog() *spanLog { return &spanLog{} }

func (l *spanLog) StartWorkflow(ctx context.Context, w StartInfo) ([]byte, func(bool, error)) {
	caller, _ := ctx.Value(spanKey{}).(string)
	tr := "start:" + w.InstanceID + "<" + caller
	if w.Previous != nil {
		tr = "cont:" + w.InstanceID + "<" + string(w.Previous)
	}
	if w.InstanceID == "trace-huge" {
		tr = strings.Repeat("x", 1000)
	}
	l.mu.Lock()
	k := len(l.starts)
	l.starts = append(l.starts, startRec{instance: w.InstanceID, trace: tr, prev: string(w.Previous)})
	l.mu.Unlock()
	return []byte(tr), func(created bool, err error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.starts[k].ended, l.starts[k].created, l.starts[k].err = true, created, err
	}
}

// startsOf returns the starts of instance id.
func (l *spanLog) startsOf(id string) []startRec {
	var out []startRec
	for _, r := range l.starts {
		if r.instance == id {
			out = append(out, r)
		}
	}
	return out
}

func (l *spanLog) StartActivity(ctx context.Context, a ActivityInfo) (context.Context, func(error)) {
	return ctx, func(err error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.attempts = append(l.attempts, loggedSpan{a.InstanceID, a.ExecutionID, string(a.Trace), a.Attempt, err})
	}
}

func (l *spanLog) StartWorkflowTask(ctx context.Context, w WorkflowTaskInfo) (context.Context, func(Status, error)) {
	return context.WithValue(ctx, spanKey{}, "task:"+w.InstanceID), func(_ Status, err error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.tasks = append(l.tasks, loggedSpan{w.InstanceID, w.ExecutionID, string(w.Trace), 0, err})
	}
}

func (l *spanLog) lostAttempt() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, a := range l.attempts {
		if a.attempt == 1 && errors.Is(a.err, ErrLeaseLost) {
			return true
		}
	}
	return false
}

func tracedLogEngine(t *testing.T, l *spanLog, st *mem.Store) *Engine {
	t.Helper()
	e, err := newEngine(st, Options{InstanceID: "trace-log", PollMax: 20 * time.Millisecond, LeaseTTL: 300 * time.Millisecond, Tracer: l})
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

var (
	traceAct = DefineActivity("trace.ctx-act", func(_ context.Context, _ Key, s string) (string, struct{}, error) {
		return s, struct{}{}, nil
	}, nil)
	traceKid  = DefineWorkflow("trace.kid", 1, func(c Ctx, s string) (string, error) { return traceAct.Run(c, s) })
	traceTree = DefineWorkflow("trace.tree", 1, func(c Ctx, n int) (string, error) {
		if _, err := traceAct.Run(c, "a"); err != nil {
			return "", err
		}
		if n > 0 {
			return "done", nil
		}
		if _, err := traceKid.Child(c, "trace-kid", "k").Get(c); err != nil {
			return "", err
		}
		return "", ContinueAsNew(c, n+1)
	})
	traceTaken = DefineWorkflow("trace.taken", 1, func(c Ctx, id string) (string, error) {
		_, err := traceKid.Child(c, id, "t").Get(c)
		if errors.Is(err, ErrInstanceExists) {
			return "taken", nil
		}
		return "", err
	})
)

// T10.10: the trace context the tracer returns at Start (from the
// caller's context) is stored with the execution and handed to every task
// and attempt; a child's start gets the starting task's context, and the
// child's own trace context goes to its tasks and attempts. Continue-as-new
// starts anew, given the previous trace context. A trace context over
// store.MaxTrace bytes is dropped.
func TestTracerCarriesTraceContext(t *testing.T) {
	l := newSpanLog()
	e := tracedLogEngine(t, l, mem.New(nil))
	caller := context.WithValue(waitCtx(t), spanKey{}, "caller")
	if out, err := traceTree.Run(caller, e, "trace-tree", 0); err != nil || out != "done" {
		t.Fatalf("%q %v", out, err)
	}
	if _, err := traceKid.Run(caller, e, "trace-huge", "h"); err != nil {
		t.Fatal(err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	first, cont := "start:trace-tree<caller", "cont:trace-tree<start:trace-tree<caller"
	tree := l.startsOf("trace-tree")
	if len(tree) != 2 || tree[0].trace != first || tree[1].trace != cont || tree[1].prev != first {
		t.Fatalf("trace-tree starts %+v", tree)
	}
	want := map[string]string{
		"trace-kid":  "start:trace-kid<task:trace-tree",
		"trace-huge": "",
	}
	for _, r := range l.starts {
		if !r.ended || !r.created || r.err != nil {
			t.Fatalf("start %+v", r)
		}
	}
	checkTreeSpans(t, l, want, first, cont)
}

// checkTreeSpans checks the trace context each span of TestTracerCarries-
// TraceContext got: want per instance, first or cont per execution of
// trace-tree (l is locked).
func checkTreeSpans(t *testing.T, l *spanLog, want map[string]string, first, cont string) {
	t.Helper()
	traces := map[int64]string{} // trace-tree execution -> trace context
	seen := map[string]int{}
	for _, s := range append(append([]loggedSpan{}, l.tasks...), l.attempts...) {
		seen[s.instance]++
		if s.instance == "trace-tree" {
			if tr, ok := traces[s.execution]; ok && tr != s.trace {
				t.Fatalf("execution %d spans with traces %q and %q", s.execution, tr, s.trace)
			}
			traces[s.execution] = s.trace
			continue
		}
		if s.trace != want[s.instance] {
			t.Fatalf("span of %s got trace %q, want %q", s.instance, s.trace, want[s.instance])
		}
	}
	if len(traces) != 2 {
		t.Fatalf("trace-tree executions %v: want 2 (continue-as-new)", traces)
	}
	for id, tr := range traces {
		if tr != first && tr != cont || traces[id] == traces[otherKey(traces, id)] {
			t.Fatalf("trace-tree executions' traces %v", traces)
		}
	}
	if seen["trace-kid"] < 2 || seen["trace-huge"] < 2 {
		t.Fatalf("spans per instance %v", seen)
	}
}

func otherKey(m map[int64]string, k int64) int64 {
	for x := range m {
		if x != k {
			return x
		}
	}
	return k
}

// T10.10 review: a start that creates nothing tells the tracer so: a
// second Start of an instance id (no new trace context is stored), and a
// child whose instance id is in use.
func TestTracerStartsThatCreateNothing(t *testing.T) {
	l := newSpanLog()
	e := tracedLogEngine(t, l, mem.New(nil))
	if _, err := traceKid.Run(waitCtx(t), e, "dup", "d"); err != nil {
		t.Fatal(err)
	}
	if _, err := traceKid.Start(context.WithValue(bg, spanKey{}, "again"), e, "dup", "d"); err != nil {
		t.Fatal(err)
	}
	if out, err := traceTaken.Run(waitCtx(t), e, "taker", "dup"); err != nil || out != "taken" {
		t.Fatalf("%q %v", out, err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	dup := l.startsOf("dup")
	if len(dup) != 3 {
		t.Fatalf("starts of dup %+v", dup)
	}
	for i, r := range dup {
		if !r.ended || r.created != (i == 0) || r.err != nil {
			t.Fatalf("start %d of dup %+v", i, r)
		}
	}
	if ex, _ := e.st.GetExecution(bg, "dup"); string(ex.Trace) != dup[0].trace {
		t.Fatalf("the existing execution's trace became %q", ex.Trace)
	}
}

var (
	lostRuns  atomic.Int32
	lostStart = make(chan struct{}, 1)
	lostAct   = DefineActivity("trace.lost", func(ctx context.Context, _ Key, _ int) (int, struct{}, error) {
		if lostRuns.Add(1) == 1 {
			lostStart <- struct{}{}
			<-ctx.Done() // until its lease is lost
			return 0, struct{}{}, ctx.Err()
		}
		return 7, struct{}{}, nil
	}, nil)
	lostWF = DefineWorkflow("trace.lost-wf", 1, func(c Ctx, _ int) (int, error) {
		return lostAct.Run(c, 0)
	})
)

// T10.10: an attempt whose lease is lost (another owner took its task)
// ends with ErrLeaseLost; the next attempt and every workflow task carry
// the same execution and trace context.
func TestTracerLostLease(t *testing.T) {
	clock := &mem.ManualClock{}
	st := mem.New(clock)
	l := newSpanLog()
	e := tracedLogEngine(t, l, st)
	lostRuns.Store(0)
	h, err := lostWF.Start(bg, e, "trace-lost", 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-lostStart:
	case <-waitCtx(t).Done():
		t.Fatal("attempt 1 never started")
	}
	// Every lease lapses; another owner takes the activity task (unless
	// the engine's own poll takes it first: either way attempt 1 is lost).
	clock.Advance(time.Minute)
	_, _ = st.ClaimActivityTasks(bg, "thief", []string{DefaultQueue}, 1, time.Second)
	waitFor(t, l.lostAttempt)
	clock.Advance(time.Minute) // the thief's lease lapses too
	if out, err := h.Wait(waitCtx(t)); err != nil || out != 7 {
		t.Fatalf("%d %v", out, err)
	}
	ex, err := e.st.GetExecution(bg, "trace-lost")
	if err != nil {
		t.Fatal(err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	tr := "start:trace-lost<"
	var later bool
	for _, a := range l.attempts {
		if a.execution != ex.ID || a.trace != tr {
			t.Fatalf("attempt of execution %d, trace %q; want %d, %q", a.execution, a.trace, ex.ID, tr)
		}
		if a.attempt > 1 && a.err == nil {
			later = true
		}
	}
	if !later {
		t.Fatalf("no later attempt succeeded: %+v", l.attempts)
	}
	if len(l.tasks) == 0 {
		t.Fatal("no workflow task traced")
	}
	for _, task := range l.tasks {
		if task.execution != ex.ID || task.trace != tr {
			t.Fatalf("task of execution %d, trace %q", task.execution, task.trace)
		}
	}
}

// panicStarts is a tracer whose StartWorkflow panics.
type panicStarts struct{ spanLog }

func (*panicStarts) StartWorkflow(context.Context, StartInfo) ([]byte, func(bool, error)) {
	panic("tracer bug")
}

// T10.10 review: a panic of the tracer at Start is logged (Options.Logger)
// and the start goes on untraced.
func TestTracerPanicAtStartIsLogged(t *testing.T) {
	var buf syncBuffer
	e, err := newEngine(mem.New(nil), Options{
		InstanceID: "trace-panic", PollMax: 20 * time.Millisecond, Tracer: &panicStarts{},
		Logger: slog.New(slog.NewTextHandler(&buf, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := traceKid.Start(bg, e, "panicky", "p"); err != nil {
		t.Fatal(err)
	}
	if ex, _ := e.st.GetExecution(bg, "panicky"); ex.Trace != nil {
		t.Fatalf("trace %q", ex.Trace)
	}
	if s := buf.String(); !strings.Contains(s, "panic in tracer") || !strings.Contains(s, "level=ERROR") {
		t.Fatalf("log %q", s)
	}
}

// syncBuffer is a bytes.Buffer safe for concurrent writes.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

var traceRerun = DefineWorkflow("trace.rerun", 1, func(c Ctx, n int) (int, error) {
	if n > 0 {
		return n, nil
	}
	_ = traceKid.Child(c, "rerun-kid", "r") // runs on, detached (T9.2)
	return 0, ContinueAsNew(c, n+1)
})

// refuseOnce refuses the first completion that continues as new with
// store.ErrPendingEvents, as a store does when signals arrived meanwhile
// (today only ends are refused: a continue-as-new carries them).
type refuseOnce struct {
	store.Store
	done atomic.Bool
}

func (r *refuseOnce) CompleteWorkflowTask(ctx context.Context, l store.Lease, c *store.Completion) ([]store.Lease, error) {
	if c.ContinueAsNew != nil && r.done.CompareAndSwap(false, true) {
		return nil, store.ErrPendingEvents
	}
	return r.Store.CompleteWorkflowTask(ctx, l, c)
}

// T10.10 review: a completion refused for undelivered events
// (ErrPendingEvents: a normal re-run) is no failed start: the starts it
// made (a child, the continue-as-new) end with ErrRerun and created false,
// and the re-run makes them again, created.
func TestTracerRerunIsNoFailedStart(t *testing.T) {
	l := newSpanLog()
	st := &refuseOnce{Store: mem.New(nil)}
	e, err := newEngine(st, Options{InstanceID: "trace-rerun", PollMax: 20 * time.Millisecond, Tracer: l})
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
	if out, err := traceRerun.Run(waitCtx(t), e, "rerun", 0); err != nil || out != 1 {
		t.Fatalf("%d %v", out, err)
	}
	if !st.done.Load() {
		t.Fatal("no completion was refused")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var cont []startRec
	for _, r := range l.startsOf("rerun") {
		if r.prev != "" {
			cont = append(cont, r)
		}
	}
	for what, rs := range map[string][]startRec{"child": l.startsOf("rerun-kid"), "continue-as-new": cont} {
		if len(rs) != 2 {
			t.Fatalf("%s starts %+v", what, rs)
		}
		if !rs[0].ended || rs[0].created || !errors.Is(rs[0].err, ErrRerun) {
			t.Fatalf("%s start of the refused completion %+v", what, rs[0])
		}
		if !rs[1].ended || !rs[1].created || rs[1].err != nil {
			t.Fatalf("%s start of the re-run %+v", what, rs[1])
		}
	}
}

// childPanics is a tracer whose StartWorkflow panics for children.
type childPanics struct{ spanLog }

func (c *childPanics) StartWorkflow(ctx context.Context, w StartInfo) ([]byte, func(bool, error)) {
	if w.Parent != "" {
		panic("tracer bug")
	}
	return c.spanLog.StartWorkflow(ctx, w)
}

// T10.10 review: the worker logs the tracer's panics (a child's start) as
// the tracer's, not the observer's; the child starts untraced.
func TestTracerPanicAtChildStartIsLogged(t *testing.T) {
	var buf syncBuffer
	e, err := newEngine(mem.New(nil), Options{
		InstanceID: "trace-child-panic", PollMax: 20 * time.Millisecond, Tracer: &childPanics{},
		Logger: slog.New(slog.NewTextHandler(&buf, nil)),
	})
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
	if v, err := parentWF.Run(waitCtx(t), e, "panic-parent", 1); err != nil || v != 30 {
		t.Fatalf("%d %v", v, err)
	}
	if s := buf.String(); !strings.Contains(s, "panic in tracer") || strings.Contains(s, "panic in observer") {
		t.Fatalf("log %q", s)
	}
}
