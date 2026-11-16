package flows

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store/mem"
)

// spanLog is a Tracer recording what each start, task and attempt got:
// a start returns "start:<instance><<the caller's span>" as the trace
// context; a task's context carries "task:<instance>".
type spanLog struct {
	mu       sync.Mutex
	starts   map[string]string // instance id -> trace context returned
	ends     map[string]error  // instance id -> the start's end
	attempts []loggedSpan
	tasks    []loggedSpan
}

type loggedSpan struct {
	instance  string
	execution int64
	trace     string
	attempt   int
	err       error
}

type spanKey struct{}

func newSpanLog() *spanLog {
	return &spanLog{starts: map[string]string{}, ends: map[string]error{}}
}

func (l *spanLog) StartWorkflow(ctx context.Context, w StartInfo) ([]byte, func(error)) {
	caller, _ := ctx.Value(spanKey{}).(string)
	tr := "start:" + w.InstanceID + "<" + caller
	if w.InstanceID == "trace-huge" {
		tr = strings.Repeat("x", 1000)
	}
	l.mu.Lock()
	l.starts[w.InstanceID] = tr
	l.mu.Unlock()
	return []byte(tr), func(err error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.ends[w.InstanceID] = err
	}
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
)

// T10.10: the trace context the tracer returns at Start (from the
// caller's context) is stored with the execution and handed to every task
// and attempt, across continue-as-new; a child's start gets the starting
// task's context, and the child's own trace context goes to its tasks and
// attempts. A trace context over store.MaxTrace bytes is dropped.
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
	waitFor(t, func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return len(l.ends) == 3
	})
	l.mu.Lock()
	defer l.mu.Unlock()
	want := map[string]string{
		"trace-tree": "start:trace-tree<caller",
		"trace-kid":  "start:trace-kid<task:trace-tree",
		"trace-huge": "",
	}
	for id, tr := range want {
		if id != "trace-huge" && l.starts[id] != tr {
			t.Fatalf("start of %s returned %q, want %q", id, l.starts[id], tr)
		}
		if err, ok := l.ends[id]; !ok || err != nil {
			t.Fatalf("start of %s ended %v %v", id, ok, err)
		}
	}
	executions := map[int64]bool{}
	seen := map[string]int{}
	for _, s := range append(append([]loggedSpan{}, l.tasks...), l.attempts...) {
		if s.trace != want[s.instance] {
			t.Fatalf("%s span of %s got trace %q, want %q", map[bool]string{true: "attempt", false: "task"}[s.attempt > 0], s.instance, s.trace, want[s.instance])
		}
		if s.instance == "trace-tree" {
			executions[s.execution] = true
		}
		seen[s.instance]++
	}
	if len(executions) != 2 {
		t.Fatalf("trace-tree executions %v: want 2 (continue-as-new)", executions)
	}
	if seen["trace-kid"] < 2 || seen["trace-huge"] < 2 {
		t.Fatalf("spans per instance %v", seen)
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
