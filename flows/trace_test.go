package flows

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store/mem"
)

// spanLog records how traced attempts and workflow tasks ended, with the
// execution each belongs to.
type spanLog struct {
	mu       sync.Mutex
	attempts []loggedSpan
	tasks    []loggedSpan
}

type loggedSpan struct {
	execution int64
	attempt   int
	err       error
}

func (l *spanLog) StartActivity(ctx context.Context, a ActivityInfo) (context.Context, func(error)) {
	return ctx, func(err error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.attempts = append(l.attempts, loggedSpan{a.ExecutionID, a.Attempt, err})
	}
}

func (l *spanLog) StartWorkflowTask(_ context.Context, w WorkflowTaskInfo) func(Status, error) {
	return func(_ Status, err error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.tasks = append(l.tasks, loggedSpan{w.ExecutionID, 0, err})
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
// the same execution id, which ties their spans together.
func TestTracerLostLease(t *testing.T) {
	clock := &mem.ManualClock{}
	st := mem.New(clock)
	l := &spanLog{}
	e, err := newEngine(st, Options{InstanceID: "trace-lost", PollMax: 20 * time.Millisecond, LeaseTTL: 300 * time.Millisecond, Tracer: l})
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
	var later bool
	for _, a := range l.attempts {
		if a.execution != ex.ID {
			t.Fatalf("attempt of execution %d, want %d", a.execution, ex.ID)
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
		if task.execution != ex.ID {
			t.Fatalf("task of execution %d, want %d", task.execution, ex.ID)
		}
	}
}
