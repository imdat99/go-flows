package flows

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

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
