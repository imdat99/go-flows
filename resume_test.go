package flows

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store/mem"
)

// T8.4: an execution parked because its code was wrong runs again once the
// code is fixed (Engine.Resume): non-determinism (a deploy changed the
// workflow without a new version), and code that does not yield.
var (
	resumeChanged   atomic.Bool
	resumeChangedWF = DefineWorkflow("resume.changed", 1, func(c Ctx, _ int) (int, error) {
		if !resumeChanged.Load() {
			if _, err := portCounted.Run(c, "resume-first"); err != nil {
				return 0, err
			}
		}
		return Await[int](c, "go", time.Hour)
	})

	resumeSpin    atomic.Bool
	resumeStuckWF = DefineWorkflow("resume.stuck", 1, func(c Ctx, _ int) (int, error) {
		v, err := Await[int](c, "go", time.Hour)
		for resumeSpin.Load() { // the busy loop under test
		}
		return v, err
	})
)

func resumeEngine(t *testing.T, id string) *Engine {
	t.Helper()
	e, err := newEngine(mem.New(nil), Options{InstanceID: id, CacheSize: -1, PollMax: 20 * time.Millisecond, StuckTimeout: 200 * time.Millisecond})
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

func parkedWith(t *testing.T, err error, reason error) {
	t.Helper()
	var re *RunError
	if !errors.As(err, &re) || re.Status != StatusNeedsAttention || !errors.Is(err, reason) {
		t.Fatalf("want a park for %v, got %v", reason, err)
	}
}

func TestResumeAfterNonDeterminismFix(t *testing.T) {
	t.Cleanup(func() { resumeChanged.Store(false) })
	e := resumeEngine(t, "resume-nd")
	h, err := resumeChangedWF.Start(bg, e, "changed", 0)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return historyHas(e, "changed", "TimerScheduled") })
	resumeChanged.Store(true) // a deploy that removed the activity call
	if err := e.Signal(bg, "changed", "go", 7); err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(waitCtx(t))
	parkedWith(t, err, ErrNonDeterministic)
	if err := e.RetryCompensation(bg, "changed"); !errors.Is(err, ErrNotActive) {
		t.Fatalf("RetryCompensation of a code park: %v", err)
	}
	if err := e.Resume(bg, "changed"); err != nil { // still the wrong code: parks again
		t.Fatal(err)
	}
	waitFor(t, func() bool { s, _ := h.Status(bg); return s == StatusNeedsAttention })
	resumeChanged.Store(false) // the fix: the old code back
	if err := e.Resume(bg, "changed"); err != nil {
		t.Fatal(err)
	}
	if v, err := h.Wait(waitCtx(t)); err != nil || v != 7 {
		t.Fatalf("after the fix: %v %v", v, err)
	}
	if err := e.Resume(bg, "changed"); !errors.Is(err, ErrNotActive) {
		t.Fatalf("Resume of a completed execution: %v", err)
	}
	if err := e.Resume(bg, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Resume of an unknown id: %v", err)
	}
}

func TestStuckWorkflowIsParkedAndResumed(t *testing.T) {
	resumeSpin.Store(true)
	t.Cleanup(func() { resumeSpin.Store(false) })
	e := resumeEngine(t, "resume-stuck")
	h, err := resumeStuckWF.Start(bg, e, "stuck", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Signal(bg, "stuck", "go", 5); err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(waitCtx(t))
	parkedWith(t, err, ErrStuck)
	resumeSpin.Store(false) // the fix (it also lets the parked attempt's goroutine go)
	if err := e.Resume(bg, "stuck"); err != nil {
		t.Fatal(err)
	}
	if v, err := h.Wait(waitCtx(t)); err != nil || v != 5 {
		t.Fatalf("after the fix: %v %v", v, err)
	}
}

// T8.4 review: an activity that took effect but whose output cannot be
// encoded (a NaN) failed uncertainly: its compensator still runs, with its
// data.
var (
	nanUndone      atomic.Value
	nanOutActivity = DefineActivity("t84.nan", func(context.Context, Key, int) (float64, string, error) {
		return math.NaN(), "charge-1", nil
	}, func(ctx context.Context, _ Key, c string) error {
		nanUndone.Store(c + ":" + strconv.FormatBool(CompensationUncertain(ctx)))
		return nil
	})
	nanOutWF = DefineWorkflow("t84.nan-wf", 1, func(c Ctx, _ int) (float64, error) {
		return nanOutActivity.Run(c, 0)
	})
)

func TestUnencodableOutputIsCompensated(t *testing.T) {
	e := testEngine(t)
	_, err := nanOutWF.Run(waitCtx(t), e, "nan", 0)
	if !errors.Is(err, ErrCompensated) {
		t.Fatalf("got %v", err)
	}
	if got, _ := nanUndone.Load().(string); got != "charge-1:true" {
		t.Fatalf("compensator saw %q", got)
	}
}

// T8.4 review: a handle of one workflow on an instance of another fails
// instead of decoding the other's output.
func TestHandleOfAnotherWorkflow(t *testing.T) {
	e := testEngine(t)
	if _, err := nanOutWF.Start(bg, e, "owned", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := resumeStuckWF.Handle(e, "owned").Wait(waitCtx(t)); !errors.Is(err, ErrInstanceConflict) {
		t.Fatalf("got %v", err)
	}
}
