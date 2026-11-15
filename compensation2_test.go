package flows

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
)

// engineOn starts an engine on st (engines sharing a store are processes
// sharing a database).
func engineOn(t *testing.T, st store.Store, id string) *Engine {
	t.Helper()
	e, err := newEngine(st, Options{InstanceID: id, PollMax: 20 * time.Millisecond, LeaseTTL: 2 * time.Second})
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
	// gate's compensator blocks until gateOpen is closed (or its context
	// ends, then it fails transiently).
	gateCalls atomic.Int32
	gateOpen  atomic.Pointer[chan struct{}]
	compGate  = DefineActivity("comp.gate", func(_ context.Context, _ Key, s string) (string, string, error) {
		return s, s, nil
	}, func(ctx context.Context, k Key, s string) error {
		gateCalls.Add(1)
		select {
		case <-*gateOpen.Load():
			comps.add(k, "undo gate "+s)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, WithCompensateRetry(RetryPolicy{Attempts: 10, Initial: time.Millisecond}))

	compPanic = DefineActivity("comp.panic", func(_ context.Context, _ Key, s string) (string, string, error) {
		return s, s, nil
	}, func(context.Context, Key, string) error {
		panic("compensator bug")
	}, WithCompensateRetry(RetryPolicy{Attempts: 1}))

	// backoff fails its first attempt and retries an hour later.
	compBackoff = DefineActivity("comp.backoff", func(context.Context, Key, string) (string, string, error) {
		return "", "", errors.New("try later")
	}, func(ctx context.Context, k Key, s string) error {
		comps.add(k, "undo backoff"+map[bool]string{true: "?"}[CompensationUncertain(ctx)])
		return nil
	}, WithRetry(RetryPolicy{Attempts: 3, Initial: time.Hour}))

	gateWF = DefineWorkflow("comp.gate-wf", 1, func(c Ctx, _ int) (int, error) {
		if _, err := compStep.Run(c, "a"); err != nil {
			return 0, err
		}
		if _, err := compGate.Run(c, "g"); err != nil {
			return 0, err
		}
		return 0, errNoStock
	})
	compPanicWF = DefineWorkflow("comp.panic-wf", 1, func(c Ctx, _ int) (int, error) {
		if _, err := compPanic.Run(c, "p"); err != nil {
			return 0, err
		}
		return 0, errNoStock
	})
	// openWorkWF ends with a child, a timer and an activity in retry
	// backoff still open.
	openWorkWF = DefineWorkflow("comp.open-work", 1, func(c Ctx, id string) (int, error) {
		if _, err := compStep.Run(c, "a"); err != nil {
			return 0, err
		}
		waitForeverWF.Child(c, id+"-child", 0)
		NewTimer(c, time.Hour)
		compBackoff.Start(c, "b")
		_, err := Signals[int](c, "never").Receive(c)
		return 0, err
	})
)

// Another process takes over a compensation whose engine stopped while the
// compensator ran.
func TestCompensationHandover(t *testing.T) {
	st := mem.New(nil)
	open := make(chan struct{})
	gateOpen.Store(&open)
	gateCalls.Store(0)
	comps.reset("handover")
	a := engineOn(t, st, "a")
	h, err := gateWF.Start(bg, a, "handover", 0)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return gateCalls.Load() == 1 })
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	_ = a.Stop(ctx) // the compensator is interrupted
	cancel()
	close(open)
	engineOn(t, st, "b")
	_, err = h.Wait(waitCtx(t))
	if !errors.Is(err, ErrCompensated) {
		t.Fatalf("%v", err)
	}
	wantJournal(t, "handover", "undo gate g", "undo a")
}

// Signals and cancel requests sent while compensating or parked are
// accepted and dropped; the compensation still ends (the end is not
// blocked by undelivered events).
func TestSignalsDuringCompensation(t *testing.T) {
	open := make(chan struct{})
	gateOpen.Store(&open)
	gateCalls.Store(0)
	comps.reset("sig")
	e := testEngine(t)
	h, err := gateWF.Start(bg, e, "sig", 0)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return gateCalls.Load() >= 1 })
	if st, _ := h.Status(bg); st != StatusCompensating {
		t.Fatalf("status %s", st)
	}
	ex, _ := e.st.GetExecution(bg, "sig")
	if ex.Err == "" {
		t.Fatal("the workflow's error is not visible while compensating")
	}
	for range 3 {
		if err := e.Signal(bg, "sig", "x", 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Cancel(bg, "sig"); err != nil {
		t.Fatal(err)
	}
	close(open)
	if _, err := h.Wait(waitCtx(t)); !errors.Is(err, ErrCompensated) || !errors.Is(err, errNoStock) {
		t.Fatalf("%v", err)
	}
	wantJournal(t, "sig", "undo gate g", "undo a")
}

// A panicking compensator parks the execution (a signal while parked is
// accepted).
func TestPanickingCompensatorParks(t *testing.T) {
	e := testEngine(t)
	_, err := compPanicWF.Run(waitCtx(t), e, "panic", 0)
	if !errors.Is(err, ErrCompensationFailed) || !errors.Is(err, ErrPanic) {
		t.Fatalf("%v", err)
	}
	if err := e.Signal(bg, "panic", "x", 1); err != nil {
		t.Fatalf("signal while parked: %v", err)
	}
}

// When the workflow ends, its open child is canceled, its timer dropped,
// and an activity waiting an hour for its retry is canceled at once (not
// compensated: its only attempt failed for certain).
func TestOpenWorkAtTheEnd(t *testing.T) {
	e := testEngine(t)
	comps.reset("open")
	h, err := openWorkWF.Start(bg, e, "open", "open")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		ex, err := e.st.GetExecution(bg, "open-child")
		return err == nil && ex.Status == StatusRunning
	})
	time.Sleep(100 * time.Millisecond) // the backoff activity's first attempt fails at once
	if err := e.Cancel(bg, "open"); err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(waitCtx(t))
	var re *RunError
	if !errors.As(err, &re) || re.Status != StatusCanceled {
		t.Fatalf("%v", err)
	}
	// The backoff activity's only attempt failed for certain (no effect):
	// canceled before its retry, it has nothing to undo.
	wantJournal(t, "open", "undo a")
	waitFor(t, func() bool {
		st, _ := waitForeverWF.Handle(e, "open-child").Status(bg)
		return st == StatusCanceled
	})
}
