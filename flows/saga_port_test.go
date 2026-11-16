package flows

// Behaviour tests ported from the saga engine's suite (saga_test.go,
// async_test.go, review_fixes_test.go, lock_test.go), on the event-history
// model. Cases covered elsewhere are not repeated: compensation order,
// park and retry, uncertain timeouts, hooks, observers and most lock cases
// (compensation_test.go, hooks_test.go, lock_test.go).

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/imdat99/go-flows/internal/store/mem"
)

var (
	// calls counts invocations per activity Key.
	calls sync.Map // Key -> *atomic.Int32

	errFlaky = errors.New("flaky")

	countCall = func(k Key) int32 {
		c, _ := calls.LoadOrStore(k, new(atomic.Int32))
		return c.(*atomic.Int32).Add(1)
	}

	// portFlaky fails its first two attempts (transiently).
	portFlaky = DefineActivity("port.flaky", func(_ context.Context, k Key, _ int) (int32, struct{}, error) {
		if n := countCall(k); n < 3 {
			return 0, struct{}{}, errFlaky
		}
		c, _ := calls.Load(k)
		return c.(*atomic.Int32).Load(), struct{}{}, nil
	}, nil, WithRetry(RetryPolicy{Attempts: 5, Initial: time.Millisecond}))
	portPermanent = DefineActivity("port.permanent", func(_ context.Context, k Key, _ int) (int, struct{}, error) {
		countCall(k)
		return 0, struct{}{}, Permanent(errFlaky)
	}, nil, WithRetry(RetryPolicy{Attempts: 5, Initial: time.Millisecond}))
	portCounted = DefineActivity("port.counted", func(_ context.Context, k Key, s string) (string, string, error) {
		countCall(k)
		return s, s, nil
	}, func(ctx context.Context, k Key, s string) error {
		comps.add(k, "undo "+s+map[bool]string{true: "?"}[CompensationUncertain(ctx)])
		return nil
	})
	portPanic = DefineActivity("port.panic", func(context.Context, Key, string) (string, string, error) {
		panic("activity bug")
	}, func(ctx context.Context, k Key, _ string) error {
		comps.add(k, "undo panic"+map[bool]string{true: "?"}[CompensationUncertain(ctx)])
		return nil
	}, WithRetry(RetryPolicy{Attempts: 3, Initial: time.Millisecond}))
	portBadText = DefineActivity("port.bad-text", func(context.Context, Key, int) (int, struct{}, error) {
		return 0, struct{}{}, Permanent(errors.New("bad \x00 text \xff\xfe"))
	}, nil)

	// portWF runs the steps of its input: "flaky", "permanent", "panic",
	// "bad-text", "wf-panic", "await" (1s timeout), "signal" (wait for "go"),
	// "lock" (key "port"), or a counted step.
	portWF = DefineWorkflow("port.wf", 1, func(c Ctx, steps []string) (string, error) {
		var out strings.Builder
		for _, s := range steps {
			var err error
			switch s {
			case "flaky":
				var n int32
				n, err = portFlaky.Run(c, 0)
				out.WriteString("flaky@" + strconv.Itoa(int(n)))
			case "permanent":
				_, err = portPermanent.Run(c, 0)
			case "panic":
				_, err = portPanic.Run(c, s)
			case "bad-text":
				_, err = portBadText.Run(c, 0)
			case "wf-panic":
				panic("workflow bug")
			case "await":
				_, err = Await[int](c, "never", 50*time.Millisecond)
			case "signal":
				var v int
				v, err = Await[int](c, "go", time.Hour)
				out.WriteString("signal@" + strconv.Itoa(v))
			case "lock":
				err = Lock(c, "port", time.Hour)
			default:
				_, err = portCounted.Run(c, s)
			}
			if err != nil {
				return out.String(), err
			}
		}
		return out.String(), nil
	})
)

// callsOf counts the calls of the activity "instanceID:scheduleID:name" in
// every execution of the instance (keys end with the execution id).
func callsOf(k string) int32 {
	var n int32
	calls.Range(func(key, c any) bool {
		if strings.HasPrefix(string(key.(Key)), k+":") {
			n += c.(*atomic.Int32).Load()
		}
		return true
	})
	return n
}

// resetCalls forgets the counts of instance id (tests reuse ids with -count).
func resetCalls(id string) {
	calls.Range(func(k, _ any) bool {
		if strings.HasPrefix(string(k.(Key)), id+":") {
			calls.Delete(k)
		}
		return true
	})
}

func runPort(t *testing.T, e *Engine, id string, steps ...string) (string, error) {
	t.Helper()
	comps.reset(id)
	resetCalls(id)
	return portWF.Run(waitCtx(t), e, id, steps)
}

// saga TestConcurrentRunsSameKeyExecuteOnce: concurrent starts with one
// instance id run one execution; every caller gets its result.
func TestConcurrentStartsRunOnce(t *testing.T) {
	e := testEngine(t)
	resetCalls("once")
	const n = 8
	var wg sync.WaitGroup
	outs := make([]string, n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() { outs[i], errs[i] = portWF.Run(waitCtx(t), e, "once", []string{"a"}) })
	}
	wg.Wait()
	for i := range n {
		if errs[i] != nil || outs[i] != outs[0] {
			t.Fatalf("caller %d: %q %v", i, outs[i], errs[i])
		}
	}
	if got := callsOf("once:1:port.counted"); got != 1 {
		t.Fatalf("activity ran %d times", got)
	}
}

// saga TestTransientFailuresAreRetriedDurably / TestPermanentErrorIsNotRetried.
func TestRetriesAreEngineSide(t *testing.T) {
	e := testEngine(t)
	out, err := runPort(t, e, "retry", "flaky")
	if err != nil || out != "flaky@3" {
		t.Fatalf("%q %v", out, err)
	}
	_, err = runPort(t, e, "perm", "permanent")
	var re *RunError
	if !errors.As(err, &re) || re.Status != StatusFailed || !IsPermanent(err) {
		t.Fatalf("%v", err)
	}
	if got := callsOf("perm:1:port.permanent"); got != 1 {
		t.Fatalf("permanent error retried: %d attempts", got)
	}
}

// saga TestPanickingStepFailsAndIsCompensated / TestPanickingWorkflowIsCompensated.
func TestPanicsAreCompensated(t *testing.T) {
	e := testEngine(t)
	_, err := runPort(t, e, "act-panic", "a", "panic")
	if !errors.Is(err, ErrPanic) || !errors.Is(err, ErrCompensated) {
		t.Fatalf("activity panic: %v", err)
	}
	wantJournal(t, "act-panic", "undo panic?", "undo a")
	_, err = runPort(t, e, "wf-panic", "a", "wf-panic")
	if !errors.Is(err, ErrPanic) || !errors.Is(err, ErrCompensated) {
		t.Fatalf("workflow panic: %v", err)
	}
	wantJournal(t, "wf-panic", "undo a")
}

// saga TestAwaitTimeoutCompensates.
func TestAwaitTimeoutCompensates(t *testing.T) {
	e := testEngine(t)
	_, err := runPort(t, e, "await-to", "a", "await")
	if !errors.Is(err, ErrAwaitTimeout) || !errors.Is(err, ErrCompensated) {
		t.Fatalf("%v", err)
	}
	wantJournal(t, "await-to", "undo a")
}

// saga TestSignalBeforeAwaitIsKept: a signal sent before the workflow
// waits for it is delivered when it does.
func TestSignalBeforeAwaitIsKept(t *testing.T) {
	e := testEngine(t)
	comps.reset("early")
	h, err := portWF.Start(bg, e, "early", []string{"a", "signal"})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Signal(bg, "early", "go", 7); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Wait(waitCtx(t)); err != nil || out != "signal@7" {
		t.Fatalf("%q %v", out, err)
	}
}

// saga TestUnstorableErrorText: invalid UTF-8 and NULs in an error become
// storable text; the error still matches its kinds.
func TestUnstorableErrorText(t *testing.T) {
	e := testEngine(t)
	_, err := runPort(t, e, "bad-text", "bad-text")
	var re *RunError
	if !errors.As(err, &re) || !IsPermanent(err) {
		t.Fatalf("%v", err)
	}
	if msg := err.Error(); !utf8.ValidString(msg) || strings.Contains(msg, "\x00") || !strings.Contains(msg, "bad") {
		t.Fatalf("stored text %q", msg)
	}
}

// saga TestStopHandOverIsTakenByAnotherEngine: an execution waiting on an
// engine that stops continues on another one; completed activities are
// not run again.
func TestStopHandOverIsTakenByAnotherEngine(t *testing.T) {
	st := mem.New(nil)
	// A long stickiness: only Stop's hand-over lets b take the execution
	// within the test's time.
	a, err := newEngine(st, Options{InstanceID: "a", PollMax: 20 * time.Millisecond, StickyFor: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(bg); err != nil {
		t.Fatal(err)
	}
	comps.reset("handoff")
	resetCalls("handoff")
	h, err := portWF.Start(bg, a, "handoff", []string{"x", "signal", "y"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return callsOf("handoff:1:port.counted") == 1 && historyHas(a, "handoff", "TimerScheduled")
	})
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	if err := a.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	b := engineOn(t, st, "b")
	if err := b.Signal(bg, "handoff", "go", 1); err != nil {
		t.Fatal(err)
	}
	wctx, wcancel := context.WithTimeout(bg, 3*time.Second)
	defer wcancel()
	if out, err := h.Wait(wctx); err != nil || out != "signal@1" {
		t.Fatalf("%q %v", out, err)
	}
	if got := callsOf("handoff:1:port.counted"); got != 1 {
		t.Fatalf("completed activity ran %d times", got)
	}
}

// saga TestShorterReplayIsNonDeterministic (and the replay checks in
// general): code that no longer issues what its history recorded is
// parked, not failed. (A replay ending early is covered by the executor's
// TestGW11 cases.)
var (
	shortReplay atomic.Bool
	changingWF  = DefineWorkflow("port.changing", 1, func(c Ctx, _ int) (int, error) {
		if !shortReplay.Load() {
			if _, err := portCounted.Run(c, "first"); err != nil {
				return 0, err
			}
		}
		_, err := Await[int](c, "go", time.Hour)
		return 0, err
	})
)

func TestChangedReplayIsParked(t *testing.T) {
	shortReplay.Store(false)
	e, err := newEngine(mem.New(nil), Options{InstanceID: "nd", CacheSize: -1, PollMax: 20 * time.Millisecond})
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
		shortReplay.Store(false)
	})
	h, err := changingWF.Start(bg, e, "changing", 0)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return historyHas(e, "changing", "TimerScheduled") })
	shortReplay.Store(true) // a deploy that removed the activity call
	if err := e.Signal(bg, "changing", "go", 1); err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(waitCtx(t))
	var re *RunError
	if !errors.As(err, &re) || re.Status != StatusNeedsAttention || !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("%v", err)
	}
}

// saga TestLockReleaseOnCompensatedKeptOnNeedsAttention: a compensated
// execution releases its lock at once; a parked one keeps it (until its
// ttl lapses).
func TestLockReleasedOnEndKeptWhenParked(t *testing.T) {
	flakyFixed.Store(false)
	e := testEngine(t)
	// Compensated: released.
	if _, err := runPort(t, e, "lock-comp", "lock", "a", "permanent"); !errors.Is(err, ErrCompensated) {
		t.Fatalf("%v", err)
	}
	if _, err := runPort(t, e, "lock-next", "lock"); err != nil {
		t.Fatalf("lock after a compensated holder: %v", err)
	}
	// Parked (its compensation fails): kept.
	comps.reset("lock-park")
	if _, err := lockParkWF.Run(waitCtx(t), e, "lock-park", 0); !errors.Is(err, ErrCompensationFailed) {
		t.Fatalf("%v", err)
	}
	if _, err := runPort(t, e, "lock-after-park", "lock"); !errors.Is(err, ErrLockHeld) {
		t.Fatalf("lock of a parked holder: %v", err)
	}
}

var lockParkWF = DefineWorkflow("port.lock-park", 1, func(c Ctx, _ int) (int, error) {
	if err := Lock(c, "port", time.Hour); err != nil {
		return 0, err
	}
	if _, err := compFlaky.Run(c, "x"); err != nil {
		return 0, err
	}
	return 0, errNoStock
})

var (
	// timeoutThenDecline times out on its first attempt (the charge may
	// have gone through), then is declined for certain.
	timeoutThenDecline = DefineActivity("port.timeout-decline", func(ctx context.Context, k Key, _ int) (string, string, error) {
		if countCall(k) == 1 {
			<-ctx.Done()
			return "", "charge-1", ctx.Err()
		}
		return "", "", Permanent(errors.New("declined"))
	}, func(ctx context.Context, k Key, c string) error {
		comps.add(k, "refund "+c+map[bool]string{true: "?"}[CompensationUncertain(ctx)])
		return nil
	}, WithTimeout(30*time.Millisecond), WithRetry(RetryPolicy{Attempts: 3, Initial: time.Millisecond}))
	timeoutDeclineWF = DefineWorkflow("port.timeout-decline-wf", 1, func(c Ctx, _ int) (string, error) {
		return timeoutThenDecline.Run(c, 0)
	})
)

// saga TestStepTimeoutIsRetriedThenCompensatedAsUncertain (review of
// T5.6): an attempt that timed out may have had its effect; a later certain
// failure does not undo that: the activity is compensated, as uncertain.
func TestUncertainAttemptThenCertainFailureIsCompensated(t *testing.T) {
	for _, eager := range []bool{true, false} {
		st := mem.New(nil)
		e, err := newEngine(st, Options{InstanceID: "unc", PollMax: 20 * time.Millisecond, DisableEager: !eager})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Start(bg); err != nil {
			t.Fatal(err)
		}
		comps.reset("unc-retry")
		resetCalls("unc-retry")
		_, err = timeoutDeclineWF.Run(waitCtx(t), e, "unc-retry", 0)
		var re *RunError
		if !errors.As(err, &re) || re.Status != StatusCompensated {
			t.Fatalf("eager=%v: %v", eager, err)
		}
		wantJournal(t, "unc-retry", "refund charge-1?") // the timed-out attempt's data is kept (T10.7)
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		_ = e.Stop(ctx)
		cancel()
	}
}

// saga TestBadSignalPayloadFailsTheStep: a signal that cannot be decoded
// fails the wait with a decode error (the workflow decides).
func TestBadSignalPayload(t *testing.T) {
	e := testEngine(t)
	comps.reset("bad-signal")
	h, err := portWF.Start(bg, e, "bad-signal", []string{"a", "signal"})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Signal(bg, "bad-signal", "go", "not a number"); err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(waitCtx(t))
	if !errors.Is(err, ErrCompensated) || !strings.Contains(err.Error(), "cannot unmarshal") {
		t.Fatalf("%v", err)
	}
	wantJournal(t, "bad-signal", "undo a")
}

// saga TestReplayCheckNoFalsePositive: a workflow using every
// deterministic API replays without a false non-determinism, with the
// cache off (every task replays from the start).
var everyAPI = DefineWorkflow("port.every-api", 1, func(c Ctx, _ int) (string, error) {
	_ = Now(c)
	r1, err := Rand(c)
	if err != nil {
		return "", err
	}
	id, err := SideEffect(c, func() string { return "id-" + strconv.FormatUint(r1%1000, 10) })
	if err != nil {
		return "", err
	}
	if err := Sleep(c, 10*time.Millisecond); err != nil {
		return "", err
	}
	if _, err := Await[int](c, "never", 10*time.Millisecond); !errors.Is(err, ErrAwaitTimeout) {
		return "", errors.New("await did not time out")
	}
	if err := Lock(c, "every-api", time.Minute); err != nil {
		return "", err
	}
	f := portCounted.Start(c, "a")
	g := portCounted.Start(c, "b")
	var first string
	Select(c,
		OnFuture(f, func(_ Ctx, v string, _ error) { first = v }),
		OnFuture(g, func(_ Ctx, v string, _ error) { first = v }),
	)
	if _, err := f.Get(c); err != nil {
		return "", err
	}
	if _, err := g.Get(c); err != nil {
		return "", err
	}
	done := false
	Go(c, func(c Ctx) {
		_ = Sleep(c, 5*time.Millisecond)
		done = true
	})
	v, err := Await[int](c, "go", time.Minute)
	if err != nil {
		return "", err
	}
	kid := handledWF.Child(c, "every-api-kid", 0)
	if _, err := kid.Get(c); err != nil {
		return "", err
	}
	if !done {
		return "", errors.New("coroutine did not run")
	}
	return id + "/" + first + "/" + strconv.Itoa(v), nil
})

func TestColdReplayHasNoFalsePositive(t *testing.T) {
	e, err := newEngine(mem.New(nil), Options{InstanceID: "cold", CacheSize: -1, PollMax: 20 * time.Millisecond})
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
	resetCalls("every")
	h, err := everyAPI.Start(bg, e, "every", 0)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return historyHas(e, "every", "ActivityCompleted") })
	time.Sleep(50 * time.Millisecond)
	if err := e.Signal(bg, "every", "go", 5); err != nil {
		t.Fatal(err)
	}
	out, err := h.Wait(waitCtx(t))
	if err != nil || !strings.HasSuffix(out, "/5") {
		t.Fatalf("%q %v", out, err)
	}
}
