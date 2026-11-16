package flows

import (
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// journal records what compensators did, per instance (the instance id is
// the prefix of an activity Key).
type journal struct {
	mu      sync.Mutex
	entries map[string][]string
}

func (j *journal) add(k Key, entry string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.entries == nil {
		j.entries = map[string][]string{}
	}
	id, _, _ := strings.Cut(string(k), ":")
	j.entries[id] = append(j.entries[id], entry)
}

// reset forgets the entries of ids (tests reuse ids with -count).
func (j *journal) reset(ids ...string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, id := range ids {
		delete(j.entries, id)
	}
}

func (j *journal) get(id string) []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return slices.Clone(j.entries[id])
}

var (
	comps journal

	errNoStock = errors.New("out of stock")

	// step returns its input as output and compensation data; its
	// compensator journals "undo <data>" (with "?" when uncertain).
	compStep = DefineActivity("comp.step", func(_ context.Context, _ Key, s string) (string, string, error) {
		return s, s, nil
	}, func(ctx context.Context, k Key, s string) error {
		mark := ""
		if CompensationUncertain(ctx) {
			mark = "?"
		}
		comps.add(k, "undo "+s+mark)
		return nil
	})
	compNoUndo = DefineActivity("comp.no-undo", func(_ context.Context, _ Key, s string) (string, struct{}, error) {
		return s, struct{}{}, nil
	}, nil)
	compFail = DefineActivity("comp.fail", func(context.Context, Key, string) (string, string, error) {
		return "", "never", Permanent(errNoStock)
	}, func(_ context.Context, k Key, s string) error {
		comps.add(k, "undo failed "+s)
		return nil
	})
	compSlow = DefineActivity("comp.slow", func(ctx context.Context, _ Key, s string) (string, string, error) {
		<-ctx.Done()
		return "", s, ctx.Err()
	}, func(ctx context.Context, k Key, s string) error {
		comps.add(k, "undo slow "+s+map[bool]string{true: "?"}[CompensationUncertain(ctx)])
		return nil
	}, WithRetry(RetryPolicy{Attempts: 1}))
	compTimeout = DefineActivity("comp.timeout", func(ctx context.Context, _ Key, s string) (string, string, error) {
		<-ctx.Done()
		return "", s, ctx.Err()
	}, func(ctx context.Context, k Key, s string) error {
		comps.add(k, "undo timeout "+s+map[bool]string{true: "?"}[CompensationUncertain(ctx)])
		return nil
	}, WithTimeout(20*time.Millisecond), WithRetry(RetryPolicy{Attempts: 1}))
	compBadData = DefineActivity("comp.bad-data", func(context.Context, Key, string) (string, float64, error) {
		return "ok", math.NaN(), nil // the effect happened; its data cannot be stored
	}, func(ctx context.Context, k Key, f float64) error {
		comps.add(k, "undo bad-data"+map[bool]string{true: "?"}[CompensationUncertain(ctx)])
		return nil
	})

	// flaky's compensator fails until flakyFixed.
	flakyFixed atomic.Bool
	compFlaky  = DefineActivity("comp.flaky", func(_ context.Context, _ Key, s string) (string, string, error) {
		return s, s, nil
	}, func(_ context.Context, k Key, s string) error {
		if !flakyFixed.Load() {
			return Permanent(errors.New("downstream is down"))
		}
		comps.add(k, "undo flaky "+s)
		return nil
	}, WithCompensateRetry(RetryPolicy{Attempts: 2, Initial: time.Millisecond}))

	// sagaWF runs the steps named in its input, then fails with
	// errNoStock (unless the last step is "ok").
	sagaWF = DefineWorkflow("comp.saga", 1, func(c Ctx, steps []string) (string, error) {
		for _, s := range steps {
			var err error
			switch {
			case s == "ok":
				return "done", nil
			case s == "no-undo":
				_, err = compNoUndo.Run(c, s)
			case s == "fail":
				_, err = compFail.Run(c, s)
			case s == "timeout":
				_, err = compTimeout.Run(c, s)
			case s == "bad-data":
				_, err = compBadData.Run(c, s)
			case strings.HasPrefix(s, "flaky"):
				_, err = compFlaky.Run(c, s)
			case s == "wait":
				_, err = Await[string](c, "go", time.Hour)
			case s == "slow+fail":
				compSlow.Start(c, "s")      // still running when the workflow fails
				_, err = compFail.Run(c, s) // fails at once
			default:
				_, err = compStep.Run(c, s)
			}
			if err != nil {
				return "", err
			}
		}
		return "", errNoStock
	})
	// handledWF recovers from a failed activity itself: nothing is
	// compensated.
	handledWF = DefineWorkflow("comp.handled", 1, func(c Ctx, _ int) (string, error) {
		if _, err := compStep.Run(c, "a"); err != nil {
			return "", err
		}
		if _, err := compFail.Run(c, "b"); errors.Is(err, errNoStock) {
			return "recovered", nil
		}
		return "", errors.New("unexpected")
	})
)

func init() { RegisterErrorKind("test_comp_no_stock", errNoStock) }

func runSaga(t *testing.T, e *Engine, id string, steps ...string) error {
	t.Helper()
	comps.reset(id)
	_, err := sagaWF.Run(waitCtx(t), e, id, steps)
	return err
}

func wantJournal(t *testing.T, id string, want ...string) {
	t.Helper()
	if got := comps.get(id); !slices.Equal(got, want) {
		t.Fatalf("%s compensations %q, want %q", id, got, want)
	}
}

func TestCompensationReverseOrder(t *testing.T) {
	e := testEngine(t)
	err := runSaga(t, e, "rev", "a", "b", "no-undo", "c")
	var re *RunError
	if !errors.As(err, &re) || re.Status != StatusCompensated || !errors.Is(err, ErrCompensated) || !errors.Is(err, errNoStock) {
		t.Fatalf("run error %v (%+v)", err, re)
	}
	wantJournal(t, "rev", "undo c", "undo b", "undo a")
}

// A certain failure is not compensated; the workflow's error stays.
func TestCompensationSkipsCertainFailure(t *testing.T) {
	e := testEngine(t)
	err := runSaga(t, e, "certain", "a", "fail")
	if !errors.Is(err, errNoStock) || !errors.Is(err, ErrCompensated) {
		t.Fatalf("%v", err)
	}
	wantJournal(t, "certain", "undo a")
}

// Nothing to compensate: the execution fails as before.
func TestNothingToCompensateFails(t *testing.T) {
	e := testEngine(t)
	err := runSaga(t, e, "nothing", "no-undo", "fail")
	var re *RunError
	if !errors.As(err, &re) || re.Status != StatusFailed || errors.Is(err, ErrCompensated) {
		t.Fatalf("%v", err)
	}
}

func TestHandledFailureIsNotCompensated(t *testing.T) {
	e := testEngine(t)
	comps.reset("handled")
	out, err := handledWF.Run(waitCtx(t), e, "handled", 0)
	if err != nil || out != "recovered" {
		t.Fatalf("%q %v", out, err)
	}
	wantJournal(t, "handled")
}

// T5.1: an activity that timed out, or whose compensation data cannot be
// stored, may have had its effect: it is compensated, told so.
func TestCompensationUncertain(t *testing.T) {
	e := testEngine(t)
	if err := runSaga(t, e, "unc-timeout", "a", "timeout"); !errors.Is(err, ErrTimeout) || !errors.Is(err, ErrCompensated) {
		t.Fatalf("%v", err)
	}
	wantJournal(t, "unc-timeout", "undo timeout timeout?", "undo a")
	if err := runSaga(t, e, "unc-data", "a", "bad-data"); !errors.Is(err, ErrCompensated) {
		t.Fatalf("%v", err)
	}
	wantJournal(t, "unc-data", "undo bad-data?", "undo a")
}

// An activity still running when the workflow fails is canceled; its
// outcome is awaited and compensated (uncertain) before the others.
func TestCompensationWaitsForRunningActivities(t *testing.T) {
	e := testEngine(t)
	if err := runSaga(t, e, "inflight", "a", "slow+fail"); !errors.Is(err, ErrCompensated) {
		t.Fatalf("%v", err)
	}
	wantJournal(t, "inflight", "undo slow s?", "undo a")
}

// A canceled workflow is compensated and ends canceled.
func TestCanceledWorkflowIsCompensated(t *testing.T) {
	e := testEngine(t)
	comps.reset("cancel")
	h, err := sagaWF.Start(bg, e, "cancel", []string{"a", "b", "wait"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(comps.get("cancel")) == 0 && stepsDone(t, e, "cancel") })
	if err := e.Cancel(bg, "cancel"); err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(waitCtx(t))
	var re *RunError
	if !errors.As(err, &re) || re.Status != StatusCanceled || !errors.Is(err, ErrCanceled) {
		t.Fatalf("%v", err)
	}
	wantJournal(t, "cancel", "undo b", "undo a")
}

// A compensation failing after its retries parks the execution; once fixed,
// RetryCompensation resumes it where it stopped.
func TestCompensationParkAndRetry(t *testing.T) {
	flakyFixed.Store(false)
	e := testEngine(t)
	err := runSaga(t, e, "park", "a", "flaky1", "b")
	var re *RunError
	if !errors.As(err, &re) || re.Status != StatusNeedsAttention || !errors.Is(err, ErrCompensationFailed) {
		t.Fatalf("parked: %v", err)
	}
	wantJournal(t, "park", "undo b")
	if err := e.RetryCompensation(bg, "park"); err != nil {
		t.Fatal(err)
	}
	if err := e.RetryCompensation(bg, "park"); !errors.Is(err, ErrNotActive) {
		t.Fatalf("retry while compensating: %v", err)
	}
	waitFor(t, func() bool { // compensating again, with the workflow's error
		ex, _ := e.st.GetExecution(bg, "park")
		return ex.Status != StatusCompensating || ex.Err == errNoStock.Error()
	})
	// Still broken: parks again.
	_, err = sagaWF.Handle(e, "park").Wait(waitCtx(t))
	if !errors.Is(err, ErrCompensationFailed) {
		t.Fatalf("parked again: %v", err)
	}
	flakyFixed.Store(true)
	if err := e.RetryCompensation(bg, "park"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		st, _ := sagaWF.Handle(e, "park").Status(bg)
		return st == StatusCompensated
	})
	_, err = sagaWF.Handle(e, "park").Wait(waitCtx(t))
	if !errors.Is(err, ErrCompensated) || !errors.Is(err, errNoStock) {
		t.Fatalf("after retry: %v", err)
	}
	wantJournal(t, "park", "undo b", "undo flaky flaky1", "undo a")
	if err := e.RetryCompensation(bg, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown: %v", err)
	}
}

// stepsDone reports whether the execution has recorded its "b" step.
func stepsDone(t *testing.T, e *Engine, id string) bool {
	t.Helper()
	ex, err := e.st.GetExecution(bg, id)
	if err != nil {
		return false
	}
	hist, _ := e.st.LoadHistory(bg, ex.ID, 0)
	n := 0
	for _, ev := range hist {
		if ev.Type.String() == "ActivityCompleted" {
			n++
		}
	}
	return n == 2
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
