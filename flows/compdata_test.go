package flows

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/testdb"
	"github.com/imdat99/go-flows/internal/worker"
)

// keptRun's input is its plan: what attempt n does, as "timeout:<data>"
// (wait for the context: an uncertain failure under a timeout),
// "uncertain:<data>" (ErrUncertain, retried), "fail:<data>" (a certain,
// retried failure), "decline" (a certain, permanent failure with no data)
// or "ok:<data>".
func keptRun(ctx context.Context, k Key, plan []string) (string, string, error) {
	n := int(countCall(k))
	if n > len(plan) {
		return "", "", Permanent(errors.New("no plan"))
	}
	do, data, _ := strings.Cut(plan[n-1], ":")
	switch do {
	case "timeout":
		<-ctx.Done()
		return "", data, ctx.Err()
	case "uncertain":
		return "", data, fmt.Errorf("charge unknown: %w", worker.ErrUncertain)
	case "fail":
		return "", data, errors.New("transient")
	case "ok":
		return "done", data, nil
	}
	return "", "", Permanent(errors.New("declined"))
}

// keptRefund journals "refund <data>", with "?" when uncertain.
func keptRefund(ctx context.Context, k Key, c string) error {
	comps.add(k, "refund "+c+map[bool]string{true: "?"}[CompensationUncertain(ctx)])
	return nil
}

var (
	keptComp = DefineActivity("test.kept-comp", keptRun, keptRefund,
		WithTimeout(50*time.Millisecond), WithRetry(RetryPolicy{Attempts: 3, Initial: time.Millisecond}))
	keptCompWF = DefineWorkflow("test.kept-comp-wf", 1, func(c Ctx, plan []string) (string, error) {
		if _, err := keptComp.Run(c, plan); err != nil {
			return "", err
		}
		return "", errNoStock // undo the completed activity too
	})

	// keptSlowRetry retries after a long delay: the workflow cancels it
	// while its retry is pending.
	keptSlowRetry = DefineActivity("test.kept-slow-retry", keptRun, keptRefund,
		WithTimeout(50*time.Millisecond), WithRetry(RetryPolicy{Attempts: 3, Initial: time.Minute}))
	keptCancelWF = DefineWorkflow("test.kept-cancel-wf", 1, func(c Ctx, plan []string) (string, error) {
		ac, cancel := WithCancel(c)
		f := keptSlowRetry.Start(ac, plan)
		if err := Sleep(c, time.Second); err != nil {
			return "", err
		}
		cancel()
		if _, err := f.Get(c); err != nil {
			return "", err
		}
		return "", errNoStock
	})

	// keptNoTimeout runs until its engine stops.
	keptNoTimeout = DefineActivity("test.kept-no-timeout", keptRun, keptRefund,
		WithRetry(RetryPolicy{Attempts: 3, Initial: time.Millisecond}))
	keptStopWF = DefineWorkflow("test.kept-stop-wf", 1, func(c Ctx, plan []string) (string, error) {
		if _, err := keptNoTimeout.Run(c, plan); err != nil {
			return "", err
		}
		return "", errNoStock
	})
)

// T10.7: the compensator gets the data of the most recent attempt that may
// have had its effect and returned non-zero data, not the last attempt's
// (zero) data.
func TestCompDataOfUncertainAttemptIsKept(t *testing.T) {
	cases := []struct {
		name string
		plan []string
		want string
	}{
		// attempt 1 may have charged; attempt 2 is declined with no data.
		{"timeout-then-decline", []string{"timeout:charge-1", "decline"}, "refund charge-1?"},
		// the most recent uncertain attempt's data wins.
		{"two-timeouts-then-decline", []string{"timeout:charge-1", "timeout:charge-2", "decline"}, "refund charge-2?"},
		// a later attempt's own data wins.
		{"timeout-then-ok", []string{"timeout:charge-1", "ok:charge-2"}, "refund charge-2"},
		// a later success with zero data keeps the earlier data.
		{"timeout-then-ok-zero", []string{"timeout:charge-1", "ok:"}, "refund charge-1"},
		// a certain failure had no effect: its data is not kept.
		{"fail-then-timeouts", []string{"fail:x-1", "timeout:", "timeout:"}, "refund ?"},
		// an uncertain attempt's data survives a later certain retry.
		{"timeout-fail-decline", []string{"timeout:charge-1", "fail:x-2", "decline"}, "refund charge-1?"},
		// ErrUncertain (not only a timeout) keeps the data.
		{"uncertain-then-decline", []string{"uncertain:charge-1", "decline"}, "refund charge-1?"},
	}
	run := func(t *testing.T, e *Engine, prefix string) {
		t.Helper()
		for _, c := range cases {
			id := prefix + "-" + c.name
			comps.reset(id)
			resetCalls(id)
			_, err := keptCompWF.Run(waitCtx(t), e, id, c.plan)
			var re *RunError
			if !errors.As(err, &re) || re.Status != StatusCompensated {
				t.Fatalf("%s: %v", c.name, err)
			}
			wantJournal(t, id, c.want)
		}
	}
	for _, eager := range []bool{true, false} {
		name := map[bool]string{true: "mem-eager", false: "mem-poll"}[eager]
		t.Run(name, func(t *testing.T) {
			e, err := newEngine(mem.New(nil), Options{InstanceID: "kept", PollMax: 20 * time.Millisecond, LeaseTTL: 2 * time.Second, DisableEager: !eager})
			if err != nil {
				t.Fatal(err)
			}
			startStopped(t, e)
			run(t, e, name)
		})
	}
	t.Run("pg", func(t *testing.T) {
		pool := testdb.New(t)
		if err := Migrate(bg, pool); err != nil {
			t.Fatal(err)
		}
		e, err := New(pool, Options{InstanceID: "kept", PollMax: 50 * time.Millisecond, LeaseTTL: 2 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		startStopped(t, e)
		run(t, e, "pg")
	})
}

// T10.7 review: canceled by the workflow while a retry is pending after an
// uncertain attempt, the activity is compensated with that attempt's data.
func TestCompDataKeptWhenCanceledAfterUncertainAttempt(t *testing.T) {
	e := testEngine(t)
	id := "kept-cancel"
	comps.reset(id)
	resetCalls(id)
	_, err := keptCancelWF.Run(waitCtx(t), e, id, []string{"timeout:charge-1", "decline"})
	var re *RunError
	if !errors.As(err, &re) || re.Status != StatusCompensated {
		t.Fatalf("%v", err)
	}
	wantJournal(t, id, "refund charge-1?")
}

// T10.7 review: an attempt interrupted by its engine's stop keeps its data
// for the attempt another engine runs.
func TestCompDataKeptAcrossStop(t *testing.T) {
	st := mem.New(nil)
	opts := Options{InstanceID: "stop-1", PollMax: 20 * time.Millisecond, LeaseTTL: 2 * time.Second}
	e1, err := newEngine(st, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := e1.Start(bg); err != nil {
		t.Fatal(err)
	}
	id := "kept-stop"
	comps.reset(id)
	resetCalls(id)
	if _, err := keptStopWF.Start(bg, e1, id, []string{"timeout:charge-1", "decline"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for callsOf(id+":1:"+keptNoTimeout.Name()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("attempt 1 did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(bg, 100*time.Millisecond)
	_ = e1.Stop(ctx) // interrupts attempt 1
	cancel()
	opts.InstanceID = "stop-2"
	e2, err := newEngine(st, opts)
	if err != nil {
		t.Fatal(err)
	}
	startStopped(t, e2)
	_, err = keptStopWF.Handle(e2, id).Wait(waitCtx(t))
	var re *RunError
	if !errors.As(err, &re) || re.Status != StatusCompensated {
		t.Fatalf("%v", err)
	}
	wantJournal(t, id, "refund charge-1?")
}

// panicZero's MarshalJSON panics on its zero value.
type panicZero struct{ V int }

func (p panicZero) MarshalJSON() ([]byte, error) {
	if p.V == 0 {
		panic("zero")
	}
	return []byte(strconv.Itoa(p.V)), nil
}

// T10.7 review: a C whose zero value cannot be encoded (even by a panic)
// has no zero encoding; defining the activity does not crash.
func TestZeroEncodingRecovers(t *testing.T) {
	if b := zeroEncoding(newCodec[panicZero]()); b != nil {
		t.Fatalf("zero encoding %q", b)
	}
	if b := zeroEncoding(newCodec[string]()); string(b) != `""` {
		t.Fatalf("zero encoding %q", b)
	}
	_ = DefineActivity("test.panic-zero", func(context.Context, Key, int) (int, panicZero, error) {
		return 0, panicZero{V: 1}, nil
	}, func(context.Context, Key, panicZero) error { return nil })
}

// startStopped starts e and stops it with the test.
func startStopped(t *testing.T, e *Engine) {
	t.Helper()
	if err := e.Start(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})
}
