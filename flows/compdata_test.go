package flows

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/testdb"
)

// keptComp's input is its plan: what attempt n does, as "timeout:<data>"
// (an uncertain failure), "fail:<data>" (a certain, retried failure),
// "decline" (a certain, permanent failure with no data) or "ok:<data>". Its
// compensator journals "refund <data>", with "?" when uncertain.
var (
	keptComp = DefineActivity("test.kept-comp", func(ctx context.Context, k Key, plan []string) (string, string, error) {
		n := int(countCall(k))
		if n > len(plan) {
			return "", "", Permanent(errors.New("no plan"))
		}
		do, data, _ := strings.Cut(plan[n-1], ":")
		switch do {
		case "timeout":
			<-ctx.Done()
			return "", data, ctx.Err()
		case "fail":
			return "", data, errors.New("transient")
		case "ok":
			return "done", data, nil
		}
		return "", "", Permanent(errors.New("declined"))
	}, func(ctx context.Context, k Key, c string) error {
		comps.add(k, "refund "+c+map[bool]string{true: "?"}[CompensationUncertain(ctx)])
		return nil
	}, WithTimeout(50*time.Millisecond), WithRetry(RetryPolicy{Attempts: 3, Initial: time.Millisecond}))
	keptCompWF = DefineWorkflow("test.kept-comp-wf", 1, func(c Ctx, plan []string) (string, error) {
		if _, err := keptComp.Run(c, plan); err != nil {
			return "", err
		}
		return "", errNoStock // undo the completed activity too
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
