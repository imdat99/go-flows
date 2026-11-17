//go:build chaos

package chaos_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	flows "github.com/imdat99/go-flows/flows"
)

// exitCode is the status of a child that the activity killed (os.Exit).
const exitCode = 3

// exiting kills its process on every attempt (as an out-of-memory crash or
// a fatal runtime error would): two attempts allowed. Its compensator
// records the undo (it is uncertain: it may have had its effect).
var (
	exiting = flows.DefineActivity("chaos.exit", func(ctx context.Context, k flows.Key, v int) (int, string, error) {
		if err := effect(ctx, "chaos.exit", k); err != nil {
			return 0, "", err
		}
		os.Exit(exitCode)
		return v, "", nil
	}, func(ctx context.Context, k flows.Key, _ string) error {
		return effect(ctx, "chaos.undo-exit", flows.Key(string(k)+"/undo"))
	}, flows.WithRetry(flows.RetryPolicy{Attempts: 2, Initial: 10 * time.Millisecond}))
	exitingWF = flows.DefineWorkflow("chaos.exiting", 1, func(c flows.Ctx, in int) (int, error) {
		return exiting.Run(c, in)
	})
)

// childExit starts (or rejoins) the exiting workflow and waits for it: the
// engine dies in the activity, or reports how the workflow ended.
func childExit(ctx context.Context, eng *flows.Engine) {
	h, err := exitingWF.Start(ctx, eng, "exit-0", 1)
	if err == nil {
		wctx, cancel := context.WithTimeout(ctx, time.Minute)
		_, err = h.Wait(wctx)
		cancel()
	}
	fmt.Printf("RESULT: %v\n", err)
}

// I (T11.3): an activity that kills its process is not retried forever
// across the cluster. Each engine that claims it dies; once its attempts
// reach the retry policy's limit, the next engine fails it permanently
// (ErrAttemptsExhausted) instead of running it, and the workflow
// compensates.
func TestActivityKillingItsProcessEnds(t *testing.T) {
	pool := setup(t)
	var out string
	for i := range 5 {
		cmd, buf := spawn(t, pool, "exit", 1)
		werr := cmd.Wait()
		out = buf.String()
		if strings.Contains(out, "RESULT:") {
			break
		}
		var ee interface{ ExitCode() int }
		if !errors.As(werr, &ee) || ee.ExitCode() != exitCode {
			t.Fatalf("child %d: %v\n%s", i, werr, out)
		}
	}
	if !strings.Contains(out, "RESULT:") || !strings.Contains(out, "attempts exhausted") {
		t.Fatalf("the last child did not end the workflow with ErrAttemptsExhausted:\n%s", out)
	}
	if n := count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE step = 'chaos.exit'`); n != 2 {
		t.Fatalf("activity ran %d times, want 2 (its retry policy)", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM chaos_effect WHERE step = 'chaos.undo-exit'`); n != 1 {
		t.Fatalf("compensations %d, want 1", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM flows.execution WHERE instance_id = 'exit-0' AND status = 'compensated'`); n != 1 {
		t.Fatal("the workflow did not end compensated")
	}
}
