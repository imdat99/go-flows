package flows

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store/mem"
)

var (
	// sigWaiter waits for "hello", then sleeps (a later task, where a
	// signal sent twice would show), and returns what it got.
	sigWaiter = DefineWorkflow("test.sig.waiter", 1, func(c Ctx, _ struct{}) (string, error) {
		got, err := Await[string](c, "hello", 0)
		if err != nil {
			return "", err
		}
		return got, Sleep(c, 150*time.Millisecond)
	})
	// sigSender signals target, checks the errors of an unknown and of
	// an ended target, signals itself, and sleeps (more tasks: with the
	// cache off, each one replays the signals).
	sigSender = DefineWorkflow("test.sig.sender", 1, func(c Ctx, in sigInput) (string, error) {
		if _, err := SignalWorkflow(c, in.Target, "hello", "hi from "+InstanceID(c)).Get(c); err != nil {
			return "", err
		}
		if _, err := SignalWorkflow(c, "sig-nobody", "hello", "x").Get(c); !errors.Is(err, ErrNotFound) {
			return "", fmt.Errorf("to an unknown instance: %w", err)
		}
		if in.Ended != "" {
			if _, err := SignalWorkflow(c, in.Ended, "hello", "x").Get(c); !errors.Is(err, ErrNotActive) {
				return "", fmt.Errorf("to an ended instance: %w", err)
			}
		}
		if _, err := SignalWorkflow(c, InstanceID(c), "self", 42).Get(c); err != nil {
			return "", err
		}
		n, err := Await[int](c, "self", 0)
		if err != nil || n != 42 {
			return "", fmt.Errorf("self signal: %d %w", n, err)
		}
		for range 3 {
			if err := Sleep(c, 20*time.Millisecond); err != nil {
				return "", err
			}
		}
		return "sent", nil
	})
	// sigNotify signals its target as it returns, without awaiting.
	sigNotify = DefineWorkflow("test.sig.notify", 1, func(c Ctx, target string) (struct{}, error) {
		SignalWorkflow(c, target, "hello", "bye")
		return struct{}{}, nil
	})
)

type sigInput struct {
	Target string `json:"target"`
	Ended  string `json:"ended"`
}

func countSignalEvents(t *testing.T, st *mem.Store, instanceID string, typ history.Type) int {
	t.Helper()
	n := 0
	for _, ex := range st.Executions() {
		if ex.InstanceID != instanceID {
			continue
		}
		hist, err := st.LoadHistory(bg, ex.ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range hist {
			if ev.Type == typ {
				n++
			}
		}
	}
	return n
}

// T12.1: workflow A signals workflow B, which waits for it; errors for an
// unknown or ended target come back to A; A signals itself. With the
// cache off, every task of A replays its signals from the history: none is
// sent twice (B records exactly one).
func TestSignalWorkflow(t *testing.T) {
	for _, cache := range []int{0, -1} {
		t.Run(fmt.Sprintf("cache=%d", cache), func(t *testing.T) {
			st := mem.New(nil)
			e, err := newEngine(st, Options{InstanceID: "sig", PollMax: 20 * time.Millisecond, CacheSize: cache})
			if err != nil {
				t.Fatal(err)
			}
			if err := e.Start(bg); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Stop(bg) })
			ctx := waitCtx(t)

			if _, err := sigWaiter.Start(ctx, e, "sig-ended", struct{}{}); err != nil {
				t.Fatal(err)
			}
			b, err := sigWaiter.Start(ctx, e, "sig-b", struct{}{})
			if err != nil {
				t.Fatal(err)
			}
			if err := e.Signal(ctx, "sig-ended", "hello", "x"); err != nil { // ends it
				t.Fatal(err)
			}
			if _, err := sigWaiter.Handle(e, "sig-ended").Wait(ctx); err != nil {
				t.Fatal(err)
			}
			out, err := sigSender.Run(ctx, e, "sig-a", sigInput{Target: "sig-b", Ended: "sig-ended"})
			if err != nil || out != "sent" {
				t.Fatalf("sender: %q %v", out, err)
			}
			got, err := b.Wait(ctx)
			if err != nil || got != "hi from sig-a" {
				t.Fatalf("waiter: %q %v", got, err)
			}
			if n := countSignalEvents(t, st, "sig-b", history.SignalReceived); n != 1 {
				t.Fatalf("b received %d signals, want 1", n)
			}
			if n := countSignalEvents(t, st, "sig-a", history.SignalSent); n != 4 {
				t.Fatalf("a sent %d signals, want 4", n)
			}
			if n := countSignalEvents(t, st, "sig-a", history.SignalReceived); n != 1 {
				t.Fatalf("a received %d signals from itself, want 1", n)
			}
		})
	}
}

// T12.1: a signal sent as the workflow returns, not awaited, is sent.
func TestSignalWorkflowAtTheEnd(t *testing.T) {
	e := testEngine(t)
	ctx := waitCtx(t)
	b, err := sigWaiter.Start(ctx, e, "sig-end-b", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sigNotify.Run(ctx, e, "sig-end-a", "sig-end-b"); err != nil {
		t.Fatal(err)
	}
	if got, err := b.Wait(ctx); err != nil || got != "bye" {
		t.Fatalf("waiter: %q %v", got, err)
	}
}

// T12.1: invalid arguments fail the call (permanent), not the workflow.
func TestSignalWorkflowInvalid(t *testing.T) {
	e := testEngine(t)
	if out, err := sigInvalid.Run(waitCtx(t), e, "sig-invalid", struct{}{}); err != nil || out != "ok" {
		t.Fatalf("%q %v", out, err)
	}
}

var sigInvalid = DefineWorkflow("test.sig.invalid", 1, func(c Ctx, _ struct{}) (string, error) {
	for _, args := range [][2]string{{"", "n"}, {"x", ""}, {"x\x00", "n"}} {
		if _, err := SignalWorkflow(c, args[0], args[1], 1).Get(c); !errors.Is(err, ErrInvalidArgument) || !IsPermanent(err) {
			return "", fmt.Errorf("%q: %w", args, err)
		}
	}
	_, err := SignalWorkflow(c, "x", "n", func() {}).Get(c) // not encodable
	if err == nil || !IsPermanent(err) {
		return "", fmt.Errorf("unencodable: %w", err)
	}
	return "ok", nil
})
