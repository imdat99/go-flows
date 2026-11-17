package flows

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store"
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
		_, err := SignalWorkflow(c, "sig-nobody", "hello", "x").Get(c)
		if !errors.Is(err, ErrSignalTargetNotFound) || errors.Is(err, ErrNotFound) {
			return "", fmt.Errorf("to an unknown instance: %w", err)
		}
		if in.Ended != "" {
			_, err := SignalWorkflow(c, in.Ended, "hello", "x").Get(c)
			if !errors.Is(err, ErrSignalTargetEnded) || errors.Is(err, ErrNotActive) {
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
	// sigParent starts a child and signals it in the same task, then waits
	// for the child's signal and its result.
	sigParent = DefineWorkflow("test.sig.parent", 1, func(c Ctx, _ struct{}) (string, error) {
		kid := InstanceID(c) + "-kid"
		f := sigKid.Child(c, kid, InstanceID(c))
		if _, err := SignalWorkflow(c, kid, "hello", "from parent").Get(c); err != nil {
			return "", err
		}
		msg, err := Await[string](c, "from-kid", 0)
		if err != nil {
			return "", err
		}
		out, err := f.Get(c)
		return msg + "|" + out, err
	})
	// sigKid waits for its parent's signal, and signals its parent as it
	// ends (not awaited).
	sigKid = DefineWorkflow("test.sig.kid", 1, func(c Ctx, parent string) (string, error) {
		got, err := Await[string](c, "hello", 0)
		if err != nil {
			return "", err
		}
		SignalWorkflow(c, parent, "from-kid", "from kid")
		return got, nil
	})
)

type sigInput struct {
	Target string `json:"target"`
	Ended  string `json:"ended"`
}

// sigStores are rcStores (memory, Postgres fast path, Postgres through
// pgx) and memory with the cache off: every task replays.
var sigStores = append(slices.Clip(rcStores), struct {
	name   string
	engine func(t *testing.T) *Engine
}{"mem-nocache", func(t *testing.T) *Engine {
	t.Helper()
	e, err := newEngine(mem.New(nil), Options{InstanceID: "sig", PollMax: 20 * time.Millisecond, CacheSize: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Stop(bg) })
	return e
}})

// T12.1: workflow A signals workflow B, which waits for it; errors for an
// unknown or ended target come back to A (their own sentinels, not
// ErrNotFound or ErrNotActive); A signals itself. With the cache off,
// every task of A replays its signals from the history: none is sent twice
// (B records exactly one).
func TestSignalWorkflow(t *testing.T) {
	for _, s := range sigStores {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			e := s.engine(t)
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
			if n := countEvents(t, e, "sig-b", "SignalReceived"); n != 1 {
				t.Fatalf("b received %d signals, want 1", n)
			}
			if n := countEvents(t, e, "sig-a", "SignalSent"); n != 4 {
				t.Fatalf("a sent %d signals, want 4", n)
			}
			if n := countEvents(t, e, "sig-a", "SignalReceived"); n != 1 {
				t.Fatalf("a received %d signals from itself, want 1", n)
			}
		})
	}
}

// T12.1: a parent starts a child and signals it in the same task; the
// child signals its parent in the task that ends it.
func TestSignalWorkflowParentAndChild(t *testing.T) {
	for _, s := range sigStores {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			e := s.engine(t)
			out, err := sigParent.Run(waitCtx(t), e, "sig-parent", struct{}{})
			if err != nil || out != "from kid|from parent" {
				t.Fatalf("parent: %q %v", out, err)
			}
			if n := countEvents(t, e, "sig-parent", "SignalReceived"); n != 1 {
				t.Fatalf("parent received %d signals, want 1", n)
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

// T12.1: the errors of a signal's target have kinds of their own: a
// workflow failing with ErrNotFound or ErrNotActive (an activity calling
// Engine.Signal, say) records no kind that would make its error match them,
// and theirs do not match ErrNotFound or ErrNotActive.
func TestSignalWorkflowKindsAreDistinct(t *testing.T) {
	for err, want := range map[error]string{
		ErrSignalTargetNotFound: store.KindSignalTargetNotFound, ErrSignalTargetEnded: store.KindSignalTargetEnded,
		ErrNotFound: "", ErrNotActive: "",
	} {
		if got := errorKinds(fmt.Errorf("x: %w", err)); got != want {
			t.Errorf("kinds of %v: %q, want %q", err, got, want)
		}
	}
	if errors.Is(ErrSignalTargetEnded, ErrNotActive) || errors.Is(ErrSignalTargetNotFound, ErrNotFound) {
		t.Fatal("signal target errors match the engine's sentinels")
	}
}
