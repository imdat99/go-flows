package flows

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store/mem"
)

func keepingEngine(t *testing.T, keep time.Duration) *Engine {
	t.Helper()
	e, err := newEngine(mem.New(nil), Options{
		InstanceID: "test", PollMax: 20 * time.Millisecond, LeaseTTL: 2 * time.Second,
		KeepPurgedIDs: keep,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Stop(bg) })
	return e
}

func purgeAll(t *testing.T, e *Engine) {
	t.Helper()
	time.Sleep(2 * time.Millisecond) // ended strictly before now
	if n, err := e.PurgeEnded(bg, 0, 100); err != nil || n == 0 {
		t.Fatalf("purge: %d %v", n, err)
	}
}

// T12.3: with KeepPurgedIDs, a purged instance id is refused by Start
// instead of running the workflow again, until forgotten.
func TestKeepPurgedIDs(t *testing.T) {
	e := keepingEngine(t, time.Hour)
	const id = "kp-order-1"
	if v, err := childWF.Run(waitCtx(t), e, id, 1); err != nil || v != 10 {
		t.Fatalf("first run: %d %v", v, err)
	}
	purgeAll(t, e)
	if _, err := childWF.Start(bg, e, id, 1); !errors.Is(err, ErrInstancePurged) {
		t.Fatalf("start of a purged id: %v", err)
	}
	if _, err := other.Start(bg, e, id, 1); !errors.Is(err, ErrInstancePurged) {
		t.Fatalf("start of a purged id by another workflow: %v", err)
	}
	if err := e.ForgetInstance(bg, id); err != nil {
		t.Fatal(err)
	}
	if err := e.ForgetInstance(bg, id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("forget twice: %v", err)
	}
	if err := e.ForgetInstance(bg, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("forget empty id: %v", err)
	}
	if v, err := childWF.Run(waitCtx(t), e, id, 2); err != nil || v != 20 {
		t.Fatalf("run after forget: %d %v", v, err)
	}
}

// Without KeepPurgedIDs (the default), a purged id starts again.
func TestPurgedIDsReusedByDefault(t *testing.T) {
	e := testEngine(t)
	const id = "kp-default-1"
	if _, err := childWF.Run(waitCtx(t), e, id, 1); err != nil {
		t.Fatal(err)
	}
	purgeAll(t, e)
	if v, err := childWF.Run(waitCtx(t), e, id, 3); err != nil || v != 30 {
		t.Fatalf("run again: %d %v", v, err)
	}
}

var purgedKidWF = DefineWorkflow("test.purged-kid-parent", 1, func(c Ctx, kid string) (string, error) {
	_, err := childWF.Child(c, kid, 5).Get(c)
	if !errors.Is(err, ErrInstancePurged) || !errors.Is(err, ErrInstanceExists) {
		return "", fmt.Errorf("child with a purged id: %w", err)
	}
	return err.Error(), nil
})

// A child started with a purged id fails, matching ErrInstancePurged and
// ErrInstanceExists; the parent carries on.
func TestPurgedChildID(t *testing.T) {
	e := keepingEngine(t, time.Hour)
	const kid = "kp-kid-1"
	if _, err := childWF.Run(waitCtx(t), e, kid, 1); err != nil {
		t.Fatal(err)
	}
	purgeAll(t, e)
	if msg, err := purgedKidWF.Run(waitCtx(t), e, "kp-parent-1", kid); err != nil {
		t.Fatalf("parent: %v", err)
	} else if msg != "child instance id purged" {
		t.Fatalf("child error %q", msg)
	}
	if _, err := childWF.Handle(e, kid).Wait(waitCtx(t)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("purged child started again: %v", err)
	}
}
