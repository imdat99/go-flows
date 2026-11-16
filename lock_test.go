package flows

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store/mem"
)

type lockIn struct {
	Key   string        `json:"key"`
	Wait  bool          `json:"wait"`  // LockWait
	Await bool          `json:"await"` // hold the lock until signal "go"
	Twice bool          `json:"twice"` // take it again (reentrant)
	Sleep time.Duration `json:"sleep"` // then sleep (not running) this long
}

var lockWF = DefineWorkflow("test.lock", 1, func(c Ctx, in lockIn) (string, error) {
	var opts []LockOption
	if in.Wait {
		opts = append(opts, LockWait(RetryPolicy{Attempts: 200, Initial: 10 * time.Millisecond}))
	}
	if _, err := compStep.Run(c, "before-lock"); err != nil {
		return "", err
	}
	if err := Lock(c, in.Key, time.Second, opts...); err != nil {
		return "", err
	}
	if in.Twice {
		if err := Lock(c, in.Key, time.Second); err != nil {
			return "", err
		}
	}
	if in.Await {
		if _, err := Await[int](c, "go", time.Hour); err != nil {
			return "", err
		}
	}
	if in.Sleep > 0 {
		if err := Sleep(c, in.Sleep); err != nil {
			return "", err
		}
	}
	return "locked " + in.Key, nil
})

func startLock(t *testing.T, e *Engine, id string, in lockIn) *Handle[string] {
	t.Helper()
	comps.reset(id)
	h, err := lockWF.Start(bg, e, id, in)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// historyHas reports whether id's history has an event of type name.
func historyHas(e *Engine, id, name string) bool {
	ex, err := e.st.GetExecution(bg, id)
	if err != nil {
		return false
	}
	hist, _ := e.st.LoadHistory(bg, ex.ID, 0)
	for _, ev := range hist {
		if ev.Type.String() == name {
			return true
		}
	}
	return false
}

func TestLockIsExclusive(t *testing.T) {
	e := testEngine(t)
	a := startLock(t, e, "lock-a", lockIn{Key: "k", Await: true, Twice: true})
	waitFor(t, func() bool { return historyHas(e, "lock-a", "LockAcquired") })
	_, err := startLock(t, e, "lock-b", lockIn{Key: "k"}).Wait(waitCtx(t))
	if !errors.Is(err, ErrLockHeld) || !errors.Is(err, ErrCompensated) {
		t.Fatalf("contender: %v", err)
	}
	wantJournal(t, "lock-b", "undo before-lock")
	// The lock is released when its holder ends.
	if err := e.Signal(bg, "lock-a", "go", 1); err != nil {
		t.Fatal(err)
	}
	if out, err := a.Wait(waitCtx(t)); err != nil || out != "locked k" {
		t.Fatalf("holder: %q %v", out, err)
	}
	if out, err := startLock(t, e, "lock-c", lockIn{Key: "k"}).Wait(waitCtx(t)); err != nil || out != "locked k" {
		t.Fatalf("after release: %q %v", out, err)
	}
}

func TestLockWait(t *testing.T) {
	e := testEngine(t)
	a := startLock(t, e, "wait-a", lockIn{Key: "w", Await: true})
	waitFor(t, func() bool { return historyHas(e, "wait-a", "LockAcquired") })
	b := startLock(t, e, "wait-b", lockIn{Key: "w", Wait: true})
	waitFor(t, func() bool { return historyHas(e, "wait-b", "LockHeld") })
	if err := e.Signal(bg, "wait-a", "go", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Wait(waitCtx(t)); err != nil {
		t.Fatal(err)
	}
	if out, err := b.Wait(waitCtx(t)); err != nil || out != "locked w" {
		t.Fatalf("waiter: %q %v", out, err)
	}
}

// A lock lapses while its holder sleeps; another execution takes it, and
// the holder fails with ErrLockLost (and compensates) when it wakes.
func TestLockLost(t *testing.T) {
	e, err := newEngine(mem.New(nil), Options{InstanceID: "lost", PollMax: 20 * time.Millisecond, LeaseTTL: 200 * time.Millisecond})
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
	// Lock asks for 1s: the holder sleeps 3s, so its lock lapses.
	a := startLock(t, e, "lost-a", lockIn{Key: "l", Sleep: 3 * time.Second})
	waitFor(t, func() bool { return historyHas(e, "lost-a", "TimerScheduled") })
	waitFor(t, func() bool {
		ex, _ := e.st.GetExecution(bg, "lost-b")
		if ex.ID == 0 {
			_ = startLock(t, e, "lost-b", lockIn{Key: "l", Wait: true})
		}
		return historyHas(e, "lost-b", "LockAcquired")
	})
	_, err = a.Wait(waitCtx(t))
	if !errors.Is(err, ErrLockLost) || !errors.Is(err, ErrCompensated) {
		t.Fatalf("holder: %v", err)
	}
	wantJournal(t, "lost-a", "undo before-lock")
}

var badLockWF = DefineWorkflow("test.bad-lock", 1, func(c Ctx, ttl time.Duration) (int, error) {
	return 0, Lock(c, "k", ttl)
})

func TestLockArguments(t *testing.T) {
	e := testEngine(t)
	if _, err := badLockWF.Run(waitCtx(t), e, "bad-lock", 0); !errors.Is(err, ErrInvalidArgument) || !IsPermanent(err) {
		t.Fatalf("ttl 0: %v", err)
	}
}

var (
	slowInside, slowMax atomic.Int32
	slowCritical        = DefineActivity("test.slow-critical", func(context.Context, Key, int) (int, struct{}, error) {
		n := slowInside.Add(1)
		for {
			m := slowMax.Load()
			if n <= m || slowMax.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(700 * time.Millisecond) // longer than the lock's ttl
		slowInside.Add(-1)
		return 0, struct{}{}, nil
	}, nil)
	slowLockWF = DefineWorkflow("test.slow-lock", 1, func(c Ctx, _ int) (int, error) {
		if err := Lock(c, "slow", time.Millisecond, LockWait(RetryPolicy{Attempts: 500, Initial: 20 * time.Millisecond})); err != nil {
			return 0, err
		}
		return slowCritical.Run(c, 0)
	})
)

// Review of T5.4 (H1): a lock stays held while its holder's activity runs,
// polled (not eager) and longer than the lock's ttl.
func TestLockHeldDuringPolledActivity(t *testing.T) {
	st := mem.New(nil)
	var es []*Engine
	for _, id := range []string{"x", "y"} {
		e, err := newEngine(st, Options{InstanceID: id, PollMax: 20 * time.Millisecond, LeaseTTL: 200 * time.Millisecond, DisableEager: true})
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
		es = append(es, e)
	}
	slowInside.Store(0)
	slowMax.Store(0)
	h1, _ := slowLockWF.Start(bg, es[0], "slow-1", 0)
	h2, _ := slowLockWF.Start(bg, es[1], "slow-2", 0)
	for _, h := range []*Handle[int]{h1, h2} {
		if _, err := h.Wait(waitCtx(t)); err != nil {
			t.Fatal(err)
		}
	}
	if m := slowMax.Load(); m != 1 {
		t.Fatalf("%d critical sections at once", m)
	}
}

// lockElsewhere runs on a queue no test engine runs: it waits there.
var lockElsewhere = DefineActivity("test.lock.elsewhere",
	func(context.Context, Key, int) (int, int, error) { return 0, 0, nil }, nil, WithQueue("elsewhere"))

var lockThenQueueWF = DefineWorkflow("test.lock.queue", 1, func(c Ctx, key string) (int, error) {
	if err := Lock(c, key, time.Millisecond); err != nil { // ttl raised to LeaseTTL
		return 0, err
	}
	return lockElsewhere.Run(c, 1)
})

// T9.4: a lock is kept past its ttl while its holder's activity waits in a
// queue, up to Options.LockHold; with LockHold negative, it lapses.
func TestLockKeptWhileActivityWaits(t *testing.T) {
	for _, c := range []struct {
		name string
		hold time.Duration
		want error // the contender's
	}{{"default hold", 0, ErrLockHeld}, {"no hold", -1, nil}} {
		t.Run(c.name, func(t *testing.T) {
			e, err := newEngine(mem.New(nil), Options{
				InstanceID: "test", PollMax: 20 * time.Millisecond, LeaseTTL: 300 * time.Millisecond, LockHold: c.hold,
			})
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
			if _, err := lockThenQueueWF.Start(bg, e, "holder", "q"); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return historyHas(e, "holder", "ActivityScheduled") })
			time.Sleep(3 * 300 * time.Millisecond) // well past the lock's ttl
			comps.reset("contender")
			_, err = lockWF.Run(waitCtx(t), e, "contender", lockIn{Key: "q"})
			if c.want == nil && err != nil || c.want != nil && !errors.Is(err, c.want) {
				t.Fatalf("contender: %v, want %v", err, c.want)
			}
		})
	}
}

var elsewhereRuns atomic.Int32

var queuedElsewhere = DefineActivity("test.queue.elsewhere",
	func(context.Context, Key, int) (int, int, error) { elsewhereRuns.Add(1); return 0, 0, nil }, nil, WithQueue("elsewhere"))

var queuedElsewhereWF = DefineWorkflow("test.queue.elsewhere", 1, func(c Ctx, _ int) (int, error) {
	return queuedElsewhere.Run(c, 1)
})

// Found in T9.4: eager execution ran an activity of a queue the engine does
// not run (WithQueue routes it to other engines).
func TestEagerKeepsToItsQueues(t *testing.T) {
	e := testEngine(t)
	if _, err := queuedElsewhereWF.Start(bg, e, "elsewhere", 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return historyHas(e, "elsewhere", "ActivityScheduled") })
	time.Sleep(100 * time.Millisecond)
	if n := elsewhereRuns.Load(); n != 0 {
		t.Fatalf("ran %d times on an engine without its queue", n)
	}
}
