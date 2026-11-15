package coro

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// newSched returns a scheduler closed when the test ends.
func newSched(tb testing.TB) *Scheduler {
	tb.Helper()
	s := NewScheduler(0)
	tb.Cleanup(s.Close)
	return s
}

func mustRun(tb testing.TB, s *Scheduler) {
	tb.Helper()
	if err := s.Run(); err != nil {
		tb.Fatalf("Run: %v", err)
	}
}

func TestCoroutineRunsToCompletion(t *testing.T) {
	s := newSched(t)
	ran := false
	s.Spawn(s.Root(), func(Context) error { ran = true; return nil })
	if ran {
		t.Fatal("fn ran before Run")
	}
	mustRun(t, s)
	if !ran || s.Live() != 0 {
		t.Fatalf("ran=%v live=%d", ran, s.Live())
	}
}

func TestFutureWakesWaiter(t *testing.T) {
	s := newSched(t)
	f := NewFuture[int]()
	var got int
	s.Spawn(s.Root(), func(ctx Context) error {
		v, err := f.Get(ctx)
		got = v
		return err
	})
	mustRun(t, s)
	if s.Live() != 1 {
		t.Fatalf("waiter should be parked, live=%d", s.Live())
	}
	if !f.Set(42, nil) {
		t.Fatal("first Set returned false")
	}
	if f.Set(7, nil) {
		t.Fatal("second Set must not overwrite")
	}
	mustRun(t, s)
	if got != 42 || s.Live() != 0 {
		t.Fatalf("got=%d live=%d", got, s.Live())
	}
}

func TestDeterministicFIFOOrder(t *testing.T) {
	for range 20 {
		s := newSched(t)
		f := NewFuture[struct{}]()
		var order []int
		for i := range 5 {
			s.Spawn(s.Root(), func(ctx Context) error {
				order = append(order, i)
				_, _ = f.Get(ctx)
				order = append(order, 10+i)
				return nil
			})
		}
		mustRun(t, s)
		f.Set(struct{}{}, nil)
		mustRun(t, s)
		want := []int{0, 1, 2, 3, 4, 10, 11, 12, 13, 14}
		if !slices.Equal(order, want) {
			t.Fatalf("order %v, want %v", order, want)
		}
	}
}

func TestGoSpawnsFromCoroutine(t *testing.T) {
	s := newSched(t)
	var order []string
	s.Spawn(s.Root(), func(ctx Context) error {
		ch := NewChannel[int](0)
		Go(ctx, func(ctx Context) {
			order = append(order, "child")
			_ = ch.Send(ctx, 1)
		})
		v, ok := ch.Receive(ctx)
		order = append(order, "main", string(rune('0'+v)))
		if !ok {
			t.Error("receive not ok")
		}
		return nil
	})
	mustRun(t, s)
	if want := []string{"child", "main", "1"}; !slices.Equal(order, want) {
		t.Fatalf("order %v, want %v", order, want)
	}
}

// GW-10: a panic in workflow code is an error with a stack, never a crash.
func TestGW10_PanicIsRecoveredWithStack(t *testing.T) {
	s := newSched(t)
	s.Spawn(s.Root(), func(ctx Context) error {
		Go(ctx, func(Context) { panic("boom") })
		return nil
	})
	err := s.Run()
	var pe *PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("Run error %v, want *PanicError", err)
	}
	if pe.Value != "boom" || !strings.Contains(string(pe.Stack), "coro_test.go") {
		t.Fatalf("panic value %v, stack missing the test frame", pe.Value)
	}
}

// GW-10: code that never yields makes Run fail with ErrStuck (go-workflows
// panicked after 40s and crashed the process).
func TestGW10_StuckCoroutineIsAnError(t *testing.T) {
	s := NewScheduler(50 * time.Millisecond)
	release := make(chan struct{})
	s.Spawn(s.Root(), func(Context) error { <-release; return nil })
	if err := s.Run(); !errors.Is(err, ErrStuck) {
		t.Fatalf("Run error %v, want ErrStuck", err)
	}
	if err := s.Run(); !errors.Is(err, ErrStuck) {
		t.Fatalf("second Run error %v, want ErrStuck", err)
	}
	s.Close() // must not wait for the stuck coroutine
	close(release)
}

func TestCloseRunsDefersOfParkedCoroutines(t *testing.T) {
	s := NewScheduler(0)
	f := NewFuture[int]()
	deferred := 0
	for range 3 {
		s.Spawn(s.Root(), func(ctx Context) error {
			defer func() { deferred++ }()
			_, _ = f.Get(ctx)
			t.Error("resumed after Close")
			return nil
		})
	}
	mustRun(t, s)
	s.Close()
	s.Close()
	if deferred != 3 {
		t.Fatalf("deferred=%d, want 3", deferred)
	}
	if err := s.Run(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Run after Close: %v", err)
	}
}

func TestErrorStopsRun(t *testing.T) {
	s := newSched(t)
	boom := errors.New("boom")
	s.Spawn(s.Root(), func(Context) error { return boom })
	if err := s.Run(); !errors.Is(err, boom) {
		t.Fatalf("Run error %v", err)
	}
}

func TestBlockingOutsideCoroutinePanics(t *testing.T) {
	s := newSched(t)
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic")
		}
	}()
	_, _ = NewFuture[int]().Get(s.Root())
}

func TestUnbufferedChannelRendezvous(t *testing.T) {
	s := newSched(t)
	ch := NewChannel[int](0)
	var got []int
	sent := 0
	s.Spawn(s.Root(), func(ctx Context) error {
		for i := range 3 {
			if err := ch.Send(ctx, i); err != nil {
				return err
			}
			sent++
		}
		ch.Close()
		return nil
	})
	mustRun(t, s)
	if sent != 0 {
		t.Fatalf("unbuffered send completed without a receiver (sent=%d)", sent)
	}
	s.Spawn(s.Root(), func(ctx Context) error {
		for {
			v, ok := ch.Receive(ctx)
			if !ok {
				return nil
			}
			got = append(got, v)
		}
	})
	mustRun(t, s)
	if !slices.Equal(got, []int{0, 1, 2}) || s.Live() != 0 {
		t.Fatalf("got %v live %d", got, s.Live())
	}
}

func TestBufferedChannelBlocksWhenFull(t *testing.T) {
	s := newSched(t)
	ch := NewChannel[int](2)
	sent := 0
	s.Spawn(s.Root(), func(ctx Context) error {
		for i := range 5 {
			_ = ch.Send(ctx, i)
			sent++
		}
		return nil
	})
	mustRun(t, s)
	if sent != 2 || ch.Len() != 2 {
		t.Fatalf("sent=%d len=%d, want 2 and 2", sent, ch.Len())
	}
	var got []int
	s.Spawn(s.Root(), func(ctx Context) error {
		for range 5 {
			v, _ := ch.Receive(ctx)
			got = append(got, v)
		}
		return nil
	})
	mustRun(t, s)
	if !slices.Equal(got, []int{0, 1, 2, 3, 4}) || sent != 5 {
		t.Fatalf("got %v sent %d", got, sent)
	}
}

// GW-14: an unbounded channel keeps every value, in order.
func TestGW14_UnboundedChannelIsFIFOAndNeverDrops(t *testing.T) {
	ch := NewChannel[int](-1)
	for i := range 1000 {
		if ok, err := ch.TrySend(i); !ok || err != nil {
			t.Fatalf("TrySend %d: %v %v", i, ok, err)
		}
	}
	for i := range 1000 {
		v, ok, done := ch.TryReceive()
		if !done || !ok || v != i {
			t.Fatalf("receive %d: v=%d ok=%v done=%v", i, v, ok, done)
		}
	}
}

func TestSendOnClosedChannelIsAnError(t *testing.T) {
	ch := NewChannel[int](1)
	ch.Close()
	if _, err := ch.TrySend(1); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("TrySend on closed: %v", err)
	}
	s := newSched(t)
	blocked := NewChannel[int](0)
	var sendErr error
	s.Spawn(s.Root(), func(ctx Context) error { sendErr = blocked.Send(ctx, 1); return nil })
	mustRun(t, s)
	blocked.Close()
	mustRun(t, s)
	if !errors.Is(sendErr, ErrChannelClosed) {
		t.Fatalf("blocked Send after Close: %v", sendErr)
	}
}

func TestSelectFirstReadyCaseWinsAndWaits(t *testing.T) {
	s := newSched(t)
	f1, f2 := NewFuture[int](), NewFuture[int]()
	ch := NewChannel[string](-1)
	var got []string
	s.Spawn(s.Root(), func(ctx Context) error {
		for range 3 {
			Select(ctx,
				OnFuture(f1, func(_ Context, v int, _ error) { got = append(got, "f1"); f1 = NewFuture[int]() }),
				OnFuture(f2, func(_ Context, v int, _ error) { got = append(got, "f2"); f2 = NewFuture[int]() }),
				OnReceive(ch, func(_ Context, v string, _ bool) { got = append(got, v) }),
			)
		}
		return nil
	})
	mustRun(t, s)
	if len(got) != 0 {
		t.Fatalf("select fired early: %v", got)
	}
	f2.Set(1, nil)
	mustRun(t, s)
	_, _ = ch.TrySend("msg")
	mustRun(t, s)
	f1.Set(1, nil)
	mustRun(t, s)
	if want := []string{"f2", "msg", "f1"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSelectDefault(t *testing.T) {
	s := newSched(t)
	f := NewFuture[int]()
	hit := ""
	s.Spawn(s.Root(), func(ctx Context) error {
		Select(ctx, OnFuture(f, func(Context, int, error) { hit = "f" }), OnDefault(func(Context) { hit = "default" }))
		return nil
	})
	mustRun(t, s)
	if hit != "default" {
		t.Fatalf("hit %q", hit)
	}
}

// A Select loop over a channel that never fires must not grow its wait list.
func TestSelectStaleWaitersArePruned(t *testing.T) {
	s := newSched(t)
	never := NewChannel[int](0)
	tick := NewChannel[int](-1)
	s.Spawn(s.Root(), func(ctx Context) error {
		for range 1000 {
			Select(ctx, OnReceive(never, func(Context, int, bool) {}), OnReceive(tick, func(Context, int, bool) {}))
		}
		return nil
	})
	for range 1000 {
		mustRun(t, s)
		_, _ = tick.TrySend(1)
	}
	mustRun(t, s)
	if n := len(never.recvWait.w); n > 1 {
		t.Fatalf("stale waiters kept: %d", n)
	}
}

func TestContextCancelPropagatesAndClosesDone(t *testing.T) {
	s := newSched(t)
	parent, cancel := WithCancelCause(s.Root())
	child, _ := WithCancel(WithValue(parent, "k", "v"))
	disc, _ := Disconnected(child)
	woke := false
	s.Spawn(s.Root(), func(ctx Context) error {
		_, ok := child.Done().Receive(ctx)
		woke = !ok
		return nil
	})
	mustRun(t, s)
	cause := errors.New("why")
	cancel(cause)
	mustRun(t, s)
	if !woke || !errors.Is(child.Err(), ErrCanceled) || !errors.Is(Cause(child), cause) {
		t.Fatalf("woke=%v err=%v cause=%v", woke, child.Err(), Cause(child))
	}
	if disc.Err() != nil || disc.Value("k") != "v" {
		t.Fatalf("disconnected ctx: err=%v value=%v", disc.Err(), disc.Value("k"))
	}
	late, _ := WithCancel(parent)
	if late.Err() == nil {
		t.Fatal("child of a canceled parent must start canceled")
	}
}

func TestWaitGroupAndErrGroup(t *testing.T) {
	s := newSched(t)
	boom := errors.New("boom")
	var wgDone bool
	var groupErr error
	var canceled bool
	s.Spawn(s.Root(), func(ctx Context) error {
		var wg WaitGroup
		wg.Wait(ctx) // no Add: returns at once
		for range 3 {
			_ = wg.Add(1)
			Go(ctx, func(Context) { _ = wg.Done() })
		}
		wg.Wait(ctx)
		wgDone = true
		if err := wg.Done(); !errors.Is(err, ErrGroupMisuse) {
			t.Errorf("negative counter: %v", err)
		}

		g, gctx := NewErrGroup(ctx)
		_ = g.Go(func(Context) error { return boom })
		_ = g.Go(func(ctx Context) error {
			_, _ = gctx.Done().Receive(ctx)
			canceled = true
			return errors.New("second")
		})
		groupErr = g.Wait(ctx)
		if err := g.Go(func(Context) error { return nil }); !errors.Is(err, ErrGroupMisuse) {
			t.Errorf("Go after Wait: %v", err)
		}
		return nil
	})
	mustRun(t, s)
	if !wgDone || !errors.Is(groupErr, boom) || !canceled {
		t.Fatalf("wg=%v err=%v canceled=%v", wgDone, groupErr, canceled)
	}
}

func TestAfterCancel(t *testing.T) {
	s := newSched(t)
	ctx, cancel := WithCancel(s.Root())
	var order []int
	AfterCancel(ctx, func() { order = append(order, 1) })
	stop := AfterCancel(ctx, func() { order = append(order, 2) })
	AfterCancel(ctx, func() { order = append(order, 3) })
	child, _ := WithCancel(ctx)
	AfterCancel(child, func() { order = append(order, 0) }) // children first
	if !stop() || stop() {
		t.Fatal("stop must succeed once")
	}
	cancel()
	if want := []int{0, 1, 3}; !slices.Equal(order, want) {
		t.Fatalf("order %v, want %v", order, want)
	}
	ran := false
	AfterCancel(ctx, func() { ran = true }) // already canceled: runs at once
	if !ran {
		t.Fatal("hook on a canceled context did not run")
	}
	never := AfterCancel(s.Root(), func() { t.Error("root is never canceled") })
	if never() {
		t.Fatal("stop on an uncancelable context reports registration")
	}
	if n := len(ctx.(*cancelCtx).hooks); n != 0 {
		t.Fatalf("hooks kept after cancel: %d", n)
	}
}

// Watching a hook that already watches another context moves it.
func TestWatchTwiceMovesTheHook(t *testing.T) {
	s := newSched(t)
	a, cancelA := WithCancel(s.Root())
	b, cancelB := WithCancel(s.Root())
	n := 0
	h := &funcHook{fn: func() { n++ }}
	Watch(a, h)
	Watch(b, h)
	cancelA()
	if n != 0 || len(a.(*cancelCtx).hooks) != 0 {
		t.Fatalf("hook still on the first context: n=%d", n)
	}
	cancelB()
	if n != 1 {
		t.Fatalf("hook ran %d times", n)
	}
}

// Watching an uncancelable or canceled context also leaves the old one.
func TestWatchElsewhereLeavesTheOldContext(t *testing.T) {
	s := newSched(t)
	a, cancelA := WithCancel(s.Root())
	canceled, cancelNow := WithCancel(s.Root())
	cancelNow()
	n := 0
	h := &funcHook{fn: func() { n++ }}
	Watch(a, h)
	Watch(s.Root(), h) // never canceled
	Watch(a, h)
	Watch(canceled, h) // runs at once
	cancelA()
	if n != 1 {
		t.Fatalf("hook ran %d times, want 1", n)
	}
}
