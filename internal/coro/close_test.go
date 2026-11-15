package coro

import (
	"errors"
	"runtime"
	"testing"
	"time"
)

// waitGoroutines waits until the goroutine count drops to at most n.
func waitGoroutines(tb testing.TB, n int) {
	tb.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > n {
		if time.Now().After(deadline) {
			tb.Fatalf("goroutine leak: %d goroutines, want <= %d", runtime.NumGoroutine(), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Unbuffered rendezvous: one receive completes exactly one of two sends.
func TestUnbufferedSendWaitsForItsOwnReceiver(t *testing.T) {
	s := newSched(t)
	ch := NewChannel[int](0)
	var sent []int
	Go(s.Root(), func(ctx Context) { _, _ = ch.Receive(ctx) })
	Go(s.Root(), func(ctx Context) { _ = ch.Send(ctx, 1); sent = append(sent, 1) })
	Go(s.Root(), func(ctx Context) { _ = ch.Send(ctx, 2); sent = append(sent, 2) })
	mustRun(t, s)
	if len(sent) != 1 || sent[0] != 1 || ch.Len() != 0 {
		t.Fatalf("sent %v buffered %d: want exactly the first send done", sent, ch.Len())
	}
}

// A Select woken by a channel that fires another case must not strand a
// plain receiver of that channel.
func TestSelectDoesNotLoseWakeups(t *testing.T) {
	s := newSched(t)
	ch := NewChannel[int](1)
	f := NewFuture[int]()
	got := false
	Go(s.Root(), func(ctx Context) {
		Select(ctx, OnFuture(f, func(Context, int, error) {}), OnReceive(ch, func(Context, int, bool) {}))
	})
	Go(s.Root(), func(ctx Context) { _, _ = ch.Receive(ctx); got = true })
	mustRun(t, s)
	f.Set(1, nil)
	_, _ = ch.TrySend(7)
	mustRun(t, s)
	if !got || s.Live() != 0 {
		t.Fatalf("receiver stranded: got=%v live=%d buffered=%d", got, s.Live(), ch.Len())
	}
}

func TestBufferedChannelSeveralBlockedSendersThenClose(t *testing.T) {
	s := newSched(t)
	ch := NewChannel[int](1)
	errs := make([]error, 3)
	for i := range 3 {
		Go(s.Root(), func(ctx Context) { errs[i] = ch.Send(ctx, i) })
	}
	mustRun(t, s)
	if v, ok, done := ch.TryReceive(); !done || !ok || v != 0 {
		t.Fatalf("first value %d %v %v", v, ok, done)
	}
	ch.Close()
	mustRun(t, s)
	// Value 1 moved into the buffer when 0 was received; 2 was still blocked.
	if errs[0] != nil || errs[1] != nil || !errors.Is(errs[2], ErrChannelClosed) {
		t.Fatalf("send errors %v", errs)
	}
	if v, ok, _ := ch.TryReceive(); !ok || v != 1 {
		t.Fatalf("buffered value after close: %d %v", v, ok)
	}
	if _, ok, done := ch.TryReceive(); ok || !done {
		t.Fatal("drained closed channel must report !ok")
	}
}

// Close runs deferred functions that block or spawn, and leaks nothing.
func TestCloseWithBlockingAndSpawningDefersLeaksNothing(t *testing.T) {
	before := runtime.NumGoroutine()
	s := NewScheduler(0)
	f, g := NewFuture[int](), NewFuture[int]()
	ran := 0
	Go(s.Root(), func(ctx Context) {
		defer func() { ran++ }()
		defer func() { _, _ = g.Get(ctx) }() // blocks during Close
		_, _ = f.Get(ctx)
	})
	Go(s.Root(), func(ctx Context) {
		defer func() { Go(ctx, func(Context) { ran++ }) }() // spawns during Close
		_, _ = f.Get(ctx)
	})
	mustRun(t, s)
	s.Close()
	if ran != 1 {
		t.Fatalf("outer deferred function ran %d times, want 1", ran)
	}
	waitGoroutines(t, before)
}

// GW-10: the watchdog breaks a stuck scheduler; Close must neither race with
// the stuck coroutine nor run other coroutines' deferred functions in
// parallel with it, and everything exits once it yields.
func TestStuckSchedulerCloseIsRaceFreeAndReaps(t *testing.T) {
	before := runtime.NumGoroutine()
	s := NewScheduler(20 * time.Millisecond)
	shared := 0
	f := NewFuture[int]()
	Go(s.Root(), func(ctx Context) {
		defer func() { shared++ }()
		_, _ = f.Get(ctx)
	})
	Go(s.Root(), func(ctx Context) {
		end := time.Now().Add(100 * time.Millisecond)
		for time.Now().Before(end) {
			shared++
		}
		_, _ = f.Get(ctx) // yields after being declared stuck
	})
	if err := s.Run(); !errors.Is(err, ErrStuck) {
		t.Fatalf("Run: %v", err)
	}
	s.Close()
	waitGoroutines(t, before)
}

// The watchdog re-plans its tick when a scheduler with a shorter timeout is
// registered while it sleeps on a long one.
func TestWatchdogReplansForShorterTimeout(t *testing.T) {
	long := NewScheduler(time.Hour)
	defer long.Close()
	s := NewScheduler(30 * time.Millisecond)
	release := make(chan struct{})
	Go(s.Root(), func(Context) { <-release })
	start := time.Now()
	if err := s.Run(); !errors.Is(err, ErrStuck) {
		t.Fatalf("Run: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("stuck detected after %v", d)
	}
	close(release)
	s.Close()
}

// A loop of WithCancel+cancel must not grow the parent.
func TestCanceledChildrenAreUnlinked(t *testing.T) {
	s := newSched(t)
	parent, _ := WithCancel(s.Root())
	for range 1000 {
		_, cancel := WithCancel(parent)
		cancel()
	}
	if n := len(parent.(*cancelCtx).children); n != 0 {
		t.Fatalf("parent keeps %d canceled children", n)
	}
}

func TestBlockingSendRecyclesOffers(t *testing.T) {
	s := newSched(t)
	ch := NewChannel[int](0)
	Go(s.Root(), func(ctx Context) {
		for {
			if _, ok := ch.Receive(ctx); !ok {
				return
			}
		}
	})
	Go(s.Root(), func(ctx Context) {
		for i := range 100 {
			_ = ch.Send(ctx, i)
		}
		ch.Close()
	})
	mustRun(t, s)
	if len(ch.free) != 1 {
		t.Fatalf("offers not recycled: free list %d", len(ch.free))
	}
}

// The watchdog fires while a coroutine is about to hand control back: exactly
// one side wins, Run reports ErrStuck, and Close plus the reap still stop
// every coroutine and run their deferred functions. Not parallel: it sets a
// package hook.
func TestWatchdogRacingAHandBackLeaksNothing(t *testing.T) {
	before := runtime.NumGoroutine()
	s := NewScheduler(20 * time.Millisecond)
	f := NewFuture[int]()
	deferred := make(chan struct{}, 1)
	Go(s.Root(), func(ctx Context) {
		defer func() { deferred <- struct{}{} }()
		_, _ = f.Get(ctx)
	})
	hook := func() { time.Sleep(100 * time.Millisecond) }
	testHookBeforeYield.Store(&hook)
	defer testHookBeforeYield.Store(nil)
	if err := s.Run(); !errors.Is(err, ErrStuck) {
		t.Fatalf("Run: %v, want ErrStuck (the watchdog fires during the hook)", err)
	}
	s.Close()
	select {
	case <-deferred:
	case <-time.After(2 * time.Second):
		t.Fatal("parked coroutine was never stopped")
	}
	testHookBeforeYield.Store(nil)
	waitGoroutines(t, before)
}

// A deferred function that spins during Close does not hang Close.
func TestCloseIsGuardedByTheWatchdog(t *testing.T) {
	before := runtime.NumGoroutine()
	s := NewScheduler(20 * time.Millisecond)
	f := NewFuture[int]()
	release := make(chan struct{})
	Go(s.Root(), func(ctx Context) {
		defer func() { <-release }() // real blocking in a deferred function
		_, _ = f.Get(ctx)
	})
	Go(s.Root(), func(ctx Context) { _, _ = f.Get(ctx) })
	mustRun(t, s)
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung on a stuck deferred function")
	}
	close(release)
	waitGoroutines(t, before)
}
