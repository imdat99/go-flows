package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store"
)

// renewStub reports chosen leases lost; the rest of the store is unused.
type renewStub struct {
	store.Store
	lost   []int64
	before func() // runs during the renewal round trip
}

func (s *renewStub) RenewLeases(_ context.Context, _ string, _, _ []store.Lease, _ time.Duration) (store.Renewal, error) {
	if s.before != nil {
		s.before()
	}
	return store.Renewal{LostWorkflows: s.lost}, nil
}

// A report about lease v1 must not touch v2 of the same execution, tracked
// while the renewal was in flight (found in review of T3.2-T3.6).
func TestHeartbeatIgnoresReportsAboutOlderLeases(t *testing.T) {
	st := &renewStub{lost: []int64{5}}
	h := newHeartbeat(st, "o", time.Minute, slog.New(slog.DiscardHandler))
	v1 := store.Lease{ID: 5, Owner: "o", Version: 1}
	ctx1, untrack1 := h.track(context.Background(), false, v1, time.Now())
	var ctx2 context.Context
	st.before = func() {
		untrack1.untrack()
		ctx2, _ = h.track(context.Background(), false, store.Lease{ID: 5, Owner: "o", Version: 2}, time.Now()) //nolint:fatcontext // created during the renewal on purpose
	}
	h.tick(context.Background())
	if ctx2.Err() != nil {
		t.Fatalf("the fresh lease was canceled by a report about the old one: %v", context.Cause(ctx2))
	}
	if !errors.Is(context.Cause(ctx1), context.Canceled) {
		t.Fatalf("old context: %v", context.Cause(ctx1))
	}
	st.before, st.lost = nil, []int64{5}
	h.tick(context.Background())
	if !errors.Is(context.Cause(ctx2), ErrLeaseLost) {
		t.Fatalf("a report about v2 itself must cancel it: %v", context.Cause(ctx2))
	}
}

// hangOnce hangs its first renewal until its context is done, recording
// why; the next ones succeed.
type hangOnce struct {
	store.Store
	calls  atomic.Int32
	cause  chan error
	second chan struct{}
}

func (s *hangOnce) RenewLeases(ctx context.Context, _ string, _, _ []store.Lease, _ time.Duration) (store.Renewal, error) {
	switch s.calls.Add(1) {
	case 1:
		<-ctx.Done()
		s.cause <- context.Cause(ctx)
		return store.Renewal{}, ctx.Err()
	case 2:
		close(s.second)
	}
	return store.Renewal{}, nil
}

// T11.1: a renewal that hangs is canceled after about an interval, and
// the renewals go on.
func TestHeartbeatRenewalHasADeadline(t *testing.T) {
	st := &hangOnce{cause: make(chan error, 1), second: make(chan struct{})}
	h := newHeartbeat(st, "o", 300*time.Millisecond, slog.New(slog.DiscardHandler))
	_, tr := h.track(context.Background(), false, store.Lease{ID: 1, Owner: "o", Version: 1}, time.Now())
	defer tr.untrack()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	select {
	case err := <-st.cause:
		if !errors.Is(err, errRenewalTimeout) {
			t.Fatalf("cause %v, want errRenewalTimeout", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the hung renewal was never canceled")
	}
	// The lease was given up meanwhile (not renewed for ttl - interval);
	// track another so the next renewal has something to send.
	_, tr2 := h.track(context.Background(), false, store.Lease{ID: 2, Owner: "o", Version: 1}, time.Now())
	defer tr2.untrack()
	select {
	case <-st.second:
	case <-time.After(10 * time.Second):
		t.Fatal("no renewal after the hung one")
	}
}

func TestRetryDelay(t *testing.T) {
	p := RetryPolicy{Attempts: 5, Initial: time.Second, Factor: 2, Max: 5 * time.Second}
	for next, want := range map[int32]time.Duration{2: time.Second, 3: 2 * time.Second, 4: 4 * time.Second, 5: 5 * time.Second} {
		if got := p.Delay(next); got != want {
			t.Errorf("Delay(%d) = %v, want %v", next, got, want)
		}
	}
	if got := (RetryPolicy{Initial: time.Second}).Delay(9); got != time.Second {
		t.Errorf("constant policy: %v", got)
	}
}

// busyStub reports every lease busy (never renewed, never lost).
type busyStub struct{ store.Store }

func (busyStub) RenewLeases(_ context.Context, _ string, wf, act []store.Lease, _ time.Duration) (store.Renewal, error) {
	var r store.Renewal
	for _, l := range wf {
		r.BusyWorkflows = append(r.BusyWorkflows, l.ID)
	}
	for _, l := range act {
		r.BusyActivities = append(r.BusyActivities, l.ID)
	}
	return r, nil
}

// T11.11: a lease whose claim was sent ttl - interval ago or more (its
// reply came late) is never held: its context is canceled with
// ErrLeaseLost at once, and the heartbeat does not track it.
func TestHeartbeatDoesNotTrackAStaleClaim(t *testing.T) {
	h := newHeartbeat(busyStub{}, "o", 3*time.Second, slog.New(slog.DiscardHandler))
	ctx, tr := h.track(context.Background(), true, store.Lease{ID: 1, Owner: "o", Version: 1}, time.Now().Add(-2*time.Second))
	defer tr.untrack()
	if !lostBeforeStart(ctx) {
		t.Fatalf("a stale claim's context: %v, want canceled with ErrLeaseLost", context.Cause(ctx))
	}
	h.mu.Lock()
	n := len(h.held)
	h.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d leases held, want 0", n)
	}
	fresh, tr2 := h.track(context.Background(), true, store.Lease{ID: 2, Owner: "o", Version: 1}, time.Now())
	defer tr2.untrack()
	if lostBeforeStart(fresh) {
		t.Fatal("a fresh claim's context is canceled")
	}
}

// T11.11: a lease tracked late, due before the heartbeat's next expire
// tick, is given up when due, not at that tick.
func TestHeartbeatGivesUpALateClaimOnTime(t *testing.T) {
	const ttl = 3 * time.Second // interval 1s, given up 2s after the claim
	h := newHeartbeat(busyStub{}, "o", ttl, slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	time.Sleep(50 * time.Millisecond) // run armed its expire timer (an interval)
	tracked := time.Now()
	lctx, tr := h.track(context.Background(), true, store.Lease{ID: 1, Owner: "o", Version: 1}, tracked.Add(-1800*time.Millisecond))
	defer tr.untrack()
	select {
	case <-lctx.Done():
		if !errors.Is(context.Cause(lctx), ErrLeaseLost) {
			t.Fatalf("cause %v, want ErrLeaseLost", context.Cause(lctx))
		}
		if d := time.Since(tracked); d > 600*time.Millisecond {
			t.Fatalf("given up %v after it was tracked, want about 200ms", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the lease was never given up")
	}
}
