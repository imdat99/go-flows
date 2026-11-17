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
