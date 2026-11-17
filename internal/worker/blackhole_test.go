package worker_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/worker"
)

// hangingRenewal is a store whose lease renewals hang once hang is set, as
// on a database that blackholes (failover, a network drop without RST):
// until their context is done, or, with ignoreCtx, until release closes.
type hangingRenewal struct {
	*mem.Store
	hang      atomic.Bool
	ignoreCtx bool
	release   chan struct{}
}

func (s *hangingRenewal) RenewLeases(ctx context.Context, owner string, wf, act []store.Lease, ttl time.Duration) (store.Renewal, error) {
	if !s.hang.Load() {
		return s.Store.RenewLeases(ctx, owner, wf, act, ttl)
	}
	if s.ignoreCtx {
		<-s.release
		return store.Renewal{}, errors.New("released")
	}
	select {
	case <-ctx.Done():
		return store.Renewal{}, context.Cause(ctx)
	case <-s.release:
		return store.Renewal{}, errors.New("released")
	}
}

// T11.1: a lease renewal that hangs does not keep a running activity going
// past its lease: its context is canceled with ErrLeaseLost once the lease
// was not renewed for ttl - interval, whether or not the renewal returns,
// so a second attempt claimed elsewhere does not overlap it.
func TestHangingRenewalGivesUpLeases(t *testing.T) {
	for _, ignoreCtx := range []bool{false, true} {
		t.Run(fmt.Sprintf("ignoreCtx=%v", ignoreCtx), func(t *testing.T) {
			const ttl = 600 * time.Millisecond
			st := &hangingRenewal{Store: mem.New(nil), ignoreCtx: ignoreCtx, release: make(chan struct{})}
			reg := newRegistry()
			reg.addWorkflow("w", func(ctx coro.Context, _ []byte) ([]byte, error) {
				return activity(ctx, "slow", nil)
			})
			var attempts atomic.Int32
			started := make(chan struct{})
			cause := make(chan error, 1)
			reg.addActivity("slow", &worker.Activity{Run: func(ctx context.Context, _ *store.ActivityTask) ([]byte, []byte, error) {
				if attempts.Add(1) > 1 {
					return nil, nil, nil // a later attempt: done
				}
				close(started)
				<-ctx.Done()
				cause <- context.Cause(ctx)
				return nil, nil, ctx.Err()
			}})
			startWorker(t, st, reg, worker.Options{LeaseTTL: ttl})
			t.Cleanup(func() { close(st.release) }) // runs before the worker's Stop
			create(t, st, "i1", "w", "")
			select {
			case <-started:
			case <-time.After(10 * time.Second):
				t.Fatal("the activity never started")
			}
			st.hang.Store(true)
			hung := time.Now()
			select {
			case err := <-cause:
				if !errors.Is(err, worker.ErrLeaseLost) {
					t.Fatalf("cause %v, want ErrLeaseLost", err)
				}
				// Given up within ttl of the last renewal (generous bound
				// for loaded CI: the point is it happens at all).
				if d := time.Since(hung); d > 10*ttl {
					t.Fatalf("given up after %v", d)
				}
			case <-time.After(20 * ttl):
				t.Fatal("the activity kept running past its lease while the renewal hung")
			}
		})
	}
}
