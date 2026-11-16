package worker_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/worker"
)

// T9.3: the stalls of a workflow's code are counted in the store, so a
// second stall at the same point parks it whichever worker runs it: with
// N workers, not N+1 attempts (each leaves a goroutine spinning).
func TestStuckIsCountedAcrossWorkers(t *testing.T) {
	var spin atomic.Bool
	spin.Store(true)
	t.Cleanup(func() { spin.Store(false) }) // lets the stuck goroutines go
	var attempts atomic.Int32
	st := mem.New(nil)
	reg := newRegistry()
	reg.addWorkflow("stuck", func(coro.Context, []byte) ([]byte, error) {
		attempts.Add(1)
		for spin.Load() { // the busy loop under test
		}
		return nil, nil
	})
	opts := func(owner string) worker.Options {
		return worker.Options{Owner: owner, StuckTimeout: 200 * time.Millisecond, HandBack: 50 * time.Millisecond}
	}
	a := startWorker(t, st, reg, opts("a"))
	create(t, st, "s", "stuck", "")
	// A's first stall hands it back; A stops, B (which never saw it) runs
	// the next attempt.
	waitUntil(t, func() bool { return attempts.Load() == 1 })
	waitUntil(t, func() bool { return !a.Busy() }) // the stall was handled: handed back
	if err := a.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	startWorker(t, st, reg, opts("b"))
	e := waitEnd(t, st, "s")
	if e.Status != store.StatusNeedsAttention || attempts.Load() != 2 {
		t.Fatalf("%s after %d attempts, want needs_attention after 2", e.Status, attempts.Load())
	}
}
