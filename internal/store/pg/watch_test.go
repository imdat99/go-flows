package pg_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/testdb"
)

// T8.2: Watch waits on its connection without pgconn's per-call context
// watch; its context ends it (a deadline cuts the wait) at any moment,
// promptly, and the connection does not stay acquired.
func TestWatchEndsWithItsContext(t *testing.T) {
	pool := testdb.New(t)
	if err := pg.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	s := pg.New(pool)
	for round := range 10 {
		ctx, cancel := context.WithCancel(context.Background())
		var n atomic.Int64
		done := make(chan error, 1)
		go func() { done <- s.Watch(ctx, func(store.WorkKind, string) { n.Add(1) }) }()
		for n.Load() < 2 { // the wakes on listening
			time.Sleep(time.Millisecond)
		}
		for range 5 {
			if _, err := pool.Exec(context.Background(), "SELECT pg_notify('flows_work', 'wq')"); err != nil {
				t.Fatal(err)
			}
		}
		for deadline := time.Now().Add(5 * time.Second); n.Load() < 7 && time.Now().Before(deadline); {
			time.Sleep(time.Millisecond)
		}
		if n.Load() < 7 {
			t.Fatalf("round %d: %d wakes", round, n.Load())
		}
		time.Sleep(time.Duration(round) * time.Millisecond) // cancel at various points of the wait
		start := time.Now()
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) || time.Since(start) > time.Second {
				t.Fatalf("round %d: %v after %v", round, err, time.Since(start))
			}
		case <-time.After(5 * time.Second):
			t.Fatal("watch did not return")
		}
	}
	if n := pool.Stat().AcquiredConns(); n != 0 {
		t.Fatalf("%d connections left acquired", n)
	}
}
