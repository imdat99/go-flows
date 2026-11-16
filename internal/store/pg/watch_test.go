package pg_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

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

// T9.6: with a listen config (Options.ListenDSN), Watch listens on a
// connection of its own, not one of the pool's: hints arrive even when the
// pool cannot serve a LISTEN (behind PgBouncer in transaction mode; here,
// a pool of one connection, held elsewhere).
func TestWatchListensOnItsOwnConnection(t *testing.T) {
	base := testdb.New(t)
	ctx := context.Background()
	if err := pg.Migrate(ctx, base); err != nil {
		t.Fatal(err)
	}
	cfg := base.Config()
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	held, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	s := pg.New(pool)
	listen := base.Config().ConnConfig.Copy()
	listen.RuntimeParams["application_name"] = "flows-listen"
	s.SetListenConfig(listen)
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	hints := make(chan string, 16)
	done := make(chan error, 1)
	go func() {
		done <- s.Watch(wctx, func(k store.WorkKind, q string) {
			if q != "" {
				hints <- q
			}
		})
	}()
	waitListening(t, base, "flows-listen")
	if _, err := held.Exec(ctx, "SELECT pg_notify('flows_work', 'wq1')"); err != nil {
		t.Fatal(err)
	}
	select {
	case q := <-hints:
		if q != "q1" {
			t.Fatalf("hint %q", q)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no hint")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("watch ended with %v", err)
	}
}

// waitListening waits until a session called app listens.
func waitListening(t *testing.T, pool *pgxpool.Pool, app string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; {
		var n int
		if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND application_name = $1 AND query LIKE 'LISTEN%'`, app).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no session %s listens", app)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
