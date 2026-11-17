package worker_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/testdb"
	"github.com/imdat99/go-flows/internal/worker"
)

// logTap records the time of the first log record whose message contains
// want (it never calls t.Logf: abandoned tasks may log after the test).
type logTap struct {
	want string
	once sync.Once
	seen chan time.Time
}

func newLogTap(want string) *logTap { return &logTap{want: want, seen: make(chan time.Time, 1)} }

func (h *logTap) Enabled(context.Context, slog.Level) bool { return true }

func (h *logTap) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, h.want) {
		h.once.Do(func() { h.seen <- time.Now() })
	}
	return nil
}

func (h *logTap) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logTap) WithGroup(string) slog.Handler      { return h }

// blackholed starts a worker on a Postgres store reached through a proxy,
// runs a one-activity workflow, freezes the proxy while the activity runs,
// then lets the activity return: recording its outcome meets a database
// that does not answer (a failover, a partition without RST). It returns
// the worker and the time the activity returned.
func blackholed(t *testing.T, eager bool, ttl time.Duration, log *slog.Logger) (*worker.Worker, time.Time) {
	t.Helper()
	pool, px := testdb.NewProxied(t)
	if err := pg.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	st := pg.New(pool)
	running, release := make(chan struct{}), make(chan struct{})
	reg := newRegistry()
	reg.addActivity("work", &worker.Activity{Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) {
		close(running)
		<-release // the outcome is ready only once the database is gone
		return []byte("done"), nil, nil
	}})
	reg.addWorkflow("w", func(ctx coro.Context, _ []byte) ([]byte, error) { return activity(ctx, "work", nil) })
	w, err := worker.New(st, reg, reg, worker.Options{
		Owner: "w1", Queues: []string{q}, Eager: eager, LeaseTTL: ttl,
		PollMin: 20 * time.Millisecond, PollMax: 100 * time.Millisecond, Logger: log,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Thawed, the abandoned tasks complete: drain them before the
		// database goes.
		px.Thaw()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = w.Stop(ctx)
	})
	create(t, st, "w", "w", "")
	select {
	case <-running:
	case <-time.After(10 * time.Second):
		t.Fatal("the activity did not run")
	}
	px.Freeze()
	close(release)
	return w, time.Now()
}

// T11.2: Stop returns once its context is done even when tasks are blocked
// on a database that stopped answering: the tasks are abandoned (their
// leases expire). The lease is long, so the bound on detached writes does
// not end them first.
func TestStopHonoursDeadlineWhenStoreFreezes(t *testing.T) {
	const (
		deadline = 2 * time.Second
		margin   = 4 * time.Second // stopGrace and the cancel requests' wait, with room for a loaded run
	)
	for _, eager := range []bool{true, false} {
		t.Run(fmt.Sprintf("eager=%v", eager), func(t *testing.T) {
			w, _ := blackholed(t, eager, time.Minute, slog.New(slog.DiscardHandler))
			time.Sleep(100 * time.Millisecond) // the outcome's write is under way
			ctx, cancel := context.WithTimeout(context.Background(), deadline)
			defer cancel()
			done := make(chan error, 1)
			start := time.Now()
			go func() { done <- w.Stop(ctx) }()
			select {
			case err := <-done:
				d := time.Since(start)
				t.Logf("Stop returned after %v: %v", d, err)
				if d > deadline+margin {
					t.Fatalf("Stop returned after %v, deadline %v", d, deadline)
				}
				if err == nil {
					t.Fatal("Stop abandoned a task and returned nil")
				}
			case <-time.After(deadline + 3*margin):
				t.Fatalf("Stop still blocked %v after its deadline of %v", 3*margin, deadline)
			}
		})
	}
}

// T11.2: an outcome's write, detached from the task's context so that a
// stop does not lose it, is still bounded: about LeaseTTL (past it the
// lease is lost and the write would be fenced anyway).
func TestDetachedWriteIsBoundedByLeaseTTL(t *testing.T) {
	const (
		ttl    = 3 * time.Second
		margin = 4 * time.Second // the cancel requests' wait and the record's retries, with room for a loaded run
	)
	for _, eager := range []bool{true, false} {
		t.Run(fmt.Sprintf("eager=%v", eager), func(t *testing.T) {
			tap := newLogTap("activity outcome not recorded")
			_, returned := blackholed(t, eager, ttl, slog.New(tap))
			select {
			case at := <-tap.seen:
				d := at.Sub(returned)
				t.Logf("write given up %v after the activity returned", d)
				if d > ttl+margin {
					t.Fatalf("write given up after %v, LeaseTTL %v", d, ttl)
				}
			case <-time.After(ttl + 3*margin):
				t.Fatalf("the outcome's write still blocked %v after the activity returned (LeaseTTL %v)", ttl+3*margin, ttl)
			}
		})
	}
}
