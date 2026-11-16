// Package goworkflows benchmarks go-workflows (commit 48e8119, the version
// audited for go-flows) on the scenario of go-flows' BenchmarkActivityRun:
// a workflow of 10 sequential no-op activities on Postgres. See
// docs/benchmarks.md. FLOWS_BENCH_PG (host:port, default localhost:5433)
// and the flows/flows user of the repository's docker compose are used.
package goworkflows

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/backend/postgres"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/worker"
	"github.com/cschleiden/go-workflows/workflow"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func Noop(_ context.Context, n int) (int, error) { return n + 1, nil }

func Chain(ctx workflow.Context, n int) (int, error) {
	for range 10 {
		var err error
		if n, err = workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, Noop, n).Get(ctx); err != nil {
			return 0, err
		}
	}
	return n, nil
}

var ids atomic.Int64

const user, password = "flows", "flows"

// pgAddr reads FLOWS_BENCH_PG.
func pgAddr(b *testing.B) (string, int) {
	b.Helper()
	hp := os.Getenv("FLOWS_BENCH_PG")
	if hp == "" {
		hp = "localhost:5433"
	}
	h, p, ok := strings.Cut(hp, ":")
	n, err := strconv.Atoi(p)
	if !ok || err != nil {
		b.Fatalf("FLOWS_BENCH_PG=%q: want host:port", hp)
	}
	return h, n
}

func run(b *testing.B, poll time.Duration) {
	b.Helper()
	host, port := pgAddr(b)
	db, err := sql.Open("pgx", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=flows sslmode=disable", host, port, user, password))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	name := fmt.Sprintf("gwbench_%d", time.Now().UnixNano())
	if _, err := db.ExecContext(context.Background(), "CREATE DATABASE "+name); err != nil {
		b.Fatal(err)
	}
	// Registered before the backend's Close (cleanups run last in, first
	// out): the database is dropped once nothing uses it.
	b.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") })
	be := postgres.NewPostgresBackend(host, port, user, password, name,
		postgres.WithBackendOptions(backend.WithLogger(slog.New(slog.DiscardHandler))))
	defer be.Close()
	opts := worker.DefaultOptions
	opts.WorkflowPollingInterval, opts.ActivityPollingInterval = poll, poll
	w := worker.New(be, &opts)
	w.RegisterWorkflow(Chain)
	w.RegisterActivity(Noop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := w.Start(ctx); err != nil {
		b.Fatal(err)
	}
	c := client.New(be)
	// GetWorkflowResult polls with a growing backoff, which adds its
	// overshoot to b.Elapsed; the history's timestamps (start to finish)
	// give the run time without it (run-ms).
	var runs time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		id := "run-" + strconv.FormatInt(ids.Add(1), 10)
		inst, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{InstanceID: id}, Chain, 0)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := client.GetWorkflowResult[int](ctx, c, inst, time.Minute); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		runs += runTime(ctx, b, be, inst)
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(runs.Microseconds())/1e3/float64(b.N), "run-ms")
	b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N*10), "µs/activity")
}

// runTime is the time from the instance's start event to its finish event.
func runTime(ctx context.Context, b *testing.B, be backend.Backend, inst *workflow.Instance) time.Duration {
	b.Helper()
	h, err := be.GetWorkflowInstanceHistory(ctx, inst, nil)
	if err != nil {
		b.Fatal(err)
	}
	var start, end time.Time
	for _, e := range h {
		switch e.Type {
		case history.EventType_WorkflowExecutionStarted:
			start = e.Timestamp
		case history.EventType_WorkflowExecutionFinished:
			end = e.Timestamp
		}
	}
	if start.IsZero() || end.IsZero() {
		b.Fatalf("%s: no start or finish event", inst.InstanceID)
	}
	return end.Sub(start)
}

func BenchmarkChainDefaultPolling(b *testing.B) { run(b, 200*time.Millisecond) }
func BenchmarkChainFastPolling(b *testing.B)    { run(b, time.Millisecond) }
