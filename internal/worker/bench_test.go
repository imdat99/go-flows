package worker_test

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/testdb"
	"github.com/imdat99/go-flows/internal/worker"
)

// BenchmarkActivityRun: one op is a workflow of 10 sequential no-op
// activities, run end to end by a worker on Postgres with eager execution
// (the plan's main performance target: 2 round trips per activity). It
// includes starting the workflow and waiting for its end.
func BenchmarkActivityRun(b *testing.B) {
	pool := testdb.New(b)
	ctx := context.Background()
	if err := pg.Migrate(ctx, pool); err != nil {
		b.Fatal(err)
	}
	if os.Getenv("FLOWS_BENCH_ASYNC_COMMIT") != "" {
		cfg := pool.Config()
		cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "off"
		async, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(async.Close)
		pool = async
	}
	st := &endSignal{Store: pg.New(pool), ended: make(chan struct{}, 1)}
	reg := newRegistry()
	reg.addWorkflow("chain", chainOf(10))
	reg.addActivity("double", &worker.Activity{Run: func(_ context.Context, t *store.ActivityTask) ([]byte, []byte, error) {
		return t.Input, nil, nil
	}})
	w, err := worker.New(st, reg, reg, worker.Options{Owner: "bench", Queues: []string{q}, Eager: true})
	if err != nil {
		b.Fatal(err)
	}
	if err := w.Start(ctx); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = w.Stop(ctx) })
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		id := "run" + strconv.Itoa(i)
		if _, _, err := st.CreateExecution(ctx, store.NewExecution{InstanceID: id, Workflow: "chain", Version: 1, Queue: q, Input: []byte("1")}); err != nil {
			b.Fatal(err)
		}
		<-st.ended
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N*10), "µs/activity")
}

// endSignal reports completed executions on a channel, so the benchmark
// waits without polling (which would count in its allocations).
type endSignal struct {
	*pg.Store
	ended chan struct{}
}

func (s *endSignal) CompleteWorkflowTask(ctx context.Context, l store.Lease, c *store.Completion) ([]store.Lease, error) {
	leases, err := s.Store.CompleteWorkflowTask(ctx, l, c)
	if err == nil && c.Status == store.StatusCompleted {
		s.ended <- struct{}{}
	}
	return leases, err
}
