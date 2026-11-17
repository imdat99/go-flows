package worker_test

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

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
func BenchmarkActivityRun(b *testing.B) { benchChain(b, 10) }

// BenchmarkActivityRun20 is BenchmarkActivityRun with 20 activities: the
// difference is the cost of 10 activities, the rest the fixed cost of a
// workflow (start, executor, end).
func BenchmarkActivityRun20(b *testing.B) { benchChain(b, 20) }

func benchChain(b *testing.B, n int) {
	b.Helper()
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
	reg.addWorkflow("chain", chainOf(n))
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
	// Warm up (the plan's target is for a warm engine): the pool opens its
	// connections (a SCRAM handshake costs ~500 allocations), and each
	// prepares the statements.
	for i := range 3 {
		if _, _, err := st.CreateExecution(ctx, store.NewExecution{InstanceID: "warm" + strconv.Itoa(i), Workflow: "chain", Version: 1, Queue: q, Input: []byte("1")}); err != nil {
			b.Fatal(err)
		}
		<-st.ended
	}
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
	b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N*n), "µs/activity")
}

// endSignal reports completed executions on a channel, so the benchmark
// waits without polling (which would count in its allocations).
type endSignal struct {
	*pg.Store
	ended  chan struct{}
	claims atomic.Int64 // activity claims (BenchmarkEagerMisses)
}

func (s *endSignal) ClaimActivityTasks(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.ActivityTask, error) {
	s.claims.Add(1)
	return s.Store.ClaimActivityTasks(ctx, owner, queues, limit, ttl)
}

func (s *endSignal) ClaimActivityTasksDue(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.ActivityTask, time.Duration, error) {
	s.claims.Add(1)
	return s.Store.ClaimActivityTasksDue(ctx, owner, queues, limit, ttl)
}

func (s *endSignal) CompleteWorkflowTask(ctx context.Context, l store.Lease, c *store.Completion) ([]store.Lease, error) {
	leases, err := s.Store.CompleteWorkflowTask(ctx, l, c)
	if err == nil && c.Status == store.StatusCompleted {
		s.ended <- struct{}{}
	}
	return leases, err
}

// BenchmarkEagerMisses (T10.21): engines sharing a queue, each with
// eager execution, run concurrent 10-activity workflows; one op is a batch
// of perEngine workflows per engine started at once and run to their end.
// It reports how often an eager launch found no free activity slot
// (misses/activity), and how many of those while an activity claim held
// the slots (claim-misses/activity): such an activity goes through the
// queue (a NOTIFY waking every engine's poller, a claim more); and the
// activity claims of all engines (claims/activity).
func BenchmarkEagerMisses(b *testing.B) {
	for _, engines := range []int{1, 3} {
		b.Run("engines="+strconv.Itoa(engines), func(b *testing.B) { benchEngines(b, engines, 4, 10) })
	}
}

func benchEngines(b *testing.B, engines, perEngine, n int) {
	b.Helper()
	pool := testdb.New(b)
	ctx := context.Background()
	if err := pg.Migrate(ctx, pool); err != nil {
		b.Fatal(err)
	}
	batch := engines * perEngine
	ended := make(chan struct{}, batch)
	reg := newRegistry()
	reg.addWorkflow("chain", chainOf(n))
	reg.addActivity("double", &worker.Activity{Run: func(_ context.Context, t *store.ActivityTask) ([]byte, []byte, error) {
		return t.Input, nil, nil
	}})
	ws := make([]*worker.Worker, engines)
	sts := make([]*endSignal, engines)
	for i := range ws {
		p, err := pgxpool.NewWithConfig(ctx, pool.Config())
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(p.Close)
		s := &endSignal{Store: pg.New(p), ended: ended}
		sts[i] = s
		w, err := worker.New(s, reg, reg, worker.Options{Owner: "bench" + strconv.Itoa(i), Queues: []string{q}, Eager: true})
		if err != nil {
			b.Fatal(err)
		}
		if err := w.Start(ctx); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = w.Stop(ctx) })
		ws[i] = w
	}
	stats := func() (launched, missed, inClaim, claims int64) {
		for i, w := range ws {
			l, m, c := w.EagerStats()
			launched, missed, inClaim, claims = launched+l, missed+m, inClaim+c, claims+sts[i].claims.Load()
		}
		return launched, missed, inClaim, claims
	}
	runBatch := func(prefix string) {
		for j := range batch {
			if _, _, err := sts[0].CreateExecution(ctx, store.NewExecution{InstanceID: prefix + strconv.Itoa(j), Workflow: "chain", Version: 1, Queue: q, Input: []byte("1")}); err != nil {
				b.Fatal(err)
			}
		}
		for range batch {
			<-ended
		}
	}
	runBatch("warm")
	l0, m0, c0, k0 := stats()
	b.ResetTimer()
	for i := range b.N {
		runBatch("run" + strconv.Itoa(i) + "-")
	}
	b.StopTimer()
	l, m, c, k := stats()
	acts := float64(b.N * batch * n)
	b.ReportMetric(float64(b.Elapsed().Microseconds())/acts, "µs/activity")
	b.ReportMetric(float64(m-m0)/acts, "misses/activity")
	b.ReportMetric(float64(c-c0)/acts, "claim-misses/activity")
	b.ReportMetric(float64(l-l0)/acts, "eager/activity")
	b.ReportMetric(float64(k-k0)/acts, "claims/activity")
}
