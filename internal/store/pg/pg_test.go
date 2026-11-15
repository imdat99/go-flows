package pg_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/store/storetest"
	"github.com/imdat99/go-flows/internal/testdb"
)

type storeT = pg.Store

func newStore(t *testing.T) *pg.Store {
	t.Helper()
	return newStoreB(t)
}

func newStoreB(tb testing.TB) *pg.Store {
	tb.Helper()
	pool := testdb.New(tb)
	if err := pg.Migrate(context.Background(), pool); err != nil {
		tb.Fatal(err)
	}
	if os.Getenv("FLOWS_BENCH_ASYNC_COMMIT") != "" {
		// Commit cost removed, so the CPU and round-trip side shows (as in
		// saga's benchmarks).
		cfg := pool.Config()
		cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "off"
		async, err := pgxpool.NewWithConfig(context.Background(), cfg)
		if err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(async.Close)
		pool = async
	}
	return pg.New(pool)
}

func TestConformance(t *testing.T) {
	storetest.Run(t, storetest.Harness{
		New: func(t *testing.T) store.Store {
			t.Helper()
			return newStore(t)
		},
		Advance: func(t *testing.T, _ store.Store, d time.Duration) {
			t.Helper()
			time.Sleep(d)
		},
		TTL: time.Second, // generous: 40 parallel cases share one Postgres
	})
}

// The pgx path, used when the fast path cannot be (pools without the
// statement cache, as behind PgBouncer in transaction mode, or with a
// tracer), passes the same suite.
func TestConformanceWithoutFastPath(t *testing.T) {
	if testing.Short() {
		t.Skip("covered by TestConformance in -short runs")
	}
	storetest.Run(t, storetest.Harness{
		New: func(t *testing.T) store.Store {
			t.Helper()
			base := testdb.New(t)
			if err := pg.Migrate(context.Background(), base); err != nil {
				t.Fatal(err)
			}
			cfg := base.Config()
			cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
			pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pool.Close)
			s := pg.New(pool)
			if s.FastPath() {
				t.Fatal("fast path enabled on an exec-mode pool")
			}
			return s
		},
		Advance: func(t *testing.T, _ store.Store, d time.Duration) {
			t.Helper()
			time.Sleep(d)
		},
		TTL: time.Second,
	})
}
