package pg_test

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/testdb"
)

// GW-7: three instances migrating the same database at once all succeed,
// and running Migrate again is a no-op.
func TestGW7_ConcurrentMigrate(t *testing.T) {
	base := testdb.New(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := range errs {
		cfg, err := pgxpool.ParseConfig(testdb.DSN(base))
		if err != nil {
			t.Fatal(err)
		}
		cfg.MaxConns = pg.MinMigrateConns
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		wg.Go(func() { errs[i] = pg.Migrate(ctx, pool) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("instance %d: %v", i, err)
		}
	}
	if err := pg.Migrate(ctx, base); err != nil {
		t.Fatalf("second run: %v", err)
	}
	var n int
	if err := base.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname = 'flows'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 9 { // execution, history, pending_event, activity_task, lock, lock_waiter, key_epoch, purged_instance, goose_db_version
		t.Fatalf("%d tables in schema flows, want 9", n)
	}
}

func TestMigrateRejectsSmallPool(t *testing.T) {
	base := testdb.New(t)
	cfg, err := pgxpool.ParseConfig(testdb.DSN(base))
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pg.Migrate(context.Background(), pool); err == nil {
		t.Fatal("Migrate with a 1-connection pool must fail (it would deadlock)")
	}
}
