package pg_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/testdb"
	"github.com/imdat99/go-flows/migrations"
)

// T10.19: on a fresh database every execution's keys carry its id.
func TestKeyEpochFresh(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if got, err := pg.New(pool).KeyEpoch(ctx); err != nil || got != 1 {
		t.Fatalf("epoch %d %v, want 1", got, err)
	}
}

// T10.19: executions that exist when 00016 runs (and ids allocated by
// transactions not committed yet) are below the epoch: they keep the
// legacy key format.
func TestKeyEpochAfterExistingExecutions(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+pg.Schema); err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDBFromPool(pool)
	defer func(db *sql.DB) { _ = db.Close() }(db)
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS,
		goose.WithTableName(pg.Schema+".goose_db_version"), goose.WithDisableGlobalRegistry(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.UpTo(ctx, 15); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c"} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO flows.execution (instance_id, workflow, version, queue, status)
			VALUES ($1, 'w', 1, 'q', 'running')`, id); err != nil {
			t.Fatal(err)
		}
	}
	// An id taken by a transaction that has not committed yet.
	if _, err := pool.Exec(ctx, `SELECT nextval(pg_get_serial_sequence('flows.execution', 'id'))`); err != nil {
		t.Fatal(err)
	}
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if got, err := pg.New(pool).KeyEpoch(ctx); err != nil || got != 5 {
		t.Fatalf("epoch %d %v, want 5", got, err)
	}
	// Running it again keeps the epoch.
	if _, err := pool.Exec(ctx, `
		INSERT INTO flows.execution (instance_id, workflow, version, queue, status)
		VALUES ('d', 'w', 1, 'q', 'running')`); err != nil {
		t.Fatal(err)
	}
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if got, err := pg.New(pool).KeyEpoch(ctx); err != nil || got != 5 {
		t.Fatalf("epoch after a second Migrate %d %v, want 5", got, err)
	}
}

// Without the migration the epoch is unknown: an error, never a guess.
func TestKeyEpochUnmigrated(t *testing.T) {
	pool := testdb.New(t)
	if _, err := pg.New(pool).KeyEpoch(context.Background()); err == nil {
		t.Fatal("no error without the key_epoch table")
	}
}
