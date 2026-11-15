package pg

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/imdat99/go-flows/migrations"
)

// Schema is the Postgres schema holding the engine's tables.
const Schema = "flows"

// migrateLockID serializes Migrate across instances starting at once; goose
// takes its own session lock inside it as well (ported from saga).
const migrateLockID = 0x666c_0001

// MinMigrateConns is the pool size Migrate needs: one connection holds the
// lock, goose's locker holds another, and the statements need a third.
const MinMigrateConns = 3

// Migrate creates the schema and applies the migrations. It holds a session
// advisory lock throughout, so instances starting at once migrate one after
// another, and returns errors instead of panicking (GW-7). Migrations use
// IF NOT EXISTS and never drop data on the way up. Run it over a direct
// connection, not through PgBouncer in transaction mode.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if n := pool.Config().MaxConns; n < MinMigrateConns {
		return fmt.Errorf("flows: Migrate needs a pool of at least %d connections, got %d", MinMigrateConns, n)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockID); err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	defer func() {
		// Unlock even if ctx is done; a broken connection drops the lock anyway.
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrateLockID)
	}()
	if _, err := conn.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+Schema); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	db := stdlib.OpenDBFromPool(pool)
	defer func(db *sql.DB) { _ = db.Close() }(db)
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("migration locker: %w", err)
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS,
		goose.WithTableName(Schema+".goose_db_version"),
		goose.WithSessionLocker(locker),
		goose.WithDisableGlobalRegistry(true))
	if err != nil {
		return fmt.Errorf("migration provider: %w", err)
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}
