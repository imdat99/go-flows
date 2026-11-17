// Package schema applies a module's own migrations to its own Postgres
// schema.
//
// Each module owns one schema (orders, payments, ...) and keeps its goose
// migrations next to its code (migrations/*.sql, embedded). Versions are
// recorded per module, in <schema>.goose_db_version, so modules evolve
// independently and a module can later move to its own database without
// renumbering anything.
package schema

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"io/fs"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// Migrate creates schema if needed and applies the migrations of fsys (the
// *.sql files of its directory "migrations") that are not applied yet.
// Several processes may run it at once: an advisory lock per schema
// serializes them. The pool needs at least 3 connections (goose holds its
// lock on one).
func Migrate(ctx context.Context, pool *pgxpool.Pool, schema string, fsys fs.FS) error {
	id := lockID(schema)
	// CREATE SCHEMA IF NOT EXISTS is not safe against itself when run
	// concurrently (a unique violation on the catalog): serialize it.
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+pgx.Identifier{schema}.Sanitize())
		return err
	})
	if err != nil {
		return fmt.Errorf("create schema %s: %w", schema, err)
	}

	dir, err := fs.Sub(fsys, "migrations")
	if err != nil {
		return fmt.Errorf("migrations of %s: %w", schema, err)
	}
	db := stdlib.OpenDBFromPool(pool)
	defer func(db *sql.DB) { _ = db.Close() }(db)
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockID(id))
	if err != nil {
		return fmt.Errorf("migrations of %s: %w", schema, err)
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, dir,
		goose.WithTableName(schema+".goose_db_version"),
		goose.WithSessionLocker(locker),
		goose.WithDisableGlobalRegistry(true))
	if err != nil {
		return fmt.Errorf("migrations of %s: %w", schema, err)
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("migrate %s: %w", schema, err)
	}
	return nil
}

// lockID derives a stable advisory lock id from the schema name.
func lockID(schema string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("modulith.schema." + schema))
	return int64(h.Sum64() >> 1) // shifted: fits in int64
}
