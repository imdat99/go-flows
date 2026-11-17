package inventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Exactly-once effects, by the caller's idempotency key, in
// inventory.processed_requests:
//
//   - once claims the key, makes the effect and stores its result, in the
//     caller's transaction: they commit together or not at all. A repeat
//     finds the key and returns the stored result. A concurrent repeat
//     waits on the key's row until the first transaction ends.
//   - abort settles a key whose effect the caller is unsure of: it
//     returns the result if the effect was made, and otherwise records a
//     tombstone, so that an attempt still in flight under the key fails
//     with ErrAborted instead of making the effect late.

// once runs fn once per key in tx. fn writes in tx.
func once[T any](ctx context.Context, tx pgx.Tx, key string, fn func() (T, error)) (T, error) {
	var zero T
	tag, err := tx.Exec(ctx, `INSERT INTO inventory.processed_requests (key) VALUES ($1) ON CONFLICT (key) DO NOTHING`, key)
	if err != nil {
		return zero, fmt.Errorf("claim %s: %w", key, err)
	}
	if tag.RowsAffected() == 0 {
		v, made, err := lookup[T](ctx, tx, key)
		switch {
		case err != nil:
			return zero, err
		case !made:
			return zero, fmt.Errorf("%w: %s", ErrAborted, key)
		}
		return v, nil
	}
	v, err := fn()
	if err != nil {
		return zero, err // the caller rolls back: the key stays free
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return zero, fmt.Errorf("encode result of %s: %w", key, err)
	}
	if _, err := tx.Exec(ctx, `UPDATE inventory.processed_requests SET result = $2 WHERE key = $1`, key, raw); err != nil {
		return zero, fmt.Errorf("store result of %s: %w", key, err)
	}
	return v, nil
}

// abort settles key in tx: the stored result and true if the effect was
// made, or a tombstone and false.
func abort[T any](ctx context.Context, tx pgx.Tx, key string) (T, bool, error) {
	var zero T
	tag, err := tx.Exec(ctx,
		`INSERT INTO inventory.processed_requests (key, aborted) VALUES ($1, true) ON CONFLICT (key) DO NOTHING`, key)
	if err != nil {
		return zero, false, fmt.Errorf("abort %s: %w", key, err)
	}
	if tag.RowsAffected() == 1 {
		return zero, false, nil // fenced off: it never happened and never will
	}
	return lookup[T](ctx, tx, key)
}

// lookup returns the stored result of key, and false for a tombstone.
func lookup[T any](ctx context.Context, tx pgx.Tx, key string) (T, bool, error) {
	var v T
	var raw []byte
	var aborted bool
	err := tx.QueryRow(ctx, `SELECT result, aborted FROM inventory.processed_requests WHERE key = $1`, key).
		Scan(&raw, &aborted)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, false, fmt.Errorf("key %s vanished after a conflict", key)
	}
	if err != nil {
		return v, false, fmt.Errorf("lookup %s: %w", key, err)
	}
	if aborted {
		return v, false, nil
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &v); err != nil {
			return v, true, fmt.Errorf("decode result of %s: %w", key, err)
		}
	}
	return v, true, nil
}
