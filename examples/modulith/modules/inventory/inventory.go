// Package inventory is the inventory module: stock levels, reservations,
// purchase orders and manual adjustments.
//
// It is pure domain: it knows nothing of the workflow engine. Every effect
// takes the caller's idempotency key and is made exactly once per key, in
// the same transaction as the key's record (inventory.processed_requests,
// see requests.go). The schema inventory is private to the module.
package inventory

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/schema"
)

// Business rejections: retrying the same request cannot change the answer
// (IsRejection). Any other error is transient.
var (
	// ErrOutOfStock is the error of a reservation or an adjustment that
	// would take a SKU below zero.
	ErrOutOfStock = errors.New("inventory: out of stock")
	// ErrUnknownSKU is the error of a SKU never stocked.
	ErrUnknownSKU = errors.New("inventory: unknown sku")
	// ErrRequestConflict is the error of an adjustment whose request id was
	// used with other parameters.
	ErrRequestConflict = errors.New("inventory: request id already used")
	// ErrAborted is the error of an effect whose key was settled as never
	// made (SettleReservation) before it landed.
	ErrAborted = errors.New("inventory: request aborted before it landed")
)

// IsRejection reports whether err is a business rejection of the module.
func IsRejection(err error) bool {
	return errors.Is(err, ErrOutOfStock) || errors.Is(err, ErrUnknownSKU) ||
		errors.Is(err, ErrRequestConflict) || errors.Is(err, ErrAborted)
}

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate applies the module's migrations (schema inventory).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return schema.Migrate(ctx, pool, "inventory", migrations)
}

// Stock is the state of a SKU.
type Stock struct {
	Available    int  `json:"available"`
	Discontinued bool `json:"discontinued"`
}

// Adjustment is a manual change of a level. RequestID is the caller's
// idempotency key, remembered for good in inventory.adjustments.
type Adjustment struct {
	RequestID string `json:"request_id"`
	SKU       string `json:"sku"`
	Delta     int    `json:"delta"`
	Reason    string `json:"reason"`
}

// Service is the module's API.
type Service struct {
	db  *pgxpool.Pool
	log *slog.Logger
}

// NewService returns the module's service on db.
func NewService(db *pgxpool.Pool, log *slog.Logger) (*Service, error) {
	if db == nil {
		return nil, errors.New("inventory: NewService: db is required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Service{db: db, log: log}, nil
}

// inTx runs fn in a transaction of the module's pool.
func inTx[T any](ctx context.Context, s *Service, fn func(tx pgx.Tx) (T, error)) (T, error) {
	var out T
	err := pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		var err error
		out, err = fn(tx)
		return err
	})
	return out, err
}

// Reserve takes qty units of sku for an order and returns the reservation
// id, once per key: a repeat returns the first reservation.
func (s *Service) Reserve(ctx context.Context, key, orderID, sku string, qty int) (int64, error) {
	id, err := inTx(ctx, s, func(tx pgx.Tx) (int64, error) {
		return once(ctx, tx, key, func() (int64, error) {
			tag, err := tx.Exec(ctx, `
				UPDATE inventory.stock SET available = available - $2, updated_at = now()
				WHERE sku = $1 AND available >= $2`, sku, qty)
			if err != nil {
				return 0, err
			}
			if tag.RowsAffected() == 0 {
				return 0, stockError(ctx, tx, sku)
			}
			var id int64
			err = tx.QueryRow(ctx, `
				INSERT INTO inventory.reservations (order_id, sku, qty) VALUES ($1, $2, $3)
				RETURNING id`, orderID, sku, qty).Scan(&id)
			return id, err
		})
	})
	if err != nil {
		return 0, fmt.Errorf("reserve %s for %s: %w", sku, orderID, err)
	}
	s.log.LogAttrs(ctx, slog.LevelInfo, "inventory: reserved",
		slog.String("order_id", orderID), slog.String("sku", sku), slog.Int("qty", qty))
	return id, nil
}

// stockError tells out of stock from unknown.
func stockError(ctx context.Context, tx pgx.Tx, sku string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM inventory.stock WHERE sku = $1)`, sku).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: %s", ErrUnknownSKU, sku)
	}
	return fmt.Errorf("%w: %s", ErrOutOfStock, sku)
}

// Release gives back a reservation. Idempotent: deleting the reservation's
// row is the record of the release.
func (s *Service) Release(ctx context.Context, reservationID int64) error {
	_, err := inTx(ctx, s, func(tx pgx.Tx) (struct{}, error) {
		return struct{}{}, release(ctx, s, tx, reservationID)
	})
	return err
}

// SettleReservation releases the reservation made under key, if it was
// made, for a caller that does not know whether Reserve landed (it timed
// out, or its process died). Otherwise it fences the key, so that a
// Reserve still in flight under it fails with ErrAborted instead of
// reserving. It waits for such an attempt's transaction to end first.
func (s *Service) SettleReservation(ctx context.Context, key string) error {
	_, err := inTx(ctx, s, func(tx pgx.Tx) (struct{}, error) {
		id, made, err := abort[int64](ctx, tx, key)
		if err != nil || !made {
			return struct{}{}, err // fenced: nothing was reserved, and nothing will be
		}
		return struct{}{}, release(ctx, s, tx, id)
	})
	return err
}

func release(ctx context.Context, s *Service, tx pgx.Tx, id int64) error {
	var sku string
	var qty int
	err := tx.QueryRow(ctx, `DELETE FROM inventory.reservations WHERE id = $1 RETURNING sku, qty`, id).Scan(&sku, &qty)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // released already
	}
	if err != nil {
		return fmt.Errorf("release reservation %d: %w", id, err)
	}
	if _, err := tx.Exec(ctx, `UPDATE inventory.stock SET available = available + $2, updated_at = now() WHERE sku = $1`,
		sku, qty); err != nil {
		return fmt.Errorf("release reservation %d: %w", id, err)
	}
	s.log.LogAttrs(ctx, slog.LevelInfo, "inventory: released", slog.String("sku", sku), slog.Int("qty", qty))
	return nil
}

// Stock returns the state of sku; ErrUnknownSKU if it was never stocked.
func (s *Service) Stock(ctx context.Context, sku string) (Stock, error) {
	var st Stock
	err := s.db.QueryRow(ctx, `SELECT available, discontinued FROM inventory.stock WHERE sku = $1`, sku).
		Scan(&st.Available, &st.Discontinued)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stock{}, fmt.Errorf("%w: %s", ErrUnknownSKU, sku)
	}
	if err != nil {
		return Stock{}, fmt.Errorf("stock of %s: %w", sku, err)
	}
	return st, nil
}

// PlacePurchaseOrder orders qty units of sku from the supplier, once per
// key, and returns the purchase order id. The stub supplier delivers at
// once, so the order and the receipt are one change.
func (s *Service) PlacePurchaseOrder(ctx context.Context, key, sku string, qty int) (int64, error) {
	id, err := inTx(ctx, s, func(tx pgx.Tx) (int64, error) {
		return once(ctx, tx, key, func() (int64, error) {
			var id int64
			if err := tx.QueryRow(ctx, `INSERT INTO inventory.purchase_orders (sku, qty) VALUES ($1, $2) RETURNING id`,
				sku, qty).Scan(&id); err != nil {
				return 0, err
			}
			_, err := tx.Exec(ctx, `UPDATE inventory.stock SET available = available + $2, updated_at = now() WHERE sku = $1`,
				sku, qty)
			return id, err
		})
	})
	if err != nil {
		return 0, fmt.Errorf("purchase order for %s: %w", sku, err)
	}
	s.log.LogAttrs(ctx, slog.LevelInfo, "inventory: purchase order received",
		slog.String("sku", sku), slog.Int("qty", qty))
	return id, nil
}

// ApplyAdjustment changes a level (creating the SKU if needed), once per
// key, and returns the new level. ErrOutOfStock if it would go below zero;
// ErrRequestConflict if a.RequestID was used by another adjustment.
func (s *Service) ApplyAdjustment(ctx context.Context, key string, a Adjustment) (int, error) {
	n, err := inTx(ctx, s, func(tx pgx.Tx) (int, error) {
		return once(ctx, tx, key, func() (int, error) {
			var n int
			err := tx.QueryRow(ctx, `
				INSERT INTO inventory.stock (sku, available) VALUES ($1, $2)
				ON CONFLICT (sku) DO UPDATE SET available = stock.available + $2, updated_at = now()
				WHERE stock.available + $2 >= 0
				RETURNING available`, a.SKU, a.Delta).Scan(&n)
			var pgErr *pgconn.PgError
			if errors.Is(err, pgx.ErrNoRows) || errors.As(err, &pgErr) && pgErr.Code == "23514" { // check_violation
				return 0, fmt.Errorf("%w: %s by %d", ErrOutOfStock, a.SKU, a.Delta)
			}
			if err != nil {
				return 0, err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO inventory.adjustments (request_id, sku, delta, reason) VALUES ($1, $2, $3, $4)`,
				a.RequestID, a.SKU, a.Delta, a.Reason); err != nil {
				if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation: the request id was used
					return 0, fmt.Errorf("%w: %s", ErrRequestConflict, a.RequestID)
				}
				return 0, err
			}
			return n, nil
		})
	})
	if err != nil {
		return 0, fmt.Errorf("adjust %s: %w", a.SKU, err)
	}
	s.log.LogAttrs(ctx, slog.LevelInfo, "inventory: adjusted",
		slog.String("sku", a.SKU), slog.Int("delta", a.Delta), slog.Int("available", n))
	return n, nil
}

// Applied reports whether a's request id was applied, and fails with
// ErrRequestConflict if it was applied with other parameters. The
// adjustments table remembers request ids for good.
func (s *Service) Applied(ctx context.Context, a Adjustment) (bool, error) {
	var got Adjustment
	err := s.db.QueryRow(ctx, `SELECT sku, delta, reason FROM inventory.adjustments WHERE request_id = $1`,
		a.RequestID).Scan(&got.SKU, &got.Delta, &got.Reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("adjustment %s: %w", a.RequestID, err)
	}
	got.RequestID = a.RequestID
	if got != a {
		return true, fmt.Errorf("%w: %s", ErrRequestConflict, a.RequestID)
	}
	return true, nil
}

// Level returns the units of sku available; ErrUnknownSKU if never stocked.
func (s *Service) Level(ctx context.Context, sku string) (int, error) {
	st, err := s.Stock(ctx, sku)
	return st.Available, err
}

// Replenishable lists the stocked SKUs that are not discontinued.
func (s *Service) Replenishable(ctx context.Context) ([]string, error) {
	rows, err := s.db.Query(ctx, `SELECT sku FROM inventory.stock WHERE NOT discontinued ORDER BY sku`)
	if err != nil {
		return nil, fmt.Errorf("list skus: %w", err)
	}
	skus, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("list skus: %w", err)
	}
	return skus, nil
}

// Discontinue marks sku discontinued, for good: it is not replenished any
// more. ErrUnknownSKU if never stocked.
func (s *Service) Discontinue(ctx context.Context, sku string) error {
	tag, err := s.db.Exec(ctx, `UPDATE inventory.stock SET discontinued = true, updated_at = now() WHERE sku = $1`, sku)
	if err != nil {
		return fmt.Errorf("discontinue %s: %w", sku, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrUnknownSKU, sku)
	}
	return nil
}

// PurgeRequests deletes up to limit records of effects older than age, and
// returns how many it deleted. Keep age well above the longest time a
// caller can take to repeat or settle an effect: a purge reopens its key.
func (s *Service) PurgeRequests(ctx context.Context, age time.Duration, limit int) (int64, error) {
	tag, err := s.db.Exec(ctx, `
		DELETE FROM inventory.processed_requests WHERE key IN (
			SELECT key FROM inventory.processed_requests
			WHERE created_at < now() - $1 * interval '1 millisecond' LIMIT $2)`, age.Milliseconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge requests: %w", err)
	}
	return tag.RowsAffected(), nil
}
