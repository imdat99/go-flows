package inventory

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/idem"
)

// LineRequest is the input of ReserveLine.
type LineRequest struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
	Qty     int    `json:"qty"`
}

// Adjustment is the input of the adjust workflow and its activity.
type Adjustment struct {
	RequestID string `json:"request_id"`
	SKU       string `json:"sku"`
	Delta     int    `json:"delta"`
	Reason    string `json:"reason"`
}

// PurchaseOrder is the input of placePurchaseOrder.
type PurchaseOrder struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

var activityOpts = []flows.ActivityOption{
	flows.WithQueue(Queue),
	flows.WithRetry(flows.Exponential(5, 200*time.Millisecond, 5*time.Second)),
	flows.WithCompensateRetry(flows.Exponential(20, time.Second, time.Minute)),
	flows.WithTimeout(10 * time.Second),
}

var (
	// ReserveLine reserves one line; its compensator releases it. Workflows
	// of other modules do not call it (they start Reserve): it is exported
	// so that their tests can mock it (flowtest.OnActivity) when Reserve
	// runs as their child.
	ReserveLine = flows.DefineActivity("inventory.reserve-line", reserveLine, releaseLine, activityOpts...)

	checkLevel         = flows.DefineActivity("inventory.check-level", checkLevelFn, nil, activityOpts...)
	placePurchaseOrder = flows.DefineActivity("inventory.place-purchase-order", placePurchaseOrderFn, nil, activityOpts...)
	applyAdjustment    = flows.DefineActivity("inventory.apply-adjustment", applyAdjustmentFn, nil, activityOpts...)
)

// inTx runs fn in a transaction of the module's pool.
func inTx[T any](ctx context.Context, fn func(c *Config, tx pgx.Tx) (T, error)) (T, error) {
	var out T
	c, err := cfg.Get()
	if err != nil {
		return out, err
	}
	err = pgx.BeginFunc(ctx, c.DB, func(tx pgx.Tx) error {
		var err error
		out, err = fn(c, tx)
		return err
	})
	return out, err
}

// reserveLine takes qty units of sku for an order, once per key: idem.Do
// commits the change and the key together, so a retried or re-run attempt
// returns the first reservation instead of reserving twice.
func reserveLine(ctx context.Context, k flows.Key, r LineRequest) (int64, int64, error) {
	id, err := inTx(ctx, func(c *Config, tx pgx.Tx) (int64, error) {
		return idem.Do(ctx, tx, requests, k, func(ctx context.Context) (int64, error) {
			tag, err := tx.Exec(ctx, `
				UPDATE inventory.stock SET available = available - $2, updated_at = now()
				WHERE sku = $1 AND available >= $2`, r.SKU, r.Qty)
			if err != nil {
				return 0, err
			}
			if tag.RowsAffected() == 0 {
				return 0, flows.Permanent(stockError(ctx, tx, r.SKU))
			}
			var id int64
			err = tx.QueryRow(ctx, `
				INSERT INTO inventory.reservations (order_id, sku, qty) VALUES ($1, $2, $3)
				RETURNING id`, r.OrderID, r.SKU, r.Qty).Scan(&id)
			if err == nil {
				c.Logger.LogAttrs(ctx, slog.LevelInfo, "inventory: reserved",
					slog.String("order_id", r.OrderID), slog.String("sku", r.SKU), slog.Int("qty", r.Qty))
			}
			return id, err
		})
	})
	if err != nil {
		return 0, 0, fmt.Errorf("reserve %s for %s: %w", r.SKU, r.OrderID, err)
	}
	return id, id, nil
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

// releaseLine undoes reserveLine. Deleting the reservation row makes it
// idempotent. When the reservation is uncertain (its attempt timed out or
// was lost), idem.Abort settles it: it returns the reservation if it
// committed, and otherwise fences the key so a late attempt cannot reserve.
func releaseLine(ctx context.Context, k flows.Key, id int64) error {
	_, err := inTx(ctx, func(c *Config, tx pgx.Tx) (struct{}, error) {
		if flows.CompensationUncertain(ctx) {
			got, done, err := idem.Abort[int64](ctx, tx, requests, k)
			if err != nil || !done {
				return struct{}{}, err // nothing was reserved
			}
			id = got
		}
		var sku string
		var qty int
		err := tx.QueryRow(ctx, `DELETE FROM inventory.reservations WHERE id = $1 RETURNING sku, qty`, id).Scan(&sku, &qty)
		if errors.Is(err, pgx.ErrNoRows) {
			return struct{}{}, nil // released already
		}
		if err != nil {
			return struct{}{}, err
		}
		_, err = tx.Exec(ctx, `UPDATE inventory.stock SET available = available + $2, updated_at = now() WHERE sku = $1`, sku, qty)
		if err == nil {
			c.Logger.LogAttrs(ctx, slog.LevelInfo, "inventory: released", slog.String("sku", sku), slog.Int("qty", qty))
		}
		return struct{}{}, err
	})
	if err != nil {
		return fmt.Errorf("release reservation %d: %w", id, err)
	}
	return nil
}

// StockLevel is the output of checkLevel.
type StockLevel struct {
	Available    int  `json:"available"`
	Discontinued bool `json:"discontinued"`
}

// checkLevelFn reads a level and the discontinued flag; reads need no
// idempotency.
func checkLevelFn(ctx context.Context, _ flows.Key, sku string) (StockLevel, struct{}, error) {
	c, err := cfg.Get()
	if err != nil {
		return StockLevel{}, struct{}{}, err
	}
	var l StockLevel
	err = c.DB.QueryRow(ctx, `SELECT available, discontinued FROM inventory.stock WHERE sku = $1`, sku).
		Scan(&l.Available, &l.Discontinued)
	if errors.Is(err, pgx.ErrNoRows) {
		return StockLevel{}, struct{}{}, flows.Permanent(fmt.Errorf("%w: %s", ErrUnknownSKU, sku))
	}
	if err != nil {
		return StockLevel{}, struct{}{}, fmt.Errorf("level of %s: %w", sku, err)
	}
	return l, struct{}{}, nil
}

// placePurchaseOrderFn orders stock from the supplier. The stub supplier
// delivers at once, so the order and the receipt are one change.
func placePurchaseOrderFn(ctx context.Context, k flows.Key, po PurchaseOrder) (int64, struct{}, error) {
	id, err := inTx(ctx, func(c *Config, tx pgx.Tx) (int64, error) {
		return idem.Do(ctx, tx, requests, k, func(ctx context.Context) (int64, error) {
			var id int64
			if err := tx.QueryRow(ctx, `INSERT INTO inventory.purchase_orders (sku, qty) VALUES ($1, $2) RETURNING id`,
				po.SKU, po.Qty).Scan(&id); err != nil {
				return 0, err
			}
			if _, err := tx.Exec(ctx, `UPDATE inventory.stock SET available = available + $2, updated_at = now() WHERE sku = $1`,
				po.SKU, po.Qty); err != nil {
				return 0, err
			}
			c.Logger.LogAttrs(ctx, slog.LevelInfo, "inventory: purchase order received",
				slog.String("sku", po.SKU), slog.Int("qty", po.Qty))
			return id, nil
		})
	})
	if err != nil {
		return 0, struct{}{}, fmt.Errorf("purchase order for %s: %w", po.SKU, err)
	}
	return id, struct{}{}, nil
}

// applyAdjustmentFn changes a level once per key and returns the new level.
func applyAdjustmentFn(ctx context.Context, k flows.Key, a Adjustment) (int, struct{}, error) {
	n, err := inTx(ctx, func(c *Config, tx pgx.Tx) (int, error) {
		return idem.Do(ctx, tx, requests, k, func(ctx context.Context) (int, error) {
			var n int
			err := tx.QueryRow(ctx, `
				INSERT INTO inventory.stock (sku, available) VALUES ($1, $2)
				ON CONFLICT (sku) DO UPDATE SET available = stock.available + $2, updated_at = now()
				WHERE stock.available + $2 >= 0
				RETURNING available`, a.SKU, a.Delta).Scan(&n)
			var pgErr *pgconn.PgError
			if errors.Is(err, pgx.ErrNoRows) || errors.As(err, &pgErr) && pgErr.Code == "23514" { // check_violation
				return 0, flows.Permanent(fmt.Errorf("%w: %s by %d", ErrOutOfStock, a.SKU, a.Delta))
			}
			if err != nil {
				return 0, err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO inventory.adjustments (request_id, sku, delta, reason) VALUES ($1, $2, $3, $4)`,
				a.RequestID, a.SKU, a.Delta, a.Reason); err != nil {
				if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation: the request id was used
					return 0, flows.Permanent(fmt.Errorf("%w: %s", ErrRequestConflict, a.RequestID))
				}
				return 0, err
			}
			c.Logger.LogAttrs(ctx, slog.LevelInfo, "inventory: adjusted",
				slog.String("sku", a.SKU), slog.Int("delta", a.Delta), slog.Int("available", n))
			return n, nil
		})
	})
	if err != nil {
		return 0, struct{}{}, fmt.Errorf("adjust %s: %w", a.SKU, err)
	}
	return n, struct{}{}, nil
}
