// Package inventory is the inventory module: stock levels, reservations,
// replenishment and manual adjustments.
//
// Public API:
//   - Reserve, a workflow that other modules start as a child: it reserves
//     the lines of an order, and when the parent compensates it is reopened
//     and releases them (its own activities' compensators, by name);
//   - Adjust and Level, Go functions for the HTTP API;
//   - StartReplenishment and StopReplenishment for the long-running
//     replenishment loop of each SKU (ContinueAsNew, flows.Lock);
//   - Queue, Configure, Migrate, ErrOutOfStock, ErrUnknownSKU.
//
// The schema inventory is private to the module. Effects are made
// exactly once with package idem, keyed by the activity's flows.Key.
package inventory

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/deps"
	"github.com/imdat99/go-flows/examples/modulith/internal/platform/schema"
	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/idem"
)

// Queue is the queue of the module's workflows and activities.
const Queue = "inventory"

var (
	// ErrOutOfStock is the error of a reservation or an adjustment that
	// would take a SKU below zero.
	ErrOutOfStock = errors.New("inventory: out of stock")
	// ErrUnknownSKU is the error of a reservation of a SKU never stocked.
	ErrUnknownSKU = errors.New("inventory: unknown sku")
	// ErrRequestConflict is the error of an adjustment whose request id was
	// used with other parameters.
	ErrRequestConflict = errors.New("inventory: request id already used")
)

func init() {
	flows.RegisterErrorKind("inventory_out_of_stock", ErrOutOfStock)
	flows.RegisterErrorKind("inventory_unknown_sku", ErrUnknownSKU)
	flows.RegisterErrorKind("inventory_request_conflict", ErrRequestConflict)
}

// Config holds the module's dependencies and settings.
type Config struct {
	DB     *pgxpool.Pool
	Logger *slog.Logger
	// ReplenishEvery is the period of the replenishment loops (default 1m).
	// It is passed to the loops when they start: changing it affects new
	// loops only (workflow code must not read configuration, see README).
	ReplenishEvery time.Duration
	// ReorderPoint and ReorderQty: below ReorderPoint units, a loop orders
	// ReorderQty more (defaults 5 and 20).
	ReorderPoint, ReorderQty int
}

var cfg = deps.New[Config]("inventory")

// Configure sets the module's dependencies. Call it before the engine
// starts, in every process that runs the module or calls its functions.
func Configure(c Config) error {
	if c.DB == nil {
		return errors.New("inventory: Configure: DB is required")
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.ReplenishEvery <= 0 {
		c.ReplenishEvery = time.Minute
	}
	if c.ReorderPoint <= 0 {
		c.ReorderPoint = 5
	}
	if c.ReorderQty <= 0 {
		c.ReorderQty = 20
	}
	cfg.Set(c)
	return nil
}

//go:embed migrations/*.sql
var migrations embed.FS

// requests deduplicates the module's effects (inventory.processed_requests,
// created by the module's first migration).
var requests = idem.MustNew("inventory")

// Migrate applies the module's migrations (schema inventory).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return schema.Migrate(ctx, pool, "inventory", migrations)
}

// Line is one SKU and quantity.
type Line struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

// ReserveRequest is the input of Reserve.
type ReserveRequest struct {
	OrderID string `json:"order_id"`
	Lines   []Line `json:"lines"`
}

// ReserveID is the instance id of the reservation of an order: instance ids
// are global across modules, so each module prefixes its own.
func ReserveID(orderID string) string { return "inventory-reserve-" + orderID }

// Level returns the units of sku available.
func Level(ctx context.Context, sku string) (int, error) {
	c, err := cfg.Get()
	if err != nil {
		return 0, err
	}
	var n int
	err = c.DB.QueryRow(ctx, `SELECT available FROM inventory.stock WHERE sku = $1`, sku).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrUnknownSKU
	}
	if err != nil {
		return 0, fmt.Errorf("level of %s: %w", sku, err)
	}
	return n, nil
}

// Adjust changes the stock of sku by delta (creating the SKU if needed) and
// returns the new level. requestID is the caller's idempotency key: the
// same request applies once (a repeat returns the current level), and the
// same id with other parameters fails with ErrRequestConflict. The
// adjustments table, not the engine, remembers request ids: the engine
// forgets them when retention purges the execution. It waits for the
// adjustment, which queues behind a replenishment cycle of the same SKU
// (flows.Lock).
func Adjust(ctx context.Context, e *flows.Engine, requestID, sku string, delta int, reason string) (int, error) {
	a := Adjustment{RequestID: requestID, SKU: sku, Delta: delta, Reason: reason}
	switch applied, err := applied(ctx, a); {
	case err != nil:
		return 0, err
	case applied:
		return Level(ctx, sku)
	}
	n, err := adjust.Run(ctx, e, "inventory-adjust-"+requestID, a)
	if err != nil {
		return 0, err
	}
	// Two different requests racing with one id: Run returned the first
	// one's result to both. Tell the second.
	if _, err := applied(ctx, a); err != nil {
		return 0, err
	}
	// A new SKU gets its replenishment loop (Start is idempotent).
	if err := startReplenish(ctx, e, sku); err != nil {
		return n, err
	}
	return n, nil
}

// applied reports whether a's request id was applied, and fails with
// ErrRequestConflict if it was applied with other parameters.
func applied(ctx context.Context, a Adjustment) (bool, error) {
	c, err := cfg.Get()
	if err != nil {
		return false, err
	}
	var got Adjustment
	err = c.DB.QueryRow(ctx, `SELECT sku, delta, reason FROM inventory.adjustments WHERE request_id = $1`,
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

// StartReplenishment starts the replenishment loop of every stocked SKU
// that is not discontinued and has none running. Run it at startup in a
// process that runs the module: the instance id makes it idempotent across
// processes and restarts.
func StartReplenishment(ctx context.Context, e *flows.Engine) error {
	c, err := cfg.Get()
	if err != nil {
		return err
	}
	rows, err := c.DB.Query(ctx, `SELECT sku FROM inventory.stock WHERE NOT discontinued ORDER BY sku`)
	if err != nil {
		return fmt.Errorf("list skus: %w", err)
	}
	skus, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return fmt.Errorf("list skus: %w", err)
	}
	for _, sku := range skus {
		if err := startReplenish(ctx, e, sku); err != nil {
			return err
		}
	}
	return nil
}

// StopReplenishment discontinues sku: its replenishment loop ends at its
// next cycle and is not started again. The flag is stored in the module's
// table, because the engine's memory of the ended loop (Start finding the
// instance id used) lasts only until retention purges it.
func StopReplenishment(ctx context.Context, e *flows.Engine, sku string) error {
	c, err := cfg.Get()
	if err != nil {
		return err
	}
	tag, err := c.DB.Exec(ctx, `UPDATE inventory.stock SET discontinued = true, updated_at = now() WHERE sku = $1`, sku)
	if err != nil {
		return fmt.Errorf("discontinue %s: %w", sku, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrUnknownSKU
	}
	err = e.Signal(ctx, replenishID(sku), SignalStopReplenish, struct{}{})
	if errors.Is(err, flows.ErrNotActive) || errors.Is(err, flows.ErrNotFound) {
		return nil // no loop running
	}
	return err
}

// SignalStopReplenish is the signal that ends a replenishment loop.
const SignalStopReplenish = "inventory.replenish.stop"

func replenishID(sku string) string { return "inventory-replenish-" + sku }

func startReplenish(ctx context.Context, e *flows.Engine, sku string) error {
	c, err := cfg.Get()
	if err != nil {
		return err
	}
	var discontinued bool
	if err := c.DB.QueryRow(ctx, `SELECT discontinued FROM inventory.stock WHERE sku = $1`, sku).Scan(&discontinued); err != nil {
		return fmt.Errorf("start replenishment of %s: %w", sku, err)
	}
	if discontinued {
		return nil
	}
	_, err = replenish.Start(ctx, e, replenishID(sku), ReplenishState{
		SKU: sku, Every: c.ReplenishEvery, ReorderPoint: c.ReorderPoint, ReorderQty: c.ReorderQty,
	})
	if err != nil {
		return fmt.Errorf("start replenishment of %s: %w", sku, err)
	}
	return nil
}
