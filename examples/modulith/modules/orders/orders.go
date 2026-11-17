// Package orders is the orders module: it takes orders and runs their
// fulfilment, a saga across the payments, inventory and shipping modules.
//
// Public API: Place, Cancel and Get (Go functions for the HTTP API), the
// events it publishes (TopicCompleted, TopicCanceled, TopicFailed,
// Event), Hook, Queue, Configure, Migrate and ErrCanceledByCustomer. The
// schema orders is private to the module.
//
// It uses the other modules only through their public API: the activities
// of payments, and the workflows of inventory and shipping started as
// children. When the fulfilment fails or the customer cancels, the engine
// undoes the completed steps of every module, latest first, by activity
// name (refund, release, void).
package orders

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
)

// Queue is the queue of the module's workflows and activities.
const Queue = "orders"

// SignalCancel is the signal of a customer's cancellation (Cancel).
const SignalCancel = "orders.cancel"

// ErrCanceledByCustomer is the error of a fulfilment the customer canceled.
var ErrCanceledByCustomer = errors.New("orders: canceled by the customer")

// ErrInvalidOrder is the error of Place for an order it refuses.
var ErrInvalidOrder = errors.New("orders: invalid order")

// kindCanceledByCustomer is the stored kind of ErrCanceledByCustomer; the
// hook reads it from RunInfo.ErrorKinds.
const kindCanceledByCustomer = "orders_canceled_by_customer"

func init() {
	flows.RegisterErrorKind(kindCanceledByCustomer, ErrCanceledByCustomer)
}

// Config holds the module's dependencies and settings.
type Config struct {
	DB     *pgxpool.Pool
	Logger *slog.Logger
	// PickupWithin is how long an order waits for the carrier (default
	// 48h). Place copies it into the workflow's input.
	PickupWithin time.Duration
}

var cfg = deps.New[Config]("orders")

// Configure sets the module's dependencies. Call it before the engine
// starts, in every process that runs the module or calls its functions.
func Configure(c Config) {
	if c.PickupWithin <= 0 {
		c.PickupWithin = 48 * time.Hour
	}
	cfg.Set(c)
}

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate applies the module's migrations (schema orders).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return schema.Migrate(ctx, pool, "orders", migrations)
}

// Line is one SKU and quantity of an order.
type Line struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

// PlaceRequest is an order as the API takes it.
type PlaceRequest struct {
	ID      string `json:"id"` // the client's order id: placing it again is a no-op
	Email   string `json:"email"`
	Address string `json:"address"`
	Amount  int64  `json:"amount"` // cents
	Lines   []Line `json:"lines"`
}

// CancelRequest is the payload of SignalCancel.
type CancelRequest struct {
	Reason string `json:"reason"`
}

// View is an order as Get returns it.
type View struct {
	ID string `json:"id"`
	// Status is the fulfilment workflow's status (running, completed,
	// compensating, compensated, needs_attention...).
	Status flows.Status `json:"status"`
	// State is the order's business state: processing, completed,
	// canceled or failed ("" before the workflow recorded it).
	State  string `json:"state"`
	Reason string `json:"reason,omitempty"`
}

// InstanceID is the instance id of an order's fulfilment. Instance ids are
// global across modules: each module prefixes its own.
func InstanceID(orderID string) string { return "orders-" + orderID }

// Place starts the fulfilment of an order. The order id is the idempotency
// key: placing the same id again starts nothing.
func Place(ctx context.Context, e *flows.Engine, r PlaceRequest) error {
	c, err := cfg.Get()
	if err != nil {
		return err
	}
	if err := validate(r); err != nil {
		return err
	}
	_, err = fulfil.Start(ctx, e, InstanceID(r.ID), Order{
		ID: r.ID, Email: r.Email, Address: r.Address, Amount: r.Amount, Lines: r.Lines,
		PickupWithin: c.PickupWithin,
	})
	if err != nil {
		return fmt.Errorf("place %s: %w", r.ID, err)
	}
	return nil
}

func validate(r PlaceRequest) error {
	switch {
	case r.ID == "" || len(r.ID) > 100:
		return fmt.Errorf("%w: id must have 1 to 100 bytes", ErrInvalidOrder)
	case r.Email == "" || r.Address == "":
		return fmt.Errorf("%w: email and address are required", ErrInvalidOrder)
	case r.Amount <= 0:
		return fmt.Errorf("%w: amount must be positive", ErrInvalidOrder)
	case len(r.Lines) == 0 || len(r.Lines) > 50:
		return fmt.Errorf("%w: 1 to 50 lines", ErrInvalidOrder)
	}
	for _, l := range r.Lines {
		if l.SKU == "" || l.Qty <= 0 {
			return fmt.Errorf("%w: line %q x %d", ErrInvalidOrder, l.SKU, l.Qty)
		}
	}
	return nil
}

// Cancel asks for the cancellation of an order. It is honored until the
// carrier picks the parcel up; after that the order completes anyway. It
// fails with flows.ErrNotActive once the order has ended.
func Cancel(ctx context.Context, e *flows.Engine, orderID, reason string) error {
	return e.Signal(ctx, InstanceID(orderID), SignalCancel, CancelRequest{Reason: reason})
}

// Get returns an order: its workflow's status and its business state.
// flows.ErrNotFound for an unknown order.
func Get(ctx context.Context, e *flows.Engine, orderID string) (View, error) {
	c, err := cfg.Get()
	if err != nil {
		return View{}, err
	}
	st, err := fulfil.Handle(e, InstanceID(orderID)).Status(ctx)
	if err != nil {
		return View{}, fmt.Errorf("order %s: %w", orderID, err)
	}
	v := View{ID: orderID, Status: st}
	err = c.DB.QueryRow(ctx, `SELECT state, reason FROM orders.orders WHERE id = $1`, orderID).Scan(&v.State, &v.Reason)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return View{}, fmt.Errorf("order %s: %w", orderID, err)
	}
	return v, nil
}
