// Package orders is the orders module: orders, their business state, and
// the events published when an order ends.
//
// It is pure domain: it knows nothing of the workflow engine nor of the
// other modules. Whatever runs the fulfilment (the process layer) records
// an order with Record and ends it with End; End stages the order's event
// in the caller's transaction (transactional outbox). The schema orders is
// private to the module; the events (Event, the Topic* constants) are its
// public contract.
package orders

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/outbox"
	"github.com/imdat99/go-flows/examples/modulith/internal/platform/schema"
)

// Business rejections: retrying the same request cannot change the answer
// (IsRejection). Any other error is transient.
var (
	// ErrInvalidOrder is the error of an order Validate refuses.
	ErrInvalidOrder = errors.New("orders: invalid order")
	// ErrOrderConflict is the error of an order id already used for an
	// order with other contents, or for an order that has ended.
	ErrOrderConflict = errors.New("orders: order id already used")
)

// IsRejection reports whether err is a business rejection of the module.
func IsRejection(err error) bool {
	return errors.Is(err, ErrInvalidOrder) || errors.Is(err, ErrOrderConflict)
}

// Business states of an order.
const (
	StateProcessing = "processing"
	StateCompleted  = "completed"
	StateCanceled   = "canceled"
	StateFailed     = "failed"
)

// Topics of the events the module publishes when an order ends.
const (
	TopicCompleted = "orders.order_completed"
	TopicCanceled  = "orders.order_canceled"
	TopicFailed    = "orders.order_failed"
)

// Event is the payload of the module's events: the contract its consumers
// depend on. Add fields, never change or remove one.
type Event struct {
	OrderID string `json:"order_id"`
	Email   string `json:"email"`
	State   string `json:"state"` // completed, canceled, failed
	Reason  string `json:"reason,omitempty"`
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

// Order is an order as the customer placed it. ID is the client's order
// id: the idempotency key.
type Order struct {
	ID      string `json:"id"`
	Email   string `json:"email"`
	Address string `json:"address"`
	Amount  int64  `json:"amount"` // cents
	Lines   []Line `json:"lines"`
}

// View is an order's business state.
type View struct {
	ID     string `json:"id"`
	State  string `json:"state"` // processing, completed, canceled, failed
	Reason string `json:"reason,omitempty"`
}

// Service is the module's API.
type Service struct {
	db  *pgxpool.Pool
	log *slog.Logger
}

// NewService returns the module's service on db.
func NewService(db *pgxpool.Pool, log *slog.Logger) (*Service, error) {
	if db == nil {
		return nil, errors.New("orders: NewService: db is required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Service{db: db, log: log}, nil
}

// Validate checks an order before anything is started for it.
func (*Service) Validate(o Order) error {
	switch {
	case o.ID == "" || len(o.ID) > 100:
		return fmt.Errorf("%w: id must have 1 to 100 bytes", ErrInvalidOrder)
	case o.Email == "" || o.Address == "":
		return fmt.Errorf("%w: email and address are required", ErrInvalidOrder)
	case o.Amount <= 0:
		return fmt.Errorf("%w: amount must be positive", ErrInvalidOrder)
	case len(o.Lines) == 0 || len(o.Lines) > 50:
		return fmt.Errorf("%w: 1 to 50 lines", ErrInvalidOrder)
	}
	for _, l := range o.Lines {
		if l.SKU == "" || l.Qty <= 0 {
			return fmt.Errorf("%w: line %q x %d", ErrInvalidOrder, l.SKU, l.Qty)
		}
	}
	return nil
}

// Placed reports whether o was recorded already, and fails with
// ErrOrderConflict if its id was recorded with other contents. The orders
// table keeps orders for good: it is the lasting idempotency of an order
// id.
func (s *Service) Placed(ctx context.Context, o Order) (bool, error) {
	lines, err := json.Marshal(o.Lines)
	if err != nil {
		return false, fmt.Errorf("placed %s: %w", o.ID, err)
	}
	var same bool
	err = s.db.QueryRow(ctx, `
		SELECT email = $2 AND address = $3 AND amount = $4 AND lines = $5::jsonb
		FROM orders.orders WHERE id = $1`, o.ID, o.Email, o.Address, o.Amount, lines).Scan(&same)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("placed %s: %w", o.ID, err)
	case !same:
		return true, fmt.Errorf("%w: %s", ErrOrderConflict, o.ID)
	}
	return true, nil
}

// Record inserts the order's row, processing. A repeat finds its own row.
// A row with other contents, or one that has ended already (the id of an
// earlier order, reused), fails with ErrOrderConflict.
func (s *Service) Record(ctx context.Context, o Order) error {
	lines, err := json.Marshal(o.Lines)
	if err != nil {
		return fmt.Errorf("record order %s: %w", o.ID, err)
	}
	var same bool
	var state string
	err = s.db.QueryRow(ctx, `
		WITH ins AS (
			INSERT INTO orders.orders (id, email, address, amount, lines, state)
			VALUES ($1, $2, $3, $4, $5, 'processing')
			ON CONFLICT (id) DO NOTHING
			RETURNING state)
		SELECT true, state FROM ins
		UNION ALL
		SELECT email = $2 AND address = $3 AND amount = $4 AND lines = $5::jsonb, state
		FROM orders.orders WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM ins)`,
		o.ID, o.Email, o.Address, o.Amount, lines).Scan(&same, &state)
	if err != nil {
		return fmt.Errorf("record order %s: %w", o.ID, err)
	}
	if !same || state != StateProcessing {
		return fmt.Errorf("%w: %s (%s)", ErrOrderConflict, o.ID, state)
	}
	return nil
}

// End sets the final state of a processing order (StateCompleted,
// StateCanceled or StateFailed) and stages its event, in tx: the state and
// the event commit with the caller's change or not at all. It reports
// whether it ended the order: no row means it was never recorded (nothing
// to tell the customer); an ended row belongs to an earlier order with
// this id (Record refused this one).
func (s *Service) End(ctx context.Context, tx pgx.Tx, orderID, state, reason string) (bool, error) {
	topic := map[string]string{StateCompleted: TopicCompleted, StateCanceled: TopicCanceled, StateFailed: TopicFailed}[state]
	if topic == "" {
		return false, fmt.Errorf("orders: end of %s: invalid state %q", orderID, state)
	}
	ev := Event{OrderID: orderID, State: state, Reason: reason}
	err := tx.QueryRow(ctx, `
		UPDATE orders.orders SET state = $2, reason = $3, updated_at = now()
		WHERE id = $1 AND state = 'processing' RETURNING email`, orderID, state, reason).Scan(&ev.Email)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("orders: end of %s: %w", orderID, err)
	}
	if err := outbox.Stage(ctx, tx, topic, orderID, ev); err != nil {
		return false, err
	}
	s.log.LogAttrs(ctx, slog.LevelInfo, "orders: ended", slog.String("order_id", orderID), slog.String("state", state))
	return true, nil
}

// Get returns an order's business state, and false if it was never
// recorded.
func (s *Service) Get(ctx context.Context, orderID string) (View, bool, error) {
	v := View{ID: orderID}
	err := s.db.QueryRow(ctx, `SELECT state, reason FROM orders.orders WHERE id = $1`, orderID).Scan(&v.State, &v.Reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, false, nil
	}
	if err != nil {
		return View{}, false, fmt.Errorf("order %s: %w", orderID, err)
	}
	return v, true, nil
}
