// Package shipping is the shipping module: labels and carrier pickups.
//
// It is pure domain: it knows nothing of the workflow engine. The schema
// shipping is private to the module.
//
// # The pickup pivot
//
// A shipment ends one of two ways: the carrier has the parcel (nothing is
// undone after that), or it is canceled (by the customer or a timeout).
// The module decides this once, on the shipment's row, under a row lock:
//
//   - ConfirmPickup (the carrier) moves the row from label_created to
//     pickup_confirmed. If the row is canceled already, it fails with
//     ErrPickupRefused (the carrier must return the parcel).
//   - Decide (whoever drives the shipment) asks for the outcome it wants,
//     label_created to pickup_confirmed or to canceled, and gets the row's
//     state: the first update wins, whoever made it.
//   - MarkPickedUp records the pickup after the decision; CancelLabel never
//     cancels a row that has left label_created.
package shipping

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/schema"
)

// States of a shipment's row.
const (
	StateLabelCreated    = "label_created"
	StatePickupConfirmed = "pickup_confirmed"
	StatePickedUp        = "picked_up"
	StateCanceled        = "canceled"
)

// Business rejections: retrying the same request cannot change the answer
// (IsRejection). Any other error is transient.
var (
	// ErrUnserviceable is the error of a label for an address no carrier
	// serves.
	ErrUnserviceable = errors.New("shipping: address not served")
	// ErrShipmentExists is the error of a second label for an order.
	ErrShipmentExists = errors.New("shipping: order has a shipment already")
	// ErrLabelCanceled is the error of a label whose cancellation ran first
	// (it left a tombstone under the label's key).
	ErrLabelCanceled = errors.New("shipping: label canceled before it was created")
	// ErrPickupRefused is the error of ConfirmPickup for a shipment that was
	// canceled first: the carrier must return the parcel.
	ErrPickupRefused = errors.New("shipping: shipment canceled, return the parcel")
	// ErrNoShipment is the error of ConfirmPickup for an order without a
	// label yet: the carrier retries later.
	ErrNoShipment = errors.New("shipping: no shipment for this order yet")
	// ErrInvalidDecision is the error of Decide asked for an outcome other
	// than StatePickupConfirmed or StateCanceled (a caller's bug).
	ErrInvalidDecision = errors.New("shipping: a decision is pickup_confirmed or canceled")
)

// IsRejection reports whether err is a business rejection of the module.
func IsRejection(err error) bool {
	return errors.Is(err, ErrUnserviceable) || errors.Is(err, ErrShipmentExists) ||
		errors.Is(err, ErrLabelCanceled) || errors.Is(err, ErrPickupRefused) ||
		errors.Is(err, ErrInvalidDecision)
}

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate applies the module's migrations (schema shipping).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return schema.Migrate(ctx, pool, "shipping", migrations)
}

// Decision is the outcome asked of Decide.
type Decision struct {
	ShipmentID string `json:"shipment_id"`
	Want       string `json:"want"`               // StatePickupConfirmed or StateCanceled
	Reason     string `json:"reason,omitempty"`   // why canceled
	Tracking   string `json:"tracking,omitempty"` // of a pickup
}

// Decided is the outcome Decide returns: the shipment's state once decided,
// and the tracking number of a confirmed pickup.
type Decided struct {
	State    string `json:"state"`
	Tracking string `json:"tracking"`
}

// Service is the module's API.
type Service struct {
	db  *pgxpool.Pool
	log *slog.Logger
}

// NewService returns the module's service on db.
func NewService(db *pgxpool.Pool, log *slog.Logger) (*Service, error) {
	if db == nil {
		return nil, errors.New("shipping: NewService: db is required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Service{db: db, log: log}, nil
}

// CreateLabel creates the label of an order's shipment and returns the
// shipment id, once per key: a repeat finds the row it made.
func (s *Service) CreateLabel(ctx context.Context, key, orderID, address string) (string, error) {
	if strings.EqualFold(address, "nowhere") {
		return "", ErrUnserviceable
	}
	var id, state string
	err := s.db.QueryRow(ctx, `
		INSERT INTO shipping.shipments (label_key, order_id, address, state)
		VALUES ($1, $2, $3, 'label_created')
		ON CONFLICT (label_key) DO UPDATE SET label_key = EXCLUDED.label_key -- no-op, returns the row
		RETURNING id, state`, key, orderID, address).Scan(&id, &state)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // another label of this order (shipments_order)
		return "", fmt.Errorf("%w: %s", ErrShipmentExists, orderID)
	}
	if err != nil {
		return "", fmt.Errorf("create label for %s: %w", orderID, err)
	}
	if state == StateCanceled {
		return "", ErrLabelCanceled
	}
	s.log.LogAttrs(ctx, slog.LevelInfo, "shipping: label created",
		slog.String("order_id", orderID), slog.String("shipment_id", id))
	return id, nil
}

// CancelLabel cancels the label created under key, by key (a tombstone if
// the label never landed), so it also settles a CreateLabel whose outcome
// the caller does not know. Only a label still label_created is canceled:
// a confirmed pickup is never undone.
func (s *Service) CancelLabel(ctx context.Context, key string) error {
	if _, err := s.db.Exec(ctx, `
		INSERT INTO shipping.shipments (label_key, state) VALUES ($1, 'canceled')
		ON CONFLICT (label_key) DO UPDATE SET state = 'canceled', updated_at = now()
		WHERE shipments.state = 'label_created'`, key); err != nil {
		return fmt.Errorf("cancel label %s: %w", key, err)
	}
	s.log.LogAttrs(ctx, slog.LevelInfo, "shipping: label canceled", slog.String("label_key", key))
	return nil
}

// ConfirmPickup records the carrier's pickup of an order's shipment:
// label_created to pickup_confirmed, unless the shipment was decided first.
// nil means the pickup is recorded for good (a repeat is nil too).
// ErrPickupRefused: the shipment was canceled first. ErrNoShipment: no
// label yet.
func (s *Service) ConfirmPickup(ctx context.Context, orderID, tracking string) error {
	// Lock the row, then decide on its latest state. (A single UPDATE with a
	// fallback SELECT would not do: under READ COMMITTED the SELECT reads
	// the statement's snapshot, older than the row the UPDATE waited for.)
	var state string
	err := pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state FROM shipping.shipments WHERE order_id = $1 AND order_id <> '' FOR UPDATE`,
			orderID).Scan(&state); err != nil {
			return err
		}
		if state != StateLabelCreated {
			return nil // decided already
		}
		state = StatePickupConfirmed
		_, err := tx.Exec(ctx, `
			UPDATE shipping.shipments SET state = 'pickup_confirmed', tracking = $2, updated_at = now()
			WHERE order_id = $1`, orderID, tracking)
		return err
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("%w: %s", ErrNoShipment, orderID)
	case err != nil:
		return fmt.Errorf("confirm pickup of %s: %w", orderID, err)
	case state == StateCanceled:
		return fmt.Errorf("%w: %s", ErrPickupRefused, orderID)
	}
	s.log.LogAttrs(ctx, slog.LevelInfo, "shipping: pickup confirmed",
		slog.String("order_id", orderID), slog.String("tracking", tracking))
	return nil // pickup_confirmed (now or before) or picked_up
}

// Decide moves the shipment out of label_created to what d wants, unless
// it has left it already (the carrier confirmed first), and returns the
// state. Idempotent: a repeat returns the decided state.
func (s *Service) Decide(ctx context.Context, d Decision) (Decided, error) {
	if d.Want != StatePickupConfirmed && d.Want != StateCanceled {
		return Decided{}, fmt.Errorf("%w: %q", ErrInvalidDecision, d.Want)
	}
	var out Decided
	var tracking *string
	// Lock the row, then decide on its latest state (see ConfirmPickup).
	err := pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state, tracking FROM shipping.shipments WHERE id = $1 FOR UPDATE`,
			d.ShipmentID).Scan(&out.State, &tracking); err != nil {
			return err
		}
		if out.State != StateLabelCreated {
			return nil // decided already: by the carrier, or by an earlier attempt
		}
		return tx.QueryRow(ctx, `
			UPDATE shipping.shipments
			SET state = $2, reason = $3, tracking = coalesce(tracking, nullif($4, '')), updated_at = now()
			WHERE id = $1 RETURNING state, tracking`,
			d.ShipmentID, d.Want, d.Reason, d.Tracking).Scan(&out.State, &tracking)
	})
	if err != nil {
		return Decided{}, fmt.Errorf("decide %s: %w", d.ShipmentID, err)
	}
	if tracking != nil {
		out.Tracking = *tracking
	}
	s.log.LogAttrs(ctx, slog.LevelInfo, "shipping: decided",
		slog.String("shipment_id", d.ShipmentID), slog.String("wanted", d.Want), slog.String("state", out.State))
	return out, nil
}

// MarkPickedUp records that the carrier has the parcel. Idempotent. The
// row cannot be anything but pickup_confirmed or picked_up here (the
// decision is final); any other state is a bug, reported as an error.
func (s *Service) MarkPickedUp(ctx context.Context, shipmentID string) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE shipping.shipments SET state = 'picked_up', updated_at = now()
		WHERE id = $1 AND state IN ('pickup_confirmed', 'picked_up')`, shipmentID)
	if err != nil {
		return fmt.Errorf("mark %s picked up: %w", shipmentID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("shipping: %s is not pickup_confirmed", shipmentID)
	}
	s.log.LogAttrs(ctx, slog.LevelInfo, "shipping: picked up", slog.String("shipment_id", shipmentID))
	return nil
}

// Find returns the state of an order's shipment: label_created,
// pickup_confirmed, picked_up or canceled; pgx.ErrNoRows if none.
func (s *Service) Find(ctx context.Context, orderID string) (string, error) {
	var state string
	err := s.db.QueryRow(ctx, `SELECT state FROM shipping.shipments WHERE order_id = $1`, orderID).Scan(&state)
	if err != nil {
		return "", fmt.Errorf("find shipment of %s: %w", orderID, err)
	}
	return state, nil
}
