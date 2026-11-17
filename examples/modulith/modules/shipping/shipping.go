// Package shipping is the shipping module: labels and carrier pickups.
//
// Public API: the workflow Ship, which other modules start as a child
// (InstanceID), ConfirmPickup for the carrier's webhook (a signal to that
// workflow), Find, Queue, Configure, Migrate, ErrUnserviceable and
// ErrPickupTimeout. The schema shipping is private to the module.
package shipping

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/deps"
	"github.com/imdat99/go-flows/examples/modulith/internal/platform/schema"
	"github.com/imdat99/go-flows/flows"
)

// Queue is the queue of the module's workflows and activities.
const Queue = "shipping"

// SignalPickedUp is the signal the carrier's pickup confirmation sends to
// the Ship workflow (ConfirmPickup).
const SignalPickedUp = "shipping.picked-up"

var (
	// ErrUnserviceable is the error of a shipment to an address no carrier
	// serves.
	ErrUnserviceable = errors.New("shipping: address not served")
	// ErrPickupTimeout is the error of a shipment the carrier did not pick
	// up in time.
	ErrPickupTimeout = errors.New("shipping: carrier did not pick up in time")
)

func init() {
	flows.RegisterErrorKind("shipping_unserviceable", ErrUnserviceable)
	flows.RegisterErrorKind("shipping_pickup_timeout", ErrPickupTimeout)
}

// Config holds the module's dependencies.
type Config struct {
	DB     *pgxpool.Pool
	Logger *slog.Logger
}

var cfg = deps.New[Config]("shipping")

// Configure sets the module's dependencies. Call it before the engine
// starts, in every process that runs the module or calls its functions.
func Configure(c Config) { cfg.Set(c) }

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate applies the module's migrations (schema shipping).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return schema.Migrate(ctx, pool, "shipping", migrations)
}

// Request is the input of Ship.
type Request struct {
	OrderID string `json:"order_id"`
	Address string `json:"address"`
	// PickupWithin bounds the wait for the carrier. It is part of the
	// input, not read from configuration, so that replays see the value the
	// execution started with.
	PickupWithin time.Duration `json:"pickup_within"`
}

// Shipment is the output of Ship.
type Shipment struct {
	ID       string `json:"id"`
	Tracking string `json:"tracking"`
}

// Pickup is the payload of SignalPickedUp.
type Pickup struct {
	Tracking string `json:"tracking"`
}

// InstanceID is the instance id of the shipment of an order.
func InstanceID(orderID string) string { return "shipping-" + orderID }

// ConfirmPickup records the carrier's pickup of an order's shipment. It
// fails with flows.ErrNotActive once the shipment has ended (picked up
// already, timed out, canceled).
func ConfirmPickup(ctx context.Context, e *flows.Engine, orderID, tracking string) error {
	return e.Signal(ctx, InstanceID(orderID), SignalPickedUp, Pickup{Tracking: tracking})
}

// Find returns the state of the latest shipment of an order, pgx.ErrNoRows
// if none.
func Find(ctx context.Context, orderID string) (string, error) {
	c, err := cfg.Get()
	if err != nil {
		return "", err
	}
	var state string
	err = c.DB.QueryRow(ctx, `
		SELECT state FROM shipping.shipments WHERE order_id = $1
		ORDER BY updated_at DESC LIMIT 1`, orderID).Scan(&state)
	if err != nil {
		return "", fmt.Errorf("find shipment of %s: %w", orderID, err)
	}
	return state, nil
}

var activityOpts = []flows.ActivityOption{
	flows.WithQueue(Queue),
	flows.WithRetry(flows.Exponential(5, 200*time.Millisecond, 5*time.Second)),
	flows.WithCompensateRetry(flows.Exponential(20, time.Second, time.Minute)),
	flows.WithTimeout(10 * time.Second),
}

var (
	// CreateLabel creates the label; its compensator cancels it. Other
	// modules start Ship rather than call it: it is exported so that their
	// tests can mock it (flowtest.OnActivity) when Ship runs as their child.
	CreateLabel = flows.DefineActivity("shipping.create-label", createLabel, cancelLabel, activityOpts...)
	// MarkPickedUp records the pickup; exported for tests, as CreateLabel.
	MarkPickedUp = flows.DefineActivity("shipping.mark-picked-up", markPickedUp, nil, activityOpts...)

	// Ship creates a label, then waits for the carrier's pickup (a signal)
	// at most PickupWithin. Without a pickup it fails with ErrPickupTimeout
	// and cancels the label; a canceled parent cancels it the same way.
	Ship = flows.DefineWorkflow("shipping.ship", 1, ship, flows.WithWorkflowQueue(Queue))
)

func ship(c flows.Ctx, r Request) (Shipment, error) {
	id, err := CreateLabel.Run(c, r)
	if err != nil {
		return Shipment{}, err
	}
	p, err := flows.Await[Pickup](c, SignalPickedUp, r.PickupWithin)
	if errors.Is(err, flows.ErrAwaitTimeout) {
		return Shipment{}, ErrPickupTimeout // cancels the label
	}
	if err != nil {
		return Shipment{}, err // canceled by the parent: cancels the label
	}
	// The pivot: once the carrier has the parcel nothing is undone, so
	// MarkPickedUp has no compensator.
	if _, err := MarkPickedUp.Run(c, PickupUpdate{ShipmentID: id, Tracking: p.Tracking}); err != nil {
		return Shipment{}, err
	}
	return Shipment{ID: id, Tracking: p.Tracking}, nil
}

// PickupUpdate is the input of MarkPickedUp.
type PickupUpdate struct {
	ShipmentID string `json:"shipment_id"`
	Tracking   string `json:"tracking"`
}

func createLabel(ctx context.Context, k flows.Key, r Request) (string, string, error) {
	c, err := cfg.Get()
	if err != nil {
		return "", "", err
	}
	if strings.EqualFold(r.Address, "nowhere") {
		return "", "", flows.Permanent(ErrUnserviceable)
	}
	var id, state string
	err = c.DB.QueryRow(ctx, `
		INSERT INTO shipping.shipments (label_key, order_id, address, state)
		VALUES ($1, $2, $3, 'label_created')
		ON CONFLICT (label_key) DO UPDATE SET label_key = EXCLUDED.label_key -- no-op, returns the row
		RETURNING id, state`, string(k), r.OrderID, r.Address).Scan(&id, &state)
	if err != nil {
		return "", "", fmt.Errorf("create label for %s: %w", r.OrderID, err)
	}
	if state == "canceled" {
		return "", "", flows.Permanent(errors.New("shipping: label canceled before it was created"))
	}
	c.Logger.LogAttrs(ctx, slog.LevelInfo, "shipping: label created",
		slog.String("order_id", r.OrderID), slog.String("shipment_id", id))
	return id, id, nil
}

// cancelLabel undoes createLabel by key (a tombstone if the label never
// landed), so it also handles an uncertain attempt.
func cancelLabel(ctx context.Context, k flows.Key, shipmentID string) error {
	c, err := cfg.Get()
	if err != nil {
		return err
	}
	if _, err := c.DB.Exec(ctx, `
		INSERT INTO shipping.shipments (label_key, state) VALUES ($1, 'canceled')
		ON CONFLICT (label_key) DO UPDATE SET state = 'canceled', updated_at = now()
		WHERE shipments.state = 'label_created'`, string(k)); err != nil {
		return fmt.Errorf("cancel label %s: %w", shipmentID, err)
	}
	c.Logger.LogAttrs(ctx, slog.LevelInfo, "shipping: label canceled", slog.String("shipment_id", shipmentID))
	return nil
}

func markPickedUp(ctx context.Context, _ flows.Key, u PickupUpdate) (struct{}, struct{}, error) {
	c, err := cfg.Get()
	if err != nil {
		return struct{}{}, struct{}{}, err
	}
	// Idempotent: setting the same state twice changes nothing.
	if _, err := c.DB.Exec(ctx, `
		UPDATE shipping.shipments SET state = 'picked_up', tracking = $2, updated_at = now()
		WHERE id = $1 AND state IN ('label_created', 'picked_up')`, u.ShipmentID, u.Tracking); err != nil {
		return struct{}{}, struct{}{}, fmt.Errorf("mark %s picked up: %w", u.ShipmentID, err)
	}
	return struct{}{}, struct{}{}, nil
}
