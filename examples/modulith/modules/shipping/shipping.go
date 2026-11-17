// Package shipping is the shipping module: labels and carrier pickups.
//
// Public API: the workflow Ship, which other modules start as a child
// (InstanceID); ConfirmPickup for the carrier's webhook and Cancel for the
// caller that wants the shipment stopped; Find, Queue, Configure, Migrate,
// ErrUnserviceable, ErrPickupTimeout, ErrCanceled, ErrPickupRefused and
// ErrNoShipment. The schema shipping is private to the module.
//
// # The pickup pivot
//
// A shipment ends one of two ways: the carrier has the parcel (nothing is
// undone after that), or it is canceled (by the customer or the timeout)
// and the order is undone. Signals cannot decide this alone: a pickup
// webhook racing a cancel would be accepted (202) and then never read. So
// the decision is made once on the shipment's row, under a row lock:
//
//   - ConfirmPickup moves the row from label_created to pickup_confirmed
//     before it signals the workflow. If the row is canceled already, it
//     fails with ErrPickupRefused (409: the carrier must return the parcel).
//   - The workflow, woken by the first of pickup, cancel or timeout, asks
//     for the outcome it wants with the Decide activity (label_created to
//     pickup_confirmed, or to canceled) and gets the row's state: the first
//     update wins, whoever made it. Signals only wake the workflow.
//   - From the decision on, the workflow cannot fail into the caller's saga
//     by accident: Decide and MarkPickedUp run on a disconnected context and
//     are retried until they succeed (untilDone).
package shipping

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/deps"
	"github.com/imdat99/go-flows/examples/modulith/internal/platform/schema"
	"github.com/imdat99/go-flows/flows"
)

// Queue is the queue of the module's workflows and activities.
const Queue = "shipping"

// Signals of the Ship workflow. They wake it; the shipment's row decides.
const (
	// SignalPickedUp follows a confirmed pickup (ConfirmPickup).
	SignalPickedUp = "shipping.picked-up"
	// SignalCancel asks for the shipment to be stopped (Cancel).
	SignalCancel = "shipping.cancel"
)

// States of a shipment's row.
const (
	stateLabelCreated    = "label_created"
	statePickupConfirmed = "pickup_confirmed"
	statePickedUp        = "picked_up"
	stateCanceled        = "canceled"
)

var (
	// ErrUnserviceable is the error of a shipment to an address no carrier
	// serves.
	ErrUnserviceable = errors.New("shipping: address not served")
	// ErrPickupTimeout is the error of a shipment the carrier did not pick
	// up in time.
	ErrPickupTimeout = errors.New("shipping: carrier did not pick up in time")
	// ErrCanceled is the error of a shipment stopped by Cancel before the
	// carrier picked it up.
	ErrCanceled = errors.New("shipping: canceled before pickup")
	// ErrPickupRefused is the error of ConfirmPickup for a shipment that was
	// canceled (or timed out) first: the carrier must return the parcel.
	ErrPickupRefused = errors.New("shipping: shipment canceled, return the parcel")
	// ErrNoShipment is the error of ConfirmPickup for an order without a
	// label yet: the carrier retries later.
	ErrNoShipment = errors.New("shipping: no shipment for this order yet")
)

func init() {
	flows.RegisterErrorKind("shipping_unserviceable", ErrUnserviceable)
	flows.RegisterErrorKind("shipping_pickup_timeout", ErrPickupTimeout)
	flows.RegisterErrorKind("shipping_canceled", ErrCanceled)
}

// Config holds the module's dependencies.
type Config struct {
	DB     *pgxpool.Pool
	Logger *slog.Logger
}

var cfg = deps.New[Config]("shipping")

// Configure sets the module's dependencies. Call it before the engine
// starts, in every process that runs the module or calls its functions.
func Configure(c Config) error {
	if c.DB == nil {
		return errors.New("shipping: Configure: DB is required")
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	cfg.Set(c)
	return nil
}

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

// CancelRequest is the payload of SignalCancel.
type CancelRequest struct {
	Reason string `json:"reason"`
}

// InstanceID is the instance id of the shipment of an order.
func InstanceID(orderID string) string { return "shipping-" + orderID }

// ConfirmPickup records the carrier's pickup of an order's shipment. nil
// means the pickup is recorded for good: the shipment will complete, and
// the order will not be undone. A repeated confirmation is nil too.
//
// ErrPickupRefused: the shipment was canceled first (409, return the
// parcel). ErrNoShipment: no label yet (retry later).
func ConfirmPickup(ctx context.Context, e *flows.Engine, orderID, tracking string) error {
	if err := confirmPickup(ctx, orderID, tracking); err != nil {
		return err
	}
	// Wake the workflow. If this fails (a crash here), the workflow still
	// finds the confirmed row when its timeout fires.
	err := e.Signal(ctx, InstanceID(orderID), SignalPickedUp, Pickup{Tracking: tracking})
	if errors.Is(err, flows.ErrNotActive) {
		return nil // a repeated confirmation of a shipment that has completed
	}
	return err
}

// confirmPickup makes the decision for the carrier: label_created to
// pickup_confirmed, unless the workflow decided first.
func confirmPickup(ctx context.Context, orderID, tracking string) error {
	c, err := cfg.Get()
	if err != nil {
		return err
	}
	// Lock the row, then decide on its latest state. (A single UPDATE with a
	// fallback SELECT would not do: under READ COMMITTED the SELECT reads
	// the statement's snapshot, older than the row the UPDATE waited for.)
	var state string
	err = pgx.BeginFunc(ctx, c.DB, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state FROM shipping.shipments WHERE order_id = $1 FOR UPDATE`,
			orderID).Scan(&state); err != nil {
			return err
		}
		if state != stateLabelCreated {
			return nil // decided already
		}
		state = statePickupConfirmed
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
	case state == stateCanceled:
		return fmt.Errorf("%w: %s", ErrPickupRefused, orderID)
	}
	return nil // pickup_confirmed (now or before) or picked_up
}

// Cancel asks the shipment of an order to stop. The shipment decides (see
// the package documentation): if the carrier's pickup was confirmed first,
// it completes anyway (the caller sees that in its outcome); otherwise it
// cancels its label and fails with ErrCanceled. flows.ErrNotActive once the
// shipment has ended.
func Cancel(ctx context.Context, e *flows.Engine, orderID, reason string) error {
	return e.Signal(ctx, InstanceID(orderID), SignalCancel, CancelRequest{Reason: reason})
}

// Find returns the state of an order's shipment: label_created,
// pickup_confirmed, picked_up or canceled; pgx.ErrNoRows if none.
func Find(ctx context.Context, orderID string) (string, error) {
	c, err := cfg.Get()
	if err != nil {
		return "", err
	}
	var state string
	err = c.DB.QueryRow(ctx, `SELECT state FROM shipping.shipments WHERE order_id = $1`, orderID).Scan(&state)
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

// pivotRetry is one round of attempts of the steps that must succeed
// (about 4 minutes); untilDone sleeps pivotPause and runs another round,
// forever.
var pivotRetry = flows.WithRetry(flows.Exponential(10, time.Second, 30*time.Second))

const pivotPause = 5 * time.Minute

var (
	// CreateLabel creates the label; its compensator cancels it. Other
	// modules start Ship rather than call it: it is exported so that their
	// tests can mock it (flowtest.OnActivity) when Ship runs as their child.
	CreateLabel = flows.DefineActivity("shipping.create-label", createLabel, cancelLabel, activityOpts...)
	// Decide makes the shipment's decision (see the package documentation);
	// exported for tests, as CreateLabel.
	Decide = flows.DefineActivity("shipping.decide", decide, nil,
		flows.WithQueue(Queue), flows.WithTimeout(10*time.Second), pivotRetry)
	// MarkPickedUp records the pickup; exported for tests, as CreateLabel.
	MarkPickedUp = flows.DefineActivity("shipping.mark-picked-up", markPickedUp, nil,
		flows.WithQueue(Queue), flows.WithTimeout(10*time.Second), pivotRetry)

	// Ship creates a label, then waits for the first of the carrier's
	// pickup, a cancel, or PickupWithin, and decides (see the package
	// documentation). Pickup: it completes. Cancel: it cancels the label and
	// fails with ErrCanceled. Timeout: the same with ErrPickupTimeout.
	Ship = flows.DefineWorkflow("shipping.ship", 1, ship, flows.WithWorkflowQueue(Queue))
)

func ship(c flows.Ctx, r Request) (Shipment, error) {
	// Listen first: a signal sent before the workflow listens is kept.
	pickups := flows.Signals[Pickup](c, SignalPickedUp)
	cancels := flows.Signals[CancelRequest](c, SignalCancel)

	id, err := CreateLabel.Run(c, r)
	if err != nil {
		return Shipment{}, err
	}

	tc, stopTimer := flows.WithCancel(c)
	timeout := flows.NewTimer(tc, r.PickupWithin)
	// What the workflow asks for, and the error it ends with if canceled.
	want := Decision{ShipmentID: id, Want: stateCanceled}
	var why error // the error the workflow ends with if canceled
	// An error from a case is flows.ErrCanceled: the workflow itself was
	// canceled (Engine.Cancel of the order). It ends canceled, unless the
	// carrier confirmed the pickup first.
	canceledBy := func(reason string, err, def error) {
		want.Reason, why = reason, def
		if err != nil {
			want.Reason, why = "canceled", err
		}
	}
	flows.Select(c,
		flows.OnSignal(pickups, func(_ flows.Ctx, p Pickup, err error) {
			if err != nil {
				canceledBy("canceled", err, err)
				return
			}
			want.Want, want.Tracking = statePickupConfirmed, p.Tracking
		}),
		flows.OnSignal(cancels, func(_ flows.Ctx, _ CancelRequest, err error) { canceledBy("customer", err, ErrCanceled) }),
		flows.OnFuture(timeout, func(_ flows.Ctx, _ struct{}, err error) { canceledBy("timeout", err, ErrPickupTimeout) }),
	)
	stopTimer()

	// The decision, and after a pickup the record of it, must not fail into
	// the caller's saga: they run disconnected from cancellation, and are
	// retried until they succeed.
	dc, done := flows.Disconnected(c)
	defer done()
	d := untilDone(dc, Decide, want)
	if d.State == stateCanceled {
		return Shipment{}, orElse(why, ErrCanceled) // the label is canceled: nothing for the carrier
	}
	// The pivot: the carrier has the parcel. Nothing is undone from here.
	untilDone(dc, MarkPickedUp, PickupUpdate{ShipmentID: id, Tracking: d.Tracking})
	return Shipment{ID: id, Tracking: d.Tracking}, nil
}

// untilDone runs a until it succeeds, for the steps that have no way back.
// Each round is the activity's own retries (pivotRetry); between rounds the
// workflow sleeps (a durable timer: no process is held). Failed attempts
// reach the engine's Observer, which is where an operator is alerted (the
// composition root logs them; alert on a shipping.decide or
// shipping.mark-picked-up attempt failing repeatedly).
func untilDone[I, O, C any](c flows.Ctx, a *flows.Activity[I, O, C], in I) O {
	for {
		out, err := a.Run(c, in)
		if err == nil {
			return out
		}
		_ = flows.Sleep(c, pivotPause) // c is disconnected: never canceled
	}
}

// orElse returns err, or def when err is nil.
func orElse(err, def error) error {
	if err != nil {
		return err
	}
	return def
}

// Decision is the input of Decide: the outcome the workflow asks for.
type Decision struct {
	ShipmentID string `json:"shipment_id"`
	Want       string `json:"want"`               // pickup_confirmed or canceled
	Reason     string `json:"reason,omitempty"`   // why canceled
	Tracking   string `json:"tracking,omitempty"` // of a pickup
}

// Decided is the output of Decide: the shipment's state once decided, and
// the tracking number of a confirmed pickup.
type Decided struct {
	State    string `json:"state"`
	Tracking string `json:"tracking"`
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
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // another label of this order (shipments_order)
		return "", "", flows.Permanent(fmt.Errorf("shipping: order %s has a shipment already", r.OrderID))
	}
	if err != nil {
		return "", "", fmt.Errorf("create label for %s: %w", r.OrderID, err)
	}
	if state == stateCanceled {
		return "", "", flows.Permanent(errors.New("shipping: label canceled before it was created"))
	}
	c.Logger.LogAttrs(ctx, slog.LevelInfo, "shipping: label created",
		slog.String("order_id", r.OrderID), slog.String("shipment_id", id))
	return id, id, nil
}

// cancelLabel undoes createLabel by key (a tombstone if the label never
// landed), so it also handles an uncertain attempt. Only a label still
// label_created is canceled: a confirmed pickup is never undone.
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

// decide moves the row out of label_created to what the workflow wants,
// unless it has left it already (the carrier confirmed first), and returns
// the state. Idempotent: a retry returns the decided state.
func decide(ctx context.Context, _ flows.Key, d Decision) (Decided, struct{}, error) {
	c, err := cfg.Get()
	if err != nil {
		return Decided{}, struct{}{}, err
	}
	var out Decided
	var tracking *string
	// Lock the row, then decide on its latest state (see confirmPickup).
	err = pgx.BeginFunc(ctx, c.DB, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state, tracking FROM shipping.shipments WHERE id = $1 FOR UPDATE`,
			d.ShipmentID).Scan(&out.State, &tracking); err != nil {
			return err
		}
		if out.State != stateLabelCreated {
			return nil // decided already: by the carrier, or by an earlier attempt
		}
		return tx.QueryRow(ctx, `
			UPDATE shipping.shipments
			SET state = $2, reason = $3, tracking = coalesce(tracking, nullif($4, '')), updated_at = now()
			WHERE id = $1 RETURNING state, tracking`,
			d.ShipmentID, d.Want, d.Reason, d.Tracking).Scan(&out.State, &tracking)
	})
	if err != nil {
		return Decided{}, struct{}{}, fmt.Errorf("decide %s: %w", d.ShipmentID, err)
	}
	if tracking != nil {
		out.Tracking = *tracking
	}
	c.Logger.LogAttrs(ctx, slog.LevelInfo, "shipping: decided",
		slog.String("shipment_id", d.ShipmentID), slog.String("wanted", d.Want), slog.String("state", out.State))
	return out, struct{}{}, nil
}

func markPickedUp(ctx context.Context, _ flows.Key, u PickupUpdate) (struct{}, struct{}, error) {
	c, err := cfg.Get()
	if err != nil {
		return struct{}{}, struct{}{}, err
	}
	// Idempotent: picked_up again changes nothing. The row cannot be
	// anything but pickup_confirmed or picked_up here (the decision is
	// final); any other state is a bug, reported as an error (and retried
	// by untilDone, so an operator sees it).
	tag, err := c.DB.Exec(ctx, `
		UPDATE shipping.shipments SET state = 'picked_up', updated_at = now()
		WHERE id = $1 AND state IN ('pickup_confirmed', 'picked_up')`, u.ShipmentID)
	if err != nil {
		return struct{}{}, struct{}{}, fmt.Errorf("mark %s picked up: %w", u.ShipmentID, err)
	}
	if tag.RowsAffected() == 0 {
		return struct{}{}, struct{}{}, fmt.Errorf("shipping: %s is not pickup_confirmed", u.ShipmentID)
	}
	c.Logger.LogAttrs(ctx, slog.LevelInfo, "shipping: picked up", slog.String("shipment_id", u.ShipmentID))
	return struct{}{}, struct{}{}, nil
}
