// Package shipment is the process that drives the shipping module's
// shipment of an order: the workflow Ship, which a saga starts as a child.
//
// # The pickup pivot
//
// The shipping module decides, once, on the shipment's row, whether the
// carrier has the parcel or the shipment is canceled (see package
// shipping). This process only drives it:
//
//   - the carrier's webhook records the pickup in the module first
//     (shipping.Service.ConfirmPickup), then wakes the workflow
//     (WakeOnPickup);
//   - the workflow, woken by the first of pickup, cancel or timeout, asks
//     the module for the outcome it wants (Decide) and gets the row's
//     state: signals only wake it;
//   - from the decision on, the workflow cannot fail into the caller's saga
//     by accident: Decide and MarkPickedUp run on a disconnected context and
//     are retried until they succeed (untilDone).
package shipment

import (
	"context"
	"errors"
	"time"

	"github.com/imdat99/go-flows/examples/modulith/modules/shipping"
	"github.com/imdat99/go-flows/examples/modulith/processes/internal/adapt"
	"github.com/imdat99/go-flows/flows"
)

// Queue is the queue of the workflow and its activities: the processes that
// run the shipping module serve it.
const Queue = "shipping"

// Signals of Ship. They wake it; the shipment's row decides.
const (
	// SignalPickedUp follows a confirmed pickup (WakeOnPickup).
	SignalPickedUp = "shipping.picked-up"
	// SignalCancel asks for the shipment to be stopped (Cancel).
	SignalCancel = "shipping.cancel"
)

var (
	// ErrPickupTimeout is the error of a shipment the carrier did not pick
	// up in time.
	ErrPickupTimeout = errors.New("shipping: carrier did not pick up in time")
	// ErrCanceled is the error of a shipment stopped by Cancel before the
	// carrier picked it up.
	ErrCanceled = errors.New("shipping: canceled before pickup")
)

func init() {
	flows.RegisterErrorKind("shipping_unserviceable", shipping.ErrUnserviceable)
	flows.RegisterErrorKind("shipping_pickup_timeout", ErrPickupTimeout)
	flows.RegisterErrorKind("shipping_canceled", ErrCanceled)
}

// Services are the module services the activities call.
type Services struct {
	Shipping *shipping.Service
}

var deps = adapt.NewRef[Services]("shipment")

// Configure sets the services. Call it before the engine starts, in every
// process.
func Configure(s Services) error {
	if s.Shipping == nil {
		return errors.New("shipment: Configure: Shipping is required")
	}
	deps.Set(s)
	return nil
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

// PickupUpdate is the input of MarkPickedUp.
type PickupUpdate struct {
	ShipmentID string `json:"shipment_id"`
	Tracking   string `json:"tracking"`
}

// InstanceID is the instance id of the shipment of an order.
func InstanceID(orderID string) string { return "shipping-" + orderID }

// WakeOnPickup wakes the shipment of an order after the shipping module
// recorded the carrier's pickup. If it fails (a crash before it), the
// workflow still finds the confirmed row when its timeout fires. A
// shipment that has ended (and perhaps been purged by retention) needs no
// waking: the pickup is recorded.
func WakeOnPickup(ctx context.Context, e *flows.Engine, orderID, tracking string) error {
	err := e.Signal(ctx, InstanceID(orderID), SignalPickedUp, Pickup{Tracking: tracking})
	if errors.Is(err, flows.ErrNotActive) || errors.Is(err, flows.ErrNotFound) {
		return nil
	}
	return err
}

// Cancel asks the shipment of an order to stop. The shipment decides: if
// the carrier's pickup was confirmed first, it completes anyway (the caller
// sees that in its outcome); otherwise it cancels its label and fails with
// ErrCanceled. flows.ErrNotActive once the shipment has ended.
func Cancel(ctx context.Context, e *flows.Engine, orderID, reason string) error {
	return e.Signal(ctx, InstanceID(orderID), SignalCancel, CancelRequest{Reason: reason})
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
	// CreateLabel creates the label; its compensator cancels it. Sagas
	// start Ship rather than call it: it is exported so that their tests
	// can mock it (flowtest.OnActivity) when Ship runs as their child.
	CreateLabel = flows.DefineActivity("shipping.create-label", createLabel, cancelLabel, activityOpts...)
	// Decide asks the module for the shipment's decision; exported for
	// tests, as CreateLabel.
	Decide = flows.DefineActivity("shipping.decide", decide, nil,
		flows.WithQueue(Queue), flows.WithTimeout(10*time.Second), pivotRetry)
	// MarkPickedUp records the pickup; exported for tests, as CreateLabel.
	MarkPickedUp = flows.DefineActivity("shipping.mark-picked-up", markPickedUp, nil,
		flows.WithQueue(Queue), flows.WithTimeout(10*time.Second), pivotRetry)

	// Ship creates a label, then waits for the first of the carrier's
	// pickup, a cancel, or PickupWithin, and has the module decide. Pickup:
	// it completes. Cancel: it cancels the label and fails with
	// ErrCanceled. Timeout: the same with ErrPickupTimeout.
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
	want := shipping.Decision{ShipmentID: id, Want: shipping.StateCanceled}
	var why error
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
			want.Want, want.Tracking = shipping.StatePickupConfirmed, p.Tracking
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
	if d.State == shipping.StateCanceled {
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

// The activity's key is the module's idempotency key of the label.
func createLabel(ctx context.Context, k flows.Key, r Request) (string, string, error) {
	s, err := deps.Get()
	if err != nil {
		return "", "", err
	}
	id, err := s.Shipping.CreateLabel(ctx, string(k), r.OrderID, r.Address)
	if err != nil {
		return "", "", adapt.Error(err, shipping.IsRejection)
	}
	return id, id, nil
}

// cancelLabel works by key (a tombstone if the label never landed), so it
// also settles an uncertain attempt. A confirmed pickup is never undone.
func cancelLabel(ctx context.Context, k flows.Key, _ string) error {
	s, err := deps.Get()
	if err != nil {
		return err
	}
	return s.Shipping.CancelLabel(ctx, string(k))
}

func decide(ctx context.Context, _ flows.Key, d shipping.Decision) (shipping.Decided, struct{}, error) {
	s, err := deps.Get()
	if err != nil {
		return shipping.Decided{}, struct{}{}, err
	}
	out, err := s.Shipping.Decide(ctx, d)
	return out, struct{}{}, err
}

func markPickedUp(ctx context.Context, _ flows.Key, u PickupUpdate) (struct{}, struct{}, error) {
	s, err := deps.Get()
	if err != nil {
		return struct{}{}, struct{}{}, err
	}
	return struct{}{}, struct{}{}, s.Shipping.MarkPickedUp(ctx, u.ShipmentID)
}
