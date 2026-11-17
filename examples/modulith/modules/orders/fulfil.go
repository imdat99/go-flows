package orders

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/imdat99/go-flows/examples/modulith/modules/inventory"
	"github.com/imdat99/go-flows/examples/modulith/modules/payments"
	"github.com/imdat99/go-flows/examples/modulith/modules/shipping"
	"github.com/imdat99/go-flows/flows"
)

// Order is the input of the fulfilment workflow: everything it needs, so
// that its code reads no configuration and no table (replays must see the
// same values).
type Order struct {
	ID           string        `json:"id"`
	Email        string        `json:"email"`
	Address      string        `json:"address"`
	Amount       int64         `json:"amount"`
	Lines        []Line        `json:"lines"`
	PickupWithin time.Duration `json:"pickup_within"`
}

// Result is the output of the fulfilment workflow.
type Result struct {
	PaymentID  string `json:"payment_id"`
	ShipmentID string `json:"shipment_id"`
	Tracking   string `json:"tracking"`
}

// ForwardedCancel is the input of forwardCancel.
type ForwardedCancel struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

var (
	// record inserts the order's row. It has no compensator: the hook sets
	// the row's final state however the workflow ends.
	record = flows.DefineActivity("orders.record", recordOrder, nil,
		flows.WithQueue(Queue), flows.WithTimeout(10*time.Second))

	// forwardCancel passes the customer's cancel on to the shipment. A
	// workflow cannot signal another workflow itself, so an activity does
	// it (at least once: the shipment reads the first cancel and ignores
	// the others).
	forwardCancel = flows.DefineActivity("orders.forward-cancel", forwardCancelFn, nil,
		flows.WithQueue(Queue), flows.WithTimeout(10*time.Second),
		flows.WithRetry(flows.Exponential(20, 200*time.Millisecond, 10*time.Second)))

	// fulfil is version 1 of the fulfilment. To change its sequence of
	// steps, define version 2 next to it and keep this one until its
	// executions have ended (README, "Versioning").
	fulfil = flows.DefineWorkflow("orders.fulfil", 1, fulfilOrder, flows.WithWorkflowQueue(Queue))
)

// fulfilOrder is the saga:
//
//  1. record the order (orders);
//  2. authorize the payment (payments, undone by void);
//  3. reserve the lines (inventory child workflow, undone by release);
//  4. capture the payment (payments, undone by refund);
//  5. ship (shipping child workflow): a label, then the carrier's pickup,
//     at most PickupWithin.
//
// Returning an error from any step compensates the steps before it, in
// reverse order, in their own modules' queues.
//
// The customer can cancel (SignalCancel) until the carrier picks the
// parcel up. Before step 5 the workflow stops by itself. During step 5 only
// the shipment knows whether the pickup came first, so the cancel is
// forwarded to it and the shipment's outcome decides: canceled, the order
// is undone; picked up, the order completes and the cancel is ignored.
func fulfilOrder(c flows.Ctx, o Order) (Result, error) {
	// Signals sent before the workflow listens are kept: a cancel that
	// arrives during steps 1 to 4 is read below.
	cancels := flows.Signals[CancelRequest](c, SignalCancel)

	if _, err := record.Run(c, o); err != nil {
		return Result{}, err
	}
	auth, err := payments.Authorize.Run(c, payments.AuthorizeRequest{OrderID: o.ID, Amount: o.Amount})
	if err != nil {
		return Result{}, err
	}
	lines := make([]inventory.Line, len(o.Lines))
	for i, l := range o.Lines {
		lines[i] = inventory.Line{SKU: l.SKU, Qty: l.Qty}
	}
	reservation := inventory.Reserve.Child(c, inventory.ReserveID(o.ID),
		inventory.ReserveRequest{OrderID: o.ID, Lines: lines})
	if _, err := reservation.Get(c); err != nil {
		return Result{}, err // voids the authorization
	}
	// Canceled meanwhile: stop before taking the money.
	if req, ok, err := cancels.TryReceive(); ok || err != nil {
		return Result{}, canceled(req, err)
	}
	if _, err := payments.Capture.Run(c, auth.PaymentID); err != nil {
		return Result{}, err
	}
	// Canceled meanwhile: do not start the shipment.
	if req, ok, err := cancels.TryReceive(); ok || err != nil {
		return Result{}, canceled(req, err)
	}

	// The shipment is not started with a cancelable context: canceling a
	// child resolves its future at once with ErrCanceled, without its real
	// outcome (it may have been picked up). The workflow waits for the
	// outcome instead.
	shipment := shipping.Ship.Child(c, shipping.InstanceID(o.ID), shipping.Request{
		OrderID: o.ID, Address: o.Address, PickupWithin: o.PickupWithin,
	})
	var (
		res       shipping.Shipment
		shipErr   error
		done      bool
		requested bool
		req       CancelRequest
	)
	flows.Select(c,
		flows.OnFuture(shipment, func(_ flows.Ctx, s shipping.Shipment, err error) { res, shipErr, done = s, err, true }),
		flows.OnSignal(cancels, func(_ flows.Ctx, r CancelRequest, err error) { req, requested = r, err == nil }),
	)
	if requested {
		// If forwarding fails for good, the shipment still ends by itself
		// (pickup or timeout): its outcome below is what counts.
		_, _ = forwardCancel.Run(c, ForwardedCancel{OrderID: o.ID, Reason: req.Reason})
	}
	if !done {
		res, shipErr = shipment.Get(c)
	}
	switch {
	case shipErr == nil:
		// Picked up: the order completes, even if a cancel arrived after
		// the pickup.
		return Result{PaymentID: auth.PaymentID, ShipmentID: res.ID, Tracking: res.Tracking}, nil
	case requested && errors.Is(shipErr, shipping.ErrCanceled):
		return Result{}, canceled(req, nil) // refund, release, void
	default:
		return Result{}, shipErr // the same
	}
}

// canceled is the error that ends a canceled order: the customer's reason,
// or the error of the signal channel (the workflow itself was canceled with
// Engine.Cancel: it then ends canceled rather than compensated).
func canceled(req CancelRequest, err error) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: %s", ErrCanceledByCustomer, req.Reason)
}

// recordOrder inserts the order's row, once: a retry finds its own row. A
// row with other contents, or one that has ended already (the id of an
// order purged from the engine, reused), fails the new order permanently.
func recordOrder(ctx context.Context, _ flows.Key, o Order) (struct{}, struct{}, error) {
	c, err := cfg.Get()
	if err != nil {
		return struct{}{}, struct{}{}, err
	}
	lines, err := json.Marshal(o.Lines)
	if err != nil {
		return struct{}{}, struct{}{}, flows.Permanent(err)
	}
	var same bool
	var state string
	err = c.DB.QueryRow(ctx, `
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
		return struct{}{}, struct{}{}, fmt.Errorf("record order %s: %w", o.ID, err)
	}
	if !same || state != "processing" {
		return struct{}{}, struct{}{}, flows.Permanent(fmt.Errorf("%w: %s (%s)", ErrOrderConflict, o.ID, state))
	}
	return struct{}{}, struct{}{}, nil
}

func forwardCancelFn(ctx context.Context, _ flows.Key, f ForwardedCancel) (struct{}, struct{}, error) {
	c, err := cfg.Get()
	if err != nil {
		return struct{}{}, struct{}{}, err
	}
	err = shipping.Cancel(ctx, c.Engine, f.OrderID, f.Reason)
	if errors.Is(err, flows.ErrNotActive) {
		err = nil // the shipment has ended: its outcome stands
	}
	return struct{}{}, struct{}{}, err
}
