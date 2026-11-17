package orders

import (
	"context"
	"encoding/json"
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

var (
	// record inserts the order's row. It has no compensator: the hook sets
	// the row's final state however the workflow ends.
	record = flows.DefineActivity("orders.record", recordOrder, nil,
		flows.WithQueue(Queue), flows.WithTimeout(10*time.Second))

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
//     at most PickupWithin; the customer can cancel until the pickup.
//
// Returning an error from any step compensates the steps before it, in
// reverse order, in their own modules' queues.
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

	shipCtx, stopShipping := flows.WithCancel(c)
	defer stopShipping()
	shipment := shipping.Ship.Child(shipCtx, shipping.InstanceID(o.ID), shipping.Request{
		OrderID: o.ID, Address: o.Address, PickupWithin: o.PickupWithin,
	})
	var (
		res              shipping.Shipment
		shipErr, stopErr error
		shipped          bool
	)
	flows.Select(c,
		flows.OnFuture(shipment, func(_ flows.Ctx, s shipping.Shipment, err error) {
			res, shipErr, shipped = s, err, true
		}),
		flows.OnSignal(cancels, func(_ flows.Ctx, req CancelRequest, err error) {
			stopErr = canceled(req, err)
		}),
	)
	if !shipped {
		// Cancel the shipment (it cancels its label) and wait for it.
		stopShipping()
		res, shipErr = shipment.Get(c)
		if shipErr == nil {
			stopErr = nil // picked up meanwhile: too late to cancel
		}
	}
	if stopErr != nil {
		return Result{}, stopErr // refund, release, void
	}
	if shipErr != nil {
		return Result{}, shipErr // the same
	}
	return Result{PaymentID: auth.PaymentID, ShipmentID: res.ID, Tracking: res.Tracking}, nil
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

func recordOrder(ctx context.Context, _ flows.Key, o Order) (struct{}, struct{}, error) {
	c, err := cfg.Get()
	if err != nil {
		return struct{}{}, struct{}{}, err
	}
	lines, err := json.Marshal(o.Lines)
	if err != nil {
		return struct{}{}, struct{}{}, flows.Permanent(err)
	}
	// Idempotent by the order id.
	if _, err := c.DB.Exec(ctx, `
		INSERT INTO orders.orders (id, email, address, amount, lines, state)
		VALUES ($1, $2, $3, $4, $5, 'processing')
		ON CONFLICT (id) DO NOTHING`, o.ID, o.Email, o.Address, o.Amount, lines); err != nil {
		return struct{}{}, struct{}{}, fmt.Errorf("record order %s: %w", o.ID, err)
	}
	return struct{}{}, struct{}{}, nil
}
