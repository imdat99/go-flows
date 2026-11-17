// Package fulfilment is the order saga: it records an order (orders),
// authorizes the payment (payment), reserves the stock (reservation, a
// child workflow), captures the payment and ships (shipment, a child
// workflow). When a step fails or the customer cancels, the engine undoes
// the completed steps of every module, latest first, by activity name, on
// each module's queue (refund, release, void).
//
// It is the only place that knows how the modules fit together: the
// modules themselves know nothing of each other nor of the engine. Its
// RunHook ends the order in the orders module (state and event) in the
// transaction that records the saga's end.
package fulfilment

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imdat99/go-flows/examples/modulith/modules/orders"
	"github.com/imdat99/go-flows/examples/modulith/processes/internal/adapt"
	"github.com/imdat99/go-flows/examples/modulith/processes/payment"
	"github.com/imdat99/go-flows/examples/modulith/processes/reservation"
	"github.com/imdat99/go-flows/examples/modulith/processes/shipment"
	"github.com/imdat99/go-flows/flows"
)

// Queue is the queue of the saga and its own activities: the processes
// that run the orders module serve it.
const Queue = "orders"

// SignalCancel is the signal of a customer's cancellation (Cancel).
const SignalCancel = "orders.cancel"

// ErrCanceledByCustomer is the error of a saga the customer canceled.
var ErrCanceledByCustomer = errors.New("orders: canceled by the customer")

// kindCanceledByCustomer is the stored kind of ErrCanceledByCustomer; the
// hook reads it from RunInfo.ErrorKinds.
const kindCanceledByCustomer = "orders_canceled_by_customer"

func init() {
	flows.RegisterErrorKind(kindCanceledByCustomer, ErrCanceledByCustomer)
	flows.RegisterErrorKind("orders_order_conflict", orders.ErrOrderConflict)
}

// Services are the module services the activities call, and the saga's
// settings.
type Services struct {
	Orders *orders.Service
	// Engine is used by the activity that forwards a cancel to the shipment
	// (any engine on the database works).
	Engine *flows.Engine
	// PickupWithin is how long an order waits for the carrier (default
	// 48h). Place copies it into the saga's input.
	PickupWithin time.Duration
}

var deps = adapt.NewRef[Services]("fulfilment")

// Configure sets the services. Call it before the engine starts, in every
// process.
func Configure(s Services) error {
	if s.Orders == nil || s.Engine == nil {
		return errors.New("fulfilment: Configure: Orders and Engine are required")
	}
	if s.PickupWithin <= 0 {
		s.PickupWithin = 48 * time.Hour
	}
	deps.Set(s)
	return nil
}

// Order is the input of the saga: everything it needs, so that its code
// reads no configuration and no table (replays must see the same values).
type Order struct {
	ID           string        `json:"id"`
	Email        string        `json:"email"`
	Address      string        `json:"address"`
	Amount       int64         `json:"amount"`
	Lines        []orders.Line `json:"lines"`
	PickupWithin time.Duration `json:"pickup_within"`
}

func (o Order) domain() orders.Order {
	return orders.Order{ID: o.ID, Email: o.Email, Address: o.Address, Amount: o.Amount, Lines: o.Lines}
}

// Result is the output of the saga.
type Result struct {
	PaymentID  string `json:"payment_id"`
	ShipmentID string `json:"shipment_id"`
	Tracking   string `json:"tracking"`
}

// CancelRequest is the payload of SignalCancel.
type CancelRequest struct {
	Reason string `json:"reason"`
}

// ForwardedCancel is the input of forwardCancel.
type ForwardedCancel struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

// InstanceID is the instance id of an order's saga. Instance ids are global:
// each process prefixes its own with its module.
func InstanceID(orderID string) string { return "orders-" + orderID }

var (
	// record inserts the order's row. It has no compensator: the hook sets
	// the row's final state however the saga ends.
	record = flows.DefineActivity("orders.record", recordOrder, nil,
		flows.WithQueue(Queue), flows.WithTimeout(10*time.Second))

	// forwardCancel passes the customer's cancel on to the shipment. A
	// workflow cannot signal another workflow itself, so an activity does
	// it (at least once: the shipment reads the first cancel and ignores
	// the others).
	forwardCancel = flows.DefineActivity("orders.forward-cancel", forwardCancelFn, nil,
		flows.WithQueue(Queue), flows.WithTimeout(10*time.Second),
		flows.WithRetry(flows.Exponential(20, 200*time.Millisecond, 10*time.Second)))

	// fulfil is version 1 of the saga. To change its sequence of steps,
	// define version 2 next to it and keep this one until its executions
	// have ended (README, "Versioning").
	fulfil = flows.DefineWorkflow("orders.fulfil", 1, fulfilOrder, flows.WithWorkflowQueue(Queue))
)

// fulfilOrder is the saga:
//
//  1. record the order (orders);
//  2. authorize the payment (payments, undone by void);
//  3. reserve the lines (inventory, a child workflow, undone by release);
//  4. capture the payment (payments, undone by refund);
//  5. ship (shipping, a child workflow): a label, then the carrier's
//     pickup, at most PickupWithin.
//
// Returning an error from any step compensates the steps before it, in
// reverse order, in their own modules' queues.
//
// The customer can cancel (SignalCancel) until the carrier picks the
// parcel up. Before step 5 the saga stops by itself. During step 5 only
// the shipment knows whether the pickup came first, so the cancel is
// forwarded to it and the shipment's outcome decides: canceled, the order
// is undone; picked up, the order completes and the cancel is ignored.
func fulfilOrder(c flows.Ctx, o Order) (Result, error) {
	// Signals sent before the saga listens are kept: a cancel that arrives
	// during steps 1 to 4 is read below.
	cancels := flows.Signals[CancelRequest](c, SignalCancel)

	if _, err := record.Run(c, o); err != nil {
		return Result{}, err
	}
	auth, err := payment.Authorize.Run(c, payment.AuthorizeRequest{OrderID: o.ID, Amount: o.Amount})
	if err != nil {
		return Result{}, err
	}
	lines := make([]reservation.Line, len(o.Lines))
	for i, l := range o.Lines {
		lines[i] = reservation.Line{SKU: l.SKU, Qty: l.Qty}
	}
	reserved := reservation.Reserve.Child(c, reservation.InstanceID(o.ID),
		reservation.Request{OrderID: o.ID, Lines: lines})
	if _, err := reserved.Get(c); err != nil {
		return Result{}, err // voids the authorization
	}
	// Canceled meanwhile: stop before taking the money.
	if req, ok, err := cancels.TryReceive(); ok || err != nil {
		return Result{}, canceled(req, err)
	}
	if _, err := payment.Capture.Run(c, auth.PaymentID); err != nil {
		return Result{}, err
	}
	// Canceled meanwhile: do not start the shipment.
	if req, ok, err := cancels.TryReceive(); ok || err != nil {
		return Result{}, canceled(req, err)
	}

	// The shipment is not started with a cancelable context: canceling a
	// child resolves its future at once with ErrCanceled, without its real
	// outcome (it may have been picked up). The saga waits for the outcome
	// instead.
	shipped := shipment.Ship.Child(c, shipment.InstanceID(o.ID), shipment.Request{
		OrderID: o.ID, Address: o.Address, PickupWithin: o.PickupWithin,
	})
	var (
		res       shipment.Shipment
		shipErr   error
		done      bool
		requested bool
		req       CancelRequest
	)
	flows.Select(c,
		flows.OnFuture(shipped, func(_ flows.Ctx, s shipment.Shipment, err error) { res, shipErr, done = s, err, true }),
		flows.OnSignal(cancels, func(_ flows.Ctx, r CancelRequest, err error) { req, requested = r, err == nil }),
	)
	if requested {
		// If forwarding fails for good, the shipment still ends by itself
		// (pickup or timeout): its outcome below is what counts.
		_, _ = forwardCancel.Run(c, ForwardedCancel{OrderID: o.ID, Reason: req.Reason})
	}
	if !done {
		res, shipErr = shipped.Get(c)
	}
	switch {
	case shipErr == nil:
		// Picked up: the order completes, even if a cancel arrived after
		// the pickup.
		return Result{PaymentID: auth.PaymentID, ShipmentID: res.ID, Tracking: res.Tracking}, nil
	case requested && errors.Is(shipErr, shipment.ErrCanceled):
		return Result{}, canceled(req, nil) // refund, release, void
	default:
		return Result{}, shipErr // the same
	}
}

// canceled is the error that ends a canceled order: the customer's reason
// (the saga ends compensated), or the error of the signal channel: the saga
// itself was canceled with Engine.Cancel, and returning that error ends it
// with status canceled. Either way its completed activities are compensated
// (refund, release, void).
func canceled(req CancelRequest, err error) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: %s", ErrCanceledByCustomer, req.Reason)
}

// recordOrder records the order in its module, once: a retry finds its own
// row. A conflict (other contents, or an ended order under this id) is a
// rejection: the saga fails without compensation.
func recordOrder(ctx context.Context, _ flows.Key, o Order) (struct{}, struct{}, error) {
	s, err := deps.Get()
	if err != nil {
		return struct{}{}, struct{}{}, err
	}
	return struct{}{}, struct{}{}, adapt.Error(s.Orders.Record(ctx, o.domain()), orders.IsRejection)
}

func forwardCancelFn(ctx context.Context, _ flows.Key, f ForwardedCancel) (struct{}, struct{}, error) {
	s, err := deps.Get()
	if err != nil {
		return struct{}{}, struct{}{}, err
	}
	err = shipment.Cancel(ctx, s.Engine, f.OrderID, f.Reason)
	if errors.Is(err, flows.ErrNotActive) {
		err = nil // the shipment has ended: its outcome stands
	}
	return struct{}{}, struct{}{}, err
}

// Place starts the saga of an order. The order id is the idempotency key:
// placing the same order again starts nothing, placing other contents
// under a used id fails with orders.ErrOrderConflict.
//
// The engine's own idempotency (Start returns the existing execution) lasts
// only until retention purges the execution, so Place asks the orders
// module first, which keeps orders for good. Two different orders racing
// for a new id both get nil here; the second one's contents are dropped
// (its Start finds the first execution).
func Place(ctx context.Context, e *flows.Engine, o orders.Order) error {
	s, err := deps.Get()
	if err != nil {
		return err
	}
	if err := s.Orders.Validate(o); err != nil {
		return err
	}
	switch placed, err := s.Orders.Placed(ctx, o); {
	case err != nil:
		return err
	case placed:
		return nil
	}
	_, err = fulfil.Start(ctx, e, InstanceID(o.ID), Order{
		ID: o.ID, Email: o.Email, Address: o.Address, Amount: o.Amount, Lines: o.Lines,
		PickupWithin: s.PickupWithin,
	})
	if err != nil {
		return fmt.Errorf("place %s: %w", o.ID, err)
	}
	return nil
}

// Cancel asks for the cancellation of an order. It is honored until the
// carrier picks the parcel up; after that the order completes anyway (the
// saga forwards the cancel to the shipment, which decides). It fails with
// flows.ErrNotActive once the order has ended.
//
// Use it, not Engine.Cancel, for customers. Engine.Cancel is an operator's
// tool: it cancels the saga's context, so the shipment child's future
// resolves at once with ErrCanceled and the saga compensates (refund,
// release, void) without waiting for the shipment's outcome. After a
// pickup that means the money is refunded and the stock released while the
// parcel ships (the shipment itself still completes: its decision stands).
func Cancel(ctx context.Context, e *flows.Engine, orderID, reason string) error {
	return e.Signal(ctx, InstanceID(orderID), SignalCancel, CancelRequest{Reason: reason})
}

// Status returns the status of an order's saga (running, completed,
// compensating, compensated, needs_attention...). flows.ErrNotFound for an
// unknown order.
func Status(ctx context.Context, e *flows.Engine, orderID string) (flows.Status, error) {
	st, err := fulfil.Handle(e, InstanceID(orderID)).Status(ctx)
	if err != nil {
		return "", fmt.Errorf("order %s: %w", orderID, err)
	}
	return st, nil
}

// Hook returns the saga's RunHook. The composition root installs it on
// every engine (client-only ones included): an end is recorded by whichever
// engine runs the last task, or calls Terminate.
func Hook(svc *orders.Service) flows.RunHook { return endHook{svc} }

type endHook struct{ orders *orders.Service }

// RunEnded ends the order in the orders module (its state and its event)
// in the transaction that records the saga's end: the read model, the
// event and the saga's status commit together or not at all.
func (h endHook) RunEnded(ctx context.Context, tx pgx.Tx, r flows.RunInfo) error {
	if r.Workflow != fulfil.Name() || tx == nil { // tx is nil on flowtest's memory store
		return nil
	}
	var state string
	switch r.Status {
	case flows.StatusNeedsAttention:
		return nil // a park, not an end: the order may still complete or compensate
	case flows.StatusCompleted:
		state = orders.StateCompleted
	default: // compensated, failed, canceled, terminated
		state = orders.StateFailed
		if r.Status == flows.StatusCanceled || slices.Contains(strings.Split(r.ErrorKinds, ","), kindCanceledByCustomer) {
			state = orders.StateCanceled
		}
	}
	_, err := h.orders.End(ctx, tx, strings.TrimPrefix(r.InstanceID, InstanceID("")), state, r.Error)
	return err
}
