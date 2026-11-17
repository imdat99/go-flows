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
	// PickupWithin is how long an order waits for the carrier (default
	// 48h). Place copies it into the saga's input.
	PickupWithin time.Duration
}

var deps = adapt.NewRef[Services]("fulfilment")

// Configure sets the services. Call it before the engine starts, in every
// process.
func Configure(s Services) error {
	if s.Orders == nil {
		return errors.New("fulfilment: Configure: Orders is required")
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

// InstanceID is the instance id of an order's saga. Instance ids are global:
// each process prefixes its own with its module.
func InstanceID(orderID string) string { return "orders-" + orderID }

var (
	// record inserts the order's row. It has no compensator: the hook sets
	// the row's final state however the saga ends.
	record = flows.DefineActivity("orders.record", recordOrder, nil,
		flows.WithQueue(Queue), flows.WithTimeout(10*time.Second))

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
// the shipment knows whether the pickup came first, so the saga requests
// the shipment's cancel (Future.RequestCancel) and the shipment's outcome
// decides: canceled, the order is undone; picked up, the order completes
// and the cancel is ignored.
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

	// The shipment is not started with a cancelable context: canceling it
	// would resolve its future at once with ErrCanceled, without its real
	// outcome (it may have been picked up). A cancel is requested instead
	// (RequestCancel), and the shipment's outcome decides.
	shipped := shipment.Ship.Child(c, shipment.InstanceID(o.ID), shipment.Request{
		OrderID: o.ID, Address: o.Address, PickupWithin: o.PickupWithin,
	})
	var (
		requested bool
		req       CancelRequest
	)
	flows.Select(c,
		flows.OnFuture(shipped, func(flows.Ctx, shipment.Shipment, error) {}),
		// A malformed cancel is ignored; later cancels stay unread.
		flows.OnSignal(cancels, func(_ flows.Ctx, r CancelRequest, err error) { req, requested = r, err == nil }),
	)
	if requested {
		// Nothing if the shipment has ended meanwhile; the future below
		// resolves with its real outcome either way.
		shipped.RequestCancel(c)
	}
	res, shipErr := shipped.Get(c)
	switch {
	case shipErr == nil:
		// Picked up: the order completes, even if a cancel arrived after
		// the pickup.
		return Result{PaymentID: auth.PaymentID, ShipmentID: res.ID, Tracking: res.Tracking}, nil
	case requested && (errors.Is(shipErr, shipment.ErrCanceled) || errors.Is(shipErr, flows.ErrCanceled)):
		// Canceled on the row, or (flows.ErrCanceled) never started: the
		// request was recorded in the task that started it.
		return Result{}, canceled(req, nil) // refund, release, void
	default:
		return Result{}, shipErr // the same
	}
}

// canceled is the error that ends a canceled order: the customer's reason
// (the saga ends compensated), or err, the decode error of a malformed
// cancel signal read before the shipment (the order stops all the same,
// compensated). Either way its completed activities are compensated
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

// Place starts the saga of an order. The order id is the idempotency key:
// placing the same order again starts nothing, placing other contents
// under a used id fails with orders.ErrOrderConflict.
//
// Two layers keep it:
//
//   - the orders module, for good and by contents: Place asks it first
//     (orders.Service.Placed), so the same order is a no-op and other
//     contents a conflict, however old the order;
//   - the engine, by instance id: Start returns the running or ended
//     execution, and with Options.KeepPurgedIDs a purged id fails with
//     flows.ErrInstancePurged for that window after retention. That covers
//     an order id whose saga ended without recording the order (its first
//     step failed for good): it is not run again, Place reports a
//     conflict.
//
// Two different orders racing for a new id both get nil here; the second
// one's contents are dropped (its Start finds the first execution).
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
	if errors.Is(err, flows.ErrInstancePurged) {
		return purged(ctx, s.Orders, o, err)
	}
	if err != nil {
		return fmt.Errorf("place %s: %w", o.ID, err)
	}
	return nil
}

// purged is Place's answer for an order id whose saga retention purged:
// the id was used. The order recorded with these contents (it was recorded
// since Placed looked): already placed, nil. Otherwise a conflict.
func purged(ctx context.Context, svc *orders.Service, o orders.Order, err error) error {
	placed, perr := svc.Placed(ctx, o)
	switch {
	case perr != nil:
		return perr // a conflict, or the module's error
	case placed:
		return nil
	}
	return fmt.Errorf("place %s: %w (%w)", o.ID, orders.ErrOrderConflict, err)
}

// Cancel asks for the cancellation of an order. It is honored until the
// carrier picks the parcel up; after that the order completes anyway (the
// saga requests the shipment's cancel, and the shipment decides). It fails
// with flows.ErrNotActive once the order has ended.
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
