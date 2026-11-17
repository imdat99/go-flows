package fulfilment

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/examples/modulith/modules/inventory"
	"github.com/imdat99/go-flows/examples/modulith/modules/orders"
	"github.com/imdat99/go-flows/examples/modulith/modules/shipping"
	"github.com/imdat99/go-flows/examples/modulith/processes/payment"
	"github.com/imdat99/go-flows/examples/modulith/processes/reservation"
	"github.com/imdat99/go-flows/examples/modulith/processes/shipment"
	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/flowtest"
)

// undoLog records the compensators that ran, in order. Activities run on
// the engine's goroutines: guard it.
type undoLog struct {
	mu    sync.Mutex
	steps []string
}

func (u *undoLog) add(s string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.steps = append(u.steps, s)
}

func (u *undoLog) get() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return slices.Clone(u.steps)
}

// newTester mocks every activity the fulfilment reaches, its own and those
// of the modules it calls (their child workflows run for real). The
// tester's engine serves every module's queue, as a single process would.
func newTester(t *testing.T) (*flowtest.Tester, *undoLog) {
	t.Helper()
	tst := flowtest.New(t, flowtest.WithOptions(flows.Options{
		Queues: []string{Queue, payment.Queue, reservation.Queue, shipment.Queue},
	}))
	undo := &undoLog{}

	flowtest.OnActivity(tst, record, func(ctx context.Context, _ flows.Key, o Order) (struct{}, struct{}, error) {
		if o.Email == "cancel-early@example.com" {
			// The customer cancels while the order is being recorded.
			if err := tst.Engine().Signal(ctx, InstanceID(o.ID), SignalCancel, CancelRequest{Reason: "early"}); err != nil {
				return struct{}{}, struct{}{}, err
			}
		}
		return struct{}{}, struct{}{}, nil
	})
	// The real forwardCancel signals through the configured engine: here,
	// the tester's.
	flowtest.OnActivity(tst, forwardCancel, func(ctx context.Context, _ flows.Key, f ForwardedCancel) (struct{}, struct{}, error) {
		undo.add("forward cancel")
		err := shipment.Cancel(ctx, tst.Engine(), f.OrderID, f.Reason)
		if errors.Is(err, flows.ErrNotActive) {
			err = nil
		}
		return struct{}{}, struct{}{}, err
	})
	flowtest.OnActivity(tst, payment.Authorize,
		func(_ context.Context, _ flows.Key, r payment.AuthorizeRequest) (payment.Authorization, string, error) {
			return payment.Authorization{PaymentID: "pay_" + r.OrderID}, "pay_" + r.OrderID, nil
		})
	flowtest.OnCompensator(tst, payment.Authorize, func(_ context.Context, _ flows.Key, id string) error {
		undo.add("void " + id)
		return nil
	})
	flowtest.OnActivity(tst, payment.Capture, func(ctx context.Context, _ flows.Key, id string) (string, string, error) {
		if id == "pay_o8" {
			// The customer cancels while the payment is being captured.
			if err := tst.Engine().Signal(ctx, InstanceID("o8"), SignalCancel, CancelRequest{Reason: "during capture"}); err != nil {
				return "", "", err
			}
		}
		return id, id, nil
	})
	flowtest.OnCompensator(tst, payment.Capture, func(_ context.Context, _ flows.Key, id string) error {
		undo.add("refund " + id)
		return nil
	})
	flowtest.OnActivity(tst, reservation.ReserveLine, func(_ context.Context, _ flows.Key, r reservation.LineRequest) (int64, int64, error) {
		if r.SKU == "sku-gone" {
			return 0, 0, flows.Permanent(inventory.ErrOutOfStock)
		}
		return int64(r.Qty), int64(r.Qty), nil // the quantity as the reservation id
	})
	flowtest.OnCompensator(tst, reservation.ReserveLine, func(_ context.Context, _ flows.Key, id int64) error {
		undo.add("release " + strconv.FormatInt(id, 10))
		return nil
	})
	flowtest.OnActivity(tst, shipment.CreateLabel, func(_ context.Context, _ flows.Key, r shipment.Request) (string, string, error) {
		return "shp_" + r.OrderID, "shp_" + r.OrderID, nil
	})
	flowtest.OnCompensator(tst, shipment.CreateLabel, func(_ context.Context, _ flows.Key, id string) error {
		undo.add("cancel label " + id)
		return nil
	})
	// The shipments table's decision: first wins (see the shipment and shipping tests).
	var decMu sync.Mutex
	decided := map[string]shipping.Decided{}
	flowtest.OnActivity(tst, shipment.Decide, func(_ context.Context, _ flows.Key, d shipping.Decision) (shipping.Decided, struct{}, error) {
		decMu.Lock()
		defer decMu.Unlock()
		if got, ok := decided[d.ShipmentID]; ok {
			return got, struct{}{}, nil
		}
		decided[d.ShipmentID] = shipping.Decided{State: d.Want, Tracking: d.Tracking}
		return decided[d.ShipmentID], struct{}{}, nil
	})
	var markFailures atomic.Int32
	markFailures.Store(35) // for TRK-FLAKY: more than three rounds of attempts
	flowtest.OnActivity(tst, shipment.MarkPickedUp, func(ctx context.Context, _ flows.Key, u shipment.PickupUpdate) (struct{}, struct{}, error) {
		if u.Tracking == "TRK-FLAKY" && markFailures.Add(-1) >= 0 {
			return struct{}{}, struct{}{}, errors.New("shipping database down")
		}
		if u.Tracking == "TRK-RACE" {
			// The customer cancels just after the pickup reached the
			// shipment, while the order still waits for it.
			if err := tst.Engine().Signal(ctx, InstanceID(strings.TrimPrefix(u.ShipmentID, "shp_")), SignalCancel,
				CancelRequest{Reason: "too late"}); err != nil {
				return struct{}{}, struct{}{}, err
			}
		}
		return struct{}{}, struct{}{}, nil
	})
	return tst, undo
}

func order(id string, lines ...orders.Line) Order {
	if len(lines) == 0 {
		lines = []orders.Line{{SKU: "sku-apple", Qty: 2}}
	}
	return Order{ID: id, Email: "c@example.com", Address: "Hanoi", Amount: 1500, Lines: lines, PickupWithin: 48 * time.Hour}
}

func TestFulfilShipped(t *testing.T) {
	tst, undo := newTester(t)
	tst.SignalAfter(time.Hour, shipment.InstanceID("o1"), shipment.SignalPickedUp, shipment.Pickup{Tracking: "TRK1"})

	res, err := flowtest.Run(tst, fulfil, InstanceID("o1"), order("o1"))
	if err != nil {
		t.Fatal(err)
	}
	want := Result{PaymentID: "pay_o1", ShipmentID: "shp_o1", Tracking: "TRK1"}
	if res != want {
		t.Fatalf("result %+v, want %+v", res, want)
	}
	if got := undo.get(); len(got) != 0 {
		t.Fatalf("compensations ran: %v", got)
	}
}

func TestFulfilCanceledByCustomer(t *testing.T) {
	tst, undo := newTester(t)
	// The parcel waits for the carrier; the customer cancels after an hour.
	tst.SignalAfter(time.Hour, InstanceID("o2"), SignalCancel, CancelRequest{Reason: "changed my mind"})

	_, err := flowtest.Run(tst, fulfil, InstanceID("o2"), order("o2"))
	if !errors.Is(err, ErrCanceledByCustomer) || !errors.Is(err, flows.ErrCompensated) {
		t.Fatalf("err = %v, want canceled by the customer and compensated", err)
	}
	// The fulfilment forwards the cancel to the shipment, which cancels its
	// own label and fails; then the fulfilment undoes its steps, latest
	// first, each in its module.
	want := []string{"forward cancel", "cancel label shp_o2", "refund pay_o2", "release 2", "void pay_o2"}
	if got := undo.get(); !slices.Equal(got, want) {
		t.Fatalf("compensations %v, want %v", got, want)
	}
}

func TestFulfilPickupTimeout(t *testing.T) {
	tst, undo := newTester(t)
	o := order("o3")
	o.PickupWithin = 2 * time.Hour
	start := tst.Now()

	_, err := flowtest.Run(tst, fulfil, InstanceID("o3"), o)
	if !errors.Is(err, shipment.ErrPickupTimeout) || !errors.Is(err, flows.ErrCompensated) {
		t.Fatalf("err = %v, want pickup timeout, compensated", err)
	}
	if waited := tst.Now().Sub(start); waited < 2*time.Hour {
		t.Fatalf("gave up after %v of virtual time", waited)
	}
	want := []string{"cancel label shp_o3", "refund pay_o3", "release 2", "void pay_o3"}
	if got := undo.get(); !slices.Equal(got, want) {
		t.Fatalf("compensations %v, want %v", got, want)
	}
}

func TestFulfilOutOfStock(t *testing.T) {
	tst, undo := newTester(t)
	o := order("o4", orders.Line{SKU: "sku-apple", Qty: 3}, orders.Line{SKU: "sku-gone", Qty: 1})

	_, err := flowtest.Run(tst, fulfil, InstanceID("o4"), o)
	if !errors.Is(err, inventory.ErrOutOfStock) {
		t.Fatalf("err = %v, want out of stock", err)
	}
	// The reservation child releases the line it got, then the fulfilment
	// voids the authorization. Nothing was captured or shipped.
	want := []string{"release 3", "void pay_o4"}
	if got := undo.get(); !slices.Equal(got, want) {
		t.Fatalf("compensations %v, want %v", got, want)
	}
}

// The carrier's pickup reaches the shipment, then the customer's cancel
// reaches the order while it still waits for the shipment: the parcel is
// with the carrier, so the order completes and nothing is refunded.
func TestFulfilCancelAfterPickup(t *testing.T) {
	tst, undo := newTester(t)
	tst.SignalAfter(time.Hour, shipment.InstanceID("o6"), shipment.SignalPickedUp, shipment.Pickup{Tracking: "TRK-RACE"})

	res, err := flowtest.Run(tst, fulfil, InstanceID("o6"), order("o6"))
	if err != nil {
		t.Fatalf("err = %v, want the order completed (the pickup came first)", err)
	}
	if res.Tracking != "TRK-RACE" {
		t.Fatalf("result %+v", res)
	}
	// The cancel was forwarded (the race happened), and the shipment, which
	// had the pickup, refused it: nothing was compensated.
	if got := undo.get(); !slices.Equal(got, []string{"forward cancel"}) {
		t.Fatalf("steps %v, want only the forwarded cancel", got)
	}
}

// A cancel that arrives before the shipment starts stops the order before
// the capture: no shipment, no capture to refund.
func TestFulfilCancelBeforeShipping(t *testing.T) {
	tst, undo := newTester(t)
	o := order("o7")
	o.Email = "cancel-early@example.com"

	_, err := flowtest.Run(tst, fulfil, InstanceID("o7"), o)
	if !errors.Is(err, ErrCanceledByCustomer) {
		t.Fatalf("err = %v, want canceled by the customer", err)
	}
	want := []string{"release 2", "void pay_o7"}
	if got := undo.get(); !slices.Equal(got, want) {
		t.Fatalf("compensations %v, want %v", got, want)
	}
	if types := tst.EventTypes(shipment.InstanceID("o7")); len(types) != 0 {
		t.Fatalf("a shipment was started: %v", types)
	}
}

// After the pickup, the shipment cannot record it for hours (its database
// is down). It keeps trying instead of failing, so the order is never
// undone: no label canceled, no refund; it completes once the record lands.
func TestFulfilPickupRecordFailsForLong(t *testing.T) {
	tst, undo := newTester(t)
	tst.SignalAfter(time.Hour, shipment.InstanceID("o9"), shipment.SignalPickedUp, shipment.Pickup{Tracking: "TRK-FLAKY"})

	res, err := flowtest.Run(tst, fulfil, InstanceID("o9"), order("o9"))
	if err != nil || res.Tracking != "TRK-FLAKY" {
		t.Fatalf("result %+v, err %v; want the order completed", res, err)
	}
	if got := undo.get(); len(got) != 0 {
		t.Fatalf("compensations ran after the pickup: %v", got)
	}
}

// A cancel that arrives while the payment is captured: the order stops
// before the shipment, and the capture is refunded.
func TestFulfilCancelDuringCapture(t *testing.T) {
	tst, undo := newTester(t)
	_, err := flowtest.Run(tst, fulfil, InstanceID("o8"), order("o8"))
	if !errors.Is(err, ErrCanceledByCustomer) {
		t.Fatalf("err = %v, want canceled by the customer", err)
	}
	want := []string{"refund pay_o8", "release 2", "void pay_o8"}
	if got := undo.get(); !slices.Equal(got, want) {
		t.Fatalf("compensations %v, want %v", got, want)
	}
	if types := tst.EventTypes(shipment.InstanceID("o8")); len(types) != 0 {
		t.Fatalf("a shipment was started: %v", types)
	}
}
