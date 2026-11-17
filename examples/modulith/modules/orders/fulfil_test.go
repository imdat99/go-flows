package orders

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/go-flows/examples/modulith/modules/inventory"
	"github.com/imdat99/go-flows/examples/modulith/modules/payments"
	"github.com/imdat99/go-flows/examples/modulith/modules/shipping"
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
		Queues: []string{Queue, payments.Queue, inventory.Queue, shipping.Queue},
	}))
	undo := &undoLog{}

	flowtest.OnActivity(tst, record, func(context.Context, flows.Key, Order) (struct{}, struct{}, error) {
		return struct{}{}, struct{}{}, nil
	})
	flowtest.OnActivity(tst, payments.Authorize,
		func(_ context.Context, _ flows.Key, r payments.AuthorizeRequest) (payments.Authorization, string, error) {
			return payments.Authorization{PaymentID: "pay_" + r.OrderID}, "pay_" + r.OrderID, nil
		})
	flowtest.OnCompensator(tst, payments.Authorize, func(_ context.Context, _ flows.Key, id string) error {
		undo.add("void " + id)
		return nil
	})
	flowtest.OnActivity(tst, payments.Capture, func(_ context.Context, _ flows.Key, id string) (string, string, error) {
		return id, id, nil
	})
	flowtest.OnCompensator(tst, payments.Capture, func(_ context.Context, _ flows.Key, id string) error {
		undo.add("refund " + id)
		return nil
	})
	flowtest.OnActivity(tst, inventory.ReserveLine, func(_ context.Context, _ flows.Key, r inventory.LineRequest) (int64, int64, error) {
		if r.SKU == "sku-gone" {
			return 0, 0, flows.Permanent(inventory.ErrOutOfStock)
		}
		return int64(r.Qty), int64(r.Qty), nil // the quantity as the reservation id
	})
	flowtest.OnCompensator(tst, inventory.ReserveLine, func(_ context.Context, _ flows.Key, id int64) error {
		undo.add("release " + strconv.FormatInt(id, 10))
		return nil
	})
	flowtest.OnActivity(tst, shipping.CreateLabel, func(_ context.Context, _ flows.Key, r shipping.Request) (string, string, error) {
		return "shp_" + r.OrderID, "shp_" + r.OrderID, nil
	})
	flowtest.OnCompensator(tst, shipping.CreateLabel, func(_ context.Context, _ flows.Key, id string) error {
		undo.add("cancel label " + id)
		return nil
	})
	flowtest.OnActivity(tst, shipping.MarkPickedUp, func(context.Context, flows.Key, shipping.PickupUpdate) (struct{}, struct{}, error) {
		return struct{}{}, struct{}{}, nil
	})
	return tst, undo
}

func order(id string, lines ...Line) Order {
	if len(lines) == 0 {
		lines = []Line{{SKU: "sku-apple", Qty: 2}}
	}
	return Order{ID: id, Email: "c@example.com", Address: "Hanoi", Amount: 1500, Lines: lines, PickupWithin: 48 * time.Hour}
}

func TestFulfilShipped(t *testing.T) {
	tst, undo := newTester(t)
	tst.SignalAfter(time.Hour, shipping.InstanceID("o1"), shipping.SignalPickedUp, shipping.Pickup{Tracking: "TRK1"})

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
	// Shipping cancels its own label when the parent cancels it; then the
	// fulfilment undoes its steps, latest first, each in its module.
	want := []string{"cancel label shp_o2", "refund pay_o2", "release 2", "void pay_o2"}
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
	if !errors.Is(err, shipping.ErrPickupTimeout) || !errors.Is(err, flows.ErrCompensated) {
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
	o := order("o4", Line{SKU: "sku-apple", Qty: 3}, Line{SKU: "sku-gone", Qty: 1})

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
