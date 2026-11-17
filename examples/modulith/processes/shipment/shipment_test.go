package shipment

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/examples/modulith/modules/shipping"
	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/flowtest"
)

// rows stands for the shipments table: the decision is first-wins, as the
// decisions of the real one (made under a row lock).
type rows struct {
	mu    sync.Mutex
	state map[string]shipping.Decided
}

// confirm is ConfirmPickup's update: label_created to pickup_confirmed.
func (r *rows) confirm(id, tracking string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.state[id]; !ok {
		r.state[id] = shipping.Decided{State: shipping.StatePickupConfirmed, Tracking: tracking}
	}
}

func (r *rows) decide(d shipping.Decision) shipping.Decided {
	r.mu.Lock()
	defer r.mu.Unlock()
	if got, ok := r.state[d.ShipmentID]; ok {
		return got
	}
	r.state[d.ShipmentID] = shipping.Decided{State: d.Want, Tracking: d.Tracking}
	return r.state[d.ShipmentID]
}

type fixture struct {
	tst      *flowtest.Tester
	rows     *rows
	canceled atomic.Int32 // label cancellations
	marked   atomic.Int32 // successful MarkPickedUp
	// markFailures makes the first MarkPickedUp attempts fail.
	markFailures atomic.Int32
}

func newTester(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{
		tst:  flowtest.New(t, flowtest.WithOptions(flows.Options{Queues: []string{Queue}})),
		rows: &rows{state: map[string]shipping.Decided{}},
	}
	flowtest.OnActivity(f.tst, CreateLabel, func(_ context.Context, _ flows.Key, r Request) (string, string, error) {
		if r.Address == "nowhere" {
			return "", "", flows.Permanent(shipping.ErrUnserviceable)
		}
		return "shp_" + r.OrderID, "shp_" + r.OrderID, nil
	})
	flowtest.OnCompensator(f.tst, CreateLabel, func(context.Context, flows.Key, string) error {
		f.canceled.Add(1)
		return nil
	})
	flowtest.OnActivity(f.tst, Decide, func(_ context.Context, _ flows.Key, d shipping.Decision) (shipping.Decided, struct{}, error) {
		return f.rows.decide(d), struct{}{}, nil
	})
	flowtest.OnActivity(f.tst, MarkPickedUp, func(context.Context, flows.Key, PickupUpdate) (struct{}, struct{}, error) {
		if f.markFailures.Add(-1) >= 0 {
			return struct{}{}, struct{}{}, errors.New("shipping database down")
		}
		f.marked.Add(1)
		return struct{}{}, struct{}{}, nil
	})
	return f
}

func req(id string) Request {
	return Request{OrderID: id, Address: "Hue", PickupWithin: 48 * time.Hour}
}

func TestShipPickedUp(t *testing.T) {
	f := newTester(t)
	f.tst.SignalAfter(3*time.Hour, InstanceID("o1"), SignalPickedUp, Pickup{Tracking: "TRK"})

	s, err := flowtest.Run(f.tst, Ship, InstanceID("o1"), req("o1"))
	if err != nil {
		t.Fatal(err)
	}
	if s != (Shipment{ID: "shp_o1", Tracking: "TRK"}) || f.canceled.Load() != 0 || f.marked.Load() != 1 {
		t.Fatalf("shipment %+v, %d label(s) canceled, %d marked", s, f.canceled.Load(), f.marked.Load())
	}
}

func TestShipPickupTimeout(t *testing.T) {
	f := newTester(t)
	_, err := flowtest.Run(f.tst, Ship, InstanceID("o2"), req("o2"))
	if !errors.Is(err, ErrPickupTimeout) || !errors.Is(err, flows.ErrCompensated) {
		t.Fatalf("err = %v, want pickup timeout, compensated", err)
	}
	if f.canceled.Load() != 1 {
		t.Fatalf("%d label(s) canceled, want 1", f.canceled.Load())
	}
}

func TestShipUnserviceable(t *testing.T) {
	f := newTester(t)
	r := req("o3")
	r.Address = "nowhere"
	_, err := flowtest.Run(f.tst, Ship, InstanceID("o3"), r)
	if !errors.Is(err, shipping.ErrUnserviceable) {
		t.Fatalf("err = %v, want unserviceable", err)
	}
	// The label failed permanently, so there is nothing to cancel.
	if errors.Is(err, flows.ErrCompensated) || f.canceled.Load() != 0 {
		t.Fatalf("err = %v, %d label(s) canceled: nothing should be compensated", err, f.canceled.Load())
	}
}

// cancelSignal is the signal of shipParent, a saga reduced to its cancel:
// it requests the shipment's cancel (Future.RequestCancel) and returns the
// shipment's real outcome.
const cancelSignal = "test.cancel"

var shipParent = flows.DefineWorkflow("test.ship-parent", 1, func(c flows.Ctx, r Request) (Shipment, error) {
	cancels := flows.Signals[struct{}](c, cancelSignal)
	shipped := Ship.Child(c, InstanceID(r.OrderID), r)
	flows.Select(c,
		flows.OnFuture(shipped, func(flows.Ctx, Shipment, error) {}),
		flows.OnSignal(cancels, func(flows.Ctx, struct{}, error) { shipped.RequestCancel(c) }),
	)
	return shipped.Get(c)
}, flows.WithWorkflowQueue(Queue))

// The saga requests the cancel while the parcel waits for the carrier: the
// shipment asks the row for canceled, cancels its label, and the saga gets
// ErrCanceled.
func TestShipCanceled(t *testing.T) {
	f := newTester(t)
	f.tst.SignalAfter(time.Hour, "parent-o4", cancelSignal, struct{}{})
	start := f.tst.Now()
	_, err := flowtest.Run(f.tst, shipParent, "parent-o4", req("o4"))
	if !errors.Is(err, ErrCanceled) || !errors.Is(err, flows.ErrCompensated) {
		t.Fatalf("err = %v, want canceled, compensated", err)
	}
	if f.canceled.Load() != 1 || f.tst.Now().Sub(start) < time.Hour {
		t.Fatalf("%d label(s) canceled after %v, want 1 after an hour", f.canceled.Load(), f.tst.Now().Sub(start))
	}
	if got := f.rows.decide(shipping.Decision{ShipmentID: "shp_o4"}); got.State != shipping.StateCanceled {
		t.Fatalf("row %+v, want canceled", got)
	}
}

// cancelDuring returns a CreateLabel that runs before (the carrier's
// confirmation, its signal), then cancels the shipment while the label is
// being created (Engine.Cancel: the same root-context cancel as the saga's
// request).
func cancelDuring(f *fixture, before func(ctx context.Context, orderID string) error) flows.InvokeFunc[Request, string, string] {
	return func(ctx context.Context, _ flows.Key, r Request) (string, string, error) {
		if before != nil {
			if err := before(ctx, r.OrderID); err != nil {
				return "", "", err
			}
		}
		return "shp_" + r.OrderID, "shp_" + r.OrderID, f.tst.Engine().Cancel(ctx, InstanceID(r.OrderID))
	}
}

// A cancel while the label is created: the label is not abandoned midway
// (it runs disconnected); the row decides canceled, and the label is
// canceled once.
func TestShipCanceledDuringLabel(t *testing.T) {
	f := newTester(t)
	flowtest.OnActivity(f.tst, CreateLabel, cancelDuring(f, nil))
	_, err := flowtest.Run(f.tst, Ship, InstanceID("o8"), req("o8"))
	if !errors.Is(err, ErrCanceled) || !errors.Is(err, flows.ErrCompensated) {
		t.Fatalf("err = %v, want canceled, compensated", err)
	}
	if f.canceled.Load() != 1 {
		t.Fatalf("%d label(s) canceled, want 1", f.canceled.Load())
	}
}

// The carrier confirms and wakes the shipment, and a cancel arrives too,
// before the shipment looks: the pickup wins, the carrier has the parcel.
func TestShipPickupBeatsCancel(t *testing.T) {
	f := newTester(t)
	flowtest.OnActivity(f.tst, CreateLabel, cancelDuring(f, func(ctx context.Context, orderID string) error {
		f.rows.confirm("shp_"+orderID, "TRK5") // ConfirmPickup's update, then its signal
		return f.tst.Engine().Signal(ctx, InstanceID(orderID), SignalPickedUp, Pickup{Tracking: "TRK5"})
	}))
	s, err := flowtest.Run(f.tst, Ship, InstanceID("o5"), req("o5"))
	if err != nil || s.Tracking != "TRK5" || f.canceled.Load() != 0 || f.marked.Load() != 1 {
		t.Fatalf("shipment %+v, err %v, %d label(s) canceled, %d marked; want picked up",
			s, err, f.canceled.Load(), f.marked.Load())
	}
}

// The carrier's confirmation reached the table first, but its signal was
// lost, and the workflow woke up for a cancel: it asks the row for
// canceled, the row says picked up, the shipment completes.
func TestShipConfirmedPickupBeatsCancel(t *testing.T) {
	f := newTester(t)
	flowtest.OnActivity(f.tst, CreateLabel, cancelDuring(f, func(_ context.Context, orderID string) error {
		f.rows.confirm("shp_"+orderID, "TRK6") // ConfirmPickup's update; no signal
		return nil
	}))
	start := f.tst.Now()
	s, err := flowtest.Run(f.tst, Ship, InstanceID("o6"), req("o6"))
	if err != nil || s.Tracking != "TRK6" || f.canceled.Load() != 0 || f.marked.Load() != 1 {
		t.Fatalf("shipment %+v, err %v, %d label(s) canceled, %d marked; want picked up",
			s, err, f.canceled.Load(), f.marked.Load())
	}
	if waited := f.tst.Now().Sub(start); waited >= time.Hour {
		t.Fatalf("decided after %v: the cancel did not wake the shipment", waited)
	}
}

// The saga's cancel arrives after the decision, while the pickup is
// recorded: nothing is undone, the saga gets the shipment.
func TestShipCancelAfterPickupDecision(t *testing.T) {
	f := newTester(t)
	flowtest.OnActivity(f.tst, MarkPickedUp, func(ctx context.Context, _ flows.Key, _ PickupUpdate) (struct{}, struct{}, error) {
		f.marked.Add(1)
		return struct{}{}, struct{}{}, f.tst.Engine().Signal(ctx, "parent-o9", cancelSignal, struct{}{})
	})
	f.tst.SignalAfter(time.Hour, InstanceID("o9"), SignalPickedUp, Pickup{Tracking: "TRK9"})
	s, err := flowtest.Run(f.tst, shipParent, "parent-o9", req("o9"))
	if err != nil || s.Tracking != "TRK9" || f.canceled.Load() != 0 || f.marked.Load() != 1 {
		t.Fatalf("shipment %+v, err %v, %d label(s) canceled, %d marked; want picked up",
			s, err, f.canceled.Load(), f.marked.Load())
	}
}

// After the pickup, recording it fails for hours (its database is down):
// the shipment keeps trying and completes; it never fails, so its label is
// never canceled.
func TestShipPickupRecordedDespiteFailures(t *testing.T) {
	f := newTester(t)
	f.markFailures.Store(35) // more than 3 rounds of pivotRetry
	f.tst.SignalAfter(time.Hour, InstanceID("o7"), SignalPickedUp, Pickup{Tracking: "TRK7"})
	start := f.tst.Now()

	s, err := flowtest.Run(f.tst, Ship, InstanceID("o7"), req("o7"))
	if err != nil || s.Tracking != "TRK7" {
		t.Fatalf("shipment %+v, err %v; want picked up", s, err)
	}
	if f.canceled.Load() != 0 || f.marked.Load() != 1 {
		t.Fatalf("%d label(s) canceled, %d marked; want 0 and 1", f.canceled.Load(), f.marked.Load())
	}
	if waited := f.tst.Now().Sub(start); waited < time.Hour+3*pivotPause {
		t.Fatalf("recorded after %v: the rounds did not pause", waited)
	}
}
