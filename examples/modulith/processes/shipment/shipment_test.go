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

func TestShipCanceled(t *testing.T) {
	f := newTester(t)
	f.tst.SignalAfter(time.Hour, InstanceID("o4"), SignalCancel, CancelRequest{Reason: "customer"})
	_, err := flowtest.Run(f.tst, Ship, InstanceID("o4"), req("o4"))
	if !errors.Is(err, ErrCanceled) || !errors.Is(err, flows.ErrCompensated) {
		t.Fatalf("err = %v, want canceled, compensated", err)
	}
	if f.canceled.Load() != 1 {
		t.Fatalf("%d label(s) canceled, want 1", f.canceled.Load())
	}
}

// A pickup and a cancel both waiting when the shipment looks: the pickup
// wins, the carrier has the parcel.
func TestShipPickupBeatsCancel(t *testing.T) {
	f := newTester(t)
	// Both arrive while the label is being created, so both are waiting
	// when the workflow selects.
	flowtest.OnActivity(f.tst, CreateLabel, func(ctx context.Context, _ flows.Key, r Request) (string, string, error) {
		e := f.tst.Engine()
		if err := e.Signal(ctx, InstanceID(r.OrderID), SignalPickedUp, Pickup{Tracking: "TRK5"}); err != nil {
			return "", "", err
		}
		return "shp_1", "shp_1", e.Signal(ctx, InstanceID(r.OrderID), SignalCancel, CancelRequest{Reason: "customer"})
	})
	s, err := flowtest.Run(f.tst, Ship, InstanceID("o5"), req("o5"))
	if err != nil || s.Tracking != "TRK5" || f.canceled.Load() != 0 {
		t.Fatalf("shipment %+v, err %v, %d label(s) canceled; want picked up", s, err, f.canceled.Load())
	}
}

// The carrier's confirmation reached the table first, but its signal was
// lost, and the workflow woke up for a cancel: the table decides, the
// shipment completes.
func TestShipConfirmedPickupBeatsCancelSignal(t *testing.T) {
	f := newTester(t)
	flowtest.OnActivity(f.tst, CreateLabel, func(ctx context.Context, _ flows.Key, r Request) (string, string, error) {
		f.rows.confirm("shp_"+r.OrderID, "TRK6") // ConfirmPickup's update; no signal
		return "shp_" + r.OrderID, "shp_" + r.OrderID,
			f.tst.Engine().Signal(ctx, InstanceID(r.OrderID), SignalCancel, CancelRequest{Reason: "customer"})
	})
	s, err := flowtest.Run(f.tst, Ship, InstanceID("o6"), req("o6"))
	if err != nil || s.Tracking != "TRK6" || f.canceled.Load() != 0 {
		t.Fatalf("shipment %+v, err %v, %d label(s) canceled; want picked up", s, err, f.canceled.Load())
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
