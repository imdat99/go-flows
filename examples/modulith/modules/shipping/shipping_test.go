package shipping

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/flowtest"
)

// newTester mocks the module's activities; canceled counts label
// cancellations.
func newTester(t *testing.T) (*flowtest.Tester, *atomic.Int32) {
	t.Helper()
	tst := flowtest.New(t, flowtest.WithOptions(flows.Options{Queues: []string{Queue}}))
	var canceled atomic.Int32
	flowtest.OnActivity(tst, CreateLabel, func(_ context.Context, _ flows.Key, r Request) (string, string, error) {
		if r.Address == "nowhere" {
			return "", "", flows.Permanent(ErrUnserviceable)
		}
		return "shp_1", "shp_1", nil
	})
	flowtest.OnCompensator(tst, CreateLabel, func(context.Context, flows.Key, string) error {
		canceled.Add(1)
		return nil
	})
	flowtest.OnActivity(tst, MarkPickedUp, func(context.Context, flows.Key, PickupUpdate) (struct{}, struct{}, error) {
		return struct{}{}, struct{}{}, nil
	})
	return tst, &canceled
}

func TestShipPickedUp(t *testing.T) {
	tst, canceled := newTester(t)
	tst.SignalAfter(3*time.Hour, InstanceID("o1"), SignalPickedUp, Pickup{Tracking: "TRK"})

	s, err := flowtest.Run(tst, Ship, InstanceID("o1"), Request{OrderID: "o1", Address: "Hue", PickupWithin: 48 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if s != (Shipment{ID: "shp_1", Tracking: "TRK"}) || canceled.Load() != 0 {
		t.Fatalf("shipment %+v, %d label(s) canceled", s, canceled.Load())
	}
}

func TestShipPickupTimeout(t *testing.T) {
	tst, canceled := newTester(t)
	_, err := flowtest.Run(tst, Ship, InstanceID("o2"), Request{OrderID: "o2", Address: "Hue", PickupWithin: 48 * time.Hour})
	if !errors.Is(err, ErrPickupTimeout) || !errors.Is(err, flows.ErrCompensated) {
		t.Fatalf("err = %v, want pickup timeout, compensated", err)
	}
	if canceled.Load() != 1 {
		t.Fatalf("%d label(s) canceled, want 1", canceled.Load())
	}
}

func TestShipUnserviceable(t *testing.T) {
	tst, canceled := newTester(t)
	_, err := flowtest.Run(tst, Ship, InstanceID("o3"), Request{OrderID: "o3", Address: "nowhere", PickupWithin: time.Hour})
	if !errors.Is(err, ErrUnserviceable) {
		t.Fatalf("err = %v, want unserviceable", err)
	}
	// The label failed permanently, so there is nothing to cancel.
	if errors.Is(err, flows.ErrCompensated) || canceled.Load() != 0 {
		t.Fatalf("err = %v, %d label(s) canceled: nothing should be compensated", err, canceled.Load())
	}
}

func TestShipCanceled(t *testing.T) {
	tst, canceled := newTester(t)
	tst.SignalAfter(time.Hour, InstanceID("o4"), SignalCancel, CancelRequest{Reason: "customer"})
	_, err := flowtest.Run(tst, Ship, InstanceID("o4"), Request{OrderID: "o4", Address: "Hue", PickupWithin: 48 * time.Hour})
	if !errors.Is(err, ErrCanceled) || !errors.Is(err, flows.ErrCompensated) {
		t.Fatalf("err = %v, want canceled, compensated", err)
	}
	if canceled.Load() != 1 {
		t.Fatalf("%d label(s) canceled, want 1", canceled.Load())
	}
}

// A pickup and a cancel both waiting when the shipment looks: the pickup
// wins, the carrier has the parcel.
func TestShipPickupBeatsCancel(t *testing.T) {
	tst, canceled := newTester(t)
	// Both arrive while the label is being created, so both are waiting
	// when the workflow selects.
	flowtest.OnActivity(tst, CreateLabel, func(ctx context.Context, _ flows.Key, r Request) (string, string, error) {
		e := tst.Engine()
		if err := ConfirmPickup(ctx, e, r.OrderID, "TRK5"); err != nil {
			return "", "", err
		}
		return "shp_1", "shp_1", Cancel(ctx, e, r.OrderID, "customer")
	})
	s, err := flowtest.Run(tst, Ship, InstanceID("o5"), Request{OrderID: "o5", Address: "Hue", PickupWithin: 48 * time.Hour})
	if err != nil || s.Tracking != "TRK5" || canceled.Load() != 0 {
		t.Fatalf("shipment %+v, err %v, %d label(s) canceled; want picked up", s, err, canceled.Load())
	}
}
