package shipping

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/imdat99/go-flows/internal/testdb"
)

// These tests run the decision on Postgres: ConfirmPickup (the carrier)
// against Decide (whoever drives the shipment), whichever comes first.

func setup(t *testing.T) *Service {
	t.Helper()
	pool := testdb.New(t)
	if err := Migrate(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	s, err := NewService(pool, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func labelKey(orderID string) string { return "shipping-" + orderID + ":1:shipping.create-label:1" }

func (s *Service) label(t *testing.T, orderID string) string {
	t.Helper()
	id, err := s.CreateLabel(t.Context(), labelKey(orderID), orderID, "Hue")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestPickupAfterCancelIsRefused(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	id := s.label(t, "o1")
	d, err := s.Decide(ctx, Decision{ShipmentID: id, Want: StateCanceled, Reason: "customer"})
	if err != nil || d.State != StateCanceled {
		t.Fatalf("decide: %+v, %v", d, err)
	}
	if err := s.ConfirmPickup(ctx, "o1", "TRK"); !errors.Is(err, ErrPickupRefused) {
		t.Fatalf("pickup after the cancel: %v, want ErrPickupRefused", err)
	}
	if err := s.ConfirmPickup(ctx, "nope", "TRK"); !errors.Is(err, ErrNoShipment) {
		t.Fatalf("pickup without a label: %v, want ErrNoShipment", err)
	}
}

func TestCancelAfterPickupKeepsThePickup(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	id := s.label(t, "o2")
	if err := s.ConfirmPickup(ctx, "o2", "TRK2"); err != nil {
		t.Fatal(err)
	}
	d, err := s.Decide(ctx, Decision{ShipmentID: id, Want: StateCanceled, Reason: "customer"})
	if err != nil || d != (Decided{State: StatePickupConfirmed, Tracking: "TRK2"}) {
		t.Fatalf("decide after the pickup: %+v, %v; want the pickup", d, err)
	}
	if err := s.MarkPickedUp(ctx, id); err != nil {
		t.Fatal(err)
	}
	// A repeated webhook is fine; the label's cancellation changes nothing.
	if err := s.ConfirmPickup(ctx, "o2", "TRK2"); err != nil {
		t.Fatalf("repeated confirmation: %v", err)
	}
	if err := s.CancelLabel(ctx, labelKey("o2")); err != nil {
		t.Fatal(err)
	}
	if st, err := s.Find(ctx, "o2"); err != nil || st != StatePickedUp {
		t.Fatalf("state %q, %v; want picked_up", st, err)
	}
}

// The carrier and the workflow decide at the same time, many times: they
// always agree on one outcome.
func TestDecisionRace(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	for i := range 20 {
		orderID := fmt.Sprintf("race-%d", i)
		id := s.label(t, orderID)
		var wg sync.WaitGroup
		var confirmErr, decideErr error
		var d Decided
		wg.Go(func() { confirmErr = s.ConfirmPickup(ctx, orderID, "TRK") })
		wg.Go(func() { d, decideErr = s.Decide(ctx, Decision{ShipmentID: id, Want: StateCanceled}) })
		wg.Wait()
		if decideErr != nil {
			t.Fatal(decideErr)
		}
		switch {
		case confirmErr == nil && d.State == StatePickupConfirmed:
		case errors.Is(confirmErr, ErrPickupRefused) && d.State == StateCanceled:
		default:
			t.Fatalf("%s: carrier got %v, workflow got %+v: they disagree", orderID, confirmErr, d)
		}
	}
}

// A label canceled before it lands leaves a tombstone: the late CreateLabel
// fails as a rejection, and the order has no shipment.
func TestLateLabelIsFenced(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	if err := s.CancelLabel(ctx, labelKey("o3")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateLabel(ctx, labelKey("o3"), "o3", "Hue"); !errors.Is(err, ErrLabelCanceled) || !IsRejection(err) {
		t.Fatalf("late label: %v, want ErrLabelCanceled", err)
	}
	// Repeats of one key find their row; another key for the order is refused.
	id := s.label(t, "o4")
	if again := s.label(t, "o4"); again != id {
		t.Fatalf("repeat made %s, want %s", again, id)
	}
	if _, err := s.CreateLabel(ctx, "other-key", "o4", "Hue"); !errors.Is(err, ErrShipmentExists) {
		t.Fatalf("second label: %v, want ErrShipmentExists", err)
	}
	if _, err := s.CreateLabel(ctx, "k5", "o5", "nowhere"); !errors.Is(err, ErrUnserviceable) || !IsRejection(err) {
		t.Fatalf("unserviceable: %v", err)
	}
}
