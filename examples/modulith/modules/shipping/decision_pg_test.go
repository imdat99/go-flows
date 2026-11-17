package shipping

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/testdb"
)

// These tests run the decision on Postgres: ConfirmPickup (the carrier)
// against Decide (the workflow), whichever comes first.

func setupDB(t *testing.T) {
	t.Helper()
	pool := testdb.New(t)
	if err := Migrate(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	if err := Configure(Config{DB: pool, Logger: slog.New(slog.DiscardHandler)}); err != nil {
		t.Fatal(err)
	}
}

func label(t *testing.T, orderID string) string {
	t.Helper()
	id, _, err := createLabel(t.Context(), flows.Key("shipping-"+orderID+":1:shipping.create-label:1"), req(orderID))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestPickupAfterCancelIsRefused(t *testing.T) {
	setupDB(t)
	ctx := t.Context()
	id := label(t, "o1")
	d, _, err := decide(ctx, "", Decision{ShipmentID: id, Want: stateCanceled, Reason: "customer"})
	if err != nil || d.State != stateCanceled {
		t.Fatalf("decide: %+v, %v", d, err)
	}
	if err := confirmPickup(ctx, "o1", "TRK"); !errors.Is(err, ErrPickupRefused) {
		t.Fatalf("pickup after the cancel: %v, want ErrPickupRefused", err)
	}
	if err := confirmPickup(ctx, "nope", "TRK"); !errors.Is(err, ErrNoShipment) {
		t.Fatalf("pickup without a label: %v, want ErrNoShipment", err)
	}
}

func TestCancelAfterPickupKeepsThePickup(t *testing.T) {
	setupDB(t)
	ctx := t.Context()
	id := label(t, "o2")
	if err := confirmPickup(ctx, "o2", "TRK2"); err != nil {
		t.Fatal(err)
	}
	d, _, err := decide(ctx, "", Decision{ShipmentID: id, Want: stateCanceled, Reason: "customer"})
	if err != nil || d != (Decided{State: statePickupConfirmed, Tracking: "TRK2"}) {
		t.Fatalf("decide after the pickup: %+v, %v; want the pickup", d, err)
	}
	if _, _, err := markPickedUp(ctx, "", PickupUpdate{ShipmentID: id}); err != nil {
		t.Fatal(err)
	}
	// A repeated webhook is fine; the label's compensation changes nothing.
	if err := confirmPickup(ctx, "o2", "TRK2"); err != nil {
		t.Fatalf("repeated confirmation: %v", err)
	}
	if err := cancelLabel(ctx, flows.Key("shipping-o2:1:shipping.create-label:1"), id); err != nil {
		t.Fatal(err)
	}
	if s, err := Find(ctx, "o2"); err != nil || s != statePickedUp {
		t.Fatalf("state %q, %v; want picked_up", s, err)
	}
}

// The carrier and the workflow decide at the same time, many times: they
// always agree on one outcome.
func TestDecisionRace(t *testing.T) {
	setupDB(t)
	ctx := t.Context()
	for i := range 20 {
		orderID := fmt.Sprintf("race-%d", i)
		id := label(t, orderID)
		var wg sync.WaitGroup
		var confirmErr error
		var d Decided
		var decideErr error
		wg.Go(func() { confirmErr = confirmPickup(ctx, orderID, "TRK") })
		wg.Go(func() { d, _, decideErr = decide(ctx, "", Decision{ShipmentID: id, Want: stateCanceled}) })
		wg.Wait()
		if decideErr != nil {
			t.Fatal(decideErr)
		}
		switch {
		case confirmErr == nil && d.State == statePickupConfirmed:
		case errors.Is(confirmErr, ErrPickupRefused) && d.State == stateCanceled:
		default:
			t.Fatalf("%s: carrier got %v, workflow got %+v: they disagree", orderID, confirmErr, d)
		}
	}
}
