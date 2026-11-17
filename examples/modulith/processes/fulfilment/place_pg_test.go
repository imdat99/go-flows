package fulfilment

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/imdat99/go-flows/examples/modulith/modules/orders"
	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/testdb"
)

// Place after retention, on Postgres: the orders table answers for an
// order it recorded, the engine's tombstone (Options.KeepPurgedIDs) for an
// order id whose saga ended without recording it.
func TestPlaceAfterPurge(t *testing.T) {
	pool := testdb.New(t)
	ctx := t.Context()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := orders.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	quiet := slog.New(slog.DiscardHandler)
	svc, err := orders.NewService(pool, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if err := Configure(Services{Orders: svc}); err != nil {
		t.Fatal(err)
	}
	// Not started: Place only starts sagas, nothing runs them, so the
	// order's row is never recorded (as when the saga's first step fails
	// for good).
	e, err := flows.New(pool, flows.Options{Logger: quiet, KeepPurgedIDs: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	o := orders.Order{
		ID: "p1", Email: "a@example.com", Address: "Hanoi", Amount: 100,
		Lines: []orders.Line{{SKU: "sku-a", Qty: 1}},
	}
	if err := Place(ctx, e, o); err != nil {
		t.Fatal(err)
	}
	if err := e.Terminate(ctx, InstanceID(o.ID), "test: the saga ends without the order's row"); err != nil {
		t.Fatal(err)
	}
	if n, err := e.PurgeEnded(ctx, 0, 10); err != nil || n != 1 {
		t.Fatalf("purged %d, %v; want 1", n, err)
	}

	// The same order again: its id was used, the saga is not run again.
	err = Place(ctx, e, o)
	if !errors.Is(err, orders.ErrOrderConflict) || !errors.Is(err, flows.ErrInstancePurged) {
		t.Fatalf("place after the purge: %v, want an order conflict (purged id)", err)
	}
	if _, err := Status(ctx, e, o.ID); !errors.Is(err, flows.ErrNotFound) {
		t.Fatalf("status: %v, want not found (nothing started)", err)
	}

	// An order recorded meanwhile (Place's race with its own saga): placed,
	// nil; other contents under its id: a conflict.
	if err := svc.Record(ctx, o); err != nil {
		t.Fatal(err)
	}
	if err := purged(ctx, svc, o, flows.ErrInstancePurged); err != nil {
		t.Fatalf("purged, recorded: %v, want nil", err)
	}
	other := o
	other.Amount = 200
	if err := purged(ctx, svc, other, flows.ErrInstancePurged); !errors.Is(err, orders.ErrOrderConflict) {
		t.Fatalf("purged, other contents: %v, want an order conflict", err)
	}
	// And through Place: the module's table answers first.
	if err := Place(ctx, e, o); err != nil {
		t.Fatalf("place a recorded order: %v, want nil", err)
	}
	if err := Place(ctx, e, other); !errors.Is(err, orders.ErrOrderConflict) {
		t.Fatalf("place other contents: %v, want an order conflict", err)
	}

	// An operator who means it forgets the id: the order runs again.
	if err := e.ForgetInstance(ctx, InstanceID("p1")); err != nil {
		t.Fatal(err)
	}
	if _, err := fulfil.Start(ctx, e, InstanceID("p1"), Order{ID: "p1"}); err != nil {
		t.Fatalf("start after ForgetInstance: %v", err)
	}
}
