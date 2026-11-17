package reservation

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"

	"github.com/imdat99/go-flows/examples/modulith/modules/inventory"
	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/flowtest"
	"github.com/imdat99/go-flows/internal/testdb"
)

func newTester(t *testing.T) *flowtest.Tester {
	t.Helper()
	return flowtest.New(t, flowtest.WithOptions(flows.Options{Queues: []string{Queue}}))
}

func TestReserveReleasesOnFailure(t *testing.T) {
	tst := newTester(t)
	var mu sync.Mutex
	var released []int64
	flowtest.OnActivity(tst, ReserveLine, func(_ context.Context, _ flows.Key, r LineRequest) (int64, int64, error) {
		if r.SKU == "sku-gone" {
			return 0, 0, flows.Permanent(inventory.ErrOutOfStock)
		}
		return int64(r.Qty), int64(r.Qty), nil
	})
	flowtest.OnCompensator(tst, ReserveLine, func(_ context.Context, _ flows.Key, id int64) error {
		mu.Lock()
		defer mu.Unlock()
		released = append(released, id)
		return nil
	})

	_, err := flowtest.Run(tst, Reserve, InstanceID("o1"), Request{OrderID: "o1", Lines: []Line{
		{SKU: "sku-a", Qty: 1}, {SKU: "sku-gone", Qty: 1}, {SKU: "sku-b", Qty: 2},
	}})
	if !errors.Is(err, inventory.ErrOutOfStock) || !errors.Is(err, flows.ErrCompensated) {
		t.Fatalf("err = %v, want out of stock, compensated", err)
	}
	mu.Lock()
	defer mu.Unlock()
	slices.Sort(released)
	if !slices.Equal(released, []int64{1, 2}) {
		t.Fatalf("released %v, want the two lines reserved", released)
	}
}

// The real adapters on the real module: a rejection of the module fails
// the line at once (permanent, one attempt), and the compensation releases
// what was reserved, by the activity's key.
func TestAdaptersOnTheModule(t *testing.T) {
	pool := testdb.New(t)
	if err := inventory.Migrate(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	svc, err := inventory.NewService(pool, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if err := Configure(Services{Inventory: svc}); err != nil {
		t.Fatal(err)
	}
	attempts := &attemptCounter{}
	tst := flowtest.New(t, flowtest.WithOptions(flows.Options{Queues: []string{Queue}, Observer: attempts}))

	// sku-plum has 3 units (the demo migration).
	_, err = flowtest.Run(tst, Reserve, InstanceID("o1"), Request{OrderID: "o1", Lines: []Line{
		{SKU: "sku-apple", Qty: 5}, {SKU: "sku-plum", Qty: 4},
	}})
	if !errors.Is(err, inventory.ErrOutOfStock) || !errors.Is(err, flows.ErrCompensated) {
		t.Fatalf("err = %v, want out of stock, compensated", err)
	}
	if n := attempts.of("inventory.reserve-line"); n != 2 {
		t.Fatalf("%d attempts of reserve-line, want 2 (no retry of the rejection)", n)
	}
	if n, err := svc.Level(t.Context(), "sku-apple"); err != nil || n != 50 {
		t.Fatalf("sku-apple %d, %v; want 50 after the release", n, err)
	}
}

type attemptCounter struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *attemptCounter) ActivityAttempt(a flows.ActivityAttempt) {
	if a.Compensation {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[a.Activity]++
}

func (*attemptCounter) RunEnded(flows.RunInfo) {}

func (c *attemptCounter) of(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[name]
}
