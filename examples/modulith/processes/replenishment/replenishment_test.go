package replenishment

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/go-flows/examples/modulith/modules/inventory"
	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/flowtest"
)

func newTester(t *testing.T) *flowtest.Tester {
	t.Helper()
	return flowtest.New(t, flowtest.WithOptions(flows.Options{Queues: []string{Queue}}))
}

func TestLoop(t *testing.T) {
	tst := newTester(t)
	// The level drops by 4 per cycle, from 12; a purchase order adds 20.
	var mu sync.Mutex
	level, ordered := 12, 0
	flowtest.OnActivity(tst, checkLevel, func(context.Context, flows.Key, string) (inventory.Stock, struct{}, error) {
		mu.Lock()
		defer mu.Unlock()
		level -= 4
		return inventory.Stock{Available: level}, struct{}{}, nil
	})
	flowtest.OnActivity(tst, placePurchaseOrder, func(_ context.Context, _ flows.Key, po PurchaseOrder) (int64, struct{}, error) {
		mu.Lock()
		defer mu.Unlock()
		level += po.Qty
		ordered++
		return int64(ordered), struct{}{}, nil
	})
	// Five cycles, then the SKU is discontinued.
	tst.SignalAfter(5*time.Hour+time.Minute, loopID("sku-a"), SignalStop, struct{}{})

	st, err := flowtest.Run(tst, loop, loopID("sku-a"), State{SKU: "sku-a", Every: time.Hour, ReorderPoint: 5, ReorderQty: 20})
	if err != nil {
		t.Fatal(err)
	}
	// Levels seen: 8, 4 (order: 24), 20, 16, 12.
	if st.Cycles != 5 || st.Ordered != 1 || st.LastError != "" {
		t.Fatalf("state %+v, want 5 cycles and 1 order", st)
	}
	// Each cycle was its own short execution (continue-as-new): the last
	// one only waited for its timer and read the stop signal.
	if n := len(tst.EventTypes(loopID("sku-a"))); n > 10 {
		t.Fatalf("the last execution has %d events: %v", n, tst.EventTypes(loopID("sku-a")))
	}
}

func TestFailedCycleGoesOn(t *testing.T) {
	tst := newTester(t)
	calls := 0
	var mu sync.Mutex
	flowtest.OnActivity(tst, checkLevel, func(context.Context, flows.Key, string) (inventory.Stock, struct{}, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return inventory.Stock{}, struct{}{}, flows.Permanent(errors.New("db down"))
		}
		return inventory.Stock{Available: 100}, struct{}{}, nil
	})
	tst.SignalAfter(2*time.Hour+time.Minute, loopID("sku-b"), SignalStop, struct{}{})

	st, err := flowtest.Run(tst, loop, loopID("sku-b"), State{SKU: "sku-b", Every: time.Hour, ReorderPoint: 5, ReorderQty: 20})
	if err != nil {
		t.Fatal(err)
	}
	if st.Cycles != 2 || st.LastError != "" {
		t.Fatalf("state %+v, want 2 cycles, the second clean", st)
	}
}

// A SKU discontinued while its loop runs (the stop signal missed it): the
// loop sees the module's flag at its next cycle and ends without ordering.
func TestLoopStopsWhenDiscontinued(t *testing.T) {
	tst := newTester(t)
	var mu sync.Mutex
	cycles, ordered := 0, 0
	flowtest.OnActivity(tst, checkLevel, func(context.Context, flows.Key, string) (inventory.Stock, struct{}, error) {
		mu.Lock()
		defer mu.Unlock()
		cycles++
		return inventory.Stock{Available: 0, Discontinued: cycles >= 2}, struct{}{}, nil
	})
	flowtest.OnActivity(tst, placePurchaseOrder, func(context.Context, flows.Key, PurchaseOrder) (int64, struct{}, error) {
		mu.Lock()
		defer mu.Unlock()
		ordered++
		return int64(ordered), struct{}{}, nil
	})
	st, err := flowtest.Run(tst, loop, loopID("sku-c"), State{SKU: "sku-c", Every: time.Hour, ReorderPoint: 5, ReorderQty: 20})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if st.Cycles != 2 || ordered != 1 {
		t.Fatalf("state %+v, %d orders; want the loop ended at its 2nd cycle after 1 order", st, ordered)
	}
}

// holdStock stands for a replenishment cycle in progress: it holds the
// SKU's lock for a minute of virtual time.
var holdStock = flows.DefineWorkflow("inventory.test-hold", 1, func(c flows.Ctx, sku string) (struct{}, error) {
	if err := flows.Lock(c, skuLock(sku), lockTTL); err != nil {
		return struct{}{}, err
	}
	return struct{}{}, flows.Sleep(c, time.Minute)
}, flows.WithWorkflowQueue(Queue))

func TestAdjustWaitsForTheLock(t *testing.T) {
	tst := newTester(t)
	flowtest.OnActivity(tst, applyAdjustment, func(_ context.Context, _ flows.Key, a inventory.Adjustment) (int, struct{}, error) {
		return 10 + a.Delta, struct{}{}, nil
	})
	start := tst.Now()
	if _, err := holdStock.Start(t.Context(), tst.Engine(), "hold", "sku-a"); err != nil {
		t.Fatal(err)
	}
	tst.Settle() // the holder takes the lock

	n, err := flowtest.Run(tst, adjust, adjustID("r1"), inventory.Adjustment{RequestID: "r1", SKU: "sku-a", Delta: 5, Reason: "count"})
	if err != nil || n != 15 {
		t.Fatalf("adjust = %d, %v; want 15", n, err)
	}
	if waited := tst.Now().Sub(start); waited < time.Minute {
		t.Fatalf("the adjustment did not wait for the lock: done after %v", waited)
	}
	if !slices.Contains(tst.EventTypes(adjustID("r1")), "LockHeld") {
		t.Fatalf("the adjustment never found the lock held: %v", tst.EventTypes(adjustID("r1")))
	}
	if _, err := flowtest.Run(tst, holdStock, "hold", "sku-a"); err != nil {
		t.Fatal(err)
	}
}
