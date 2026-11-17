// Package replenishment is the process that keeps the inventory module's
// stock up: a long-running loop per SKU (ContinueAsNew) that orders more
// when the level is low, and manual adjustments, serialized with the loop
// by a lock on the SKU (flows.Lock).
package replenishment

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/imdat99/go-flows/examples/modulith/modules/inventory"
	"github.com/imdat99/go-flows/examples/modulith/processes/internal/adapt"
	"github.com/imdat99/go-flows/flows"
)

// Queue is the queue of the workflows and activities: the processes that
// run the inventory module serve it.
const Queue = "inventory"

// SignalStop is the signal that ends a replenishment loop.
const SignalStop = "inventory.replenish.stop"

func init() {
	flows.RegisterErrorKind("inventory_request_conflict", inventory.ErrRequestConflict)
}

// Services are the module services the activities call, and the loop's
// settings.
type Services struct {
	Inventory *inventory.Service
	// Every is the period of the loops (default 1m). It is passed to a loop
	// when it starts: changing it affects new loops only (workflow code
	// must not read configuration, see README).
	Every time.Duration
	// ReorderPoint and ReorderQty: below ReorderPoint units, a loop orders
	// ReorderQty more (defaults 5 and 20).
	ReorderPoint, ReorderQty int
}

var deps = adapt.NewRef[Services]("replenishment")

// Configure sets the services. Call it before the engine starts, in every
// process.
func Configure(s Services) error {
	if s.Inventory == nil {
		return errors.New("replenishment: Configure: Inventory is required")
	}
	if s.Every <= 0 {
		s.Every = time.Minute
	}
	if s.ReorderPoint <= 0 {
		s.ReorderPoint = 5
	}
	if s.ReorderQty <= 0 {
		s.ReorderQty = 20
	}
	deps.Set(s)
	return nil
}

// State is the input of each execution of a loop: its settings and what it
// carries from one cycle to the next.
type State struct {
	SKU          string        `json:"sku"`
	Every        time.Duration `json:"every"`
	ReorderPoint int           `json:"reorder_point"`
	ReorderQty   int           `json:"reorder_qty"`
	Cycles       int           `json:"cycles"`  // cycles run so far
	Ordered      int           `json:"ordered"` // purchase orders placed so far
	LastError    string        `json:"last_error,omitempty"`
}

// PurchaseOrder is the input of placePurchaseOrder.
type PurchaseOrder struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

func loopID(sku string) string { return "inventory-replenish-" + sku }

func adjustID(requestID string) string { return "inventory-adjust-" + requestID }

// lockTTL bounds how long a holder that stopped running (its process died)
// keeps the others out. A cycle and an adjustment take a few activities,
// far below it.
const lockTTL = time.Minute

// lockWait makes a cycle or an adjustment wait for the other instead of
// failing (waiters are served in order).
var lockWait = flows.LockWait(flows.Exponential(50, 100*time.Millisecond, 5*time.Second))

// skuLock is the business lock of one SKU's stock decisions.
func skuLock(sku string) string { return "inventory:sku:" + sku }

var activityOpts = []flows.ActivityOption{
	flows.WithQueue(Queue),
	flows.WithRetry(flows.Exponential(5, 200*time.Millisecond, 5*time.Second)),
	flows.WithTimeout(10 * time.Second),
}

var (
	checkLevel         = flows.DefineActivity("inventory.check-level", checkLevelFn, nil, activityOpts...)
	placePurchaseOrder = flows.DefineActivity("inventory.place-purchase-order", placePurchaseOrderFn, nil, activityOpts...)
	applyAdjustment    = flows.DefineActivity("inventory.apply-adjustment", applyAdjustmentFn, nil, activityOpts...)

	// loop is the replenishment loop of one SKU (Start).
	loop = flows.DefineWorkflow("inventory.replenish", 1, cycle, flows.WithWorkflowQueue(Queue))

	// adjust applies a manual adjustment (Adjust).
	adjust = flows.DefineWorkflow("inventory.adjust", 1, adjustStock, flows.WithWorkflowQueue(Queue))
)

// cycle is one cycle of the loop, one execution each: it waits for the
// period (or a stop signal), checks the level and orders more if it is
// low, then continues as new. Each execution's history stays a few events
// long however long the loop lives, and the lock is released at every
// continue-as-new.
func cycle(c flows.Ctx, st State) (State, error) {
	stop := flows.Signals[struct{}](c, SignalStop)
	tc, cancelTimer := flows.WithCancel(c)
	timer := flows.NewTimer(tc, st.Every)
	stopped := false
	flows.Select(c,
		flows.OnSignal(stop, func(flows.Ctx, struct{}, error) { stopped = true }),
		flows.OnFuture(timer, func(flows.Ctx, struct{}, error) {}),
	)
	cancelTimer()
	if stopped {
		return st, nil // the loop ends
	}

	st.Cycles++
	st.LastError = ""
	// Forward recovery: a failed cycle is recorded and the loop goes on;
	// failing the workflow would end the loop for good.
	discontinued, err := checkAndOrder(c, &st)
	if err != nil {
		st.LastError = err.Error()
	}
	if discontinued {
		// Discontinued after this loop was started (Stop's signal can miss
		// a loop started concurrently): end it here.
		return st, nil
	}
	return st, flows.ContinueAsNew(c, st)
}

// checkAndOrder reads the level and decides on a purchase order under the
// SKU's lock: an adjustment cannot land between the read and the order, so
// the decision is never made on a stale level. It reports a discontinued
// SKU, which ends the loop.
func checkAndOrder(c flows.Ctx, st *State) (discontinued bool, err error) {
	if err := flows.Lock(c, skuLock(st.SKU), lockTTL, lockWait); err != nil {
		return false, err
	}
	l, err := checkLevel.Run(c, st.SKU)
	if err != nil || l.Discontinued || l.Available >= st.ReorderPoint {
		return l.Discontinued, err
	}
	if _, err := placePurchaseOrder.Run(c, PurchaseOrder{SKU: st.SKU, Qty: st.ReorderQty}); err != nil {
		return false, err
	}
	st.Ordered++
	return false, nil
}

func adjustStock(c flows.Ctx, a inventory.Adjustment) (int, error) {
	if err := flows.Lock(c, skuLock(a.SKU), lockTTL, lockWait); err != nil {
		return 0, err
	}
	return applyAdjustment.Run(c, a)
}

// checkLevelFn reads a level and the discontinued flag; reads need no
// idempotency.
func checkLevelFn(ctx context.Context, _ flows.Key, sku string) (inventory.Stock, struct{}, error) {
	s, err := deps.Get()
	if err != nil {
		return inventory.Stock{}, struct{}{}, err
	}
	st, err := s.Inventory.Stock(ctx, sku)
	return st, struct{}{}, adapt.Error(err, inventory.IsRejection)
}

func placePurchaseOrderFn(ctx context.Context, k flows.Key, po PurchaseOrder) (int64, struct{}, error) {
	s, err := deps.Get()
	if err != nil {
		return 0, struct{}{}, err
	}
	id, err := s.Inventory.PlacePurchaseOrder(ctx, string(k), po.SKU, po.Qty)
	return id, struct{}{}, adapt.Error(err, inventory.IsRejection)
}

func applyAdjustmentFn(ctx context.Context, k flows.Key, a inventory.Adjustment) (int, struct{}, error) {
	s, err := deps.Get()
	if err != nil {
		return 0, struct{}{}, err
	}
	n, err := s.Inventory.ApplyAdjustment(ctx, string(k), a)
	return n, struct{}{}, adapt.Error(err, inventory.IsRejection)
}

// Start starts the loop of every stocked SKU that is not discontinued and
// has none running. Run it at startup in a process that runs the inventory
// module: the instance id makes it idempotent across processes and
// restarts.
func Start(ctx context.Context, e *flows.Engine) error {
	s, err := deps.Get()
	if err != nil {
		return err
	}
	skus, err := s.Inventory.Replenishable(ctx)
	if err != nil {
		return err
	}
	for _, sku := range skus {
		if err := startLoop(ctx, e, s, sku); err != nil {
			return err
		}
	}
	return nil
}

// startLoop starts the loop of sku unless the module says it is
// discontinued: the flag lives in the module's table, because the engine's
// memory of an ended loop (Start finding the instance id used) lasts only
// until retention purges it.
func startLoop(ctx context.Context, e *flows.Engine, s *Services, sku string) error {
	st, err := s.Inventory.Stock(ctx, sku)
	if err != nil {
		return fmt.Errorf("start replenishment of %s: %w", sku, err)
	}
	if st.Discontinued {
		return nil
	}
	_, err = loop.Start(ctx, e, loopID(sku), State{
		SKU: sku, Every: s.Every, ReorderPoint: s.ReorderPoint, ReorderQty: s.ReorderQty,
	})
	if err != nil {
		return fmt.Errorf("start replenishment of %s: %w", sku, err)
	}
	return nil
}

// Stop discontinues sku in the module, then ends its loop. The loop also
// reads the flag every cycle, so a loop started concurrently (after
// startLoop read the flag, before it was set) ends by itself.
func Stop(ctx context.Context, e *flows.Engine, sku string) error {
	s, err := deps.Get()
	if err != nil {
		return err
	}
	if err := s.Inventory.Discontinue(ctx, sku); err != nil {
		return err
	}
	err = e.Signal(ctx, loopID(sku), SignalStop, struct{}{})
	if errors.Is(err, flows.ErrNotActive) || errors.Is(err, flows.ErrNotFound) {
		return nil // no loop running
	}
	return err
}

// Adjust changes the stock of sku by delta (creating the SKU if needed) and
// returns the new level. requestID is the caller's idempotency key: the
// same request applies once (a repeat returns the current level), and the
// same id with other parameters fails with inventory.ErrRequestConflict.
// The module, not the engine, remembers request ids: the engine forgets
// them when retention purges the execution. It waits for the adjustment,
// which queues behind a replenishment cycle of the same SKU.
func Adjust(ctx context.Context, e *flows.Engine, requestID, sku string, delta int, reason string) (int, error) {
	s, err := deps.Get()
	if err != nil {
		return 0, err
	}
	a := inventory.Adjustment{RequestID: requestID, SKU: sku, Delta: delta, Reason: reason}
	switch applied, err := s.Inventory.Applied(ctx, a); {
	case err != nil:
		return 0, err
	case applied:
		return s.Inventory.Level(ctx, sku)
	}
	n, err := adjust.Run(ctx, e, adjustID(requestID), a)
	if err != nil {
		return 0, err
	}
	// Two different requests racing with one id: Run returned the first
	// one's result to both. Tell the second.
	if _, err := s.Inventory.Applied(ctx, a); err != nil {
		return 0, err
	}
	// A new SKU gets its loop (Start is idempotent).
	if err := startLoop(ctx, e, s, sku); err != nil {
		return n, err
	}
	return n, nil
}
