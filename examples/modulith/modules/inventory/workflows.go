package inventory

import (
	"errors"
	"time"

	"github.com/imdat99/go-flows/flows"
)

// Reservation is the output of Reserve.
type Reservation struct {
	IDs []int64 `json:"ids"`
}

// ReplenishState is the input of each execution of a replenishment loop:
// the loop's settings and what it carries from one cycle to the next.
type ReplenishState struct {
	SKU          string        `json:"sku"`
	Every        time.Duration `json:"every"`
	ReorderPoint int           `json:"reorder_point"`
	ReorderQty   int           `json:"reorder_qty"`
	Cycles       int           `json:"cycles"`  // cycles run so far
	Ordered      int           `json:"ordered"` // purchase orders placed so far
	LastError    string        `json:"last_error,omitempty"`
}

// stockLockTTL bounds how long a holder that stopped running (its process
// died) keeps the others out. The cycle and an adjustment take a few
// activities, far below it.
const stockLockTTL = time.Minute

// lockWait makes a cycle or an adjustment wait for the other instead of
// failing (waiters are served in order).
var lockWait = flows.LockWait(flows.Exponential(50, 100*time.Millisecond, 5*time.Second))

// stockLock is the business lock of one SKU's stock decisions.
func stockLock(sku string) string { return "inventory:sku:" + sku }

var (
	// Reserve reserves every line of an order, or none: when a line fails,
	// the lines already reserved are released (the workflow fails and
	// compensates). Start it as a child (ReserveID): if the parent fails
	// later, this execution is reopened and releases its lines.
	Reserve = flows.DefineWorkflow("inventory.reserve", 1, reserve, flows.WithWorkflowQueue(Queue))

	// replenish is the replenishment loop of one SKU (StartReplenishment).
	replenish = flows.DefineWorkflow("inventory.replenish", 1, replenishCycle, flows.WithWorkflowQueue(Queue))

	// adjust applies a manual adjustment (Adjust).
	adjust = flows.DefineWorkflow("inventory.adjust", 1, adjustStock, flows.WithWorkflowQueue(Queue))
)

func reserve(c flows.Ctx, r ReserveRequest) (Reservation, error) {
	if len(r.Lines) == 0 {
		return Reservation{}, errors.New("inventory: nothing to reserve")
	}
	// All lines in one checkpoint, run concurrently.
	futures := make([]*flows.Future[int64], len(r.Lines))
	for i, l := range r.Lines {
		futures[i] = ReserveLine.Start(c, LineRequest{OrderID: r.OrderID, SKU: l.SKU, Qty: l.Qty})
	}
	// Await every one, even after a failure: the compensation must know the
	// outcome of each (and flowtest fails a test that leaves one pending).
	out := Reservation{IDs: make([]int64, 0, len(futures))}
	var firstErr error
	for _, f := range futures {
		id, err := f.Get(c)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out.IDs = append(out.IDs, id)
	}
	if firstErr != nil {
		return Reservation{}, firstErr // releases the lines reserved
	}
	return out, nil
}

// replenishCycle is one cycle of the loop, one execution each: it waits
// for the period (or a stop signal), checks the level and orders more if
// it is low, then continues as new. Each execution's history stays a few
// events long however long the loop lives, and the lock is released at
// every continue-as-new.
func replenishCycle(c flows.Ctx, st ReplenishState) (ReplenishState, error) {
	stop := flows.Signals[struct{}](c, SignalStopReplenish)
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
	if err := checkAndOrder(c, &st); err != nil {
		st.LastError = err.Error()
	}
	return st, flows.ContinueAsNew(c, st)
}

// checkAndOrder reads the level and decides on a purchase order under the
// SKU's lock: an adjustment (adjustStock) cannot land between the read and
// the order, so the decision is never made on a stale level.
func checkAndOrder(c flows.Ctx, st *ReplenishState) error {
	if err := flows.Lock(c, stockLock(st.SKU), stockLockTTL, lockWait); err != nil {
		return err
	}
	n, err := checkLevel.Run(c, st.SKU)
	if err != nil || n >= st.ReorderPoint {
		return err
	}
	if _, err := placePurchaseOrder.Run(c, PurchaseOrder{SKU: st.SKU, Qty: st.ReorderQty}); err != nil {
		return err
	}
	st.Ordered++
	return nil
}

func adjustStock(c flows.Ctx, a Adjustment) (int, error) {
	if err := flows.Lock(c, stockLock(a.SKU), stockLockTTL, lockWait); err != nil {
		return 0, err
	}
	return applyAdjustment.Run(c, a)
}
