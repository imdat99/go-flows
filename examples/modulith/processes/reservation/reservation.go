// Package reservation is the process that reserves an order's stock in the
// inventory module: the workflow Reserve, which a saga starts as a child.
// When the saga compensates, the engine reopens the completed Reserve and
// releases its lines (the compensator of ReserveLine, by name).
package reservation

import (
	"context"
	"errors"
	"time"

	"github.com/imdat99/go-flows/examples/modulith/modules/inventory"
	"github.com/imdat99/go-flows/examples/modulith/processes/internal/adapt"
	"github.com/imdat99/go-flows/flows"
)

// Queue is the queue of the workflow and its activities: the processes
// that run the inventory module serve it.
const Queue = "inventory"

func init() {
	flows.RegisterErrorKind("inventory_out_of_stock", inventory.ErrOutOfStock)
	flows.RegisterErrorKind("inventory_unknown_sku", inventory.ErrUnknownSKU)
}

// Services are the module services the activities call.
type Services struct {
	Inventory *inventory.Service
}

var deps = adapt.NewRef[Services]("reservation")

// Configure sets the services. Call it before the engine starts, in every
// process.
func Configure(s Services) error {
	if s.Inventory == nil {
		return errors.New("reservation: Configure: Inventory is required")
	}
	deps.Set(s)
	return nil
}

// Line is one SKU and quantity.
type Line struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

// Request is the input of Reserve.
type Request struct {
	OrderID string `json:"order_id"`
	Lines   []Line `json:"lines"`
}

// Reservation is the output of Reserve.
type Reservation struct {
	IDs []int64 `json:"ids"`
}

// LineRequest is the input of ReserveLine.
type LineRequest struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
	Qty     int    `json:"qty"`
}

// InstanceID is the instance id of the reservation of an order: instance
// ids are global, so each process prefixes its own with its module.
func InstanceID(orderID string) string { return "inventory-reserve-" + orderID }

var (
	// ReserveLine reserves one line; its compensator releases it. Sagas do
	// not call it (they start Reserve): it is exported so that their tests
	// can mock it (flowtest.OnActivity) when Reserve runs as their child.
	ReserveLine = flows.DefineActivity("inventory.reserve-line", reserveLine, releaseLine,
		flows.WithQueue(Queue),
		flows.WithRetry(flows.Exponential(5, 200*time.Millisecond, 5*time.Second)),
		flows.WithCompensateRetry(flows.Exponential(20, time.Second, time.Minute)),
		flows.WithTimeout(10*time.Second))

	// Reserve reserves every line of an order, or none: when a line fails,
	// the lines already reserved are released (the workflow fails and
	// compensates). Start it as a child (InstanceID): if the parent fails
	// later, this execution is reopened and releases its lines.
	Reserve = flows.DefineWorkflow("inventory.reserve", 1, reserve, flows.WithWorkflowQueue(Queue))
)

func reserve(c flows.Ctx, r Request) (Reservation, error) {
	if len(r.Lines) == 0 {
		return Reservation{}, errors.New("reservation: nothing to reserve")
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

// reserveLine reserves once per activity key (the module's idempotency
// key): a retried or re-run attempt returns the first reservation.
func reserveLine(ctx context.Context, k flows.Key, r LineRequest) (int64, int64, error) {
	s, err := deps.Get()
	if err != nil {
		return 0, 0, err
	}
	id, err := s.Inventory.Reserve(ctx, string(k), r.OrderID, r.SKU, r.Qty)
	if err != nil {
		return 0, 0, adapt.Error(err, inventory.IsRejection)
	}
	return id, id, nil
}

// releaseLine undoes reserveLine. When the reservation is uncertain (its
// attempt timed out or was lost), the module settles it by key: it releases
// the reservation if it committed, and otherwise fences the key so a late
// attempt cannot reserve.
func releaseLine(ctx context.Context, k flows.Key, id int64) error {
	s, err := deps.Get()
	if err != nil {
		return err
	}
	if flows.CompensationUncertain(ctx) {
		return s.Inventory.SettleReservation(ctx, string(k))
	}
	return s.Inventory.Release(ctx, id)
}
