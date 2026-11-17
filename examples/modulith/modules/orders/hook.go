package orders

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/outbox"
	"github.com/imdat99/go-flows/flows"
)

// Topics of the events the module publishes when an order ends.
const (
	TopicCompleted = "orders.order_completed"
	TopicCanceled  = "orders.order_canceled"
	TopicFailed    = "orders.order_failed"
)

// Event is the payload of the module's events: the contract its consumers
// (notifications) depend on. Add fields, never change or remove one.
type Event struct {
	OrderID string `json:"order_id"`
	Email   string `json:"email"`
	State   string `json:"state"` // completed, canceled, failed
	Reason  string `json:"reason,omitempty"`
}

// Hook returns the module's RunHook. The composition root installs it on
// every engine (client-only ones included): an end is recorded by whichever
// engine runs the last task, or calls Terminate.
func Hook() flows.RunHook { return endHook{} }

type endHook struct{}

// RunEnded sets the order's final state and stages its event, in the
// transaction that records the fulfilment's end: the read model, the event
// and the workflow's status commit together or not at all.
func (endHook) RunEnded(ctx context.Context, tx pgx.Tx, r flows.RunInfo) error {
	if r.Workflow != fulfil.Name() || tx == nil { // tx is nil on flowtest's memory store
		return nil
	}
	var state, topic string
	switch r.Status {
	case flows.StatusNeedsAttention:
		return nil // a park, not an end: the order may still complete or compensate
	case flows.StatusCompleted:
		state, topic = "completed", TopicCompleted
	default: // compensated, failed, canceled, terminated
		state, topic = "failed", TopicFailed
		if r.Status == flows.StatusCanceled || slices.Contains(strings.Split(r.ErrorKinds, ","), kindCanceledByCustomer) {
			state, topic = "canceled", TopicCanceled
		}
	}
	orderID := strings.TrimPrefix(r.InstanceID, InstanceID(""))
	ev := Event{OrderID: orderID, State: state, Reason: r.Error}
	// Only a row this execution recorded and that is still processing: no
	// row means the record step never ran (nothing to tell the customer);
	// an ended row belongs to an earlier order with this id (the record
	// step refused this one).
	err := tx.QueryRow(ctx, `
		UPDATE orders.orders SET state = $2, reason = $3, updated_at = now()
		WHERE id = $1 AND state = 'processing' RETURNING email`, orderID, state, r.Error).Scan(&ev.Email)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("orders: end of %s: %w", orderID, err)
	}
	return outbox.Stage(ctx, tx, topic, orderID, ev)
}
