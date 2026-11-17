// Package payment adapts the payments module to the engine: the activities
// Authorize and Capture, which sagas call as steps, and their undo (void,
// refund), which the engine runs by name when such a saga compensates.
//
// There is no workflow: a payment is a step of somebody else's process. A
// step that needs waiting or coordination of its own is a workflow,
// started as a child (reservation, shipment).
package payment

import (
	"context"
	"errors"
	"time"

	"github.com/imdat99/go-flows/examples/modulith/modules/payments"
	"github.com/imdat99/go-flows/examples/modulith/processes/internal/adapt"
	"github.com/imdat99/go-flows/flows"
)

// Queue is the queue of the activities: the processes that run the
// payments module serve it.
const Queue = "payments"

func init() {
	// Kinds keep errors.Is(err, payments.ErrDeclined) working on errors
	// read back from a history, in any process.
	flows.RegisterErrorKind("payments_declined", payments.ErrDeclined)
}

// Services are the module services the activities call.
type Services struct {
	Payments *payments.Service
}

var deps = adapt.NewRef[Services]("payment")

// Configure sets the services. Call it before the engine starts, in every
// process.
func Configure(s Services) error {
	if s.Payments == nil {
		return errors.New("payment: Configure: Payments is required")
	}
	deps.Set(s)
	return nil
}

// AuthorizeRequest is the input of Authorize.
type AuthorizeRequest struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"` // cents
}

// Authorization is the output of Authorize.
type Authorization struct {
	PaymentID string `json:"payment_id"`
}

// retry is the policy of the gateway calls: a few quick retries.
var retry = flows.WithRetry(flows.Exponential(5, 200*time.Millisecond, 5*time.Second))

// compensateRetry: an undo must eventually succeed, so it retries for longer;
// past that the saga is parked for an operator (RetryCompensation).
var compensateRetry = flows.WithCompensateRetry(flows.Exponential(20, time.Second, time.Minute))

var (
	// Authorize holds the amount on the customer's card. If the calling
	// workflow fails later, the hold is voided.
	Authorize = flows.DefineActivity("payments.authorize", authorize, void,
		flows.WithQueue(Queue), retry, compensateRetry, flows.WithTimeout(10*time.Second))

	// Capture takes the authorized amount (input: the payment id). If the
	// calling workflow fails later, it is refunded.
	Capture = flows.DefineActivity("payments.capture", capture, refund,
		flows.WithQueue(Queue), retry, compensateRetry, flows.WithTimeout(10*time.Second))
)

// The activity's key is the module's idempotency key: a retry, or a run
// again after a crash, finds the row the first attempt made.
func authorize(ctx context.Context, k flows.Key, r AuthorizeRequest) (Authorization, string, error) {
	s, err := deps.Get()
	if err != nil {
		return Authorization{}, "", err
	}
	id, err := s.Payments.Authorize(ctx, string(k), r.OrderID, r.Amount)
	if err != nil {
		return Authorization{}, "", adapt.Error(err, payments.IsRejection)
	}
	return Authorization{PaymentID: id}, id, nil
}

// void works by key: it needs no compensation data, and also settles an
// uncertain authorization (flows.CompensationUncertain), see
// payments.Service.Void. It runs after refund (latest first), and refunds a
// capture that landed after refund looked.
func void(ctx context.Context, k flows.Key, _ string) error {
	s, err := deps.Get()
	if err != nil {
		return err
	}
	_, err = s.Payments.Void(ctx, string(k))
	return err
}

func capture(ctx context.Context, k flows.Key, paymentID string) (string, string, error) {
	s, err := deps.Get()
	if err != nil {
		return "", "", err
	}
	if err := s.Payments.Capture(ctx, string(k), paymentID); err != nil {
		return "", "", adapt.Error(err, payments.IsRejection)
	}
	return paymentID, paymentID, nil
}

// refund works by key: nothing to do if the capture never landed.
func refund(ctx context.Context, k flows.Key, _ string) error {
	s, err := deps.Get()
	if err != nil {
		return err
	}
	_, err = s.Payments.Refund(ctx, string(k))
	return err
}
