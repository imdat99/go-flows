// Package payments is the payments module: authorizations, captures and
// their undo (void, refund).
//
// Public API: the activities Authorize and Capture, which other modules'
// workflows call (their compensators, void and refund, run by name when such
// a workflow compensates), the query Find, ErrDeclined, Queue, Configure and
// Migrate. The table payments.payments is private to the module.
//
// The module has no workflow of its own: a payment is a step of somebody
// else's process. A module whose steps need waiting or coordination of
// their own exposes a workflow instead (inventory, shipping), which callers
// start as a child.
package payments

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/deps"
	"github.com/imdat99/go-flows/examples/modulith/internal/platform/schema"
	"github.com/imdat99/go-flows/flows"
)

// Queue is the queue of the module's activities: the processes that run
// the payments module serve it.
const Queue = "payments"

// ErrDeclined is the error of an authorization the gateway refused.
var ErrDeclined = errors.New("payments: declined")

// errFenced: an authorization whose void ran first (its attempt outlived its
// lease, the workflow compensated meanwhile).
var errFenced = errors.New("payments: authorization voided before it landed")

func init() {
	// Kinds keep errors.Is(err, ErrDeclined) working on errors read back
	// from a history, in any process.
	flows.RegisterErrorKind("payments_declined", ErrDeclined)
}

// declineAbove is the stub gateway's limit, in cents.
const declineAbove = 1_000_000

// Config holds the module's dependencies.
type Config struct {
	DB     *pgxpool.Pool
	Logger *slog.Logger
}

var cfg = deps.New[Config]("payments")

// Configure sets the module's dependencies. Call it before the engine
// starts, in every process that runs the module or calls its functions
// (Find).
func Configure(c Config) error {
	if c.DB == nil {
		return errors.New("payments: Configure: DB is required")
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	cfg.Set(c)
	return nil
}

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate applies the module's migrations (schema payments).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return schema.Migrate(ctx, pool, "payments", migrations)
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

// Payment is a payment as Find returns it.
type Payment struct {
	ID     string `json:"id"`
	Amount int64  `json:"amount"`
	State  string `json:"state"` // authorized, captured, refunded, voided
}

// retry is the policy of the gateway calls: a few quick retries.
var retry = flows.WithRetry(flows.Exponential(5, 200*time.Millisecond, 5*time.Second))

// compensateRetry: an undo must eventually succeed, so it retries for longer;
// past that the order is parked for an operator (RetryCompensation).
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

// authorize is idempotent by key: a retry, or a run again after a crash,
// finds the row it made. A real gateway gets k as its idempotency key.
func authorize(ctx context.Context, k flows.Key, r AuthorizeRequest) (Authorization, string, error) {
	c, err := cfg.Get()
	if err != nil {
		return Authorization{}, "", err
	}
	if r.Amount <= 0 {
		return Authorization{}, "", flows.Permanent(fmt.Errorf("payments: invalid amount %d", r.Amount))
	}
	if r.Amount > declineAbove {
		return Authorization{}, "", flows.Permanent(ErrDeclined)
	}
	var id, state string
	err = c.DB.QueryRow(ctx, `
		INSERT INTO payments.payments (auth_key, order_id, amount, state)
		VALUES ($1, $2, $3, 'authorized')
		ON CONFLICT (auth_key) DO UPDATE SET auth_key = EXCLUDED.auth_key -- no-op, returns the row
		RETURNING id, state`, string(k), r.OrderID, r.Amount).Scan(&id, &state)
	if err != nil {
		return Authorization{}, "", fmt.Errorf("authorize %s: %w", r.OrderID, err)
	}
	if state == "voided" {
		return Authorization{}, "", flows.Permanent(errFenced)
	}
	c.Logger.LogAttrs(ctx, slog.LevelInfo, "payments: authorized",
		slog.String("order_id", r.OrderID), slog.String("payment_id", id), slog.Int64("amount", r.Amount))
	return Authorization{PaymentID: id}, id, nil
}

// void undoes Authorize, and with it everything the authorization allowed.
// It works by key, so it needs no compensation data and also handles an
// uncertain authorization (one that timed out, maybe after it landed): if
// no row exists yet it writes a voided tombstone, and a late attempt then
// fails with errFenced instead of holding money.
//
// It also refunds a captured payment. Compensators run latest first, so
// refund (the capture's undo) ran before; but when the capture was
// uncertain, refund found nothing, and a capture attempt still running
// elsewhere (it outlived its lease) can commit after refund. void closes
// that window: it refunds such a capture, and once the row is voided or
// refunded a later capture attempt finds it not authorized and fails.
func void(ctx context.Context, k flows.Key, paymentID string) error {
	c, err := cfg.Get()
	if err != nil {
		return err
	}
	tag, err := c.DB.Exec(ctx, `
		INSERT INTO payments.payments (auth_key, state) VALUES ($1, 'voided')
		ON CONFLICT (auth_key) DO UPDATE
		SET state = CASE payments.state WHEN 'captured' THEN 'refunded' ELSE 'voided' END, updated_at = now()
		WHERE payments.state IN ('authorized', 'captured')`, string(k))
	if err != nil {
		return fmt.Errorf("void %s: %w", paymentID, err)
	}
	c.Logger.LogAttrs(ctx, slog.LevelInfo, "payments: voided",
		slog.String("payment_id", paymentID), slog.Bool("changed", tag.RowsAffected() > 0),
		slog.Bool("uncertain", flows.CompensationUncertain(ctx)))
	return nil
}

func capture(ctx context.Context, k flows.Key, paymentID string) (string, string, error) {
	c, err := cfg.Get()
	if err != nil {
		return "", "", err
	}
	// The stub's row lock stands in for the gateway's own idempotency and
	// ordering: a real gateway has no FOR UPDATE. Pass it the Key as the
	// idempotency key, and rely on its rules (a capture of a voided or
	// refunded authorization fails) for what the lock does here.
	err = pgx.BeginFunc(ctx, c.DB, func(tx pgx.Tx) error {
		var state string
		var key *string
		if err := tx.QueryRow(ctx, `SELECT state, capture_key FROM payments.payments WHERE id = $1 FOR UPDATE`,
			paymentID).Scan(&state, &key); err != nil {
			return fmt.Errorf("load %s: %w", paymentID, err)
		}
		switch {
		case state == "captured" && key != nil && *key == string(k):
			return nil // this capture, retried
		case state != "authorized":
			return flows.Permanent(fmt.Errorf("payments: cannot capture %s: it is %s", paymentID, state))
		}
		_, err := tx.Exec(ctx, `
			UPDATE payments.payments SET state = 'captured', capture_key = $2, updated_at = now()
			WHERE id = $1`, paymentID, string(k))
		return err
	})
	if err != nil {
		return "", "", fmt.Errorf("capture %s: %w", paymentID, err)
	}
	c.Logger.LogAttrs(ctx, slog.LevelInfo, "payments: captured", slog.String("payment_id", paymentID))
	return paymentID, paymentID, nil
}

// refund undoes Capture, by key: nothing to do if the capture never landed
// (uncertain). A late capture landing after it is refunded by void, the
// authorization's compensator, which always runs after it (latest first);
// after void, a capture finds the payment voided or refunded and fails.
func refund(ctx context.Context, k flows.Key, paymentID string) error {
	c, err := cfg.Get()
	if err != nil {
		return err
	}
	tag, err := c.DB.Exec(ctx, `
		UPDATE payments.payments SET state = 'refunded', updated_at = now()
		WHERE capture_key = $1 AND state = 'captured'`, string(k))
	if err != nil {
		return fmt.Errorf("refund %s: %w", paymentID, err)
	}
	c.Logger.LogAttrs(ctx, slog.LevelInfo, "payments: refunded",
		slog.String("payment_id", paymentID), slog.Bool("changed", tag.RowsAffected() > 0))
	return nil
}

// Find returns the latest payment of an order, pgx.ErrNoRows if none.
func Find(ctx context.Context, orderID string) (Payment, error) {
	c, err := cfg.Get()
	if err != nil {
		return Payment{}, err
	}
	var p Payment
	err = c.DB.QueryRow(ctx, `
		SELECT id, amount, state FROM payments.payments
		WHERE order_id = $1 ORDER BY updated_at DESC LIMIT 1`, orderID).Scan(&p.ID, &p.Amount, &p.State)
	if err != nil {
		return Payment{}, fmt.Errorf("find payment of %s: %w", orderID, err)
	}
	return p, nil
}
