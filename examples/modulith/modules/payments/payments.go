// Package payments is the payments module: authorizations, captures and
// their undo (void, refund).
//
// It is pure domain: it knows nothing of the workflow engine. Its callers
// (the process layer) give every effect an idempotency key; the module
// stores the keys in its own table, so a repeated call finds the row it
// made, and an undo works by key even when the caller does not know
// whether the effect landed. The table payments.payments is private to the
// module.
package payments

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/schema"
)

// Business rejections: retrying the same request cannot change the answer
// (IsRejection). Any other error is transient.
var (
	// ErrDeclined is the error of an authorization the gateway refused.
	ErrDeclined = errors.New("payments: declined")
	// ErrInvalidAmount is the error of an authorization of a non-positive
	// amount.
	ErrInvalidAmount = errors.New("payments: invalid amount")
	// ErrVoided is the error of an authorization whose void ran first (the
	// caller gave up on it, and the void left a tombstone under its key).
	ErrVoided = errors.New("payments: authorization voided before it landed")
	// ErrNotCapturable is the error of a capture of a payment that is not
	// authorized any more (voided, refunded, captured under another key).
	ErrNotCapturable = errors.New("payments: payment cannot be captured")
)

// IsRejection reports whether err is a business rejection of the module:
// the same request will always fail the same way.
func IsRejection(err error) bool {
	return errors.Is(err, ErrDeclined) || errors.Is(err, ErrInvalidAmount) ||
		errors.Is(err, ErrVoided) || errors.Is(err, ErrNotCapturable)
}

// declineAbove is the stub gateway's limit, in cents.
const declineAbove = 1_000_000

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate applies the module's migrations (schema payments).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return schema.Migrate(ctx, pool, "payments", migrations)
}

// Payment is a payment as Find returns it.
type Payment struct {
	ID     string `json:"id"`
	Amount int64  `json:"amount"`
	State  string `json:"state"` // authorized, captured, refunded, voided
}

// Service is the module's API.
type Service struct {
	db  *pgxpool.Pool
	log *slog.Logger
}

// NewService returns the module's service on db.
func NewService(db *pgxpool.Pool, log *slog.Logger) (*Service, error) {
	if db == nil {
		return nil, errors.New("payments: NewService: db is required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Service{db: db, log: log}, nil
}

// Authorize holds amount on the customer's card for an order and returns
// the payment id. It is idempotent by key: a repeat finds the row it made.
// A real gateway gets key as its idempotency key.
func (s *Service) Authorize(ctx context.Context, key, orderID string, amount int64) (string, error) {
	if amount <= 0 {
		return "", fmt.Errorf("%w: %d", ErrInvalidAmount, amount)
	}
	if amount > declineAbove {
		return "", ErrDeclined
	}
	var id, state string
	err := s.db.QueryRow(ctx, `
		INSERT INTO payments.payments (auth_key, order_id, amount, state)
		VALUES ($1, $2, $3, 'authorized')
		ON CONFLICT (auth_key) DO UPDATE SET auth_key = EXCLUDED.auth_key -- no-op, returns the row
		RETURNING id, state`, key, orderID, amount).Scan(&id, &state)
	if err != nil {
		return "", fmt.Errorf("authorize %s: %w", orderID, err)
	}
	if state == "voided" {
		return "", ErrVoided
	}
	s.log.LogAttrs(ctx, slog.LevelInfo, "payments: authorized",
		slog.String("order_id", orderID), slog.String("payment_id", id), slog.Int64("amount", amount))
	return id, nil
}

// Void undoes the authorization made under authKey, and with it everything
// the authorization allowed. It works by key, so it also settles an
// authorization whose outcome the caller does not know: if no row exists
// yet it writes a voided tombstone, and an Authorize still in flight under
// that key then fails with ErrVoided instead of holding money.
//
// It also refunds a captured payment. Refund (the capture's undo) runs
// before it; but when the capture's outcome was unknown, Refund found
// nothing, and a capture still in flight can commit after it. Void closes
// that window: it refunds such a capture, and once the row is voided or
// refunded a later Capture fails with ErrNotCapturable. It reports whether
// it changed anything.
func (s *Service) Void(ctx context.Context, authKey string) (bool, error) {
	tag, err := s.db.Exec(ctx, `
		INSERT INTO payments.payments (auth_key, state) VALUES ($1, 'voided')
		ON CONFLICT (auth_key) DO UPDATE
		SET state = CASE payments.state WHEN 'captured' THEN 'refunded' ELSE 'voided' END, updated_at = now()
		WHERE payments.state IN ('authorized', 'captured')`, authKey)
	if err != nil {
		return false, fmt.Errorf("void %s: %w", authKey, err)
	}
	changed := tag.RowsAffected() > 0
	s.log.LogAttrs(ctx, slog.LevelInfo, "payments: voided",
		slog.String("auth_key", authKey), slog.Bool("changed", changed))
	return changed, nil
}

// Capture takes the authorized amount of a payment, once per key: a repeat
// under the same key succeeds without capturing again.
func (s *Service) Capture(ctx context.Context, key, paymentID string) error {
	// The stub's row lock stands in for the gateway's own idempotency and
	// ordering: a real gateway has no FOR UPDATE. Pass it the key as the
	// idempotency key, and rely on its rules (a capture of a voided or
	// refunded authorization fails) for what the lock does here.
	err := pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		var state string
		var captureKey *string
		if err := tx.QueryRow(ctx, `SELECT state, capture_key FROM payments.payments WHERE id = $1 FOR UPDATE`,
			paymentID).Scan(&state, &captureKey); err != nil {
			return fmt.Errorf("load: %w", err)
		}
		switch {
		case state == "captured" && captureKey != nil && *captureKey == key:
			return nil // this capture, repeated
		case state != "authorized":
			return fmt.Errorf("%w: it is %s", ErrNotCapturable, state)
		}
		_, err := tx.Exec(ctx, `
			UPDATE payments.payments SET state = 'captured', capture_key = $2, updated_at = now()
			WHERE id = $1`, paymentID, key)
		return err
	})
	if err != nil {
		return fmt.Errorf("capture %s: %w", paymentID, err)
	}
	s.log.LogAttrs(ctx, slog.LevelInfo, "payments: captured", slog.String("payment_id", paymentID))
	return nil
}

// Refund undoes the capture made under captureKey, by key: nothing to do
// if it never landed. A late capture landing after it is refunded by Void,
// which the caller runs after it. It reports whether it changed anything.
func (s *Service) Refund(ctx context.Context, captureKey string) (bool, error) {
	tag, err := s.db.Exec(ctx, `
		UPDATE payments.payments SET state = 'refunded', updated_at = now()
		WHERE capture_key = $1 AND state = 'captured'`, captureKey)
	if err != nil {
		return false, fmt.Errorf("refund %s: %w", captureKey, err)
	}
	changed := tag.RowsAffected() > 0
	s.log.LogAttrs(ctx, slog.LevelInfo, "payments: refunded",
		slog.String("capture_key", captureKey), slog.Bool("changed", changed))
	return changed, nil
}

// Find returns the latest payment of an order, pgx.ErrNoRows if none.
func (s *Service) Find(ctx context.Context, orderID string) (Payment, error) {
	var p Payment
	err := s.db.QueryRow(ctx, `
		SELECT id, amount, state FROM payments.payments
		WHERE order_id = $1 ORDER BY updated_at DESC LIMIT 1`, orderID).Scan(&p.ID, &p.Amount, &p.State)
	if err != nil {
		return Payment{}, fmt.Errorf("find payment of %s: %w", orderID, err)
	}
	return p, nil
}
