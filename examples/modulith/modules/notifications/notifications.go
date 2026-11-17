// Package notifications is the notifications module: it sends messages to
// customers.
//
// It is pure domain: it knows nothing of the workflow engine nor of the
// modules whose events lead to a message (the process layer translates
// them into a Message). The schema notifications is private to the module.
package notifications

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

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate applies the module's migrations (schema notifications).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return schema.Migrate(ctx, pool, "notifications", migrations)
}

// Message is a message to a customer. EventID is the idempotency key: the
// id of the event that caused it.
type Message struct {
	EventID  string `json:"event_id"`
	OrderID  string `json:"order_id"`
	Email    string `json:"email"`
	Template string `json:"template"`
}

// Service is the module's API.
type Service struct {
	db  *pgxpool.Pool
	log *slog.Logger
}

// NewService returns the module's service on db.
func NewService(db *pgxpool.Pool, log *slog.Logger) (*Service, error) {
	if db == nil {
		return nil, errors.New("notifications: NewService: db is required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Service{db: db, log: log}, nil
}

// Deliver sends m, once per EventID: the delivery row, keyed by the event
// id, stops a second send. The stub mail server is the log; a real mail
// API would get the event id as its idempotency key.
func (s *Service) Deliver(ctx context.Context, m Message) error {
	tag, err := s.db.Exec(ctx, `
		INSERT INTO notifications.deliveries (event_id, order_id, email, template)
		VALUES ($1, $2, $3, $4) ON CONFLICT (event_id) DO NOTHING`, m.EventID, m.OrderID, m.Email, m.Template)
	if err != nil {
		return fmt.Errorf("deliver %s: %w", m.EventID, err)
	}
	if tag.RowsAffected() > 0 {
		s.log.LogAttrs(ctx, slog.LevelInfo, "notifications: email sent",
			slog.String("to", m.Email), slog.String("template", m.Template), slog.String("order_id", m.OrderID))
	}
	return nil
}

// Sent lists the templates sent for an order, oldest first.
func (s *Service) Sent(ctx context.Context, orderID string) ([]string, error) {
	rows, err := s.db.Query(ctx, `
		SELECT template FROM notifications.deliveries WHERE order_id = $1 ORDER BY created_at, event_id`, orderID)
	if err != nil {
		return nil, fmt.Errorf("sent to %s: %w", orderID, err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("sent to %s: %w", orderID, err)
	}
	return out, nil
}
