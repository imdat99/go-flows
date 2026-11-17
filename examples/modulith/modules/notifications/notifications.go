// Package notifications is the notifications module: it tells customers
// how their orders ended.
//
// It is driven by events, not called: Subscribe registers its consumers on
// the bus, where the outbox relay publishes the orders module's events.
// Each event starts a small workflow whose instance id is the event id, so
// an event delivered twice (the relay delivers at least once) sends one
// message, and a failing mail server is retried by the engine.
//
// Public API: Subscribe, Sent, Queue, Configure, Migrate. The schema
// notifications is private to the module.
package notifications

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/deps"
	"github.com/imdat99/go-flows/examples/modulith/internal/platform/outbox"
	"github.com/imdat99/go-flows/examples/modulith/internal/platform/schema"
	"github.com/imdat99/go-flows/examples/modulith/modules/orders"
	"github.com/imdat99/go-flows/flows"
)

// Queue is the queue of the module's workflows and activities.
const Queue = "notifications"

// Config holds the module's dependencies.
type Config struct {
	DB     *pgxpool.Pool
	Logger *slog.Logger
}

var cfg = deps.New[Config]("notifications")

// Configure sets the module's dependencies. Call it before the engine
// starts, in every process that runs the module.
func Configure(c Config) { cfg.Set(c) }

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate applies the module's migrations (schema notifications).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return schema.Migrate(ctx, pool, "notifications", migrations)
}

// Subscriber is the part of a bus consumers register on.
type Subscriber interface {
	Subscribe(topic string, h outbox.Handler)
}

// Subscribe registers the module's consumers of the orders events. The
// consumers only start workflows: e can be any engine on the database.
func Subscribe(bus Subscriber, e *flows.Engine) {
	h := func(ctx context.Context, m outbox.Message) error {
		var ev orders.Event
		if err := json.Unmarshal(m.Payload, &ev); err != nil {
			// A malformed event will never decode: log it and move on
			// rather than block the key forever.
			if c, cerr := cfg.Get(); cerr == nil {
				c.Logger.LogAttrs(ctx, slog.LevelError, "notifications: bad event",
					slog.String("event_id", m.EventID), slog.Any("error", err))
			}
			return nil
		}
		_, err := send.Start(ctx, e, "notifications-"+m.EventID, Message{
			EventID: m.EventID, OrderID: ev.OrderID, Email: ev.Email, Template: "order_" + ev.State,
		})
		return err
	}
	for _, topic := range []string{orders.TopicCompleted, orders.TopicCanceled, orders.TopicFailed} {
		bus.Subscribe(topic, h)
	}
}

// Message is the input of the send workflow.
type Message struct {
	EventID  string `json:"event_id"`
	OrderID  string `json:"order_id"`
	Email    string `json:"email"`
	Template string `json:"template"`
}

var (
	deliver = flows.DefineActivity("notifications.deliver", deliverFn, nil,
		flows.WithQueue(Queue),
		// The mail server may be down for a while: retry for about an hour.
		flows.WithRetry(flows.Exponential(15, time.Second, 10*time.Minute)),
		flows.WithTimeout(30*time.Second))

	send = flows.DefineWorkflow("notifications.send", 1, func(c flows.Ctx, m Message) (struct{}, error) {
		return deliver.Run(c, m)
	}, flows.WithWorkflowQueue(Queue))
)

// deliverFn "sends" the message: the stub mail server is the log. The
// delivery row, keyed by the event id, stops a second send of the same
// event; a real mail API would get the event id as its idempotency key.
func deliverFn(ctx context.Context, _ flows.Key, m Message) (struct{}, struct{}, error) {
	c, err := cfg.Get()
	if err != nil {
		return struct{}{}, struct{}{}, err
	}
	tag, err := c.DB.Exec(ctx, `
		INSERT INTO notifications.deliveries (event_id, order_id, email, template)
		VALUES ($1, $2, $3, $4) ON CONFLICT (event_id) DO NOTHING`, m.EventID, m.OrderID, m.Email, m.Template)
	if err != nil {
		return struct{}{}, struct{}{}, fmt.Errorf("deliver %s: %w", m.EventID, err)
	}
	if tag.RowsAffected() > 0 {
		c.Logger.LogAttrs(ctx, slog.LevelInfo, "notifications: email sent",
			slog.String("to", m.Email), slog.String("template", m.Template), slog.String("order_id", m.OrderID))
	}
	return struct{}{}, struct{}{}, nil
}

// Sent lists the templates sent for an order, oldest first.
func Sent(ctx context.Context, orderID string) ([]string, error) {
	c, err := cfg.Get()
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.Query(ctx, `
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
