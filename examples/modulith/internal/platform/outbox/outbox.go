// Package outbox publishes module events reliably: the transactional outbox
// pattern, shared by all modules.
//
// A module stages an event in the transaction that changes its state (Stage);
// the event commits or rolls back with that change. A Relay then copies
// staged events to the Bus, at least once and, per key, in staging order.
// Consumers deduplicate on Message.EventID.
//
// In this example the only producer is the orders module's RunHook, which
// stages an orders.order_* event in the transaction that records
// the end of the fulfilment workflow: no event for an order that did not
// end, and no lost event for one that did. See examples/outbox for the
// variant where activities stage events that the hook releases or drops.
package outbox

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/schema"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate creates the outbox schema and table.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return schema.Migrate(ctx, pool, "outbox", migrations)
}

// Message is an event as consumers receive it.
type Message struct {
	EventID string          `json:"event_id"` // stable across redeliveries
	Topic   string          `json:"topic"`
	Key     string          `json:"key"`
	Payload json.RawMessage `json:"payload"`
}

// Stage records an event in tx. It is published once tx commits.
func Stage(ctx context.Context, tx pgx.Tx, topic, key string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s: %w", topic, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox.events (topic, key, payload) VALUES ($1, $2, $3)`, topic, key, b); err != nil {
		return fmt.Errorf("stage %s: %w", topic, err)
	}
	return nil
}

// Bus is the message broker (Kafka, NATS, SNS...).
type Bus interface {
	Publish(ctx context.Context, m Message) error
}

// Handler consumes a message. An error makes the relay publish it again
// later, so handlers must be idempotent (dedupe on EventID).
type Handler func(ctx context.Context, m Message) error

// LocalBus is an in-process Bus: Publish calls the topic's handlers
// synchronously. It stands in for a broker, so the relay and the consumers
// must run in the same process.
type LocalBus struct {
	mu   sync.RWMutex
	subs map[string][]Handler
}

// Subscribe adds h to the handlers of topic.
func (b *LocalBus) Subscribe(topic string, h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subs == nil {
		b.subs = map[string][]Handler{}
	}
	b.subs[topic] = append(b.subs[topic], h)
}

// Publish delivers m to the handlers of its topic; the first error stops.
func (b *LocalBus) Publish(ctx context.Context, m Message) error {
	b.mu.RLock()
	hs := b.subs[m.Topic]
	b.mu.RUnlock()
	for _, h := range hs {
		if err := h(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// Relay publishes staged events. Several relays may run at once (one per
// process): they skip each other's rows.
type Relay struct {
	Pool   *pgxpool.Pool
	Bus    Bus
	Logger *slog.Logger
	// Interval between scans when idle (default 200ms).
	Interval time.Duration
}

// Run publishes events until ctx is done.
func (r *Relay) Run(ctx context.Context) {
	every := r.Interval
	if every <= 0 {
		every = 200 * time.Millisecond
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		n, err := r.batch(ctx, 50)
		if err != nil && ctx.Err() == nil {
			r.Logger.LogAttrs(ctx, slog.LevelWarn, "outbox: relay batch failed", slog.Any("error", err))
		}
		if n > 0 {
			continue // more may be waiting
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// batch claims up to n events, publishes them and records the outcome in one
// transaction: a crash before the commit leaves them pending, so they are
// published again (at least once).
func (r *Relay) batch(ctx context.Context, n int) (int, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// An event is skipped while an earlier unsent event of its key exists:
	// per-key order.
	rows, err := tx.Query(ctx, `
		SELECT id, event_id::text, topic, key, payload, attempts
		FROM outbox.events e
		WHERE sent_at IS NULL AND next_at <= now()
		  AND NOT EXISTS (SELECT 1 FROM outbox.events p
		                  WHERE p.key = e.key AND p.sent_at IS NULL AND p.id < e.id)
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, n)
	if err != nil {
		return 0, fmt.Errorf("claim: %w", err)
	}
	type row struct {
		id       int64
		msg      Message
		attempts int
	}
	claimed, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		var e row
		err := r.Scan(&e.id, &e.msg.EventID, &e.msg.Topic, &e.msg.Key, &e.msg.Payload, &e.attempts)
		return e, err
	})
	if err != nil {
		return 0, fmt.Errorf("claim: %w", err)
	}
	for _, e := range claimed {
		perr := r.Bus.Publish(ctx, e.msg)
		if perr == nil {
			if _, err := tx.Exec(ctx, `UPDATE outbox.events SET sent_at = now() WHERE id = $1`, e.id); err != nil {
				return 0, fmt.Errorf("mark %s sent: %w", e.msg.EventID, err)
			}
			continue
		}
		if ctx.Err() != nil {
			return 0, errors.Join(perr, ctx.Err())
		}
		r.Logger.LogAttrs(ctx, slog.LevelWarn, "outbox: publish failed",
			slog.String("event_id", e.msg.EventID), slog.String("topic", e.msg.Topic), slog.Any("error", perr))
		backoff := min(100*time.Millisecond<<min(e.attempts, 8), time.Minute)
		if _, err := tx.Exec(ctx, `
			UPDATE outbox.events
			SET attempts = attempts + 1, last_error = $2, next_at = now() + $3 * interval '1 millisecond'
			WHERE id = $1`, e.id, perr.Error(), backoff.Milliseconds()); err != nil {
			return 0, fmt.Errorf("record failure of %s: %w", e.msg.EventID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return len(claimed), nil
}
