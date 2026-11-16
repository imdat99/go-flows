// Command outbox publishes events to an external system with the
// transactional outbox pattern:
//
//	make up
//	go run ./examples/outbox
//
// FLOWS_DSN points at another database.
//
// Why an outbox: publishing to a broker from a workflow or an activity is a
// dual write. The broker and Postgres do not commit together, so a crash
// between the two either loses the event (state committed, event not sent)
// or sends one for something that never happened (event sent, then the
// transaction rolls back or the workflow compensates). Instead, events are
// written to a table in the same transaction as the state they describe,
// and a relay copies them to the broker afterwards.
//
// The flow here:
//
//  1. An activity stages a business event (ready = false) in its own
//     transaction, deduplicated by its event id: activities run at least
//     once, so a retry finds the row already there. In a real activity
//     the insert goes in the same transaction as the business change.
//  2. The RunHook runs inside the transaction that ends the execution: on
//     completed it releases the staged events (ready = true); on any other
//     end (failed, compensated, canceled, terminated) it drops them. It
//     also stages a "workflow ended" event. Nothing is published for an
//     order that did not go through, and nothing is lost for one that did.
//  3. A relay claims ready rows with FOR UPDATE SKIP LOCKED (several relays
//     can run side by side), publishes them, and marks them sent. A failed
//     publish is retried with backoff.
//
// Delivery is at least once: the relay can publish and crash before it
// marks the row sent, and then publishes it again. Every event carries a
// stable event id that consumers deduplicate on.
//
// Ordering: among released rows, the events of one key (here, one order)
// are published in id order: a row is not claimed while an earlier unsent
// released row of the same key exists. That is the staging order when one
// execution writes the key. There is no attempt cap or dead letter here, so
// an event the bus keeps refusing blocks its key forever.
//
// Not covered: ContinueAsNew. The hook does not run for a continued
// execution, only for the final one, and its rows are selected by instance
// id: the final execution's status would release or drop the events of
// every earlier segment too (a failure in the last segment drops the events
// of the successful ones). A workflow that continues as new should release
// its events itself before continuing, or key the rows by segment.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	flows "github.com/imdat99/go-flows/flows"
)

// The outbox lives in the application's own schema, not in the engine's.
const ddl = `
CREATE SCHEMA IF NOT EXISTS app_outbox;
CREATE TABLE IF NOT EXISTS app_outbox.events (
	id          bigserial PRIMARY KEY,                           -- staging order
	event_id    text NOT NULL UNIQUE DEFAULT gen_random_uuid()::text, -- consumers dedupe on it
	instance_id text NOT NULL,                                   -- the execution that staged it
	topic       text NOT NULL,
	key         text NOT NULL,                                   -- ordering / partition key
	payload     jsonb NOT NULL,
	ready       boolean NOT NULL DEFAULT false,                  -- released by the RunHook
	attempts    int NOT NULL DEFAULT 0,
	next_at     timestamptz NOT NULL DEFAULT now(),              -- backoff after a failed publish
	last_error  text,
	sent_at     timestamptz
);
-- The relay's scan (ORDER BY id) and its per-key ordering check (key, id < $).
CREATE INDEX IF NOT EXISTS events_pending ON app_outbox.events (id) WHERE ready AND sent_at IS NULL;
CREATE INDEX IF NOT EXISTS events_pending_key ON app_outbox.events (key, id) WHERE ready AND sent_at IS NULL;
-- The hook's release or drop, and the cleanup of orphans (see stageConfirmed).
CREATE INDEX IF NOT EXISTS events_instance ON app_outbox.events (instance_id) WHERE NOT ready;`

// EventBus is the external system (Kafka, NATS, SNS, a webhook...).
type EventBus interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
}

// stubBus prints what it is given and fails every third call, so the
// relay's retries show.
type stubBus struct{ calls atomic.Int64 }

func (b *stubBus) Publish(_ context.Context, topic, key string, payload []byte) error {
	if b.calls.Add(1)%3 == 0 {
		fmt.Printf("  bus: %s key=%s -> unavailable\n", topic, key)
		return errors.New("broker unavailable")
	}
	fmt.Printf("  bus: %s key=%s %s\n", topic, key, payload)
	return nil
}

type Order struct {
	ID     string `json:"id"` // also the instance id of its execution
	Amount int64  `json:"amount"`
	Ship   string `json:"ship"` // "nowhere" cannot be shipped to
}

var errUnshippable = errors.New("destination not served")

func init() { flows.RegisterErrorKind("order_unshippable", errUnshippable) }

// db is the pool the activities write to; set in run.
var db *pgxpool.Pool

var (
	// stageConfirmed stages order.confirmed. The row commits in the
	// activity's own transaction but stays invisible to the relay
	// (ready = false) until the execution completes.
	//
	// The event id is the activity's Key, the same on every attempt: a
	// retried attempt inserts nothing. The Key names the execution too, so
	// a later execution of the same instance id (ContinueAsNew, or an id
	// reused after retention) stages its own event instead of hitting ON
	// CONFLICT.
	//
	// Version 1 of placeOrder predates that Key (migration 00016): its
	// executions may carry a Key without the execution id (see flows.Key),
	// so it drew a nonce with flows.Rand and passed {order, nonce} to the
	// activity outbox.stage-confirmed, which used Key + "/" + nonce as the
	// event id. Version 2 below drops the nonce and passes the Order to
	// this activity, under a new name since its input changed. A
	// deployment upgrading from version 1 keeps version 1's workflow and
	// activity definitions until its executions have ended.
	//
	// A late attempt can still insert after the run ended (the attempt
	// outlived its lease, then the run failed and the hook dropped what was
	// there). That orphan stays not ready, but a later run reusing the
	// instance id (after retention purged it) would release it on
	// completion. A periodic job should delete not-ready rows of instances
	// that have ended (they have a workflow.ended row), and do so
	// before an ended instance's id can be reused. Do not age them by time
	// alone (a run waiting on a signal can last days), or only past the
	// longest possible run, with a staged_at column.
	stageConfirmed = flows.DefineActivity("outbox.stage-confirmed.v2",
		func(ctx context.Context, k flows.Key, o Order) (struct{}, struct{}, error) {
			payload, err := json.Marshal(o)
			if err != nil {
				return struct{}{}, struct{}{}, flows.Permanent(fmt.Errorf("encode order.confirmed: %w", err))
			}
			_, err = db.Exec(ctx, `
				INSERT INTO app_outbox.events (event_id, instance_id, topic, key, payload)
				VALUES ($1, $2, 'order.confirmed', $3, $4)
				ON CONFLICT (event_id) DO NOTHING`, string(k), o.ID, o.ID, payload)
			if err != nil {
				return struct{}{}, struct{}{}, fmt.Errorf("stage order.confirmed: %w", err)
			}
			return struct{}{}, struct{}{}, nil
		}, nil)

	ship = flows.DefineActivity("outbox.ship",
		func(_ context.Context, _ flows.Key, o Order) (string, struct{}, error) {
			if o.Ship == "nowhere" {
				return "", struct{}{}, flows.Permanent(errUnshippable)
			}
			return "shp_" + o.ID, struct{}{}, nil
		}, nil)

	placeOrder = flows.DefineWorkflow("outbox.place-order", 2, func(c flows.Ctx, o Order) (string, error) {
		if _, err := stageConfirmed.Run(c, o); err != nil {
			return "", err
		}
		return ship.Run(c, o) // failing here drops the staged order.confirmed
	})
)

// outboxHook releases or drops the staged events, and stages a
// workflow.ended event, in the transaction that records the end. If it
// returns an error the end rolls back with it and is retried later.
type outboxHook struct{}

func (outboxHook) RunEnded(ctx context.Context, tx pgx.Tx, r flows.RunInfo) error {
	switch r.Status {
	case flows.StatusNeedsAttention:
		return nil // a park, not an end: the execution may still complete or compensate
	case flows.StatusCompleted:
		if _, err := tx.Exec(ctx, `UPDATE app_outbox.events SET ready = true WHERE instance_id = $1 AND NOT ready`, r.InstanceID); err != nil {
			return fmt.Errorf("release events of %s: %w", r.InstanceID, err)
		}
	default: // failed, compensated, canceled, terminated
		if _, err := tx.Exec(ctx, `DELETE FROM app_outbox.events WHERE instance_id = $1 AND NOT ready`, r.InstanceID); err != nil {
			return fmt.Errorf("drop events of %s: %w", r.InstanceID, err)
		}
	}
	// RunInfo carries no workflow output: an event that needs it is staged
	// by an activity, as order.confirmed is.
	payload, err := json.Marshal(map[string]string{
		"instance_id": r.InstanceID, "workflow": r.Workflow, "status": string(r.Status), "error": r.Error,
	})
	if err != nil {
		return fmt.Errorf("encode workflow.ended: %w", err)
	}
	// event_id defaults to a fresh uuid: this insert commits once, with the end.
	_, err = tx.Exec(ctx, `
		INSERT INTO app_outbox.events (instance_id, topic, key, payload, ready)
		VALUES ($1, 'workflow.ended', $1, $2, true)`, r.InstanceID, payload)
	if err != nil {
		return fmt.Errorf("stage workflow.ended of %s: %w", r.InstanceID, err)
	}
	return nil
}

// relay publishes ready events until ctx is done.
func relay(ctx context.Context, pool *pgxpool.Pool, bus EventBus) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		if err := relayBatch(ctx, pool, bus, 10); err != nil && ctx.Err() == nil {
			fmt.Printf("  relay: %v\n", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// relayBatch claims up to n events, publishes them and records the outcome,
// in one transaction: the row locks keep other relays off these events
// while they are published, and a crash before the commit leaves them
// pending (so they are published again: at least once).
func relayBatch(ctx context.Context, pool *pgxpool.Pool, bus EventBus, n int) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin relay batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, event_id, topic, key, payload, attempts
		FROM app_outbox.events e
		WHERE ready AND sent_at IS NULL AND next_at <= now()
		  AND NOT EXISTS (SELECT 1 FROM app_outbox.events p
		                  WHERE p.key = e.key AND p.ready AND p.sent_at IS NULL AND p.id < e.id)
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, n)
	if err != nil {
		return fmt.Errorf("claim events: %w", err)
	}
	type event struct {
		id             int64
		eventID, topic string
		key            string
		payload        []byte
		attempts       int
	}
	events, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (event, error) {
		var e event
		err := r.Scan(&e.id, &e.eventID, &e.topic, &e.key, &e.payload, &e.attempts)
		return e, err
	})
	if err != nil {
		return fmt.Errorf("claim events: %w", err)
	}

	for _, e := range events {
		// The event id travels with the message (a header on a real broker)
		// so consumers can drop the duplicates at-least-once delivery makes.
		msg, err := json.Marshal(map[string]any{"event_id": e.eventID, "data": json.RawMessage(e.payload)})
		if err != nil {
			return fmt.Errorf("encode %s: %w", e.eventID, err)
		}
		if perr := bus.Publish(ctx, e.topic, e.key, msg); perr != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("relay stopped: %w", ctx.Err()) // stopping: leave the rest to the next relay
			}
			backoff := min(100*time.Millisecond<<min(e.attempts, 6), 5*time.Second) // clamp before shifting
			if _, err := tx.Exec(ctx, `
				UPDATE app_outbox.events
				SET attempts = attempts + 1, last_error = $2, next_at = now() + $3 * interval '1 millisecond'
				WHERE id = $1`, e.id, perr.Error(), backoff.Milliseconds()); err != nil {
				return fmt.Errorf("record failure of %s: %w", e.eventID, err)
			}
			continue
		}
		// Marked sent rather than deleted, for auditing; a periodic job can
		// delete old sent rows.
		if _, err := tx.Exec(ctx, `UPDATE app_outbox.events SET sent_at = now() WHERE id = $1`, e.id); err != nil {
			return fmt.Errorf("mark %s sent: %w", e.eventID, err)
		}
	}
	return tx.Commit(ctx)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	dsn := os.Getenv("FLOWS_DSN")
	if dsn == "" {
		dsn = "postgres://flows:flows@localhost:5433/flows?sslmode=disable" //nolint:gosec // the docker compose database of the repository
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()
	db = pool
	if err := flows.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if _, err := pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("create outbox: %w", err)
	}
	// Set the hook on every engine, client-only ones included.
	engine, err := flows.New(pool, flows.Options{Hook: outboxHook{}})
	if err != nil {
		return fmt.Errorf("new engine: %w", err)
	}
	if err := engine.Start(ctx); err != nil {
		return fmt.Errorf("start engine: %w", err)
	}
	defer func() { _ = engine.Stop(context.Background()) }()

	// The relay usually runs in its own process; a goroutine here.
	relayCtx, stopRelay := context.WithCancel(ctx)
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		relay(relayCtx, pool, &stubBus{})
	}()
	defer func() { stopRelay(); <-relayDone }() // graceful stop on every path

	suffix := time.Now().Format("150405.000")
	ok := Order{ID: "order-ok-" + suffix, Amount: 25, Ship: "Hanoi"}
	if _, err := placeOrder.Run(ctx, engine, ok.ID, ok); err != nil {
		return fmt.Errorf("%s: %w", ok.ID, err)
	}
	fmt.Println(ok.ID, "completed")
	bad := Order{ID: "order-bad-" + suffix, Amount: 40, Ship: "nowhere"}
	if _, err := placeOrder.Run(ctx, engine, bad.ID, bad); !errors.Is(err, errUnshippable) {
		return fmt.Errorf("%s should have failed, got %w", bad.ID, err)
	}
	fmt.Println(bad.ID, "failed")

	// Wait for the relay to drain these orders, then stop it gracefully.
	for {
		var pending int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM app_outbox.events
			WHERE instance_id = ANY($1) AND ready AND sent_at IS NULL`, []string{ok.ID, bad.ID}).Scan(&pending); err != nil {
			return fmt.Errorf("count pending events: %w", err)
		}
		if pending == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for the relay: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	stopRelay()
	<-relayDone

	rows, err := pool.Query(ctx, `
		SELECT instance_id, topic, attempts FROM app_outbox.events
		WHERE instance_id = ANY($1) ORDER BY id`, []string{ok.ID, bad.ID})
	if err != nil {
		return fmt.Errorf("list events: %w", err)
	}
	// The failed order has no order.confirmed row: the hook dropped it.
	fmt.Println("outbox:")
	var inst, topic string
	var attempts int
	var badConfirmed bool
	if _, err := pgx.ForEachRow(rows, []any{&inst, &topic, &attempts}, func() error {
		badConfirmed = badConfirmed || (inst == bad.ID && topic == "order.confirmed")
		fmt.Printf("  %-26s %-16s sent after %d failed attempt(s)\n", inst, topic, attempts)
		return nil
	}); err != nil {
		return fmt.Errorf("list events: %w", err)
	}
	if badConfirmed {
		return fmt.Errorf("%s: order.confirmed was not dropped", bad.ID)
	}
	return nil
}
