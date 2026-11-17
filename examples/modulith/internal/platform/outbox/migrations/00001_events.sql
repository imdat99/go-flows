-- +goose Up
-- Module events waiting to be published. A module inserts them in the same
-- transaction as the state change they describe (here: the RunHook's
-- transaction, which records a workflow's end), and the relay publishes them.
CREATE TABLE outbox.events (
	id         bigserial PRIMARY KEY,                          -- publication order
	event_id   uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(), -- consumers deduplicate on it
	topic      text NOT NULL,
	key        text NOT NULL,                                  -- ordering key (an order id)
	payload    jsonb NOT NULL,
	attempts   int NOT NULL DEFAULT 0,
	next_at    timestamptz NOT NULL DEFAULT now(),             -- backoff after a failed publish
	last_error text,
	created_at timestamptz NOT NULL DEFAULT now(),
	sent_at    timestamptz
);
CREATE INDEX events_pending ON outbox.events (id) WHERE sent_at IS NULL;
CREATE INDEX events_pending_key ON outbox.events (key, id) WHERE sent_at IS NULL;

-- +goose Down
DROP TABLE outbox.events;
