-- +goose Up
-- The orders read model. The fulfilment workflow inserts the row (an
-- activity); the RunHook sets its final state in the transaction that
-- records the workflow's end.
CREATE TABLE orders.orders (
	id         text PRIMARY KEY,
	email      text NOT NULL,
	address    text NOT NULL,
	amount     bigint NOT NULL,
	lines      jsonb NOT NULL,
	state      text NOT NULL CHECK (state IN ('processing', 'completed', 'canceled', 'failed')),
	reason     text NOT NULL DEFAULT '',
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE orders.orders;
