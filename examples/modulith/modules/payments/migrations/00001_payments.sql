-- +goose Up
-- One row per authorization. auth_key and capture_key are the flows.Key of
-- the activities that made them: retries find the row instead of acting
-- twice, and compensators find it by key even without compensation data.
CREATE TABLE payments.payments (
	id          text PRIMARY KEY DEFAULT 'pay_' || replace(gen_random_uuid()::text, '-', ''),
	auth_key    text NOT NULL UNIQUE,
	capture_key text UNIQUE,
	order_id    text NOT NULL DEFAULT '',
	amount      bigint NOT NULL DEFAULT 0,
	-- authorized -> captured -> refunded; authorized -> voided. A row created
	-- voided is a tombstone: the void ran before the authorization landed.
	state       text NOT NULL CHECK (state IN ('authorized', 'captured', 'refunded', 'voided')),
	updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX payments_order ON payments.payments (order_id);

-- +goose Down
DROP TABLE payments.payments;
