-- +goose Up
-- One row per message sent; event_id makes a redelivered event a no-op.
CREATE TABLE notifications.deliveries (
	event_id   text PRIMARY KEY,
	order_id   text NOT NULL,
	email      text NOT NULL,
	template   text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX deliveries_order ON notifications.deliveries (order_id);

-- +goose Down
DROP TABLE notifications.deliveries;
