-- +goose Up
-- label_key is the flows.Key of the activity that created the label.
--
-- The row is where a shipment's fate is decided, atomically:
--   label_created -> pickup_confirmed   by ConfirmPickup (the carrier) or Decide
--   label_created -> canceled           by Decide (a cancel or the timeout)
-- Each decision locks the row first (SELECT ... FOR UPDATE): whichever runs
-- first wins; the other sees the result.
-- Then pickup_confirmed -> picked_up (MarkPickedUp). A row created canceled
-- is a tombstone: the label's compensation ran before the label landed.
CREATE TABLE shipping.shipments (
	id         text PRIMARY KEY DEFAULT 'shp_' || replace(gen_random_uuid()::text, '-', ''),
	label_key  text NOT NULL UNIQUE,
	order_id   text NOT NULL DEFAULT '',
	address    text NOT NULL DEFAULT '',
	state      text NOT NULL
		CHECK (state IN ('label_created', 'pickup_confirmed', 'picked_up', 'canceled')),
	tracking   text,
	reason     text NOT NULL DEFAULT '', -- why canceled: customer, timeout
	updated_at timestamptz NOT NULL DEFAULT now()
);
-- One shipment per order (tombstones have no order).
CREATE UNIQUE INDEX shipments_order ON shipping.shipments (order_id) WHERE order_id <> '';

-- +goose Down
DROP TABLE shipping.shipments;
