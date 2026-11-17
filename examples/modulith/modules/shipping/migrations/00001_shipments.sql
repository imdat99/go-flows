-- +goose Up
-- label_key is the flows.Key of the activity that created the label.
CREATE TABLE shipping.shipments (
	id         text PRIMARY KEY DEFAULT 'shp_' || replace(gen_random_uuid()::text, '-', ''),
	label_key  text NOT NULL UNIQUE,
	order_id   text NOT NULL DEFAULT '',
	address    text NOT NULL DEFAULT '',
	-- label_created -> picked_up; label_created -> canceled. A row created
	-- canceled is a tombstone: the cancel ran before the label landed.
	state      text NOT NULL CHECK (state IN ('label_created', 'picked_up', 'canceled')),
	tracking   text,
	updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX shipments_order ON shipping.shipments (order_id);

-- +goose Down
DROP TABLE shipping.shipments;
