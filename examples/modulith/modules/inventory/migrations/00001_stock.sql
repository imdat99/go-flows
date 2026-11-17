-- +goose Up
CREATE TABLE inventory.stock (
	sku          text PRIMARY KEY,
	available    int NOT NULL CHECK (available >= 0),
	-- Discontinued SKUs get no replenishment loop (StopReplenishment).
	discontinued boolean NOT NULL DEFAULT false,
	updated_at   timestamptz NOT NULL DEFAULT now()
);

-- One row per reserved order line; deleted when released.
CREATE TABLE inventory.reservations (
	id         bigserial PRIMARY KEY,
	order_id   text NOT NULL,
	sku        text NOT NULL REFERENCES inventory.stock (sku),
	qty        int NOT NULL CHECK (qty > 0),
	created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX reservations_order ON inventory.reservations (order_id);

-- Purchase orders placed by the replenishment loop (the stub supplier
-- delivers at once).
CREATE TABLE inventory.purchase_orders (
	id         bigserial PRIMARY KEY,
	sku        text NOT NULL REFERENCES inventory.stock (sku),
	qty        int NOT NULL CHECK (qty > 0),
	created_at timestamptz NOT NULL DEFAULT now()
);

-- Manual stock adjustments (counts, damage, receipts). request_id is the
-- caller's idempotency key.
CREATE TABLE inventory.adjustments (
	id         bigserial PRIMARY KEY,
	request_id text NOT NULL UNIQUE,
	sku        text NOT NULL REFERENCES inventory.stock (sku),
	delta      int NOT NULL,
	reason     text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now()
);

-- The table of package idem (idem.Do, idem.Abort), with the same DDL as
-- idem's CreateTable, here so that goose applies it under its lock.
CREATE TABLE inventory.processed_requests (
	key        text PRIMARY KEY,
	result     jsonb,
	aborted    boolean NOT NULL DEFAULT false,
	created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX processed_requests_created_idx ON inventory.processed_requests (created_at);

-- +goose Down
DROP TABLE inventory.processed_requests;
DROP TABLE inventory.adjustments;
DROP TABLE inventory.purchase_orders;
DROP TABLE inventory.reservations;
DROP TABLE inventory.stock;
