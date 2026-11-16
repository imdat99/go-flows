-- +goose Up

-- How long past its TTL a lock stays its holder's while one of the
-- holder's activity tasks waits (in its queue or for a retry):
-- store.LockRequest.Hold, T9.4. A constant default: no table rewrite.
ALTER TABLE flows.lock ADD COLUMN IF NOT EXISTS hold_us bigint NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE flows.lock DROP COLUMN IF EXISTS hold_us;
