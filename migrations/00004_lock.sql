-- +goose Up

-- Business locks (flows.Lock): a key is held by one execution until
-- `until`; the holder's claims and lease renewals extend it by ttl_us. A
-- lock whose holder ended is deleted with it.
CREATE TABLE IF NOT EXISTS flows.lock (
    key          text        PRIMARY KEY,
    execution_id bigint      NOT NULL REFERENCES flows.execution (id) ON DELETE CASCADE,
    sched        int         NOT NULL,
    until        timestamptz NOT NULL,
    ttl_us       bigint      NOT NULL CHECK (ttl_us > 0)
);

CREATE INDEX IF NOT EXISTS lock_execution ON flows.lock (execution_id);

-- +goose Down
DROP TABLE IF EXISTS flows.lock;
