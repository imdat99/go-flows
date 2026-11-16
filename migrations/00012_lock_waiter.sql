-- +goose Up

-- Lock waiters (store.LockRequest.Wait, T9.8): queued requests for a
-- business lock, in id order. A release wakes the first live one, whose
-- turn (the free lock is its only) lasts until woken_until; a waiter's
-- place lapses at expires_at (its next attempt, overdue by the lock's
-- TTL). It leaves the queue when it gets the lock, asks without waiting,
-- or ends.
CREATE TABLE IF NOT EXISTS flows.lock_waiter (
    id           bigserial   PRIMARY KEY,
    key          text        NOT NULL,
    execution_id bigint      NOT NULL REFERENCES flows.execution (id) ON DELETE CASCADE,
    expires_at   timestamptz NOT NULL,
    woken_until  timestamptz,
    UNIQUE (key, execution_id)
);

CREATE INDEX IF NOT EXISTS lock_waiter_turn ON flows.lock_waiter (key, id);

-- A lock row reserved for the waiter a release woke (execution_id), until
-- its turn ends (until): requests waiting for the row see the turn when
-- they read it again. Not extended by claims or renewals; taking it over
-- tells nobody (LockLost is for holders).
ALTER TABLE flows.lock ADD COLUMN IF NOT EXISTS reserved boolean NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS lock_waiter_execution ON flows.lock_waiter (execution_id);

-- +goose Down
ALTER TABLE flows.lock DROP COLUMN IF EXISTS reserved;
DROP TABLE IF EXISTS flows.lock_waiter;
