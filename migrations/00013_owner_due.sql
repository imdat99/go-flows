-- +goose Up

-- When an execution next falls due is its owner's business, not other
-- engines' (T10.2): set while an engine keeps the lease between eager tasks,
-- when ready_at holds the lease expiry (recovery's, not a due time). It is
-- only written with ready_at (completions, releases), so lease renewals
-- stay HOT updates.
ALTER TABLE flows.execution ADD COLUMN IF NOT EXISTS owner_due boolean NOT NULL DEFAULT false;

-- Due times of the executions of a queue another engine may claim
-- (store.DueClaimer): the earliest future ready_at is the first entry after
-- now(), however many kept leases fall due before it.
CREATE INDEX IF NOT EXISTS execution_due_idx
    ON flows.execution (queue, ready_at)
    WHERE status IN ('running', 'compensating') AND ready_at IS NOT NULL AND NOT owner_due;

-- +goose Down
DROP INDEX IF EXISTS flows.execution_due_idx;
ALTER TABLE flows.execution DROP COLUMN IF EXISTS owner_due;
