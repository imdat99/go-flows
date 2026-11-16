-- +goose Up

-- An execution whose next due time is its owner's business, not other
-- engines' (store.DueClaimer): an engine keeping the lease between eager
-- tasks (ready_at holds the lease expiry, recovery's, not a due time,
-- T10.2), or one it is sticky to that wakes by itself for its next timer
-- (store.Completion.DueKnown, T10.3). due_owner names that owner; the mark
-- only counts while sticky_owner names it too (a kept lease's sticky_owner
-- is its owner, with no sticky_until), so a binary older than due_owner,
-- which clears sticky_owner when it releases or completes the execution
-- and never writes due_owner, leaves no stale mark hiding a due time. Both
-- are only written by completions, releases and Unstick, which change
-- indexed columns anyway (ready_at, sticky_until): claims and lease
-- renewals never write them and stay HOT updates.
ALTER TABLE flows.execution ADD COLUMN IF NOT EXISTS due_owner text NOT NULL DEFAULT '';

-- Due times of the executions of a queue another engine may claim: the
-- earliest future ready_at is the first entry after now(), however many
-- kept leases fall due before it.
CREATE INDEX IF NOT EXISTS execution_due_idx
    ON flows.execution (queue, ready_at)
    WHERE status IN ('running', 'compensating') AND ready_at IS NOT NULL
      AND (due_owner = '' OR due_owner <> sticky_owner);

-- +goose Down
DROP INDEX IF EXISTS flows.execution_due_idx;
ALTER TABLE flows.execution DROP COLUMN IF EXISTS due_owner;
