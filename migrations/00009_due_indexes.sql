-- +goose Up

-- Pollers ask when the next work of their queues becomes claimable
-- (store.DueClaimer, T9.1).

-- Hand-backs (not_before) of active executions, by queue. Small: a claim
-- clears not_before.
CREATE INDEX IF NOT EXISTS execution_not_before_idx
    ON flows.execution (queue, not_before)
    WHERE not_before IS NOT NULL AND status IN ('running', 'compensating');

-- Activity tasks waiting for a retry delay (no lease): running tasks, whose
-- visible_at is their lease expiry, are left out.
CREATE INDEX IF NOT EXISTS activity_task_delayed_idx
    ON flows.activity_task (queue, visible_at)
    WHERE locked_until IS NULL;

-- +goose Down
DROP INDEX IF EXISTS flows.activity_task_delayed_idx;
DROP INDEX IF EXISTS flows.execution_not_before_idx;
