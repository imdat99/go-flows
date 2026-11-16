-- +goose Up

-- Workflow claims order by (ready_at, id): with id in the index, a claim
-- reads the ready executions of a queue in index order and stops at its
-- limit, instead of sorting the whole ready backlog (T8.4 review).
DROP INDEX IF EXISTS flows.execution_ready_idx;
CREATE INDEX IF NOT EXISTS execution_ready_idx
    ON flows.execution (queue, ready_at, id)
    WHERE status IN ('running', 'compensating') AND ready_at IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS flows.execution_ready_idx;
CREATE INDEX IF NOT EXISTS execution_ready_idx
    ON flows.execution (queue, ready_at)
    WHERE status IN ('running', 'compensating') AND ready_at IS NOT NULL;
