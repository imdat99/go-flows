-- +goose Up

-- Executions sticky to an owner, for Unstick when that owner stops.
-- sticky_owner outlives its sticky_until (it is cleared on the next claim,
-- release or end), so Unstick filters on sticky_until too.
CREATE INDEX IF NOT EXISTS execution_sticky_idx ON flows.execution (sticky_owner, sticky_until)
    WHERE sticky_owner <> '';

-- +goose Down
DROP INDEX IF EXISTS flows.execution_sticky_idx;
