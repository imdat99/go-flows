-- +goose Up

-- Children of an execution, to cancel them when it ends.
CREATE INDEX IF NOT EXISTS execution_parent_idx ON flows.execution (parent_id)
    WHERE parent_id <> 0;

-- +goose Down
DROP INDEX IF EXISTS flows.execution_parent_idx;
