-- +goose Up

-- Activity keys (flows.Key) carry the execution id from this release on
-- (T10.19): before, the key of the first activity of every execution of an
-- instance id was the same. Executions that exist when this migration runs
-- keep the earlier format for their whole life, so an activity retried or
-- compensated after the upgrade keeps the key its earlier attempts used
-- (idempotency records stored under it stay valid). first_execution_id is
-- the first execution id whose keys carry the execution id: one past every
-- id allocated so far (the sequence also counts ids of transactions not
-- committed yet). A single row.
CREATE TABLE IF NOT EXISTS flows.key_epoch (
    only_row           boolean PRIMARY KEY DEFAULT true CHECK (only_row),
    first_execution_id bigint  NOT NULL
);

INSERT INTO flows.key_epoch (first_execution_id)
SELECT greatest(
           coalesce(max(id), 0),
           coalesce(pg_sequence_last_value(pg_get_serial_sequence('flows.execution', 'id')), 0)
       ) + 1
FROM flows.execution
ON CONFLICT (only_row) DO NOTHING;

-- +goose Down
DROP TABLE IF EXISTS flows.key_epoch;
