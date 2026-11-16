-- +goose Up

-- The compensation data returned by the most recent attempt of an activity
-- that may have had its effect (an uncertain attempt that is retried), kept
-- across attempts: if the final attempt returns none, its compensator gets
-- this data instead of the zero value. NULL: none kept.
ALTER TABLE flows.activity_task ADD COLUMN IF NOT EXISTS comp_data bytea;

-- +goose Down
ALTER TABLE flows.activity_task DROP COLUMN IF EXISTS comp_data;
