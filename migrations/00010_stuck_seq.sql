-- +goose Up

-- The history position at which the execution's workflow code last ran
-- without yielding (store.NoteStuck, T9.3): a second stall there, on any
-- engine, parks it.
ALTER TABLE flows.execution ADD COLUMN IF NOT EXISTS stuck_seq bigint;

-- +goose Down
ALTER TABLE flows.execution DROP COLUMN IF EXISTS stuck_seq;
