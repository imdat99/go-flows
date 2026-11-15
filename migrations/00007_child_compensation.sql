-- +goose Up

-- A completed child reopened for compensation by its parent: its end is
-- reported to the parent as CompensationCompleted, not as a child outcome.
ALTER TABLE flows.execution ADD COLUMN IF NOT EXISTS compensate_requested boolean NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE flows.execution DROP COLUMN IF EXISTS compensate_requested;
