-- +goose Up

-- Set when the workflow canceled the activity's context
-- (ActivityCancelRequested): RenewLeases reports it, and the worker running
-- the activity cancels its context (GW-13). The outcome is still recorded:
-- a canceled activity may have had its effect, and its compensation data
-- matters.
ALTER TABLE flows.activity_task ADD COLUMN IF NOT EXISTS cancel_requested boolean NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE flows.activity_task DROP COLUMN IF EXISTS cancel_requested;
