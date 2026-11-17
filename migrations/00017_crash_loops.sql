-- +goose Up

-- The total attempts the activity's retry policy allows, recorded when the
-- task is created (store.NewActivityTask.MaxAttempts, T11.3): a claim that
-- would start an attempt beyond it after a lost one (its process crashed or
-- lost the lease) reports the task exhausted instead of running it again.
-- 0: unknown (rows written by earlier releases, or an activity not defined
-- in the scheduling binary), no limit.
ALTER TABLE flows.activity_task ADD COLUMN IF NOT EXISTS max_attempts integer NOT NULL DEFAULT 0;

-- Workflow task claims since the execution's last completion or release
-- (store.WorkflowTask.Claims, T11.3): an execution claimed again and again
-- without its task ever completing (its engine crashes mid-task) is parked.
ALTER TABLE flows.execution ADD COLUMN IF NOT EXISTS claims integer NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE flows.execution DROP COLUMN IF EXISTS claims;
ALTER TABLE flows.activity_task DROP COLUMN IF EXISTS max_attempts;
