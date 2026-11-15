-- +goose Up

-- A compensation task carries whether the activity it undoes may or may not
-- have had its effect (crash, timeout, panic, cancel): the compensator is
-- told (CompensationUncertain).
ALTER TABLE flows.activity_task ADD COLUMN IF NOT EXISTS uncertain boolean NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE flows.activity_task DROP COLUMN IF EXISTS uncertain;
