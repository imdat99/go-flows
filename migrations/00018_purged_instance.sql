-- +goose Up

-- Tombstones of purged instance ids (T12.3): with Options.KeepPurgedIDs,
-- PurgeEnded records here each instance id it leaves without an execution,
-- in the transaction that deletes it, and Start (and child starts) refuse
-- an id found here (ErrInstancePurged) instead of running it again.
-- PurgeEnded deletes the rows older than KeepPurgedIDs; ForgetInstance
-- deletes one. Empty unless the option is set.
CREATE TABLE IF NOT EXISTS flows.purged_instance (
    instance_id text        PRIMARY KEY,
    workflow    text        NOT NULL,
    -- Status and end time of the last execution purged.
    status      text        NOT NULL,
    ended_at    timestamptz NOT NULL,
    purged_at   timestamptz NOT NULL DEFAULT now()
);

-- Tombstone retention: oldest first.
CREATE INDEX IF NOT EXISTS purged_instance_purged_idx ON flows.purged_instance (purged_at);

-- +goose Down
DROP TABLE IF EXISTS flows.purged_instance;
