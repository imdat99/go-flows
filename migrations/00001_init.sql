-- +goose Up

-- One row per execution of a workflow instance. ContinueAsNew ends an
-- execution and inserts the next one with the same instance_id.
CREATE TABLE IF NOT EXISTS flows.execution (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    instance_id   text        NOT NULL,
    workflow      text        NOT NULL,
    version       int         NOT NULL,
    queue         text        NOT NULL,
    status        text        NOT NULL,
    -- Parent execution (child workflows); no foreign key: retention may
    -- remove a parent before its children.
    parent_id     bigint      NOT NULL DEFAULT 0,
    parent_sched  int         NOT NULL DEFAULT 0,
    input         bytea,
    output        bytea,
    err           text        NOT NULL DEFAULT '',
    err_kinds     text        NOT NULL DEFAULT '',
    -- Lease of the workflow task (GW-2): owner_id + lease_version fence
    -- every write, lease_version grows at every grant.
    owner_id      text        NOT NULL DEFAULT '',
    lease_version bigint      NOT NULL DEFAULT 0,
    locked_until  timestamptz,
    -- Stickiness to the owner whose executor caches the history.
    sticky_owner  text        NOT NULL DEFAULT '',
    sticky_until  timestamptz,
    -- Earliest next claim after a release with delay (hand-back, backoff).
    not_before    timestamptz,
    -- Earliest visible_at of the pending events; NULL when there are none.
    -- The claim scans it through execution_ready_idx (GW-8).
    ready_at      timestamptz,
    -- Seq of the last history event.
    last_seq      bigint      NOT NULL DEFAULT 0,
    created_at    timestamptz NOT NULL DEFAULT now(),
    ended_at      timestamptz,
    CONSTRAINT execution_status_check CHECK (status IN (
        'running', 'compensating', 'needs_attention',
        'completed', 'failed', 'compensated', 'canceled', 'terminated', 'continued'))
);

-- At most one active execution per instance id (GW-1).
CREATE UNIQUE INDEX IF NOT EXISTS execution_active_instance_idx
    ON flows.execution (instance_id)
    WHERE status IN ('running', 'compensating', 'needs_attention');

-- Latest execution of an instance id (idempotent start, signals, status).
CREATE INDEX IF NOT EXISTS execution_instance_idx
    ON flows.execution (instance_id, id DESC);

-- Workflow task claims: ready executions of a queue, oldest first.
CREATE INDEX IF NOT EXISTS execution_ready_idx
    ON flows.execution (queue, ready_at)
    WHERE status IN ('running', 'compensating') AND ready_at IS NOT NULL;

-- Retention: ended executions by end time.
CREATE INDEX IF NOT EXISTS execution_ended_idx
    ON flows.execution (ended_at)
    WHERE ended_at IS NOT NULL;

-- The history of an execution. Columns map one to one onto
-- internal/history.Event; unused fields hold their zero value.
CREATE TABLE IF NOT EXISTS flows.history (
    execution_id bigint      NOT NULL REFERENCES flows.execution (id) ON DELETE CASCADE,
    seq          bigint      NOT NULL,
    type         smallint    NOT NULL,
    schedule_id  int         NOT NULL DEFAULT 0,
    attempt      int         NOT NULL DEFAULT 0,
    name         text        NOT NULL DEFAULT '',
    version      int         NOT NULL DEFAULT 0,
    ref          text        NOT NULL DEFAULT '',
    queue        text        NOT NULL DEFAULT '',
    arg_hash     bigint      NOT NULL DEFAULT 0,
    payload      bytea,
    comp_data    bytea,
    err          text        NOT NULL DEFAULT '',
    err_kinds    text        NOT NULL DEFAULT '',
    uncertain    boolean     NOT NULL DEFAULT false,
    permanent    boolean     NOT NULL DEFAULT false,
    at           timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (execution_id, seq)
);

-- Events waiting for the next workflow task of an execution: same columns
-- as history, without seq, plus visibility.
CREATE TABLE IF NOT EXISTS flows.pending_event (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id bigint      NOT NULL REFERENCES flows.execution (id) ON DELETE CASCADE,
    visible_at   timestamptz NOT NULL,
    type         smallint    NOT NULL,
    schedule_id  int         NOT NULL DEFAULT 0,
    attempt      int         NOT NULL DEFAULT 0,
    name         text        NOT NULL DEFAULT '',
    version      int         NOT NULL DEFAULT 0,
    ref          text        NOT NULL DEFAULT '',
    queue        text        NOT NULL DEFAULT '',
    arg_hash     bigint      NOT NULL DEFAULT 0,
    payload      bytea,
    comp_data    bytea,
    err          text        NOT NULL DEFAULT '',
    err_kinds    text        NOT NULL DEFAULT '',
    uncertain    boolean     NOT NULL DEFAULT false,
    permanent    boolean     NOT NULL DEFAULT false,
    at           timestamptz
);

CREATE INDEX IF NOT EXISTS pending_event_execution_idx
    ON flows.pending_event (execution_id, id);

-- Activity invocations and compensations waiting to run or running.
-- A claim moves visible_at to the lease expiry, so activity_task_ready_idx
-- holds exactly the claimable tasks in visible_at order.
CREATE TABLE IF NOT EXISTS flows.activity_task (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id  bigint      NOT NULL REFERENCES flows.execution (id) ON DELETE CASCADE,
    instance_id   text        NOT NULL,
    schedule_id   int         NOT NULL,
    compensate    boolean     NOT NULL DEFAULT false,
    name          text        NOT NULL,
    queue         text        NOT NULL,
    input         bytea,
    attempt       int         NOT NULL DEFAULT 0,
    owner_id      text        NOT NULL DEFAULT '',
    lease_version bigint      NOT NULL DEFAULT 0,
    locked_until  timestamptz,
    visible_at    timestamptz NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (execution_id, schedule_id, compensate)
);

CREATE INDEX IF NOT EXISTS activity_task_ready_idx
    ON flows.activity_task (queue, visible_at, id);

-- +goose Down
DROP TABLE IF EXISTS flows.activity_task;
DROP TABLE IF EXISTS flows.pending_event;
DROP TABLE IF EXISTS flows.history;
DROP TABLE IF EXISTS flows.execution;
