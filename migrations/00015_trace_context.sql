-- +goose Up

-- The trace context of an execution (T10.10): opaque bytes a tracer
-- returned when the execution was started (flows.Tracer.StartWorkflow;
-- otelflows: trace id, span id and flags of its start span), NULL without
-- a tracer. Continue-as-new stores the new execution's own (a new trace,
-- linked by the tracer to the previous one). Activity tasks copy it at
-- insert, so claims return it without a join.
ALTER TABLE flows.execution ADD COLUMN IF NOT EXISTS trace bytea;
ALTER TABLE flows.activity_task ADD COLUMN IF NOT EXISTS trace bytea;

-- +goose Down
ALTER TABLE flows.activity_task DROP COLUMN IF EXISTS trace;
ALTER TABLE flows.execution DROP COLUMN IF EXISTS trace;
