# Operations

[Tiếng Việt](vi/van-hanh.md)

## Setup

```go
pool, err := pgxpool.New(ctx, dsn)
err = flows.Migrate(ctx, pool) // every process may call it; an advisory lock serializes
engine, err := flows.New(pool, flows.Options{
	Logger: slog.Default(),
	// Hook, Observer, Tracer: see below.
})
err = engine.Start(ctx)
defer engine.Stop(shutdownCtx)
```

- **Migrations.** `Migrate` creates the schema `flows` and applies the
  embedded goose migrations, forward only. Each change is a new file.
  `CHANGELOG.md` lists the migrations of each release and the upgrade notes
  for rolling deploys. Applied migrations are never edited, so a few of
  their comments are out of date; the Corrections of `CHANGELOG.md` give
  the right behaviour (for example, a claim does not clear `sticky_owner`).
- **Client-only processes.** An engine that is not started can still start
  workflows, send signals, cancel, terminate and wait. An API process can use
  one without running tasks.
- **Queues.** `Options.Queues` selects the queues an engine runs (default
  `"default"`). Route activities with `WithQueue` and workflows with
  `WithWorkflowQueue`, for example to keep slow activities on dedicated
  processes. An activity whose queue no engine runs waits forever: its
  workflow does not fail.
- **Instance ids of engines.** `Options.InstanceID` names the engine in
  leases and must be unique per process. The default (hostname plus a random
  suffix) is.
- **Hooks everywhere.** Set the same `Options.Hook` on every engine,
  client-only ones included: an engine runs its own hook for the ends it
  records, such as a `Terminate` it performs.
- **Registration.** Define workflows and activities at package level, in
  every binary that runs them. An engine runs only the workflow versions and
  activities its binary defines. It hands back the others with a delay, so
  that an engine that has them can take them.

## Connections

- **LISTEN.** Each started engine takes **one connection** out of the pool
  for its lifetime, for `LISTEN`. The connection leaves the pool (it no
  longer counts against `MaxConns`), so each engine uses one server
  connection beyond its pool: count it against Postgres `max_connections`.
- **Tasks.** They borrow connections only while they talk to the store.
  Up to `MaxWorkflowTasks + MaxActivities` tasks run at once (default 64
  each), but most of their time is spent in your activities, not in the
  database.
- **Pool size.** pgxpool's default `MaxConns` is `max(4, NumCPU)`. Raise it
  when many activities complete at once, or when your activities use the
  same pool.
- **Fast path.** The hot statements use pgconn directly, prepared once per
  connection. This needs the pool's default `QueryExecModeCacheStatement` and
  no `ConnConfig.Tracer`. Otherwise the engine falls back to ordinary pgx
  calls: correct, but slower. With a `Hook`, the completions that end an
  execution use an explicit transaction too.
- **Cancellation.** On the fast path, a call whose context ends is cut short
  and its connection closed. The server is also sent a cancel request, so the
  statement stops instead of committing after the caller saw an error. The
  call returns once that request is answered, after at most 1 s if the
  database cannot be reached. As with any interrupted call, a commit just
  before the cancel remains possible; fencing makes the retry safe.
- **Poolers.** Behind PgBouncer in transaction mode, `LISTEN` receives
  nothing. The engine still works on its own polls (up to `PollMax` of extra
  latency per hand-off). Set `Options.ListenDSN` to a direct or
  session-mode connection string: the engine then listens on one connection
  of its own there, and keeps the pool for everything else. The pool's hooks
  do not apply to it; set `sslmode=verify-full` for a direct connection. A
  connection that fails shows as "flows: watch failed" in the logs. Run `Migrate` over a direct connection too (it holds an advisory
  lock on one connection while goose uses others: the pool needs
  `MaxConns >= 3`).

## Tuning

| Option | Default | Effect |
| --- | --- | --- |
| `LeaseTTL` | 30s | How long a dead engine holds its work. Leases renew every third of it. Shorter: faster recovery, more renewals. |
| `MaxWorkflowTasks`, `MaxActivities` | 64, 64 | Concurrency per engine. |
| `PollMax` | 1s | The longest idle poll interval, at most `LeaseTTL`/3 (recovery of a crashed engine's work). Notifications and due times wake pollers earlier. |
| `StickyFor`, `CacheSize`, `CacheBytes` | 10s, 1000, 256 MiB | Keep a replayed execution in memory, with its next task on this engine. `CacheSize: -1` disables the cache (every task replays). `CacheBytes` bounds the estimated memory of the cache (a few KiB per execution and per live coroutine, plus the encoded payloads of its history); the least recently used are evicted beyond either bound. `CacheBytes: -1` bounds by count only. |
| `DisableEager` | false | Run every activity through the queue instead of in the scheduling engine. |
| `ListenDSN` | (pool) | Connection string for the engine's `LISTEN` connection (one connection), for pools behind PgBouncer in transaction mode. |
| `LockHold` | 1h | How long past its `ttl` an execution keeps a lock while one of its activities waits (in its queue, for a retry, or for another engine after a crash), or while its next task waits in the queue (events delivered, not claimed yet). Bounds how long a forgotten workflow keeps others out; negative: the lock lapses after its `ttl`. |
| `StuckTimeout` | 10s | How long workflow code may run without yielding. Once: handed back; twice at the same point, on any engine (counted in the database): parked (`ErrStuck`); `Resume` resets the count. Raise it if workflow code legitimately computes for long. |
| `EagerWindow` | 200ms | How long an execution waits for its eager activities before letting signals and timers in. |

Measured costs and their causes are in [benchmarks.md](benchmarks.md).

## Shutdown and deploys

- **`Stop(ctx)` drains.** It stops claiming, waits for running tasks until
  `ctx` is done, and cancels the rest. Their leases are released or left to
  expire, and another engine resumes them. Sticky executions are released at
  once, so the next engine does not wait `StickyFor`.
- **Claims at `Stop`.** No new claim is started after `Stop` has begun,
  apart from one already being issued (bounded by the grace period below).
  A claim already in flight may commit in the database even if canceled, so `Stop`
  lets it complete, for up to 1 second (or until `ctx` is done, if sooner),
  and the tasks it claimed run as part of the drain. A claim still running
  then (a database out of reach) is canceled: the stop of an idle engine
  stays prompt, and `Stop` returns `nil` when no task was interrupted.
  Tasks such a canceled claim may have committed stay leased to the stopped
  engine until `LeaseTTL` passes, then another engine takes them.
- **Rolling deploys are safe:**
  - executions of a version the new binary lacks are handed to old
    engines;
  - activities run at least once across the switch;
  - compensations continue on any engine that has the compensators by
    name.
- **Changing a workflow's code.** Bump its version and keep the old
  definition until its executions have ended. To find them:

  ```sql
  SELECT workflow, version, status, count(*)
  FROM flows.execution
  WHERE status IN ('running', 'compensating', 'needs_attention')
  GROUP BY 1, 2, 3;
  ```

## Retention

Ended executions stay, with their history, until purged. The engine has no
background retention: call `PurgeEnded` from a periodic job on one process.

```go
n, err := engine.PurgeEnded(ctx, 30*24*time.Hour, 1000) // ended over 30 days ago, up to 1000 per call
```

- **What is deleted.** Purging cascades to history, pending events, activity
  tasks and locks.
- **What is kept.** Descendants of an active execution (running,
  compensating or parked) are kept, children and grandchildren alike,
  because compensation may reach them.
- **Ids.** A purged instance id can be started again. Until then,
  `Workflow.Start` with that id returns the old execution.
- **`idem` tables.** Purge them with `idem.Purge`, using an age well above
  your longest retry and compensation horizon.

## Monitoring

- **Metrics and traces:** `otelflows.NewObserver` and `otelflows.NewTracer`.
  - `flows.activity.attempts` and `flows.activity.duration` are labeled by
    activity, outcome (`completed`, `failed`, `retrying`) and compensation.
  - `flows.runs.ended` is labeled by workflow and status.
  - Traces follow executions. `Workflow.Start` emits a short span
    `flows.start <workflow>`, a child of the span in the caller's context
    (a new root without one). Its span context (26 bytes) is stored with the
    execution (column `trace`, migration 00015) and returned by the claims,
    so every workflow task and activity attempt, in any process, is a child
    of it. A child workflow's start span is a child of the parent's task
    that started it.
  - A trace lasts as long as its execution. An execution that waits for
    days (timers, signals) makes a trace that long: some backends cap trace
    duration or size, and tail samplers give up waiting for its end.
  - Continue-as-new starts a new trace: the next execution's start span is
    a new root (`flows.continued=true`) with a span link to the previous
    execution's start span. A workflow that loops through continue-as-new
    makes one bounded trace per execution, chained by links, never one
    trace without end.
  - A start that creates nothing ends with `flows.outcome=existing`: a
    second `Workflow.Start` of an instance id (not an error; nothing is
    stored, the execution keeps its own trace), or a child whose instance
    id is taken (error status; the child fails with `ErrInstanceExists`).
    A start whose completion was refused because signals arrived meanwhile
    ends with `flows.outcome=rerun` (not an error): the task runs again
    and starts it again.
  - Sampling is decided once, at the start, by your sampler; the decision
    stored with the execution holds for all its spans (`ParentBased`). The
    trace state is not stored: vendor entries and OpenTelemetry's
    consistent-probability threshold (`ot=th`) do not reach the later
    spans.
  - An attempt or task whose lease was lost ends with an error status and
    `flows.outcome=lease_lost`; the attempt that replaces it is in the same
    trace. Spans carry `flows.instance_id` and `flows.execution_id`.
  - Executions started before the upgrade, or without a tracer, have no
    stored trace context: their spans are roots.
- **Alerts:**
  - any execution in `needs_attention`;
  - a growing count of `compensating` executions;
  - a rising `retrying` rate on one activity.
- **Logs:** pass `Options.Logger`. Through `slog`, the engine logs failures
  to claim, record or renew at warning level. At error level it logs parks
  for non-determinism, panics, failed compensation tasks, and workflow tasks
  handed back.

Useful queries:

```sql
-- Parked executions and why.
SELECT instance_id, workflow, version, err, err_kinds, created_at
FROM flows.execution WHERE status = 'needs_attention';

-- Work that is due but not taken (should stay near zero).
SELECT queue, count(*) FROM flows.execution
WHERE status IN ('running', 'compensating') AND ready_at <= now()
  AND (locked_until IS NULL OR locked_until < now())
  AND (not_before IS NULL OR not_before <= now())   -- handed back for a while
  AND (sticky_until IS NULL OR sticky_until < now()) -- reserved for one engine
GROUP BY queue;

-- Activities waiting, by queue.
SELECT queue, count(*) FROM flows.activity_task GROUP BY queue;
```

The schema is internal and may change between versions. Use it to observe,
and write through the engine's API.

## Incidents

| Symptom | Cause | Action |
| --- | --- | --- |
| `needs_attention`, `err_kinds` has `compensation_failed` | A compensator gave up after its retries. | Fix the cause (data, downstream system), then `engine.RetryCompensation(ctx, id)`. The failed compensator runs again, then the remaining ones. |
| `needs_attention` for wrong code (`err_kinds` has `non_deterministic` or `stuck`; the `Handle.Wait` error matches `ErrNonDeterministic` or `ErrStuck`) | The deployed code no longer matches the history (usually a workflow changed without a version bump), or it blocks without yielding. | Deploy code that matches the history (the old code under the old version), or that yields, then `engine.Resume(ctx, id)`: it replays and runs on (parked again if it still does not match). A stuck attempt's goroutine runs until its process exits: restart the processes. `Terminate` is the alternative; it runs no compensation. |
| An execution stays `compensating` | It waits for an activity that ignores its context, runs on a queue no engine serves, or retries for a long time. | Check `flows.activity_task` for the execution. `Terminate` ends it without compensation. |
| An execution stays `running` with nothing in progress | It waits for a signal, a timer or a child, as designed. | Send the signal, or `Cancel` it, which lets the workflow clean up and compensate. |
| Work resumes slowly after a crash | Leases of the dead engine run until `LeaseTTL`. | Lower `LeaseTTL` if recovery time matters more than renewal traffic. |
| A workflow fails with `ErrLockLost` | Its lock lapsed while it slept or waited longer than the lock's `ttl`, and another execution took it. | Choose a `ttl` above the expected sleeps and awaits; activities waiting in a queue or for a retry, and workflow tasks waiting in the queue, are covered by `LockHold` (raise it for longer waits). |
