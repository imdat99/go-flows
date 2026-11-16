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
- **Poolers.** Behind PgBouncer in transaction mode, `LISTEN` receives
  nothing. The engine still works on its own polls (up to `PollMax` of extra
  latency per hand-off). Prefer a direct or session-mode connection for
  engines. Run `Migrate` over a direct connection too (it holds an advisory
  lock on one connection while goose uses others: the pool needs
  `MaxConns >= 3`).

## Tuning

| Option | Default | Effect |
| --- | --- | --- |
| `LeaseTTL` | 30s | How long a dead engine holds its work. Leases renew every third of it. Shorter: faster recovery, more renewals. |
| `MaxWorkflowTasks`, `MaxActivities` | 64, 64 | Concurrency per engine. |
| `PollMax` | 1s | The longest idle poll interval. Notifications and local due times wake pollers earlier. |
| `StickyFor`, `CacheSize` | 10s, 1000 | Keep a replayed execution in memory, with its next task on this engine. `CacheSize: -1` disables the cache (every task replays). |
| `DisableEager` | false | Run every activity through the queue instead of in the scheduling engine. |
| `StuckTimeout` | 10s | How long workflow code may run without yielding. Once: handed back; twice at the same point: parked (`ErrStuck`). Raise it if workflow code legitimately computes for long. |
| `EagerWindow` | 200ms | How long an execution waits for its eager activities before letting signals and timers in. |

Measured costs and their causes are in [benchmarks.md](benchmarks.md).

## Shutdown and deploys

- **`Stop(ctx)` drains.** It stops claiming, waits for running tasks until
  `ctx` is done, and cancels the rest. Their leases are released or left to
  expire, and another engine resumes them. Sticky executions are released at
  once, so the next engine does not wait `StickyFor`.
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
  - Spans are roots, tied together by `flows.instance_id`.
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
| A workflow fails with `ErrLockLost` | Its lock lapsed while it slept or waited longer than the lock's `ttl`, and another execution took it. | Choose a `ttl` above the expected waits (queue waits, retry delays, sleeps). |
