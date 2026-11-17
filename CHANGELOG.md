# Changelog

All notable changes to go-flows. Versions follow semantic versioning; under
0.x a breaking change bumps the minor version, anything else the patch.
Releases are made by hand with the release workflow
(`.github/workflows/release.yml`): its notes are the section of this file
headed by the tag. The `otelflows` and `analyzer` modules are tagged
`otelflows/vX.Y.Z` and `analyzer/vX.Y.Z`, with sections headed the same
way.

Each release lists the migrations it adds. `Migrate` applies them forward
only; run it (or let the first engine run it) before the new binaries take
work. Upgrade notes say what a rolling deploy from the previous release
must take care of.

## Unreleased

### Added

- `examples/modulith`: a modular monolith blueprint (five modules, cross-module sagas, split deployments by queue).
- `flows.InstanceID(c)` and `flows.ExecutionID(c)`: workflow code reads
  its own instance id (kept by continue-as-new) and execution id;
  deterministic under replay, no allocation (T11.9).
- An engine logs once, when it is created, if it lowers `PollMax` to
  `LeaseTTL`/3
  (T11.8).
- Work that crashes its process (out of memory, `os.Exit`, a fatal runtime
  error) is no longer retried forever across the cluster (T11.3):
  - an activity task records its retry policy's total attempts when it is
    scheduled; once the attempts reach it and the last one was lost (its
    lease expired without a result), the next engine does not run it but
    fails it permanently with `flows.ErrAttemptsExhausted` (kind
    `attempts_exhausted`), uncertain, so the workflow compensates as for any
    final failure (a compensator so exhausted parks the execution as
    `compensation_failed`);
  - `Options.MaxTaskClaims` (default 10, negative: no bound): a workflow
    task claimed more than that many times without completing parks its
    execution as `needs_attention` with `flows.ErrCrashLoop` (kind
    `crash_loop`), never fails it; `Engine.Resume` resumes it. A completed
    task starts the count over; a hand-back (an engine that does not
    define the workflow's version) undoes only its own claim. Compensating
    executions are not counted (a known gap: planning a compensation loads
    the history).

### Changed

- Releases are made by hand (release workflow, `workflow_dispatch` with an
  explicit version checked against this file); pushing to main no longer
  tags. Submodules are tagged `otelflows/vX.Y.Z` and `analyzer/vX.Y.Z`.
- `otelflows` requires a released go-flows (v0.1.2) instead of `v0.0.0`
  with a `replace`, so `go get github.com/imdat99/go-flows/otelflows`
  works; local development uses `otelflows/go.work`.
- A lost activity attempt (its engine died, was partitioned or paused, or
  lost its lease) now counts against its `RetryPolicy` (T11.3). An activity
  with `Attempts <= 1` now fails (uncertain, compensated) after a single
  engine crash, a `SIGKILL` during a deploy, or a database outage longer
  than `LeaseTTL`, instead of running again. A lost attempt may still be
  running when the activity is failed and compensated: key effects with
  `idem` and fence the key in the compensator (`idem.Abort`). The limit
  applies only when the binary that schedules the activity defines it.

### Fixed

- When the database stopped responding without closing connections
  (failover, network drop without RST), the lease heartbeat hung on the
  renewal and never gave up leases, so a running activity kept going past
  its lease while another engine ran the next attempt concurrently. Each
  renewal now has a deadline of one heartbeat interval (`LeaseTTL`/3), and
  leases not renewed for `LeaseTTL - LeaseTTL/3` are given up (their
  context canceled with `ErrLeaseLost`) on a timer that does not wait for
  the renewal to return (T11.1).
- `Engine.Stop(ctx)` honours ctx's deadline when the database stops
  answering (a failover, a partition without RST) (T11.2). Once ctx is
  done, Stop waits at most about 1 second more; tasks still blocked are
  abandoned, their leases expire, and another instance resumes them.
  Writes made after a task's context ends (activity outcomes, lease
  releases) are now bounded by about `LeaseTTL` instead of having no
  deadline, so an abandoned task ends by itself within that time; until
  then `pool.Close()` may wait for its connection. An outcome given up this
  way is redelivered (the activity runs again once its lease expires).
  Behaviour change: Stop likewise gives up, 1 second after the deadline, on
  activities that ignore their context's cancellation, where it used to
  wait for them indefinitely; such an activity may run again elsewhere
  while it still runs in the stopped process. Sticky executions are still
  handed over at Stop (with a budget of their own, up to 5 seconds, which
  may still run briefly after Stop returns).

### Migrations

| Migration | Adds | For |
|---|---|---|
| `00017_crash_loops` | `activity_task.max_attempts`, `execution.claims` | the attempt limit of a task, and the claims of an execution since its last completed task, less its releases (T11.3) |

### Upgrade notes

- **`00017` (T11.3)** adds two integer columns with a constant default:
  metadata only, no table rewrite and no long lock.
- Activity tasks inserted by earlier binaries (and by binaries that do not
  define the activity) have `max_attempts` 0: no limit, the earlier
  behaviour.
- Earlier binaries neither count, decrement nor reset `execution.claims`.
  Each task lost by a new engine and then completed by an old one leaves
  the count one higher; this is harmless during a rolling deploy (the
  default bound is 10, and the first task a new engine completes resets
  it).
- If engines are killed mid-activity during deploys, give the activities
  that must survive that at least two attempts.

## v0.1.2 (2026-11-17)

A simplification of the v0.1.1 code (shared helpers, one `next_due`
subquery in the statement that completes a workflow task). No API,
schema or behaviour change, except:

### Fixed

- The memory store's `Executions` (used by `flowtest`) returns a copy of
  each execution's trace context instead of sharing it with the store.

### Migrations

None (the schema is still `00001` to `00016`).

## v0.1.1 (2026-11-17)

> **Breaking changes, released as a patch version by mistake.** The
> release workflow of the time derived the version from Conventional
> Commit prefixes (`feat:`, `fix:`, `type!:`); this repository's commits
> (`T10.x: ...`) have none, so every release was a patch. Under 0.x
> semantic versioning this release should have been **v0.2.0**. Published
> tags are not moved: v0.1.1 and v0.1.2 stay as they are, and **the next
> release will be v0.2.0**. Releases are now made by hand, with the version
> chosen when releasing (T11.4). Read the breaking changes and the upgrade
> notes below before upgrading from v0.1.0.

### Breaking changes

- **`flows.Tracer`** has a new method `StartWorkflow` (its end gets whether
  the start created the execution), and `StartWorkflowTask` returns the
  task's context too (T10.10). Custom tracers no longer compile until they
  implement it; `otelflows` does (it needs go-flows v0.1.1 or later).
- **`flows.Key` format** is `instanceID:scheduleID:name:executionID` for
  executions started after migration `00016` (T10.19). Code that parsed
  keys by splitting on `:` must treat them as opaque. See the upgrade
  notes: stop old engines before migrating.
- **Migrations `00013` to `00016` are required**: `Engine.Start` reads the
  key epoch from the database and fails if `Migrate` has not run.
- `flowtest` reports futures abandoned by descendants (children,
  recursively), not only by the instance it ran (T10.11): tests that
  passed may now fail with an abandoned future.
- `flowsvet` reports more non-deterministic code (T10.12): builds that run
  it may report new findings.

### Added

- `Options.CacheBytes`: the sticky executor cache is bounded by an
  estimate of its size too (default 256 MiB, `-1` for a count bound only),
  evicting the least recently used executors (T10.6).
- Traces follow executions (T10.10): the trace context is captured at
  `Workflow.Start` (and when a workflow starts a child), stored with the
  execution and carried to every workflow task and activity attempt;
  `flows.ErrLeaseLost`; `ExecutionID` and `Trace` on `ActivityInfo` and
  `WorkflowTaskInfo`; otelflows marks lost leases `flows.outcome=lease_lost`
  and starts that create nothing `flows.outcome=existing` (`rerun` for a
  start whose task runs again, `flows.ErrRerun`). A trace lasts as
  long as its execution; continue-as-new starts a new trace linked to the
  previous one. The trace state (including `ot=th`) is not kept.
- `flows.StartInfo`; `otelflows` attributes
  `flows.execution_id`, `flows.parent_instance_id`, `flows.continued`
  (T10.10).
- `flowtest` reports the futures abandoned by every descendant of the
  instance it ran, and each ended execution exactly once, at the first
  `Run` whose tree contains it (T10.11).
- `flowsvet` reports every `make(chan ...)` in workflow code and variables
  whose struct type holds a channel or a sync primitive in a field; a
  function literal held in a variable and only passed to `flows.SideEffect`
  is no longer reported (T10.12).
- `examples/outbox`: a transactional outbox with a stub event bus (T10.20).

### Changed

- `flows.Key` carries the execution id (breaking):
  `instanceID:scheduleID:name:executionID` (T10.19). Schedule ids restart
  at 1 in every execution, so the first activity of every execution of a
  continue-as-new chain, or of an instance id reused after retention purged
  the earlier execution, had the same key, and idempotency records keyed by
  it (`idem`, an outbox) silently skipped the later executions' work.
  Executions that exist when migration `00016` runs keep the old format for
  their whole life (see the upgrade notes). `Engine.Start` now reads the key
  epoch from the database and fails if `Migrate` has not run.
- `Engine.Terminate` no longer reads the execution again for the observer,
  and a hooked end no longer costs an extra `SELECT` round trip (T10.9).
- `PollMax` is capped at a third of `LeaseTTL`: idle engines poll at least
  that often, so the work of a crashed engine resumes within about
  `LeaseTTL` instead of waiting for a long `PollMax` (T10.1).
- The due time a claim reports no longer stops at 64 kept leases: a timer
  behind any number of them is found through an index (T10.2). A timer
  falling due while its execution is sticky to the engine that will claim
  it is no longer reported to other engines, which woke for an empty claim
  (T10.3).
- A compensator receives the compensation data of the most recent attempt
  that may have had its effect (a timeout, panic, `ErrUncertain` or
  interrupted attempt that was retried), not the last attempt's: an attempt
  returning the zero value returns no data, and the earlier data is kept.
  Previously a timeout followed by a declined attempt compensated with the
  zero value (T10.7).
- An activity claim leaves one free activity slot for eager launches
  when more than one is free: eager launches that went through the queue
  during a claim fell from about 3% to under 0.1% (T10.21).
- An executor larger than `CacheBytes` is not made sticky (T10.6).
- The memory store (`flowtest`) indexes executions by queue and by parent:
  waking and claiming no longer scan every execution (T10.8).

### Fixed

- A claim in flight when `Engine.Stop` begins is no longer dropped
  (T10.13). The claim was canceled with the polls, yet on Postgres it could
  still commit: its tasks stayed leased to the stopped engine for a whole
  `LeaseTTL`, and other engines found them only at a timed poll. Now no new
  claim is started after `Stop` begins, apart from one already being
  issued; a claim in flight completes within a grace period of 1 second
  (or `Stop`'s deadline, if sooner), its tasks run as part of the drain,
  and a claim still running then (a database out of reach) is canceled, so
  an idle engine stops promptly. `Stop` returns its context's error only
  if running tasks were canceled: an idle engine whose deadline passes
  returns nil.
- Ending an execution (completion, `Terminate`) takes its lock rows in key
  order and `Unstick` its execution rows by id, as every other statement
  does: no deadlock (detected by Postgres and retried) against a claim or
  a lock request. A claim over several queues locks only the executions it
  claims, not up to its limit in each queue, so a lock whose holder sits
  in another queue is no longer refused for the time of that claim. Key
  order is byte order in every statement, whatever the database collation
  (T10.4).
- A lock no longer lapses while its holder's next task waits in the queue
  (an activity's result, a signal or a fired timer delivered, no engine
  free to claim it for longer than the lock's `ttl`): such a holder keeps
  it up to `Options.LockHold` past `ttl`, as while its activities wait
  (T10.5).

### Migrations

| Migration | Adds | For |
|---|---|---|
| `00013_due_owner` | `execution.due_owner`, `execution_due_idx` | due times of kept leases and sticky timers are their owner's (T10.2, T10.3) |
| `00014_kept_comp_data` | `activity_task.comp_data` | compensation data kept across attempts (T10.7) |
| `00015_trace_context` | `execution.trace`, `activity_task.trace` | the trace context of an execution (T10.10) |
| `00016_key_epoch` | table `key_epoch` (one row) | the first execution id whose activity keys carry it (T10.19) |

### Upgrade notes

- **Activity keys (T10.19).** An activity whose attempts or compensation
  span the upgrade must see one key, or `idem` runs its effect twice (a
  retry) or finds nothing to undo (`idem.Abort` in an uncertain
  compensation). So the key format is fixed per execution: `00016` records
  one past the highest execution id allocated so far, and executions below
  it keep `instanceID:scheduleID:name` until they end (their continue-as-new
  successors get the new format). Nothing to do for those. The one gap is a
  mixed fleet: an execution started after `00016` ran gets the new format
  on new engines, but old engines still give its activities the old key, so
  an attempt by an old engine retried or compensated by a new one sees two
  keys. To close it, stop the old engines (`Stop` drains them) before
  running `Migrate` and starting the new ones; with a rolling deploy, the
  exposure is limited to activities of executions started during the
  deploy that an old engine attempted. Code that parsed keys by splitting
  on `:` must treat them as opaque (or strip the trailing `:executionID`).
- **Restores (T10.19).** `flows.key_epoch` is data: a data-only restore
  (`pg_dump --data-only`, logical replication) into a freshly migrated
  database must bring its row along, or set it to one past the highest
  restored execution id
  (`UPDATE flows.key_epoch SET first_execution_id = (SELECT max(id) + 1 FROM flows.execution)`).
  Otherwise the fresh database's value (1) gives every restored execution
  the new key format, and their activities retried or compensated after the
  restore see a key their idempotency records do not have.
- `00013` adds a column with a constant default (no table rewrite) and an
  index built without `CONCURRENTLY`: writes to `flows.execution` wait while
  it builds. On large tables, run the migration in a quiet window.
- While old and new binaries run side by side, a timer behind more than 64
  leases kept by old binaries is found by the timed poll (at most
  `PollMax` late) instead of the reported due time.
- `00014` adds a nullable column with no default: a metadata-only change
  (no table rewrite). Old binaries ignore it; activities they retry keep no
  data, as before.
- `00015` adds nullable `trace` columns with no default (metadata only).
  Executions started before it, or by an old binary during a rolling
  deploy, carry no trace context: their spans are roots, as before
  (T10.10). Activity tasks an old binary inserts for a traced execution
  carry none either, so those attempts' spans are roots too.
- While old and new binaries run side by side, an old binary still takes
  a lapsed lock whose holder's next task waits in the queue (T10.5): the
  holder fails with `ErrLockLost` as before the fix.

## v0.1.0 (2026-11-16)

First release: durable workflows with event history (coroutines, Select,
signals, timers, child workflows, ContinueAsNew), saga compensation by
activity name, business locks tied to the lease, run hooks and observer,
`idem`, `flowtest`, the `flowsvet` analyzer and the `otelflows` tracing
module. See `README.md` and `docs/`.

### Migrations

`00001` to `00012`. The ones added late in development (P9), relevant to
deployments that ran a development build:

| Migration | Adds | For |
|---|---|---|
| `00009_due_indexes` | `execution_not_before_idx`, `activity_task_delayed_idx` | pollers ask when the next work falls due (T9.1) |
| `00010_stuck_seq` | `execution.stuck_seq` | a second stall at the same point parks the execution, on any engine (T9.3) |
| `00011_lock_hold` | `lock.hold_us` | a lock outlives its TTL while its holder's activities wait, up to `Options.LockHold` (T9.4) |
| `00012_lock_waiter` | table `lock_waiter`, `lock.reserved` | `LockWait` waiters served in order, woken on release (T9.8) |

All four are additive (new indexes, columns with constant defaults, a new
table): no table rewrite, and old binaries still run on the new schema.
The indexes of `00009` are not built `CONCURRENTLY`: on large tables, writes
to `flows.execution` and `flows.activity_task` wait while they are built.

### Upgrade notes (from a development build)

**Rule: if your workflows use `Lock`, do not run engines older than T9.8
(commit `56b1d18`) next to engines of this release.** Stop every old engine (`Stop` drains),
then start the new ones. Locks are the part of the engine whose correctness
depends on every engine reading and writing the same lock columns, and the
old code does not know `hold_us`, `reserved` or `lock_waiter`. Running both
side by side has effects such as these (the list is not exhaustive):

- an old engine taking a lock does not clear `lock.reserved`, so a new
  engine can take that lock from the execution holding it: exclusivity is
  lost;
- an old engine's claims extend reserved rows, and its releases wake no
  waiter, so waiters wait for their own timer;
- before T9.4 (`af75012`): a lock row an old engine creates has
  `hold_us = 0` (it lapses at its TTL while its holder's activity waits),
  and an old engine taking over a row keeps its `hold_us`;
- before T9.3 (`fd5c250`): old engines never write `stuck_seq`, so stalls
  are counted per engine, not shared (each still parks on its own second
  stall).

Workflows that do not use `Lock` can be upgraded with a rolling deploy:
besides the `stuck_seq` effect, engines older than T9.5 (`abbd2bb`) lock
rows in another order, so a parent and a child ending together can
deadlock; Postgres detects it and the statement is retried (latency, not
correctness).

**Executions that called `Lock` with `LockWait` are parked.** This holds
whatever the deploy, because it is a change of the history the code
writes. `LockWait` used to issue only the lock request, then sleep between
attempts with a plain timer (`flows.Sleep`). It now issues the request
together with a timer named `flows.lock:<key>` (which a release fires
early) on every attempt but the last, and that timer takes a schedule id
even when the lock is granted at once (it is then canceled). An execution
started under the older code that called `Lock` with `LockWait` (more than
one attempt) therefore replays differently under the new code if it issued
any command after that call, whether it got the lock or was still waiting:
its history does not match and it is parked as `needs_attention` (never
failed).

- Prevent it: while only old engines run, stop starting workflows that use
  `LockWait` and let every execution that used it end; then stop the old
  engines (and old client-only processes) and start the new ones. New executions
  started by the new binary are not affected.
- A parked one stays parked under the new code (`Engine.Resume` replays it
  again, with the same mismatch). The only way out is `Engine.Terminate`,
  which runs no compensation: undo the effects of its completed activities
  by hand if needed, then start the work again under a new instance id
  (the terminated instance keeps its id until retention removes it).

### Corrections

- The comment of migration `00005_unstick` says `sticky_owner` "is cleared
  on the next claim, release or end". A claim does not touch it. The
  sticky columns change only:
  - when a workflow task completes: `sticky_owner` and `sticky_until` are
    set when the execution stays sticky to the engine, and cleared
    otherwise (also when the lease is kept);
  - on a release of the lease, and when the execution ends or is
    terminated: both are cleared;
  - on `Unstick(owner)`: both are cleared, for the executions whose
    `sticky_until` is still in the future.

  So `sticky_owner` can outlive `sticky_until` (it stays set while the
  execution is claimed by another engine after the sticky time ran out).
  The index and `Unstick` are correct. Applied migrations are never
  edited, so the comment stays.
