# Changelog

All notable changes to go-flows. Versions follow semantic versioning; the
release workflow tags them from the commit messages
(`.github/scripts/next-version.sh`).

Each release lists the migrations it adds. `Migrate` applies them forward
only; run it (or let the first engine run it) before the new binaries take
work. Upgrade notes say what a rolling deploy from the previous release
must take care of.

## Unreleased

### Upgrade notes

- No new migration yet.

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
