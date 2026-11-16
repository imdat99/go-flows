# Guarantees

[Tiếng Việt](vi/bao-dam.md)

What go-flows promises, and what it leaves to the application. Every item
refers to tests: the GW-* entries are bugs of go-workflows that go-flows
reproduces and fixes ([regressions.md](regressions.md)), and `test/chaos`
kills, freezes and races real processes.

## What the engine guarantees

**One running execution per instance id.** A partial unique index on
`instance_id` covers the active statuses. Concurrent starts of the same id
create one execution, and the others get a handle on it (GW-1).

**Workflow code runs as if exactly once.** The function is replayed, but each
recorded decision is taken once: an event is appended under the execution's
lease, with a unique `(execution, seq)`.

**Activities run at least once, and their outcome is recorded once.** An
attempt whose lease was lost (its process paused, partitioned or died) is
run again elsewhere, and its late result is refused by fencing (chaos test B).
Use the `Key` to make the effect happen once (`idem.Do`).

**Fencing.** Every write made for a running execution or an activity task
checks `owner_id` and `lease_version` on the locked row:

- zero rows means `ErrLeaseLost`, never a silent success;
- a lease is taken only once it has expired, and is never granted again while
  live, not even to the same owner;
- every acquisition increments the version, so a stale writer with a
  recycled owner name is still refused (GW-2).

**One transaction per checkpoint.** Completing a workflow task is one
transaction. It writes the history, consumes the delivered events, and
creates the activity tasks, timers, children and locks. It also renews or
releases the lease and runs the hook. Nothing is half written.

**Database time only.** Leases, timers, retry delays, lock TTLs and
retention use Postgres `now()`. Process clocks can drift without effect
(GW-5). Workflow code sees the time of its task (`flows.Now`), the same on
replay.

**Non-determinism parks, never fails.** A replay that diverges from the
history parks the execution as `needs_attention` (GW-11). Diverging means a
different call type, name, argument hash or timer duration, or a replay
shorter than the history. An unknown workflow version is handed back to
another engine rather than failed (GW-12).

**Panics never escape.** Panics in activities, compensators, workflow code,
coroutines and signal decoding are recovered and become errors (`ErrPanic`)
(GW-10); a panicking activity counts as uncertain. A panic in a hook rolls
its transaction back (the end is retried later, or the error returned to
`Terminate`'s caller). Panics in observers and
tracers are recovered and logged.

**Signals are not lost.** A signal sent to a running execution is stored in
the same transaction that checks the execution is active. It is delivered in
arrival order, with no silent limit (GW-14). It survives a continue-as-new,
including one that races with it (GW-3). A signal to an ended instance fails
with `ErrNotActive`.

**Compensation completes.**

- Once an execution compensates, its compensators run in reverse order of
  the activities' outcomes, one at a time, each until it succeeds or its
  policy gives up.
- A crash restarts the compensation where it stopped. A compensator may run
  more than once, so it must be idempotent; chaos test D checks that the undo
  takes effect once.
- A cancel received during compensation is accepted and has no effect.
- A compensator that gives up parks the execution (`ErrCompensationFailed`)
  until `RetryCompensation`.

**Cancellation reaches activities.** Canceling a workflow, or the context of
an activity, cancels the running attempt's context through the heartbeat
(GW-13).

**Locks are exclusive while held.** Only one execution holds a key. A holder
whose lock lapsed and was taken fails with `ErrLockLost` before it can
complete as if it had held the lock throughout.

**Migrations are safe to run concurrently.** `flows.Migrate` takes an
advisory lock and applies the embedded goose migrations once (GW-7).

## What it does not guarantee

- **Exactly-once side effects.** Activities run at least once; deduplicate
  by `Key` in the module that performs the effect (`idem`).
- **That a compensator has something to undo.** For an uncertain activity
  (`CompensationUncertain`), the compensation data may be the zero value.
- **Ordering between instances.** Executions of different instances run
  concurrently, in no particular order. Locks give mutual exclusion, not
  queueing: waiters are not served in order.
- **Latency bounds under failure.**
  - A crashed engine's work resumes when its leases expire (`LeaseTTL`,
    default 30 s), at the next poll (`PollMax`, default 1 s).
  - Delayed work that another engine committed can be found up to `PollMax`
    late. So can work that arrives without notifications, for example
    behind a transaction-mode pooler.
- **Determinism of code you change.** Changing a deployed workflow version's
  sequence of calls parks its running executions. Deploy changes as a new
  version.
- **Recovery from a non-determinism park.** No engine call reopens such an
  execution today, and redeploying the old code does not either (it only
  stops further parks). The way out is `Terminate`, which runs **no
  compensation**: activities that completed keep their effects, so undo them
  by hand or let the new instance account for them. (`RetryCompensation`
  applies only to failed compensations.)
- **Progress of a workflow that never yields.** Workflow code that blocks
  without calling the engine (a busy loop) is not parked: its task is handed
  back, and it is retried again and again. The engine logs it at error
  level. Each attempt also leaves its goroutine spinning (nothing can stop
  it), so CPU use grows until the process restarts: fix and redeploy.
- **Size limits.** Payloads are stored as JSON in `bytea` columns. Large
  inputs, outputs and signals cost replay time and memory; keep them to IDs.
