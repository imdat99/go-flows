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
run again elsewhere, while its retry policy allows another attempt (see
below), and its late result is refused by fencing (chaos test B). Use the
`Key` to make the effect happen once (`idem.Do`).

**Work that crashes its process is not retried forever.**

- A lost attempt counts against the activity's retry policy. Once the
  attempts reach the policy's limit and the last one was lost (the activity
  kills its process: out of memory, `os.Exit`, a fatal runtime error), the
  next engine does not run it again. It fails it permanently with
  `ErrAttemptsExhausted`, uncertain (it is compensated), as any final
  failure (chaos test I). An activity with `Attempts <= 1` therefore fails
  after a single lost attempt: an engine crash, a `SIGKILL` during a deploy,
  or a database outage longer than `LeaseTTL`.
- "Lost" means only that the lease expired without a result: the attempt
  may still be running, in a process that is partitioned or paused (GC, a
  frozen VM). The compensator can then run while that attempt's effect is
  still landing. Key effects by the `Key` (`idem`), and have the
  compensator fence the key (`idem.Abort`) so that a late effect is
  refused.
- The limit is recorded when the task is created, from the retry policy of
  the binary that schedules the activity. If that binary does not define
  the activity (it runs elsewhere, `WithQueue`), there is no limit: lost
  attempts are retried as before.
- A workflow task claimed more than `MaxTaskClaims` times (default 10)
  without completing parks its execution (`ErrCrashLoop`), never fails it;
  `Engine.Resume` runs it again once the cause is fixed. A completed task
  starts the count over; a hand-back (an engine that does not define the
  workflow's version) undoes only its own claim, so a rolling deploy whose
  new version crashes the new engines still parks.
- Known gap: a compensating execution is not counted. Its tasks run no
  workflow code (its compensators are activity tasks, bounded as above),
  but planning a compensation loads the history; if that crashes the
  engines (a history too large for their memory), it is retried forever.

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
shorter than the history. Code that runs without yielding parks the same
way (`ErrStuck`), when it does so twice at the same point. `Engine.Resume` runs such an execution again once fixed
code is deployed. An unknown workflow version, or an unknown event type, is
handed back to another engine rather than failed (GW-12).

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
  concurrently, in no particular order. Locks are the exception: waiters
  with `LockWait` are served in the order they asked, at each release: the
  first waiter still waiting (not parked) is woken and, for one `ttl`, the
  free lock is its only (others asking are refused, even if it died). A
  lock lapsing, or
  a woken waiter that leaves without the lock, wakes nobody: the next
  waiter asks at its own next `LockWait` attempt, and the lock is anyone's
  meanwhile.
- **Latency bounds under failure.**
  - A crashed engine's work resumes when its leases expire (`LeaseTTL`,
    default 30 s), at the next poll (`PollMax`, default 1 s, and at most a
    third of `LeaseTTL`: idle engines poll at least that often, since
    nothing announces a lease running out).
  - Work that arrives without a notification (for example behind a
    transaction-mode pooler) can be found up to `PollMax` late. Delayed work
    is claimed when it falls due: each claim also asks the store when the
    next work of its queues does, and hand-backs, retry delays and the
    executions an engine releases at `Stop` are announced to the others. A
    timer stays with the engine that committed it while that engine runs;
    if it crashes, its work waits for the recovery above.
- **Determinism of code you change.** Changing a deployed workflow version's
  sequence of calls parks its running executions. Deploy changes as a new
  version.
- **That a parked execution resumes by itself.** Deploying fixed code does
  not resume an execution parked for its code: call `Engine.Resume`. The
  other way out, `Terminate`, runs **no compensation**: activities that
  completed keep their effects.
- **Stopping code that never yields.** A stuck workflow is parked, but its
  goroutine cannot be stopped: it keeps running (spinning, for a busy loop)
  until the process exits. Fix the code and restart the processes.
- **Size limits.** Payloads are stored as JSON in `bytea` columns. Large
  inputs, outputs and signals cost replay time and memory; keep them to IDs.
