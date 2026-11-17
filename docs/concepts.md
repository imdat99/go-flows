# Concepts

[Tiếng Việt](vi/khai-niem.md)

go-flows is a library. The application embeds an `Engine`, and Postgres (schema
`flows`) is the only shared state: any number of processes can run engines on
the same database.

## Workflow, execution, instance id

A workflow is a Go function defined once at package level:

```go
var placeOrder = flows.DefineWorkflow("place-order", 1, func(c flows.Ctx, o Order) (Receipt, error) { ... })
```

- **Name and version are recorded.** Each execution stores them, and only an
  engine that has that name and version runs it. An engine that lacks the
  version hands the task back so that another engine can take it, for example
  during a rolling deploy.
- **The instance id is the workflow's idempotency key.** `placeOrder.Start(ctx,
  engine, "order-42", o)` creates an execution. Starting the same id again
  returns a handle on that execution, whether it is running or ended, until
  retention purges it. If the id belongs to another workflow, Start fails with
  `ErrInstanceConflict`.
- **An instance can have several executions.** `ContinueAsNew` ends one
  execution and starts the next under the same instance id. `Handle.Wait`
  follows the chain and returns the last execution's result.

Statuses of an execution:

| Status | Meaning |
| --- | --- |
| `running` | Workflow code runs or waits (activities, timers, signals, children). |
| `compensating` | It failed or was canceled; compensators are running. |
| `needs_attention` | Parked: its code is wrong (replay diverged from the history, `ErrNonDeterministic`; or it ran without yielding, `ErrStuck`), its task never completes (its engines crash mid-task, `ErrCrashLoop`), or a compensator failed after its retries (`ErrCompensationFailed`). Nothing runs until an operator acts: `Engine.Resume` after deploying fixed code (or fixing the crash), `Engine.RetryCompensation` after fixing the compensation's cause. |
| `completed` | The workflow returned a result. |
| `compensated` | It failed, and at least one compensator ran (its completed activities were undone). A child reopened for its parent's compensation also ends here. |
| `failed` | It failed with nothing to compensate. |
| `canceled` | It was canceled and returned the cancellation error; its completed activities, if any, were undone. |
| `terminated` | `Engine.Terminate` stopped it, without compensation. |
| `continued` | It continued as new: the next execution of the instance runs on. |

## Event history and replay

Every decision of the workflow is recorded as an event in `flows.history`.
These include an activity scheduled and its result, a timer started and
fired, a signal received, a child started and its result, and a lock taken.

The engine does not save the workflow's Go state. To continue an execution, it
runs the function again from the top and replays the history: each recorded
call returns its recorded result without running, until the function reaches
the point where it stopped. New calls are recorded and executed.

This is why workflow code must be **deterministic**:

- branch only on the input and on results returned through `flows.Ctx`;
- use `flows.Now`, `flows.Rand`, `flows.SideEffect`, `flows.Sleep`,
  `flows.NewTimer` instead of `time.Now`, random numbers, `time.Sleep`;
- use `flows.Go`, `flows.Select`, `flows.WaitGroup` and signals instead of
  goroutines, `select`, channels and `sync`;
- do no I/O: I/O belongs in activities.

`flows.InstanceID` and `flows.ExecutionID` give the running workflow its own
ids, the same on replay, for activity inputs or idempotency keys:
continue-as-new keeps the instance id and starts a new execution id.

The `flowsvet` analyzer (module `analyzer/`) reports these mistakes.

The replay checks each recorded call against the code: its type, name,
argument hash and timer duration. It also fails a replay that ends before the
history does. On any mismatch the execution is **parked** as
`needs_attention` with `ErrNonDeterministic`. It is never failed, because
failing would compensate work that did happen; once code matching its
history is deployed, `Engine.Resume` runs it again. To change a workflow's
sequence of calls, deploy it as a new version and keep the old definition
registered until its executions have ended. Workflow code that runs without
yielding to the engine for `StuckTimeout` (a blocking call, a busy loop) is
handed back once (the process may only have stalled), and parked the same
way, with `ErrStuck`, when it happens again at the same point. An event type the binary does not know (written by a newer
version) hands the execution back to another engine instead.

A replayed execution is cached in the process that ran it (`CacheSize`,
`CacheBytes`), and
its next task stays on that process for `StickyFor`. A cache hit skips the
replay; a miss replays. Both give the same result.

## Activities and keys

An activity is the side-effecting call, defined with an optional compensator:

```go
var charge = flows.DefineActivity("charge",
	func(ctx context.Context, k flows.Key, o Order) (chargeID string, comp string, err error) { ... },
	func(ctx context.Context, k flows.Key, chargeID string) error { ... }, // compensator, or nil
	flows.WithRetry(flows.Exponential(5, time.Second, time.Minute)),
	flows.WithTimeout(30*time.Second))
```

- **Activities run at least once.** A crash or a lost lease means another
  engine runs the attempt again.
- **Each activity gets a `flows.Key`**
  (`instanceID:scheduleID:name:executionID`). The key is the same on every
  attempt and replay, and its compensator gets it too, so a module
  deduplicates its effect on it (package `idem`): the effect then happens
  once. It names the execution, so the executions of a continue-as-new chain
  (whose schedule ids restart at 1) and an instance id reused after retention
  never share a key. Executions started before migration `00016` (v0.1.0)
  keep the older `instanceID:scheduleID:name` for their whole life. Treat the
  key as opaque.
- **Retries happen on the activity task** (`RetryPolicy`; default
  `DefaultRetry`: 5 attempts, delays from 1 s doubling to 1 min), not in the
  workflow. The history records only the final outcome, so changing a policy
  is safe.
  - `flows.Permanent(err)` stops the retries.
  - `WithTimeout` bounds each attempt.
- **Outputs, inputs and compensation data are JSON.** Strings must be valid
  UTF-8 (invalid bytes become U+FFFD): pass binary data as `[]byte`. Keep
  them small: IDs,
  not documents.

**Eager execution.** When a workflow task schedules an activity of a
queue the engine runs, and the engine has a free slot, the activity task is created already leased to that
engine, in the same transaction, and runs at once. When it completes, the
workflow continues in the same round trip. Polling is used only for recovery
and for work started elsewhere.

## Compensation (saga)

When a workflow returns an error, or is canceled and returns an error, the
execution becomes `compensating`. The engine then undoes the activities that
completed, **latest first, one at a time**, with the compensation data each
returned and its original `Key`:

- **Planned from the history.** The compensation does not need the workflow
  function: any engine that has the compensators by name can run it.
- **Uncertain activities are compensated too.** These are activities that
  crashed, timed out, panicked or were canceled mid-attempt and may have had
  their effect. The compensator sees `flows.CompensationUncertain(ctx) ==
  true` and must check by key whether there is anything to undo
  (`idem.Abort`).
- **Children are compensated too.** A child workflow that completed is
  reopened and undoes its own activities, in reverse order with the parent's
  activities.
- **Failures park the execution.** Compensators retry with
  `WithCompensateRetry`. When one gives up, the execution is parked
  `needs_attention` with `ErrCompensationFailed`.
  `Engine.RetryCompensation` resumes it after the cause is fixed.
- **How it ends.** A failed workflow ends `compensated` when a compensator
  ran, otherwise `failed`; its `Handle.Wait` error matches `ErrCompensated`
  in the first case. A canceled workflow ends `canceled` either way (see
  below). The `*RunError` from `Wait` also matches the recorded error
  kinds.

## Waiting: timers, signals, children

- **Timers are durable.** `flows.Sleep(c, d)` and `flows.NewTimer(c, d)`
  record a timer due at database time. No goroutine or lease is held while it
  waits; any engine fires it.
- **Signals queue in order.** `Engine.Signal(ctx, id, name, payload)` stores
  a signal for the running execution. The workflow reads it with
  `flows.Await[T](c, name, timeout)` or `flows.Signals[T](c, name)`, in
  arrival order, with none dropped. A signal sent before the workflow listens
  is kept. Unread signals move to the next execution on `ContinueAsNew`.
- **Children are workflows started from a workflow.** `wf.Child(c, id, in)`
  returns a `Future`. Canceling the parent cancels its running children,
  and so does its end. What the workflow starts without awaiting it in the
  task that ends it:
  - **when it ends** (completed, failed, canceled): neither children nor
    activities are started;
  - **when it continues as new:** its children are started and run on,
    detached, like the children it started before (not compensated with
    the chain); its activities are not started.

  Anything not started is logged as a warning, and `flowtest` fails the
  test.
- **`flows.Select` waits for the first ready case.** Its cases are futures
  and signal channels, tried in order (deterministic), with an optional
  default.

## Cancel and terminate

- **`Engine.Cancel` asks the workflow to stop.** It cancels the workflow's
  root context, and with it its activities' contexts, timers and children.
  - The workflow decides what to do. It can clean up in
    `flows.Disconnected(c)`.
  - If it returns the cancellation error (`ErrCanceled`, as returned by
    the canceled calls, possibly wrapped), its activities are compensated and
    it ends `canceled`; `Handle.Wait`'s error matches `ErrCanceled`.
  - If it returns another error, it ends like any failure: `compensated` or
    `failed`. If it returns a result, it ends `completed`.
- **`Engine.Terminate` ends the execution at once**, without running workflow
  code or compensation. It is meant for stuck executions.

## Locks

`flows.Lock(c, key, ttl)` takes a business lock for the rest of the
execution (`ttl` is raised to at least the engine's `LeaseTTL`):

- **Release:** at the end, or at continue-as-new.
- **Kept alive while the execution works:** while it runs a task or one of
  its activities runs.
- **Kept up to `Options.LockHold` (default 1h) past `ttl`** while one of its
  activities waits (in its queue, for a retry, or for another engine after a
  crash), or while events wait for its next task (an activity's result, a
  signal, a timer that fired) because no engine has claimed it yet.
- **Lapses after `ttl` otherwise:** while the execution sleeps, waits or is
  dead. Another execution may then take the lock, and the first one fails
  with `ErrLockLost` at its next task.
- **Waiting:** `flows.LockWait(policy)` waits for a busy lock instead of
  failing with `ErrLockHeld`. Waiters are queued and served in the order
  they asked; a release wakes the next one at once.

## Leases and fencing

- **Owner and lease version.** The engine running an execution's task (or an
  activity task) holds its lease: `owner_id`, `lease_version`,
  `locked_until`.
- **Renewal.** A heartbeat renews all of an engine's leases every
  `LeaseTTL/3`, in one round trip.
- **Fencing.** Every write locks the row and checks owner and version. An
  engine that lost a lease (paused, partitioned) cannot write any more: zero
  rows means `ErrLeaseLost`, never a silent success.
- **Database time.** All lease, timer and retry arithmetic uses Postgres
  `now()`, never the process clock.

## Hooks, observers, tracing

- **`Options.Hook`** (`RunHook`) runs inside the transaction that records an
  end or a park. What it writes commits with the status, which makes it the
  place for a transactional outbox. It belongs to the engine that records
  the end: set the same hook on every engine, client-only ones included
  (`Terminate` from a client-only engine runs that engine's hook).
- **`Options.Observer`** receives activity attempts and ends after the fact,
  for metrics.
- **`Options.Tracer`** wraps starts, workflow tasks and attempts in spans;
  the trace context taken at the start is stored with the execution, so
  all its spans share the caller's trace.
  `otelflows` implements both the tracer and the observer with
  OpenTelemetry.
- Unset, they cost nothing.
