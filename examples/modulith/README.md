# Modular monolith on go-flows

[Tiếng Việt](README.vi.md)

A blueprint for a real system: several bounded contexts in one Go binary,
coordinated by durable workflows, deployable as one process or split into
several without code changes. Copy the layout and the rules; replace the
stub gateway, carrier, supplier and mail server with your own.

```sh
make up                                   # Postgres on :5433
go run ./examples/modulith/cmd/modulith   # every module, HTTP API on :8080

curl -XPOST localhost:8080/orders -d '{"id":"42","email":"a@example.com","address":"Hanoi",
  "amount":1299,"lines":[{"sku":"sku-apple","qty":2}]}'
curl localhost:8080/orders/42                                    # status, payment, shipment
curl -XPOST localhost:8080/shipments/42/pickup -d '{"tracking":"TRK1"}'   # the carrier's webhook
curl -XPOST localhost:8080/orders/43/cancel -d '{"reason":"changed my mind"}'
curl -XPOST localhost:8080/inventory/sku-pear/adjust -d '{"request_id":"r1","delta":5,"reason":"count"}'
```

Tests: `go test -race ./examples/modulith/...` (the integration test needs
`make up`).

## Layout

```
examples/modulith/
  cmd/modulith/main.go          flags/env, pgxpool, signals, graceful shutdown
  internal/app/                 composition root: migrations, Configure, engine, hook, HTTP API
  internal/platform/
    schema/                     goose migrations per module, versions in <schema>.goose_db_version
    outbox/                     transactional outbox: Stage, Relay, Bus, LocalBus
    deps/                       holder for a module's dependencies (see "Dependencies")
  modules/
    orders/                     fulfilment saga, read model, RunHook, events
    payments/                   authorize/void, capture/refund (activities only)
    inventory/                  Reserve (child workflow), replenishment loop, adjustments
    shipping/                   Ship (child workflow): label, carrier signal, timeout
    notifications/              consumes order events, one workflow per event
```

Each module is one Go package. Its exported identifiers are its public API;
everything else, its tables included, is private.

| Module | Schema | Queue | Public API |
| --- | --- | --- | --- |
| orders | `orders` | `orders` | `Place`, `Cancel`, `Get`, `Hook`, events `TopicCompleted`/`TopicCanceled`/`TopicFailed` + `Event` |
| payments | `payments` | `payments` | activities `Authorize`, `Capture`; `Find`, `ErrDeclined` |
| inventory | `inventory` | `inventory` | workflow `Reserve` (+ `ReserveID`), `Adjust`, `Level`, `StartReplenishment`, `StopReplenishment` |
| shipping | `shipping` | `shipping` | workflow `Ship` (+ `InstanceID`), `ConfirmPickup`, `Cancel`, `Find`, `ErrPickupTimeout`, `ErrCanceled`, `ErrPickupRefused`, `ErrNoShipment` |
| notifications | `notifications` | `notifications` | `Subscribe`, `Sent` |

Every module also exports `Queue`, `Configure` and `Migrate`.

## How an order flows

```
HTTP POST /orders ──> orders.Place ──> workflow orders.fulfil (queue orders)
   1. orders.record                activity      orders
   2. payments.Authorize           activity      payments       undo: void
   3. inventory.Reserve            child         inventory      undo: release (per line)
   4. payments.Capture             activity      payments       undo: refund
   5. shipping.Ship                child         shipping       undo: cancel label
        label, then Select { signal picked-up | signal shipping.cancel | timer PickupWithin }
        then Decide in shipping.shipments (first decision wins, carrier or workflow)
        picked up: MarkPickedUp, retried until it succeeds
      the order: Select { shipment done | signal orders.cancel -> forward to the shipment }
                 then wait for the shipment's real outcome
 end ──> RunHook (same transaction): orders.orders.state + outbox event
     ──> relay ──> bus ──> notifications consumer ──> workflow notifications.send
```

- **Compensation crosses modules by name.** When the carrier does not come
  (`ErrPickupTimeout`) or the customer cancels, the shipping child cancels
  its label, then the engine refunds the capture, reopens the completed
  `inventory.Reserve` child to release its lines, and voids the
  authorization. Each compensator runs on its module's queue, so on the
  processes running that module. Nothing needs the `fulfil` function.
- **The pivot is a row, not a signal.** Whether the carrier has the
  parcel is decided once, in `shipping.shipments`, under a row lock: the
  row leaves `label_created` for `pickup_confirmed` or `canceled`, and
  whoever comes first wins.
  - The carrier's webhook (`ConfirmPickup`, `POST
    /shipments/{order}/pickup`) moves the row to `pickup_confirmed`
    before it signals the workflow. 202 means the pickup is recorded for
    good: the order will complete. If the shipment was canceled first
    (the customer, or the timeout), it gets 409 `ErrPickupRefused`
    (return the parcel); before the label exists, 404 `ErrNoShipment`
    (retry). A pickup is never accepted and then dropped.
  - The workflow, woken by the first signal or its timer, asks for the
    outcome it wants (`Decide`) and gets the row's state. Signals only
    wake it: if the carrier's signal is lost, the timer wakes it and the
    row still says `pickup_confirmed`.
  - From the decision on, the shipment cannot fail into the saga: `Decide`
    and `MarkPickedUp` run on `flows.Disconnected` and are retried until
    they succeed (rounds of attempts, a durable 5-minute sleep between
    rounds; the observer's failed attempts are the alert). With the
    shipping database down for hours after a pickup, the order completes
    late; it is never undone. `MarkPickedUp` has no compensator, and
    `cancelLabel` only cancels a row still `label_created`.
  - Once the order has ended, `orders.Cancel` returns `ErrNotActive` (HTTP
    409).
- **Who decides on a cancel.** `orders.Cancel` signals the order. Sent
  during steps 1 to 4, the signal waits in the channel and the order stops
  itself before the capture or before starting the shipment. During step 5
  only the shipment knows whether the pickup came first, so the order does
  not cancel the child (in go-flows, canceling a child resolves its future
  at once with `ErrCanceled`, without its real outcome). It forwards the
  cancel to the shipment (an activity calling `shipping.Cancel`, because a
  workflow cannot signal another one) and waits for the shipment's
  outcome: canceled, the order is undone; picked up, the order completes
  and the cancel is ignored.
- **Tests of these paths.**
  - flowtest: `TestFulfilCancelAfterPickup` (the cancel reaches the order
    after the pickup reached the shipment), `TestFulfilCancelBeforeShipping`,
    `TestFulfilCancelDuringCapture`, `TestFulfilPickupRecordFailsForLong`,
    `TestShipPickupBeatsCancel`, `TestShipConfirmedPickupBeatsCancelSignal`,
    `TestShipPickupRecordedDespiteFailures`.
  - Postgres: `TestPickupAfterCancelIsRefused`,
    `TestCancelAfterPickupKeepsThePickup`, and `TestDecisionRace` (the
    carrier and the workflow decide at the same time, 20 times: they
    always agree).
  - The integration test confirms a pickup and then cancels through the
    API (the order completes whatever the timing, because the 202 of the
    pickup is final), and posts a pickup after a cancel (409).
- **`Engine.Cancel` is an operator's tool, not a customer cancel.** It
  cancels the order's context: the shipment child's future resolves at
  once with `ErrCanceled`, and the order compensates without waiting for
  the shipment's outcome. After a pickup, that refunds the payment and
  releases the stock while the parcel ships (the shipment itself still
  completes). Use it only for an order that must be stopped whatever it
  costs, and reconcile by hand.

Other features, where to find them:

| Feature | Where |
| --- | --- |
| Activities with compensators, idempotent by `flows.Key` (unique key column, tombstone fencing) | `payments/payments.go`, `shipping/shipping.go` |
| Exactly-once effects with `idem.Do`, uncertain undo with `idem.Abort` | `inventory/activities.go` |
| Child workflows across modules, compensated by reopening | `orders/fulfil.go`, `inventory/workflows.go` (`Reserve`), `shipping/shipping.go` (`Ship`) |
| `Select` over two signals and a timeout timer; a decision made once in a table row; steps retried until they succeed on `Disconnected` | `shipping/shipping.go` |
| `Select` over a child and a signal; forwarding a signal to a child through an activity | `orders/fulfil.go` |
| Long-running loop with `ContinueAsNew` and a stop signal | `inventory/workflows.go` (`replenishCycle`) |
| `flows.Lock` with `LockWait` on a business key | `inventory/workflows.go` (`checkAndOrder`, `adjustStock`) |
| RunHook updating a read model and staging an event in the end's transaction | `orders/hook.go` |
| Transactional outbox, relay with `SKIP LOCKED`, per-key order, backoff | `internal/platform/outbox` |
| Event consumer deduplicating by starting a workflow per event id | `notifications/notifications.go` |
| Queues per module, one binary split by flags | `internal/app/app.go`, `cmd/modulith/main.go` |
| Observer (logs; alerts on `needs_attention`) | `internal/app/app.go` |
| flowtest unit tests: happy path, compensation, cancel, timeout, lock, continue-as-new | `modules/*/*_test.go` |
| Two engines, one per module group, on real Postgres | `internal/app/app_test.go` |

## Rules for the team

### Module boundaries

1. A module owns one Postgres schema. Only its own code reads or writes
   it. Other modules call its Go API; they never query its tables, not
   even read-only. (The HTTP layer composes several modules' views through
   their APIs: `internal/app/http.go`.)
2. Cross-module work that must happen or be undone together goes through
   workflows: call the other module's **activities** for a single step
   (payments), or start its **workflow as a child** when the step has its
   own waiting or several steps of its own (inventory, shipping). Never
   call another module's Go functions from inside an activity to chain
   side effects: the engine would not know how to undo them.
3. Facts other modules react to are **events** through the outbox
   (orders to notifications). The producer owns the event type (`orders.Event`);
   add fields, never change or remove one.
4. Instance ids are global across all workflows: prefix them with the
   module (`orders-42`, `shipping-42`, `inventory-reserve-42`). Each
   module exports a function that builds its ids.
5. Error kinds (`flows.RegisterErrorKind`) are global too: prefix them
   with the module (`payments_declined`). They keep `errors.Is` working on
   errors read back from a history, in any process.

### Determinism (workflow code)

Workflow functions are replayed. Inside them:

- branch only on the input and on what `flows.*` calls return;
- no I/O, no clock, no randomness, no goroutines, no reads of
  configuration or package state that can change: put such values in the
  workflow's input when it starts (`orders.Order.PickupWithin`,
  `inventory.ReplenishState.Every`);
- use `flows.Now`, `flows.Sleep`, `flows.NewTimer`, `flows.Go`,
  `flows.Select`, `flows.SideEffect`.

Run `flowsvet` (module `analyzer/`) in CI to catch mistakes.

### Activities

- They run **at least once**. Make every effect idempotent by its
  `flows.Key`: a unique key column (`payments.payments.auth_key`) or
  `idem.Do` in the module's transaction (`inventory`).
- A compensator must be idempotent too, and must handle
  `flows.CompensationUncertain(ctx)`: the activity may or may not have had
  its effect. Undo by key, and fence the key so that a late attempt cannot
  land afterwards (`idem.Abort`, or the tombstone rows of payments and
  shipping).
- Mark errors retrying cannot fix with `flows.Permanent`.
- Keep inputs and outputs small (ids, not documents).

### Registration in every binary

Definitions are package-level values in a global registry: a process
runs only what its binary defines. Build one binary that imports every
module (`internal/app` does), and use it for every deployment. Install the
same `RunHook` on every engine, client-only ones included: an end is
recorded by whichever engine runs the last task.

### Dependencies

Activities are package-level, so they cannot take constructor arguments.
Each module keeps its dependencies in a `deps.Ref`, set by `Configure`
from the composition root before the engine starts. `Configure` checks
what it is given and `app.New` refuses to start on an error: a wiring
mistake fails at startup. An activity that still runs unconfigured fails
with `deps.ErrNotConfigured`, which is retried, not permanent: a
misconfigured replica delays work instead of failing (and compensating)
real orders. Configure every module in every process: their Go APIs
(`orders.Get`, `payments.Find`...) are used by the HTTP API of any
process.

### Queues and splitting the deployment

Each module runs its workflows and activities on its own queue
(`WithQueue`, `WithWorkflowQueue`). `-modules` selects the queues a
process serves; any process can start workflows, send signals and serve
the API, because that only writes to Postgres.

```sh
# one process (the monolith)
go run ./examples/modulith/cmd/modulith

# split: same binary, two deployments
go run ./examples/modulith/cmd/modulith -modules=orders,notifications -http=:8080 -id=front-1
go run ./examples/modulith/cmd/modulith -modules=payments,inventory,shipping -http= -id=back-1
```

Scale each group independently by running more replicas. Rules:

- every queue must be served by at least one running process: an activity
  whose queue nobody serves waits forever (its workflow does not fail);
- move a module by deploying the new group first, then removing the module
  from the old one; in-flight work continues on whichever process serves
  the queue;
- the in-process `LocalBus` ties the relay to the notifications process.
  With a real broker the relay can run anywhere and consumers subscribe to
  the broker;
- to give a module its own database later, its schema, migrations and
  activities move with it; the workflows still need the shared `flows`
  schema, so cross-database steps become activities calling the other
  side's API.

### Versioning a workflow

A running execution replays with the code registered under its name and
version. Changing the sequence of `flows.*` calls of a deployed version
parks its running executions (`needs_attention`, `ErrNonDeterministic`).
To change one, add a version next to the old one:

```go
var (
	// Version 1: kept, unchanged, until its executions have ended.
	fulfilV1 = flows.DefineWorkflow("orders.fulfil", 1, fulfilOrderV1, flows.WithWorkflowQueue(Queue))
	// Version 2 adds a fraud check before the capture. Place starts it.
	fulfil = flows.DefineWorkflow("orders.fulfil", 2, fulfilOrder, flows.WithWorkflowQueue(Queue))
)
```

1. Copy the old function as `fulfilOrderV1` (frozen), change `fulfilOrder`,
   bump the version. New starts use version 2; running version 1
   executions keep replaying version 1.
2. An activity whose input or output type changes gets a new name
   (`payments.capture.v2`); keep the old definition while version 1 can
   still call or compensate it.
3. Timeouts and activity implementations (same types, same meaning) can
   change freely: they are not in the history. Retry policies too, with
   one catch: the attempt limit of an activity is recorded on its task
   when it is scheduled, so a new limit applies to activities scheduled
   after the deploy, not to those already waiting or retrying.
4. Remove version 1 once nothing runs it:

   ```sql
   SELECT version, status, count(*) FROM flows.execution
   WHERE workflow = 'orders.fulfil' AND status IN ('running', 'compensating', 'needs_attention')
   GROUP BY 1, 2;
   ```

5. During a rolling deploy, an old engine that does not know version 2
   hands such executions back to a new one: deploy the new version
   everywhere before relying on it.

### Operations checklist

- `flows.Migrate` and every module's `Migrate` run at startup (advisory
  locks make concurrent starts safe); pool `MaxConns >= 3`.
- Unique `EngineID` per process (the default is fine); `Stop(ctx)` with a
  deadline on SIGTERM (`cmd/modulith`), shorter than the orchestrator's
  kill timeout.
- Alert on `needs_attention` executions (the observer logs them at error
  level), on a growing number of `compensating` ones, and on outbox rows
  with high `attempts`.
- Periodic jobs on one process: `engine.PurgeEnded`, `idem.Purge` for
  `inventory.processed_requests`, deletion of old sent outbox rows. A
  purge frees the instance ids it deletes: whatever must stay idempotent
  after it is checked in the module's tables (see "API friction").
- Bound request sizes at the API (`maxBody`): the engine stores inputs and
  signals as given.
- A parked execution: fix the cause, then `engine.Resume` (code) or
  `engine.RetryCompensation` (a compensator gave up).
- See [docs/operations.md](../../docs/operations.md) for tuning, poolers,
  autovacuum and incidents.

## API friction and known limits

What go-flows made hard here, and how the example works around it:

- **No "cancel a child and get its real outcome".** Canceling a child's
  context resolves its future at once with `ErrCanceled`; the child may
  still have completed (here: been picked up). Route the cancel to the
  child as a signal and let it decide (`orders/fulfil.go`).
- **No workflow-to-workflow signal.** A workflow cannot call
  `Engine.Signal`. The order forwards a cancel with an activity
  (`orders.forward-cancel`), which needs an engine in the module's
  dependencies and runs at least once (the shipment ignores repeats).
- **Instance-id idempotency ends at retention.** `Start` with a used id
  returns the existing execution only until `PurgeEnded` deletes it; then
  the same id starts a new execution. Keep idempotency that must last in
  the module's tables: `orders.Place` checks `orders.orders` (other
  contents under a used id: `ErrOrderConflict`, 409), `inventory.Adjust`
  checks `inventory.adjustments.request_id` (`ErrRequestConflict`), and a
  discontinued SKU is a column, so its replenishment loop is not started
  again after a purge. Two different orders racing for a new id both get
  202; the second one's contents are dropped.
- **`idem.CreateTable` does not fit SQL migrations.** It runs DDL outside
  goose and its lock (concurrent `CREATE TABLE` can fail on `pg_type`).
  The inventory module creates the table in its first migration instead:

  ```sql
  CREATE TABLE inventory.processed_requests (
  	key        text PRIMARY KEY,
  	result     jsonb,
  	aborted    boolean NOT NULL DEFAULT false,
  	created_at timestamptz NOT NULL DEFAULT now()
  );
  CREATE INDEX processed_requests_created_idx ON inventory.processed_requests (created_at);
  ```

- **Definitions are global.** Activities cannot take dependencies
  (`deps.Ref`), and two engines in one process share them.
- **flowtest cannot mock a child workflow.** Testing `orders.fulfil`
  means mocking the activities of the inventory and shipping children,
  which those modules export for that reason only.
- **`Workflow.Start` cannot join the caller's transaction.** The order
  row is recorded by the workflow's first activity, not by `Place`.
- **One `RunHook` per engine, and `RunInfo` has no input or output.** The
  composition root chains the modules' hooks; the orders hook reads its
  table, and tells a customer cancel by parsing `RunInfo.ErrorKinds`.
- **Signals are not decisions.** A signal is accepted while the execution
  is running, compensating or parked, read or not: a workflow that has
  chosen another branch never reads it, and the sender got nil. A signal
  before the instance exists fails (`ErrNotFound`). Anything a sender
  must know was accepted (the carrier's pickup) is decided in the module's
  table first (`shipping.ConfirmPickup`), and the signal only wakes the
  workflow.
- **No "retry forever" policy.** A step that must succeed (after a pivot)
  is a workflow loop over a finite retry policy with a durable sleep
  (`untilDone` in `shipping/shipping.go`); its failed attempts reach the
  observer.
- **Outbox order is by sequence, not commit.** See
  `internal/platform/outbox`: fine for one event per key per
  transaction, not for concurrent producers of one key.

Limits of the example itself:

- The gateway, carrier, supplier and mail server are stubs in the modules'
  own tables.
- The integration test runs both deployments in one process, so the
  modules' `Configure` is shared; what it proves is the queue split (each
  engine runs only its modules' activities and records the ends of its
  modules' workflows).
- The tests import `internal/testdb` of this repository; a copy of the
  blueprint needs its own test database helper.
