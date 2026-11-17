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
| shipping | `shipping` | `shipping` | workflow `Ship` (+ `InstanceID`), `ConfirmPickup`, `Find`, `ErrPickupTimeout` |
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
        label, then Await(shipping.picked-up, PickupWithin)
        Select { shipment done | signal orders.cancel }
 end ──> RunHook (same transaction): orders.orders.state + outbox event
     ──> relay ──> bus ──> notifications consumer ──> workflow notifications.send
```

- **Compensation crosses modules by name.** When the carrier does not come
  (`ErrPickupTimeout`) or the customer cancels, the shipping child cancels
  its label, then the engine refunds the capture, reopens the completed
  `inventory.Reserve` child to release its lines, and voids the
  authorization. Each compensator runs on its module's queue, so on the
  processes running that module. Nothing needs the `fulfil` function.
- **The pivot.** Once the carrier has the parcel, nothing is undone:
  `MarkPickedUp` has no compensator, and a cancel arriving after the
  pickup is ignored. Once the order has ended, `orders.Cancel` returns
  `ErrNotActive` (HTTP 409).
- **A cancel signal is never lost.** Sent during steps 1 to 4, it waits in
  the channel; the workflow checks it before capturing and again while it
  waits for the carrier.

Other features, where to find them:

| Feature | Where |
| --- | --- |
| Activities with compensators, idempotent by `flows.Key` (unique key column, tombstone fencing) | `payments/payments.go`, `shipping/shipping.go` |
| Exactly-once effects with `idem.Do`, uncertain undo with `idem.Abort` | `inventory/activities.go` |
| Child workflows across modules, compensated by reopening | `orders/fulfil.go`, `inventory/workflows.go` (`Reserve`), `shipping/shipping.go` (`Ship`) |
| Signal with a timeout timer (`Await`) | `shipping/shipping.go` |
| `Select` over a child and a signal, cancelling the child (`WithCancel`) | `orders/fulfil.go` |
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
from the composition root before the engine starts. An activity of a
module that was not configured fails permanently with
`deps.ErrNotConfigured`. Configure every module in every process: their
Go APIs need it even where their queues are not served.

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
3. Retry policies, timeouts, and activity implementations (same types,
   same meaning) can change freely: they are not in the history.
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
  `inventory.processed_requests`, deletion of old sent outbox rows.
- Bound request sizes at the API (`maxBody`): the engine stores inputs and
  signals as given.
- A parked execution: fix the cause, then `engine.Resume` (code) or
  `engine.RetryCompensation` (a compensator gave up).
- See [docs/operations.md](../../docs/operations.md) for tuning, poolers,
  autovacuum and incidents.

## Limits of the example

- The gateway, carrier, supplier and mail server are stubs in the modules'
  own tables.
- A pickup confirmation arriving before the shipping workflow exists gets
  404: a real webhook handler should retry, or record it for the workflow
  to read.
- The integration test runs both deployments in one process, so the
  modules' `Configure` is shared; the queue split is what it proves (each
  engine runs only its modules' activities).
- The integration test imports `internal/testdb` of this repository; a
  copy of the blueprint needs its own test database helper.
