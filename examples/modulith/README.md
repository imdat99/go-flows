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

Tests: `go test -race ./examples/modulith/...` (most need `make up`).

## Architecture: three layers, one dependency rule

```
          cmd/modulith  ──>  internal/app  (composition root)
                                │  constructs services, wires them in,
                                │  migrations, engine, hook, HTTP API
                 ┌──────────────┴───────────────┐
                 v                              v
      processes/  (orchestration)  ────>  modules/  (domain)
      the only code importing go-flows    orders, payments, inventory,
      workflows, activities, signals,     shipping, notifications
      queues, compensations, RunHook      know nothing of the engine
                 │                              │
                 v                              v
            go-flows                 internal/platform (schema, outbox)
```

- **Domain modules** (`modules/<name>/`) are plain Go and SQL: types, a
  `Service` constructed with its dependencies (`NewService(pool, logger)`,
  no package-level state), its repository SQL, its own migrations and
  schema, and its domain errors. Each module classifies its errors itself:
  `IsRejection(err)` is true for a business rejection (declined, out of
  stock, conflict...) that retrying cannot change; anything else is
  transient. Every effect takes a caller-supplied idempotency key
  (`Authorize(ctx, key, ...)`, `Reserve(ctx, key, ...)`) stored in the
  module's tables, with the fences that make undo-by-key safe. A module
  imports no go-flows package and no other module; it may publish domain
  events through the shared outbox (orders does).
- **Processes** (`processes/<name>/`) are the orchestration layer and the
  only code that imports `flows`: workflows, activities, signals, queues,
  compensations, the pivot logic, the RunHook. An activity is a thin
  adapter: it calls a module service with `flows.Key` as the idempotency
  key and turns a rejection into `flows.Permanent` (`adapt.Error`).
- **The composition root** (`internal/app`, `cmd/modulith`) constructs the
  services, hands them to the processes (`Configure`), runs the
  migrations, builds the engine and serves the HTTP API. HTTP handlers
  query module services directly and go through the processes to start or
  signal workflows.

**The rule, enforced.** `architecture_test.go` runs `go list` and fails if
a package under `modules/` depends, even transitively, on any go-flows
package (`flows`, `idem`, `flowtest`, `otelflows`, `internal/...`), on
another module, on `processes/` or on `internal/app`; module tests may
import only the repository's `internal/testdb` besides. It also checks
that `internal/platform` stays free of the engine.

## Layout

```
examples/modulith/
  architecture_test.go          the dependency rule (go list)
  cmd/modulith/main.go          flags/env, pgxpool, signals, graceful shutdown
  internal/app/                 composition root: services, Configure, migrations, engine, hook, HTTP API
  internal/platform/
    schema/                     goose migrations per module, versions in <schema>.goose_db_version
    outbox/                     transactional outbox: Stage, Relay, Bus, LocalBus
  modules/                      the domain: no go-flows, no other module
    orders/                     orders, their state, Event and topics (outbox)
    payments/                   authorize/void, capture/refund, by key, with tombstones
    inventory/                  stock, reservations by key (requests.go), adjustments, discontinued flag
    shipping/                   labels, the pickup decision on the shipment row
    notifications/              deliveries, once per event id
  processes/                    the orchestration: the only importers of go-flows
    internal/adapt/             holder of a process's services, error translation
    fulfilment/                 the order saga, its RunHook, Place/Cancel/Status    queue orders
    payment/                    activities Authorize, Capture (void, refund)        queue payments
    reservation/                child workflow Reserve (release by name)            queue inventory
    replenishment/              per-SKU loop (ContinueAsNew, Lock), Adjust          queue inventory
    shipment/                   child workflow Ship, pivot, WakeOnPickup, Cancel    queue shipping
    notify/                     orders events -> notifications, one workflow each   queue notifications
```

| Module | Schema | Service API | Processes on its queue |
| --- | --- | --- | --- |
| orders | `orders` | `Validate`, `Placed`, `Record`, `End`, `Get`; `Event`, `Topic*`; `ErrInvalidOrder`, `ErrOrderConflict` | fulfilment (`orders`) |
| payments | `payments` | `Authorize`, `Void`, `Capture`, `Refund`, `Find`; `ErrDeclined`, `ErrInvalidAmount`, `ErrVoided`, `ErrNotCapturable` | payment (`payments`) |
| inventory | `inventory` | `Reserve`, `Release`, `SettleReservation`, `Stock`, `Level`, `PlacePurchaseOrder`, `ApplyAdjustment`, `Applied`, `Discontinue`, `Replenishable`, `PurgeRequests`; `ErrOutOfStock`, `ErrUnknownSKU`, `ErrRequestConflict`, `ErrAborted` | reservation, replenishment (`inventory`) |
| shipping | `shipping` | `CreateLabel`, `CancelLabel`, `ConfirmPickup`, `Decide`, `MarkPickedUp`, `Find`; `ErrUnserviceable`, `ErrShipmentExists`, `ErrLabelCanceled`, `ErrPickupRefused`, `ErrNoShipment` | shipment (`shipping`) |
| notifications | `notifications` | `Deliver`, `Sent` | notify (`notifications`) |

Every module also exports `Migrate`, `NewService` and `IsRejection`
(notifications has no rejections). Every process exports `Queue` and
`Configure`.

## How dependencies reach activities

go-flows definitions are package-level values in a global registry: an
activity function cannot take constructor arguments. So each process
package has one explicit injection point, and that is the only
package-level mutable state of the example:

```go
// processes/payment
type Services struct{ Payments *payments.Service }

var deps = adapt.NewRef[Services]("payment") // atomic pointer, set once

func Configure(s Services) error { /* checks */ deps.Set(s); return nil }

func authorize(ctx context.Context, k flows.Key, r AuthorizeRequest) (Authorization, string, error) {
	s, err := deps.Get() // adapt.ErrNotConfigured, retried: never compensates real orders
	if err != nil {
		return Authorization{}, "", err
	}
	id, err := s.Payments.Authorize(ctx, string(k), r.OrderID, r.Amount)
	if err != nil {
		return Authorization{}, "", adapt.Error(err, payments.IsRejection) // rejection -> Permanent
	}
	return Authorization{PaymentID: id}, id, nil
}
```

`app.New` constructs the five services once, then calls every process's
`Configure`, and refuses to start on an error: a wiring mistake fails at
startup. Every process package is configured in every process, its module
served there or not, because any process's HTTP API may start workflows
(`fulfilment.Place`, `replenishment.Adjust`). The domain modules take
their dependencies in constructors and are tested without any engine.

## How an order flows

```
HTTP POST /orders ──> fulfilment.Place ──> workflow orders.fulfil (queue orders)
   1. orders.record                activity      orders        Orders.Record
   2. payments.authorize           activity      payments      Payments.Authorize    undo: Void
   3. inventory.reserve            child         inventory     Inventory.Reserve     undo: Release (per line)
   4. payments.capture             activity      payments      Payments.Capture      undo: Refund
   5. shipping.ship                child         shipping      Shipping.CreateLabel  undo: CancelLabel
        label, then Select { signal picked-up | signal shipping.cancel | timer PickupWithin }
        then Shipping.Decide on the shipment row (first decision wins, carrier or workflow)
        picked up: Shipping.MarkPickedUp, retried until it succeeds
      the order: Select { shipment done | signal orders.cancel -> forward to the shipment }
                 then wait for the shipment's real outcome
 end ──> RunHook (same transaction): Orders.End -> orders.orders.state + outbox event
     ──> relay ──> bus ──> notify consumer ──> workflow notifications.send ──> Notifications.Deliver
```

- **Compensation crosses modules by name.** When the carrier does not come
  (`shipment.ErrPickupTimeout`) or the customer cancels, the shipment child
  cancels its label, then the engine refunds the capture, reopens the
  completed `inventory.reserve` child to release its lines, and voids the
  authorization. Each compensator runs on its module's queue, so on the
  processes running that module, and calls that module's service by key.
  Nothing needs the saga's function.
- **The pivot is a row, not a signal.** Whether the carrier has the
  parcel is decided once, by the shipping module, on `shipping.shipments`
  under a row lock: the row leaves `label_created` for `pickup_confirmed`
  or `canceled`, and whoever comes first wins. The domain decides; the
  engine is only woken up.
  - The carrier's webhook (`POST /shipments/{order}/pickup`) calls
    `shipping.Service.ConfirmPickup`, which moves the row to
    `pickup_confirmed`; only then does the handler wake the workflow
    (`shipment.WakeOnPickup`). 202 means the pickup is recorded for good:
    the order will complete. If the shipment was canceled first (the
    customer, or the timeout), it gets 409 `ErrPickupRefused` (return the
    parcel); before the label exists, 404 `ErrNoShipment` (retry). A pickup
    is never accepted and then dropped.
  - The workflow, woken by the first signal or its timer, asks for the
    outcome it wants (`Shipping.Decide`) and gets the row's state. Signals
    only wake it: if the carrier's signal is lost, the timer wakes it and
    the row still says `pickup_confirmed`.
  - From the decision on, the shipment cannot fail into the saga: the
    decide and mark-picked-up activities run on `flows.Disconnected` and are
    retried until they succeed (rounds of attempts, a durable 5-minute
    sleep between rounds; the observer's failed attempts are the alert).
    With the shipping database down for hours after a pickup, the order
    completes late; it is never undone. Marking picked up has no
    compensator, and `CancelLabel` only cancels a row still
    `label_created`.
  - Once the order has ended, `fulfilment.Cancel` returns `ErrNotActive`
    (HTTP 409).
- **Who decides on a cancel.** `fulfilment.Cancel` signals the order. Sent
  during steps 1 to 4, the signal waits in the channel and the order stops
  itself before the capture or before starting the shipment. During step 5
  only the shipment knows whether the pickup came first, so the order does
  not cancel the child (in go-flows, canceling a child resolves its future
  at once with `ErrCanceled`, without its real outcome). It forwards the
  cancel to the shipment (an activity calling `shipment.Cancel`, because a
  workflow cannot signal another one) and waits for the shipment's
  outcome: canceled, the order is undone; picked up, the order completes
  and the cancel is ignored.
- **Tests of these paths.**
  - Process layer (flowtest, activities mocked): `TestFulfilShipped`,
    `TestFulfilCanceledByCustomer`, `TestFulfilPickupTimeout`,
    `TestFulfilOutOfStock`, `TestFulfilCancelAfterPickup` (the cancel
    reaches the order after the pickup reached the shipment),
    `TestFulfilCancelBeforeShipping`, `TestFulfilCancelDuringCapture`,
    `TestFulfilPickupRecordFailsForLong`, `TestShipPickupBeatsCancel`,
    `TestShipConfirmedPickupBeatsCancelSignal`,
    `TestShipPickupRecordedDespiteFailures`, `TestShipUnserviceable`,
    `TestReserveReleasesOnFailure`, the replenishment loop tests,
    `TestAdjustWaitsForTheLock`, `TestSendRetriesTheMailServer`. The
    adapters themselves run on the real modules in
    `TestAdaptersCompensateByKey`, `TestDeclineIsPermanent` (payment) and
    `TestAdaptersOnTheModule` (reservation).
  - Domain (Postgres, no engine): `TestPickupAfterCancelIsRefused`,
    `TestCancelAfterPickupKeepsThePickup`, `TestDecisionRace` (the carrier
    and the workflow decide at the same time, 20 times: they always agree),
    `TestLateLabelIsFenced`, `TestLateCaptureIsRefundedByVoid`,
    `TestLateAuthorizationIsFenced`, `TestSettleReservation`,
    `TestAdjustmentsByRequestID`, `TestRecordAndEnd`.
  - The integration test runs two engines (front: orders, notifications;
    back: payments, inventory, shipping) and checks which engine ran which
    workflow and activity. It confirms a pickup and then cancels through the
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
| Effects idempotent by a caller key (unique key column, tombstone fencing) | `modules/payments`, `modules/shipping` |
| Exactly-once effects in the module's transaction, uncertain undo that fences the key | `modules/inventory/requests.go`, `SettleReservation` |
| Domain errors classified by the module, translated to `flows.Permanent` by the adapter | `modules/*/IsRejection`, `processes/internal/adapt` |
| Child workflows across modules, compensated by reopening | `processes/fulfilment`, `processes/reservation`, `processes/shipment` |
| `Select` over two signals and a timeout timer; a decision made once in a table row; steps retried until they succeed on `Disconnected` | `processes/shipment`, `modules/shipping` |
| `Select` over a child and a signal; forwarding a signal to a child through an activity | `processes/fulfilment` |
| Long-running loop with `ContinueAsNew` and a stop signal | `processes/replenishment` (`cycle`) |
| `flows.Lock` with `LockWait` on a business key | `processes/replenishment` (`checkAndOrder`, `adjustStock`) |
| RunHook ending the order (read model and event) in the end's transaction | `processes/fulfilment` (`Hook`), `modules/orders` (`End`) |
| Transactional outbox, relay with `SKIP LOCKED`, per-key order, backoff | `internal/platform/outbox` |
| Event consumer translating one module's events for another, deduplicated by a workflow per event id | `processes/notify` |
| Queues per module, one binary split by flags | `internal/app/app.go`, `cmd/modulith/main.go` |
| Observer (logs; alerts on `needs_attention`) | `internal/app/app.go` |
| The dependency rule, checked with `go list` | `architecture_test.go` |
| Two engines, one per module group, on real Postgres | `internal/app/app_test.go` |

## Rules for the team

### Module boundaries

1. A module owns one Postgres schema. Only its own code reads or writes
   it. Other code calls its service; nothing else queries its tables, not
   even read-only. (The HTTP layer composes several modules' views through
   their services: `internal/app/http.go`.)
2. A module does not import the engine, nor another module. Data that
   crosses modules goes through the process layer, which translates (the
   saga passes the payment id from payments to payments, the notify
   process turns an `orders.Event` into a `notifications.Message`).
3. Cross-module work that must happen or be undone together is a process:
   an **activity** for a single step (payment), a **child workflow** when
   the step has its own waiting or several steps (reservation, shipment).
   Never call a second module's service from inside an activity to chain
   side effects: the engine would not know how to undo them.
4. Facts other modules react to are **events** through the outbox
   (orders to notifications). The producer owns the event type (`orders.Event`);
   add fields, never change or remove one.
5. Instance ids are global across all workflows: prefix them with the
   module (`orders-42`, `shipping-42`, `inventory-reserve-42`). Each
   process exports a function that builds its ids.
6. Error kinds (`flows.RegisterErrorKind`) are global too: prefix them
   with the module (`payments_declined`). The process packages register
   them, for the module's sentinels and their own. They keep `errors.Is`
   working on errors read back from a history, in any process.

### Adding a module

1. `modules/<name>/`: the types, a `Service` with `NewService`, the SQL,
   `migrations/*.sql` (applied by `schema.Migrate` into the schema
   `<name>`), the errors and `IsRejection`. Every effect takes an
   idempotency key and stores it (a unique column, or `once`/`abort` as in
   inventory); every undo works by key and leaves a tombstone. Test it on
   Postgres, without the engine.
2. `processes/<process>/`: `Queue` (the module's name), `Services`,
   `Configure`, the activities as adapters (`flows.Key` as the key,
   `adapt.Error(err, <name>.IsRejection)`), the workflows, the error
   kinds. Test it with flowtest and mocked activities.
3. `internal/app`: add the module to `modules` (name, queue, `Migrate`),
   construct its service in `newServices`, call the process's `Configure`
   in `New`, and add handlers if it has an API.
4. `architecture_test.go` covers it without a change.

### Swapping the engine

Only `processes/` and `internal/app` know go-flows. Replacing the engine
(another workflow library, a message-driven saga, plain jobs) rewrites the
process packages and the wiring; the modules, their tables, their
migrations and their tests stay as they are. The modules' contract with any
orchestrator is: an idempotency key per effect, undo by key, and
`IsRejection` to tell "do not retry" from "retry".

### Determinism (workflow code)

Workflow functions are replayed. Inside them:

- branch only on the input and on what `flows.*` calls return;
- no I/O, no clock, no randomness, no goroutines, no reads of
  configuration or package state that can change, no module service
  calls (those are activities): put such values in the workflow's input
  when it starts (`fulfilment.Order.PickupWithin`,
  `replenishment.State.Every`);
- use `flows.Now`, `flows.Sleep`, `flows.NewTimer`, `flows.Go`,
  `flows.Select`, `flows.SideEffect`.

Run `flowsvet` (module `analyzer/`) in CI to catch mistakes.

### Activities

- They run **at least once**. Pass `flows.Key` to the module as the
  idempotency key; the module makes the effect once per key: a unique key
  column (`payments.payments.auth_key`) or its own record of processed
  keys in the same transaction (`inventory.processed_requests`).
- A compensator must be idempotent too, and must handle
  `flows.CompensationUncertain(ctx)`: the activity may or may not have had
  its effect. Undo by key, and fence the key so that a late attempt cannot
  land afterwards (`inventory.Service.SettleReservation`, the tombstone
  rows of payments and shipping).
- Translate the module's rejections with `adapt.Error`: they become
  `flows.Permanent` (no retry; the saga compensates). Everything else is
  retried.
- Keep inputs and outputs small (ids, not documents).

### Registration in every binary

Definitions are package-level values in a global registry: a process
runs only what its binary defines. Build one binary that imports every
process package (`internal/app` does), and use it for every deployment.
Install the same `RunHook` on every engine, client-only ones included: an
end is recorded by whichever engine runs the last task.

### Queues and splitting the deployment

Each module's processes run their workflows and activities on the
module's queue (`WithQueue`, `WithWorkflowQueue`). `-modules` selects the
queues a process serves; any process can start workflows, send signals
and serve the API, because that only writes to Postgres.

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
  service move with it (its service gets the other pool); the workflows
  still need the shared `flows` schema.

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
   still call or compensate it. The same holds for a module type used as
   an activity's input or output (`shipping.Decision`).
3. Timeouts and activity implementations (same types, same meaning) can
   change freely: they are not in the history, and neither is anything
   inside a module. Retry policies too, with one catch: the attempt limit
   of an activity is recorded on its task when it is scheduled, so a new
   limit applies to activities scheduled after the deploy, not to those
   already waiting or retrying.
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
- Periodic jobs on one process: `engine.PurgeEnded`,
  `inventory.Service.PurgeRequests` for `inventory.processed_requests`,
  deletion of old sent outbox rows. A purge frees the instance ids it
  deletes: whatever must stay idempotent after it is checked in the
  module's tables (see "API friction").
- Bound request sizes at the API (`maxBody`): the engine stores inputs and
  signals as given.
- A parked execution: fix the cause, then `engine.Resume` (code) or
  `engine.RetryCompensation` (a compensator gave up).
- See [docs/operations.md](../../docs/operations.md) for tuning, poolers,
  autovacuum and incidents.

## API friction and known limits

What go-flows made hard here, and how the example works around it:

- **Definitions are global, so dependencies need a holder.** Activities
  are package-level values in a global registry and cannot take
  constructor arguments. Each process package has one explicit injection
  point (`Configure`, an `adapt.Ref` set once from the composition root):
  the only package-level mutable state of the example. Two engines in one
  process share the definitions and these holders (the integration test
  configures twice; the second call wins, which changes nothing there).
- **No "cancel a child and get its real outcome".** Canceling a child's
  context resolves its future at once with `ErrCanceled`; the child may
  still have completed (here: been picked up). Route the cancel to the
  child as a signal and let it decide (`processes/fulfilment`).
- **No workflow-to-workflow signal.** A workflow cannot call
  `Engine.Signal`. The order forwards a cancel with an activity
  (`orders.forward-cancel`), which needs an engine in the process's
  services and runs at least once (the shipment ignores repeats).
- **Instance-id idempotency ends at retention.** `Start` with a used id
  returns the existing execution only until `PurgeEnded` deletes it; then
  the same id starts a new execution. Keep idempotency that must last in
  the module's tables: `fulfilment.Place` asks `orders.Service.Placed`
  (other contents under a used id: `ErrOrderConflict`, 409),
  `replenishment.Adjust` asks `inventory.Service.Applied`
  (`inventory.adjustments.request_id`, `ErrRequestConflict`), and a
  discontinued SKU is a column, so its replenishment loop is not started
  again after a purge. Two different orders racing for a new id both get
  202; the second one's contents are dropped.
- **`idem` would tie the domain to go-flows.** Package `idem` gives
  exactly-once effects keyed by `flows.Key`, but a module importing it
  would depend on go-flows. The inventory module keeps its own small
  version (`requests.go`: claim the key, make the effect, store the result
  in one transaction; settle an uncertain key with a tombstone) over the
  same table layout, created by its first migration.
- **flowtest cannot mock a child workflow.** Testing `orders.fulfil`
  means mocking the activities of the reservation and shipment children,
  which those processes export for that reason only.
- **`Workflow.Start` cannot join the caller's transaction.** The order
  row is recorded by the saga's first activity, not by `Place`.
- **One `RunHook` per engine, and `RunInfo` has no input or output.** The
  composition root chains the processes' hooks; the fulfilment hook ends
  the order through `orders.Service.End` (which reads the email from its
  table), and tells a customer cancel by parsing `RunInfo.ErrorKinds`.
- **Signals are not decisions.** A signal is accepted while the execution
  is running, compensating or parked, read or not: a workflow that has
  chosen another branch never reads it, and the sender got nil. A signal
  before the instance exists fails (`ErrNotFound`). Anything a sender
  must know was accepted (the carrier's pickup) is decided in the module's
  table first (`shipping.Service.ConfirmPickup`), and the signal only
  wakes the workflow.
- **No "retry forever" policy.** A step that must succeed (after a pivot)
  is a workflow loop over a finite retry policy with a durable sleep
  (`untilDone` in `processes/shipment`); its failed attempts reach the
  observer.
  Its history grows by a few events per round (about 600 a day): fine for
  outages of hours or days; for longer ones, end the loop with
  `ContinueAsNew` or alert and repair by hand.
- **Two narrow windows break the 202 of a pickup.** If `CreateLabel`
  commits its row and then exhausts its attempts while a carrier confirms
  in between, or an operator terminates a shipment still at
  `label_created` before a confirmation arrives, the carrier got 202 but
  the order is undone. Watch for shipments whose row says
  `pickup_confirmed` while their order is compensated.
- **Outbox order is by sequence, not commit.** See
  `internal/platform/outbox`: fine for one event per key per
  transaction, not for concurrent producers of one key.

Limits of the example itself:

- The gateway, carrier, supplier and mail server are stubs in the modules'
  own tables.
- The integration test runs both deployments in one process, so the
  processes' `Configure` is shared; what it proves is the queue split (each
  engine runs only its modules' activities and records the ends of its
  modules' workflows).
- The tests import `internal/testdb` of this repository; a copy of the
  blueprint needs its own test database helper.
