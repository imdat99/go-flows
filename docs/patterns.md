# Patterns

[Tiếng Việt](vi/mau-thiet-ke.md)

Each pattern is short code against the public API. Runnable programs are in
[`examples/`](../examples): `quickstart` (activity, signal), `order`
(compensation), `fanout` (concurrency), `outbox` (publishing events).

## Exactly-once effects with `idem`

Activities run at least once. Make the effect happen once by writing it in
the module's own transaction, together with the activity key:

```go
var requests = idem.MustNew("inventory") // inventory.processed_requests

var reserve = flows.DefineActivity("inventory.reserve",
	func(ctx context.Context, k flows.Key, sku string) (string, string, error) {
		id, err := inTx(ctx, func(tx pgx.Tx) (string, error) {
			return idem.Do(ctx, tx, requests, k, func(ctx context.Context) (string, error) {
				return insertReservation(ctx, tx, sku)
			})
		})
		return id, id, err // output, compensation data
	},
	func(ctx context.Context, k flows.Key, id string) error {
		return inTxErr(ctx, func(tx pgx.Tx) error {
			if flows.CompensationUncertain(ctx) {
				// The attempt may or may not have happened: settle it by key.
				// Abort also fences off an attempt still running elsewhere.
				got, done, err := idem.Abort[string](ctx, tx, requests, k)
				if err != nil || !done {
					return err // nothing to undo
				}
				id = got
			}
			return deleteReservation(ctx, tx, id)
		})
	})
```

`inTx` and `inTxErr` stand for your own transaction helpers. Call
`requests.CreateTable` in the module's migration. Purge the table
periodically with `idem.Purge`.

## Saga: undo on failure

Return an error from the workflow, and the completed activities are undone,
latest first:

```go
var book = flows.DefineWorkflow("trip.book", 1, func(c flows.Ctx, t Trip) (Booking, error) {
	flight, err := bookFlight.Run(c, t.Flight)
	if err != nil {
		return Booking{}, err // nothing to undo yet
	}
	hotel, err := bookHotel.Run(c, t.Hotel)
	if err != nil {
		return Booking{}, err // cancelFlight runs
	}
	if _, err := charge.Run(c, t.Payment); err != nil {
		return Booking{}, err // cancelHotel, then cancelFlight
	}
	return Booking{flight, hotel}, nil
})
```

- **A pivot step.** After a step that must not be undone (the goods
  shipped), returning an error would still compensate everything before it.
  From there on, handle failures in workflow code (retry forward, alert,
  wait for a signal) and return success. Activities without a compensator are
  skipped by compensation; the others are still undone.
- **Error kinds.** `RegisterErrorKind("out_of_stock", ErrOutOfStock)` keeps
  `errors.Is(err, ErrOutOfStock)` working on the error `Handle.Wait`
  returns, even in another process.

## Waiting for a person or an external system

```go
approved, err := flows.Await[bool](c, "approval", 72*time.Hour)
if errors.Is(err, flows.ErrAwaitTimeout) {
	return Order{}, ErrNotApproved // compensates
}
```

The sender calls `engine.Signal(ctx, orderID, "approval", true)`, from any
process. A signal sent early is kept until the workflow reads it.

To wait for several things at once, use a timer and a signal in `Select`:

```go
reminders := flows.Signals[string](c, "reminder")
deadline := flows.NewTimer(c, 24*time.Hour)
for done := false; !done; {
	flows.Select(c,
		flows.OnSignal(reminders, func(c flows.Ctx, msg string, err error) { /* ... */ }),
		flows.OnFuture(deadline, func(c flows.Ctx, _ struct{}, err error) { done = true }),
	)
}
```

## Timeout on an activity

`WithTimeout` bounds each attempt. To bound the whole call from the workflow,
including retries, race it with a timer and cancel the loser:

```go
ac, cancel := flows.WithCancel(c)
res := slowCall.Start(ac, in)
timeout := flows.NewTimer(ac, 5*time.Minute)
var out Result
var err error
flows.Select(c,
	flows.OnFuture(res, func(c flows.Ctx, v Result, e error) { out, err = v, e }),
	flows.OnFuture(timeout, func(c flows.Ctx, _ struct{}, _ error) { err = ErrTooSlow }),
)
cancel() // cancels whichever is still pending: the attempt's context too
```

## Fan-out and fan-in

```go
futures := make([]*flows.Future[Price], len(suppliers))
for i, s := range suppliers {
	futures[i] = quote.Start(c, s) // all scheduled in one checkpoint
}
for i, f := range futures {
	if prices[i], err = f.Get(c); err != nil {
		return nil, err // completed quotes are compensated (if quote has a compensator)
	}
}
```

For per-branch logic, use `flows.Go` with a `flows.WaitGroup`:

```go
var wg flows.WaitGroup
for _, item := range items {
	wg.Add(1)
	flows.Go(c, func(c flows.Ctx) {
		defer wg.Done()
		// Several activities per item; coroutines are deterministic.
	})
}
if err := wg.Wait(c); err != nil {
	return err
}
```

## Long-lived workflows

A workflow that loops forever (a subscription, an entity) grows its history.
Continue as new periodically:

```go
var account = flows.DefineWorkflow("account", 1, func(c flows.Ctx, st State) (State, error) {
	events := flows.Signals[Event](c, "event")
	for range 1000 {
		ev, err := events.Receive(c)
		if err != nil {
			return st, err
		}
		st = st.apply(ev)
	}
	return st, flows.ContinueAsNew(c, st) // unread signals carry over
})
```

## Child workflows

```go
shipment, err := shipWorkflow.Child(c, "ship-"+order.ID, order).Get(c)
```

- **Instance ids.** The child's id is its own instance id. Derive it from the
  parent's data, so that a replay starts the same child.
- **Compensation.** If the parent later fails, a completed child is
  compensated with it.
- **Cancel.** Canceling the parent cancels a running child.

## Mutual exclusion on a business key

```go
if err := flows.Lock(c, "account:"+id, time.Minute, flows.LockWait(flows.Exponential(20, 50*time.Millisecond, 5*time.Second))); err != nil {
	return err // ErrLockHeld once the wait policy ran out
}
// Exclusive until the execution ends.
```

Pick a `ttl` above the longest time the execution may sleep or await while
holding the lock. While one of its activities waits in a queue or for a retry,
the lock is kept up to `Options.LockHold` (default 1h) past its `ttl`. Take
several locks in a fixed order.

## Releasing events only on success (outbox)

Stage messages in your activities, then publish or drop them in the end's
transaction:

```go
type outbox struct{}

func (outbox) RunEnded(ctx context.Context, tx pgx.Tx, r flows.RunInfo) error {
	var err error
	switch r.Status {
	case flows.StatusCompleted:
		_, err = tx.Exec(ctx, `UPDATE app.outbox SET ready = true WHERE instance_id = $1`, r.InstanceID)
	case flows.StatusNeedsAttention:
		// A park, not an end: the execution may still compensate.
	default: // compensated, failed, canceled, terminated
		_, err = tx.Exec(ctx, `DELETE FROM app.outbox WHERE instance_id = $1`, r.InstanceID)
	}
	return err
}

engine, err := flows.New(pool, flows.Options{Hook: outbox{}})
```

Set the hook on every engine, client-only ones included. A relay process
then publishes the ready rows to the broker of your choice.
`RunEnded` also runs for parks (`needs_attention`), which are not ends:
leave the messages there.

[`examples/outbox`](../examples/outbox) puts it together: an activity stages
an event deduplicated by its `Key` plus a per-execution nonce, the hook
releases or drops it and stages a `workflow.ended` event, and a relay
publishes ready rows to a stub bus with `FOR UPDATE SKIP LOCKED` and retries
with backoff. Delivery is at least once,
with an event id to deduplicate on; released events of one key go out in id
order (the staging order when one execution writes the key). It does not
cover ContinueAsNew.

## Testing

```go
func TestBook(t *testing.T) {
	tst := flowtest.New(t)
	flowtest.OnActivity(tst, charge, func(ctx context.Context, k flows.Key, p Payment) (string, string, error) {
		return "", "", flows.Permanent(errors.New("declined"))
	})
	_, err := flowtest.Run(tst, book, "trip-1", trip)
	if !errors.Is(err, flows.ErrCompensated) {
		t.Fatal(err)
	}
	// tst.EventTypes("trip-1") lists the history; tst.Advance moves virtual time.
}
```

- **Real engine.** `flowtest` runs the real engine on an in-memory store with
  virtual time. A workflow that sleeps for a week finishes in milliseconds.
- **Blocked workflows.** A workflow that can never end fails with
  `flowtest.ErrBlocked`.
- **Pending futures.** Activities and children started and never awaited
  fail the test, unless you pass `AllowPendingFutures`. The rule: each
  ended execution is reported exactly once, at the first `Run` whose tree
  contains it. The tree of `Run(id)` is every execution of instance `id`
  (whatever its parent, all executions of its continue-as-new chains, even
  started before `Run`) plus every execution whose parent is in the tree.
  Only ended executions are checked: one still running when `Run` returns
  (a child detached by continue-as-new) waits for a later `Run` whose tree
  contains it, if any.
- **Activities take no virtual time.** Virtual time does not move while an
  activity runs. An activity that waits for the cancel a workflow timer
  would send never sees that timer fire, and `Run` fails with
  `flowtest.ErrNeverIdle`: mock such an activity to return. The mock then
  wins the `Select`, so the branch where the timer wins over an activity
  cannot be tested with `flowtest`.
