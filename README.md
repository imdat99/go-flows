# go-flows

[Tiếng Việt](docs/vi/README.md)

Durable workflows with saga compensation for Go, embedded in your
application and backed by Postgres.

- **Workflows are plain Go functions.** They are replayed from an event
  history, so they survive crashes and deploys. Inside them you can run
  activities concurrently, wait on timers and signals with `Select`, start
  child workflows, and continue as new.
- **Compensation is built in.** Pair an activity with a compensator. When a
  workflow fails or is canceled, the engine undoes what completed, latest
  first, durably, and without needing the workflow function. When a
  compensator gives up, the execution is parked for an operator rather than
  failed.
- **Several processes can share one database safely.**
  - Every write is fenced by a lease version.
  - All times come from the database clock.
  - A checkpoint is one transaction.
- **It is fast where it counts.** The process that schedules an activity runs
  it eagerly, with about 2 round trips per activity. Typed generics replace
  reflection.

It combines the execution model of
[go-workflows](https://github.com/cschleiden/go-workflows) with the
multi-instance guarantees and saga layer of the saga engine. Ported code is
credited in [NOTICE](NOTICE). The comparison that motivated it is in
[docs/so-sanh-go-workflows.md](docs/so-sanh-go-workflows.md), in Vietnamese.

## Example

```go
var (
	charge = flows.DefineActivity("charge",
		func(ctx context.Context, k flows.Key, o Order) (string, string, error) {
			id, err := payments.Charge(ctx, string(k), o.Amount) // k: idempotency key
			return id, id, err                                   // output, compensation data
		},
		func(ctx context.Context, k flows.Key, chargeID string) error {
			return payments.Refund(ctx, string(k), chargeID)
		})

	ship = flows.DefineActivity("ship", shipOrder, nil)

	placeOrder = flows.DefineWorkflow("place-order", 1, func(c flows.Ctx, o Order) (string, error) {
		if _, err := charge.Run(c, o); err != nil {
			return "", err
		}
		if _, err := flows.Await[bool](c, "approved", 24*time.Hour); err != nil {
			return "", err // the charge is refunded
		}
		return ship.Run(c, o.ID)
	})
)

func main() {
	ctx := context.Background()
	pool, _ := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	_ = flows.Migrate(ctx, pool)
	engine, _ := flows.New(pool, flows.Options{})
	_ = engine.Start(ctx)
	defer engine.Stop(ctx)

	h, _ := placeOrder.Start(ctx, engine, "order-42", Order{ID: "42", Amount: 1299}) // the id is the idempotency key
	_ = engine.Signal(ctx, "order-42", "approved", true)
	shipment, err := h.Wait(ctx)
	// ...
}
```

Runnable versions are in [`examples/`](examples). Start Postgres with
`make up`, then run `go run ./examples/quickstart`.

## Documentation

| | English | Tiếng Việt |
| --- | --- | --- |
| Concepts: workflows, replay, activities, compensation, leases | [concepts](docs/concepts.md) | [khái niệm](docs/vi/khai-niem.md) |
| Guarantees and their limits | [guarantees](docs/guarantees.md) | [bảo đảm](docs/vi/bao-dam.md) |
| Operations: setup, tuning, deploys, retention, monitoring, incidents | [operations](docs/operations.md) | [vận hành](docs/vi/van-hanh.md) |
| Patterns: idempotency, saga, signals, timeouts, fan-out, locks, outbox, testing | [patterns](docs/patterns.md) | [mẫu thiết kế](docs/vi/mau-thiet-ke.md) |
| Benchmarks against saga and go-workflows | [benchmarks](docs/benchmarks.md) | |
| go-workflows bugs this engine fixes (GW-1 to GW-18) | [regressions](docs/regressions.md) | |

The API reference is the package documentation (`go doc
github.com/imdat99/go-flows`).

## Packages and modules

| Path | Purpose |
| --- | --- |
| `flows` (root) | The engine and the workflow API. |
| `flowtest` | Tests on the real engine, with an in-memory store, virtual time and mocked activities. |
| `idem` | Exactly-once effects for activities, keyed by `flows.Key`. |
| `otelflows` (separate module) | OpenTelemetry tracer and metrics observer. |
| `analyzer` (separate module) | `flowsvet`: reports non-deterministic workflow code, for `go vet` or golangci-lint. |

The root module depends on pgx and goose only.

## Development

```sh
make up      # Postgres 16 on :5433 (docker compose)
make test    # race tests on the memory and Postgres stores
make lint
make bench && make benchgate   # benchmarks and the allocation budget
make chaos   # multi-process kill/freeze tests
```

See [AGENTS.md](AGENTS.md) for the invariants and the process, and
[TASKS.md](TASKS.md) for the task list (Vietnamese).

## Status

The software is pre-release: the API and the schema may still change.
Requires Go 1.27; tested with Postgres 16.

## License

MIT. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
