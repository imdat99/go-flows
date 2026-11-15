# go-flows - agent guide

Durable workflow engine for Go with saga compensation as a first-class feature. It is a library embedded in the application, and Postgres is the source of truth. It combines the event-history model of go-workflows (coroutines, Select, signals, sub-workflows, ContinueAsNew) with the multi-instance guarantees and saga layer of the saga engine (`github.com/imdat99/saga`). Code ported from go-workflows is credited in `NOTICE`, and every ported file carries a header naming its origin.

## Start here

1. **`TASKS.md`:** the task list and the per-task process. Work on the first unchecked task unless told otherwise.
2. **`docs/regressions.md`:** the GW-* catalog of confirmed go-workflows bugs. Each one needs a test named `TestGW<n>_*` that fails before the fix.
3. **`docs/so-sanh-go-workflows.md`:** the comparison of the two engines and the reasons behind the combination (Vietnamese).

## Per-task process (mandatory)

1. Write the test first (a GW-* reproduction or a behaviour test) and see it fail.
2. Implement.
3. Test:
   - `make test` (race, mem and pg stores);
   - `make lint`;
   - for tasks marked ⚡, `make bench` and compare with `benchstat` against the previous numbers.
4. Run an independent review by a subagent. Give it the diff and the task's criteria, not your own conclusions. It checks:
   - correctness;
   - the invariants below;
   - allocations;
   - whether the tests are sufficient.
5. If the review fails, fix and review again. **After more than 3 failed reviews, stop:**
   - record why under the task in `TASKS.md`;
   - rethink the approach;
   - tell the user before continuing.
6. When it passes, tick the task in `TASKS.md` with its benchmark numbers (if any), then commit.

## Architecture invariants (do not break)

- **Fencing:** every write made for a running instance or an activity task checks `owner_id` and `lease_version` on the locked row. Zero rows means `ErrLeaseLost`, never a silent success.
- **Leases:** a lease is only taken when expired, and never re-granted while live, not even to the same owner. Every acquisition increments `lease_version`.
- **Time:** leases, timers, visibility, deadlines and retention use Postgres `now()` only, never the process clock. Workflow code gets time from the history (`flows.Now`).
- **One transaction per checkpoint:** completing a workflow task, meaning history plus pending events plus activity tasks plus timers plus children plus lease plus `RunHook`, is a single transaction. There are no dual writes.
- **Replay:** workflow functions are re-executed. Any mismatch between replay and history parks the instance as `needs_attention` and never fails it. Mismatches include type, name, argument hash, timer duration, and a replay shorter than the history.
- **Panics never escape:** user code, coroutines, hooks and signal decoding are recovered and turned into errors.
- **Compensation by name:** compensators are looked up by activity name, with persisted `comp_data`. Recovery never needs the workflow function.

## Performance budget (first-class requirement)

- **Hot path:** no `reflect` and no `fmt`, and no `any` in the data path. Generic definitions build typed closures.
- **SQL:** hot-path statements run on `pgconn` (fast path). Claiming and loading take one statement. Completing a workflow task is one pipelined transaction.
- **Eager execution** brings the cost to about 2 round trips per activity.
- **Allocation:** pools for encode buffers; an arena per execution for keys and commands; nothing per step without a measured reason.
- **Benchmarks:** every ⚡ task reports `-benchmem -count 6` numbers. Allocs/op must not grow. The targets are in `TASKS.md`.

## Commands

```bash
make up / make down       # docker compose Postgres on :5433, waits until healthy (tests need it)
make test                 # go test -race -count=1 ./...   (alias: make race)
make race10               # -count=10 flakiness check before ticking a task
make lint / make fmt
make bench                # benchmarks (-benchmem -count 6) to bench.txt
make benchcmp OLD=a NEW=b # benchstat before/after, for reviews of ⚡ tasks
make benchgate            # perf gate: bench.txt medians vs perf-budget.txt
make chaos                # multi-process chaos tests (from T8.1)
FLOWS_TEST_DSN=postgres://... make test   # another server
```

## Code conventions

- **Toolchain:** Go 1.27 and `pgx/v5`. `database/sql` appears only inside `Migrate`, for goose.
- **Migrations:** goose, embedded in `migrations/`. A change gets a new file; never edit an applied migration.
- **Layout:** the public API lives in the root package `flows`, internals under `internal/`.
- **Direct dependencies are an allowlist:** pgx, goose, uuid. Adding one is a design decision; record it in `TASKS.md` under decisions.
- **Errors:** wrap with `%w` and context. Exported sentinels go in `errors.go`.
- **Tests:** integration tests run against real Postgres via `internal/testdb`, which gives each test a fresh database.
- **Logging:** use `slog` with `LogAttrs` only.
- Do not push unless the user asks. No emojis in code.

## Skills

Before any Go task, load `golang-how-to`. It routes to the relevant skills in `.agents/skills/`, for example `golang-database`, `golang-concurrency`, `golang-performance` and `golang-benchmark`.
