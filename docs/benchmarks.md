# Benchmarks

Measured on 2026-11-15 and 2026-11-16 (task T8.2) on the development machine: Xeon E5-4669 v3
(2.1 GHz, 6 CPUs), Postgres 16 in Docker on the same host (raw round trip
~0.45 ms, fsync ~2.7 ms), Go 1.27.3. Numbers are medians of `-count 6` unless
noted; all three engines ran against the same Postgres server, each in a
fresh database.

## The scenario: a workflow of 10 sequential no-op activities

Each engine starts a workflow that calls a no-op activity 10 times in a row,
and the benchmark waits for it to end. One op is one workflow.

| Engine | Durable commit: time/workflow | per activity | allocs/workflow | Async commit: time/workflow | allocs |
| --- | --- | --- | --- | --- | --- |
| saga (`BenchmarkStepRun`, inline runs) | **96 ms** | 9.6 ms (p50 8 ms) | **63** | **22 ms** | **44** |
| go-flows (`BenchmarkActivityRun`, eager execution) | 150 ms | 15 ms | 75 | 60 ms | ~75 |
| go-flows public typed API (`BenchmarkPublicActivityRun`) | ~145 ms | ~14.5 ms | 140 | | |
| go-workflows 48e8119, default polling (200 ms) | 3.1 s (run 2.5 s) | 310 ms (250 ms) | ~26 000 | | |
| go-workflows 48e8119, polling 1 ms | 1.0 s (run 0.77 s) | 100 ms (77 ms) | ~150 000 | | |

Reading:

- **go-flows against go-workflows**: 4.5x faster than go-workflows tuned for
  latency (1 ms polling), 15x faster than its defaults, with 350 to 2 000
  times fewer allocations. The waits differ: go-flows' benchmark learns of the end
  from the store call that commits it, go-workflows' from
  `client.GetWorkflowResult`, which polls with a growing backoff (1 ms x1.5,
  capped at 1 s) and then loads the history. The ratios use the run time from
  go-workflows' history (start to finish event, "run" in parentheses, the
  `run-ms` metric), which excludes that overshoot; its allocation counts
  still include the client's polls. The difference is structural: go-flows runs an
  activity eagerly in the worker that scheduled it and resumes the workflow
  in the same round trip as the activity's result (2 round trips per
  activity, no polling), where go-workflows polls for every workflow and
  activity task and replays through reflection.
- **go-flows against saga**: about 1.6x slower (durable) and 2.7x slower
  (async commit), with ~1.2x the allocations. saga runs a step in ~3
  statements; go-flows runs ~9 per activity (below). go-flows buys event
  history (concurrency in workflows, Select, timers, signals, children,
  continue-as-new, replay) with that.
- **The typed public layer** adds ~65 allocations per workflow over the
  internal worker: the JSON encoding and decoding of each activity's input
  and output (codec lookups are 0 allocs) and each activity's key.

## Where the time goes (go-flows, async commit)

With `synchronous_commit=off` an activity costs ~6.6 ms (20-activity chain:
fixed cost ~15 ms per workflow). Timing the store calls showed the eager loop
works as designed (one resume per activity, no polls, no hand-overs), and that
the two round trips cost ~3.4 ms (complete workflow task) and ~2.3 ms
(complete activity, resume) of **server time**. Postgres statement logging
gave, per execution (bind + execute):

| Statement | ms |
| --- | --- |
| `insertHistory` (unnest of the event columns) | 0.94 |
| `finishTask` (delete task, insert result) | 0.92 |
| `updateRunning` (lease, ready_at) | 0.93 |
| `insertActivities` | 0.71 |
| `wakeForTask` | 0.51 |
| `fence`, `deletePending`, `resumeExtend`, `resumeWorkflow` | 0.2 to 0.4 each |

Forcing generic plans (`plan_cache_mode=force_generic_plan`) saved ~8%,
within noise. The cost is the number of statements per activity on this CPU.

## Allocations (go-flows, 10-activity workflow, ~75)

T8.2 brought `BenchmarkActivityRun` from ~1 240 to ~75 allocations per
workflow (-94%), with the same SQL and round trips, guided by memory
profiles (`-memprofilerate 1`; the difference of two runs of 100 and 400
iterations gives the steady state exactly):

| Change | allocations per workflow |
| --- | --- |
| The fast path writes Bind/Execute/Sync messages into a pooled buffer and reads replies message by message, instead of a `pgconn.Batch` per call (its buffer, descriptions and formats were allocated each time) | ~1 240 to ~750 |
| The claim decodes its pending-event arrays by hand (pgx planned a scan per array) | ~750 to ~600 |
| The benchmark warms up first (the pool's SCRAM handshakes were counted); tasks carry no input/output (store.WorkflowTask); no history load for a new execution | ~600 to ~385 |
| The context is watched once per context (a deadline, then a cancel request, when it ends), not by pgconn per call | ~385 to ~350 |
| Eager activities run on reused runner goroutines; heartbeat entries by value; a lease's untrack is a value, not a closure | ~350 to ~250 |
| The executor reuses its completion; resumed tasks are written into the driver's two buffers (their pending slice reused); calls are allocated in blocks; workflow names, queues and statuses are interned | ~250 to ~130 |
| Workflow start (`CreateExecution`) on the fast path; completions return leases in the completion's scratch space | ~130 to ~105 |
| The LISTEN connection is watched once, not per notification; drivers reuse their eager window timer and result channel; executor options built once; workflow state and scheduler hold their first buffers and root context; workflow tasks run on reused runner goroutines | ~105 to ~78 |
| Activity claims on the fast path (idle polls cost little) | ~78 to ~75 |

What remains (~75): a 20-activity workflow takes ~110, so ~40 per workflow
(start, claim, executor, scheduler, first coroutine, driver, end) and ~3.5
per activity, two of them the attempt's cancelable context (its
`context.Cause` is the documented way to tell a canceled activity, so it is
a real `context.WithCancelCause`).

## Polling (T8.2)

Idle pollers used to reset their backoff on every store hint, so each hint
cost a series of empty claims. A hint now polls once without resetting the
backoff. Work a worker commits with a delay (timers, retry delays) gets no
hint, since it is not ready at commit: the worker records its due time and
polls then, so a backed-off poller does not run it up to `PollMax` late
(`TestDelayedWorkIsNotPollMaxLate`).
- **Due times** are rounded up to 10 ms (`PollMin` if shorter, at least
  1 ms) and deduplicated (at most 100 such polls a second by default), and
  only those within 64 `PollMax` are kept (at most 6 400 times); a new
  earliest time shortens the poller's sleep without a claim.
- **A timed poll that finds work** resets the interval, so work without hints
  (LISTEN down) is not found at `PollMax` intervals either. A watcher that
  starts (or restarts) listening wakes every queue once: work made ready
  before it listened got no hint.
- **The store's due times (T9.1).** Each claim also returns, in the same
  round trip, when the next work of the queues becomes claimable: a future
  `ready_at` (timers, retries), a `not_before` (hand-backs), an activity
  task's future `visible_at` (retry delays), whichever engine committed it.
  These are index lookups, a row or so per queue
  (`TestDueStatementsReadFewRows`). Hand-backs, retry delays and the
  executions an engine releases at `Stop` are announced (a notification), so
  idle engines claim once and learn the due time. A timer committed by a
  running engine is not announced: the execution is sticky to that engine,
  which knows the due time. Only recovery after a lost lease is still found
  by a timed poll, so up to `PollMax` late. A timer of an execution sticky
  to another engine may wake idle engines once, for an empty claim; kept
  leases (eager execution) are not due times. The `ready_at` lookup reads
  at most 64 index rows per queue: a queue with more kept executions due
  before its first timer falls back to the timed poll.

Runs that take 0.5 to 1.7 s instead of ~0.15 s appear now and then on the
development machine. A trace of store calls showed every statement of
such runs taking 50 to 80 ms instead of ~5 ms, with the engine's flow
unchanged (no hand-back, no poll): stalls of the host's disk (Postgres
fsync), not of the engine.

## Other benchmarks (perf gate)

`make bench && make benchgate` checks medians against `perf-budget.txt`:

| Benchmark | Measured | Budget |
| --- | --- | --- |
| `BenchmarkScheduler` (coroutine hand-off) | 0 allocs, ~660 ns/switch | 0 allocs |
| `BenchmarkChannelPingPong`, `BenchmarkCommitActivity`, `BenchmarkHashArgs` | 0 allocs | 0 |
| `BenchmarkReplay` (cold, 302 events) | 30 allocs, ~0.85 us/event | 34 |
| `BenchmarkEagerActivity` (store side, one activity) | 6 allocs, ~780 B | 8, 1 KiB |
| `BenchmarkActivityRun` (50 iterations per sample) | ~75 allocs | 85 |
| `BenchmarkPublicActivityRun` (50 iterations per sample) | ~140 allocs | 155 |
| `BenchmarkCodec` encode / decode / lookup | 1 / 1 / 0 allocs | same |

The end-to-end benchmarks run a fixed 50 iterations per sample (`make
bench`): with the default ~7, a few timed polls of the pollers swung their
per-op allocations by +-20%. Their samples now stay within +-3%; the
budgets keep ~12% headroom. `flowtest`'s `BenchmarkFanOut` (256
coroutines each running a mocked activity: ~5 ms for 16, ~6 to 10 ms for 64,
~10 to 18 ms for 256, close to linear, against go-workflows' O(n^3) tester,
issue #377) has no rule: it counts the allocations of the whole tester
process.

## Reproducing

```sh
make up
make bench && make benchgate                          # go-flows, perf gate
FLOWS_BENCH_ASYNC_COMMIT=1 go test -run '^$' -bench BenchmarkActivityRun -benchmem -count 6 ./internal/worker
(cd bench/goworkflows && go test -run '^$' -bench . -benchmem -benchtime 10x -count 3 .)
# saga, from its repository, against the same server:
SAGA_TEST_DSN='postgres://flows:flows@localhost:5433/flows?sslmode=disable' \
  go test -run '^$' -bench 'BenchmarkStepRun$' -benchmem -count 6 .
```

`bench/goworkflows` is a separate module, so go-workflows is not a dependency
of go-flows.
