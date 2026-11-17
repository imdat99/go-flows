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
  by a timed poll (T10.1): nothing announces a lease running out, and while
  it was renewed no claim could report it, so `PollMax` is capped at a third
  of `LeaseTTL` (the renewal interval). An idle engine thus polls at least
  that often, and a crashed engine's work resumes within about 4/3
  `LeaseTTL` (`TestCrashedEngineWorkIsRecoveredWithinLeaseTTL`). Kept leases
  (eager execution) are not due times: their `ready_at` is the lease expiry.
  Nor is a timer falling due while its execution is sticky to the engine
  that ran its last task (T10.3), when that engine says it wakes for it
  (`Completion.DueKnown`: it adds the execution's next timer, whoever
  committed it, to its local due times, within half their horizon): that
  engine claims it, and an idle engine waking for it made an empty claim
  (`TestStickyTimersDoNotWakeOthers`, `TestStickyOwnerFiresTimersOfOthersOnTime`);
  if that engine dies, the timed poll above finds it. Both are marked
  (`due_owner`, written only with `ready_at`, so claims and renewals stay
  HOT updates; it counts while `sticky_owner` names the same engine, which
  older binaries clear, so they leave no stale mark; `Unstick` clears it) and left out of a partial
  index (`execution_due_idx`, T10.2), so the `ready_at` lookup reads one
  entry per queue however many kept leases fall due before the first timer.

An activity claim holds the free activity slots it takes for its round
trip; before T10.21 it took all of them, so an eager launch meanwhile found
none and its activity went through the queue: a NOTIFY waking every
engine's poller, which all claim (one finds it), and so more claims holding
the slots. `BenchmarkEagerMisses` (internal/worker; 4 concurrent
10-activity workflows per engine, a fixed 20 batches per sample in `make
bench`, `-count 6`) counts it. The claim now leaves one slot free when more
than one is (a single free slot is still taken): roughly 80 to 85% fewer
misses and activity claims with 3 engines, allocations unchanged. The
spread between runs is wide; two measurements:

| 3 engines | misses before | misses after | activity claims |
| --- | --- | --- | --- |
| T10.21 | 2.9% | 0.06% | 0.094 to 0.012 per activity (-87%) |
| its review | 8.9% | 1.4% | -80% |

Every miss came during a claim. Allocations per batch: 1270 to 1159
(p=0.09). With one engine the misses were ~0.4% (timed polls only) and stay
about that.

Runs that take 0.5 to 1.7 s instead of ~0.15 s appear now and then on the
development machine. A trace of store calls showed every statement of
such runs taking 50 to 80 ms instead of ~5 ms, with the engine's flow
unchanged (no hand-back, no poll): stalls of the host's disk (Postgres
fsync), not of the engine.

T10.16 confirmed it with three simultaneous traces over 2000 runs of the
`BenchmarkActivityRun` workflow (10 minutes): the store calls of every run
(timed in a wrapping store), every `fdatasync`/`fsync` of every Postgres
process (`strace -f -T` from a container sharing the Postgres container's
pid namespace), and the VM's disk (`iostat -x 1`). 21 runs took 0.9 to
1.7 s, in three episodes of ~10 s, minutes apart. In each:

- the store calls add up to the run's time (within 20 to 60 ms of it): the
  engine waits on Postgres, nothing else;
- the backends' WAL `fdatasync` takes 50 to 90 ms (median ~55 ms) instead
  of ~3 ms, every one of them, for the whole episode, so the commits per
  second drop from ~75 to ~14;
- the disk's write latency (`w_await`) rises from ~1.5 ms to ~30 ms while
  it writes 5 times less (~150 to ~30 writes/s): the device is slow, not
  busy. No checkpoint, autovacuum or other Postgres `fsync` falls in the
  episodes (the checkpointer's `fsync` calls came at other times).

Postgres ran with its defaults, `fsync=on` and `synchronous_commit=on`
(`wal_sync_method=fdatasync`): a commit returns after the WAL `fdatasync`,
so a commit's latency is that call's. The development machine is a VM
whose virtual disk stalls for seconds at a time, and durable commits expose
it. It is not the engine, and there is nothing in it to fix. No Go
execution trace (`-trace`) was taken: the store-call timings already cover
the whole run time, so a trace could only show the goroutines blocked on
the Postgres socket. The development VM stalls this way, and a shared CI
runner can too: benchmarks that commit to Postgres gate allocations only,
never time (`perf-budget.txt`).

To reproduce (the Postgres of `make up`; the strace and libunwind of the
host, mounted in a glibc image; `-p` every running postgres process, `-f`
follows the backends forked later):

```sh
L=/lib/x86_64-linux-gnu
docker run --rm --name flows-strace --pid=container:workflows-postgres-1 \
  --cap-add SYS_PTRACE -v "$PWD":/out -v /usr/bin/strace:/usr/local/bin/strace:ro \
  -v $L/libunwind-ptrace.so.0:$L/libunwind-ptrace.so.0:ro \
  -v $L/libunwind-x86_64.so.8:$L/libunwind-x86_64.so.8:ro \
  -v $L/libunwind.so.8:$L/libunwind.so.8:ro postgres:16 sh -c '
    P=""; for d in /proc/[0-9]*; do [ "$(cat $d/comm 2>/dev/null)" = postgres ] && P="$P -p ${d#/proc/}"; done
    exec strace -f $P -T -ttt -e trace=fdatasync,fsync -o /out/fsync.txt' &
iostat -x -t -y vda 1 > iostat.txt &
go test -run '^$' -bench '^BenchmarkActivityRun$' -benchtime 2000x ./internal/worker
docker stop flows-strace; kill %2
```

The store-call timings came from throwaway test code (not kept): a store
wrapping `pg.Store` that timed `CompleteWorkflowTask`,
`CompleteActivityTask` and the two `Claim*Due` calls and printed the calls
of every run longer than 3 times the median.

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

Headroom on a shared CI runner (T10.16): the CI's bench logs and its
`bench.txt` artifact need a GitHub login, so they were not read (the bench
job now writes the gate's table to its step summary). The public runs API
(`/repos/imdat99/go-flows/actions/runs` and each run's jobs) gave the bench
job's conclusion: success in all 5 ci runs up to 2026-11-16 (the medians
themselves unknown). Emulating a
4-vCPU runner (the benchmarks pinned to 4 cores, two of them kept busy by a
CPU hog) moved no allocation median: `BenchmarkActivityRun` 76 (samples
72-77), `BenchmarkPublicActivityRun` 141 (136-145), `BenchmarkEagerActivity`
5.5 allocs and 740-824 B, `BenchmarkLockStep` 0 allocs and 95-176 B,
`BenchmarkReplay` 30, against 76.5 / 140 / 6 / 0 / 30 on the quiet 6-CPU
machine. Allocation counts barely depend on the CPU or its load, rather on
how many timed polls and claims fall in a sample, so the ~10% headroom of
the end-to-end rules (single samples reach 81 and 145) is kept, not cut.
The only time rule, `BenchmarkScheduler` ns/switch <= 2000, has ~2.5x
headroom even so (~730 median, 620-800, against ~650 quiet); the end-to-end
benchmarks have no time rule, since a disk stall as above multiplies their
time.

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
