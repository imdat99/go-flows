# Kế hoạch: go-flows — engine workflow + saga trên Postgres

## Context

Hai engine hiện có, mỗi bên mạnh đúng chỗ bên kia yếu (chi tiết: `docs/so-sanh-go-workflows.md`):
- **saga** (`/home/dat/projects/saga`): bảo đảm nhiều instance chặt (fencing `lease_version`, chỉ dùng `now()` của DB, heartbeat gộp `SKIP LOCKED`, `Migrate` có advisory lock, enqueue cùng transaction), lớp saga hạng nhất (compensator theo tên, step `uncertain`, `needs_attention`, `RetryCompensation`), versioning, business lock, `RunHook`, làm sạch dữ liệu lưu. Thiếu: concurrency trong workflow, Select, sub-workflow, huỷ, ContinueAsNew, tester.
- **go-workflows** (`/home/dat/projects/go-workflows`, `48e8119` = upstream master): mô hình kiểu Temporal (event history, coroutine, Select, Channel, sub-workflow, ContinueAsNew), tester, analyzer. Audit 2026-11-15 cho thấy **backend Postgres không an toàn khi chạy nhiều instance** và executor có lỗi nghiêm trọng. Upstream cũng bảo trì chậm (19 issue mở, 13 PR sửa lỗi chưa merge, có PR treo từ 2025).

Mục tiêu: repo mới `/home/dat/projects/workflows` (module `github.com/imdat99/go-flows`) với mô hình event history của go-workflows, bảo đảm và lớp saga của saga, và **API mới hoàn toàn** (generic, kiểu saga).

### Quyết định đã chốt
- **Port có chọn lọc.** Copy có ghi nguồn (MIT, `NOTICE`) phần đáng giữ của go-workflows. Không mang git history. **Tách hẳn upstream.**
- **Backend:** Postgres (pgx/v5 trực tiếp) và store bộ nhớ (cho tester và unit test). Bỏ MySQL, SQLite, Redis.
- **Hàng đợi:** bảng riêng của engine, không dùng River. Lý do:
  - River chỉ fence bằng `state='running'`, nên fencing vẫn phải nằm trên dòng instance.
  - River tốn khoảng ≥ 4 round trip mỗi activity cộng một dòng `river_job` cho mỗi lần đánh thức; `ready_at` + eager tốn khoảng 2.
  - Cần xử lý tuần tự theo instance.
  - Phải tự làm những gì River có sẵn: fetch theo lô, NOTIFY, backoff, retention. Các việc này nằm ở T2.5, T2.11, T2.12.
- **Lưu trạng thái:** event history, cùng `pending_event` và `activity_task`.
- **API:** mới hoàn toàn, không ràng buộc tương thích với go-workflows.

## Kết quả audit go-workflows → danh mục hồi quy (mỗi mục là một test viết trước)

| Mã | Lỗi (đã xác nhận trong code) | Cách xử lý trong go-flows |
|---|---|---|
| GW-1 | Instance active trùng khi tạo đồng thời (#489); signal tới copy ngẫu nhiên | Unique index một phần `(instance_id) WHERE active`; `23505` → `ErrInstanceExists` / idempotent như saga |
| GW-2 | Fencing chỉ theo tên worker; ABA cùng worker; `ExtendActivityTask` không kiểm tra số dòng; `WithWorkerName` trùng nhau là tắt fencing | `owner_id` + `lease_version` trên instance và activity_task; mọi lần ghi khoá dòng và kiểm tra; 0 dòng → `ErrLeaseLost` |
| GW-3 | Signal/cancel mất khi xen với ContinueAsNew/kết thúc (giống #493) | `INSERT … SELECT` từ dòng active `FOR SHARE`; ContinueAsNew chuyển pending signal sang execution mới trong cùng tx |
| GW-4 | Retention bỏ sót `pending_events`, `activities` → phình mãi | FK `ON DELETE CASCADE`; dọn khi kết thúc; xoá theo lô `SKIP LOCKED` |
| GW-5 | `time.Now()` của client ở ~10 chỗ (lock, sticky, timer, completed_at) | Chỉ `now()` của DB; deadline = `now() + $n * interval '1 microsecond'` |
| GW-6 | Lỗi heartbeat tạm thời → activity bị ghi thất bại vĩnh viễn | Mất lease thì bỏ, không complete; heartbeat lỗi tạm thì "busy", không phải "lost" (như saga lỗi 6) |
| GW-7 | Migration panic, `DROP TABLE IF EXISTS`, không đóng driver | goose + `pg_advisory_lock` như `saga/migrate.go`; trả lỗi; `IF NOT EXISTS` |
| GW-8 | Poll quét toàn bộ, không ORDER BY, poll 200 ms không backoff | Cột `ready_at` + partial index, `ORDER BY ready_at`, nhận theo lô, LISTEN/NOTIFY, backoff |
| GW-9 | Cache sticky giữ executor sau complete thất bại; 2 task dùng chung executor (data race) | Evict khi có lỗi; mutex theo instance; khoá cache theo `(execution_id, last_seq, lease_version)` |
| GW-10 | Panic làm sập process (coroutine timeout 40 s, signal sai kiểu, `invalidStateTransition`) | `recover` ở mọi biên; chuyển thành lỗi; deadlock → lỗi, không panic |
| GW-11 | Kiểm tra non-determinism nông; khi phát hiện thì fail vĩnh viễn; không có versioning | So khớp loại, tên, hash argument, thời lượng timer, replay ngắn hơn lịch sử; **park `needs_attention`**; `name@version` |
| GW-12 | Registry lấy tên theo reflection (trùng `pkgA.F`/`pkgB.F`, đổi tên là hỏng); workflow chưa đăng ký → fail vĩnh viễn (#464) | Tên tường minh; trùng thì panic; chưa biết version → bàn giao (hand back) như saga |
| GW-13 | Activity không bao giờ bị huỷ (#468); không có terminate (#196) | Huỷ lan tới context của activity qua heartbeat; `Terminate` |
| GW-14 | Signal đến sớm giao theo thứ tự ngược; quá 100 thì bỏ âm thầm | FIFO, không giới hạn ngầm (giới hạn rõ ràng → lỗi) |
| GW-15 | Retry chạy trong coroutine (khoảng 6 event mỗi lần, đổi `RetryOptions` là non-determinism, off-by-one, `CanRetry` không dùng `errors.As`) | Retry phía engine trên `activity_task` (`attempt`, `visible_at`); history chỉ ghi kết quả cuối; `errors.As` |
| GW-16 | Scheduler O(n) mỗi event; TTL cache 10 s nên thường replay toàn bộ | Scheduler chỉ đánh thức coroutine sẵn sàng; TTL dài hơn, giới hạn theo bộ nhớ |
| GW-17 | Tester: #494, #481, #436, #377 | Viết lại trên store bộ nhớ + đồng hồ ảo tất định |
| GW-18 | `SideEffect` nuốt lỗi convert; `ParamsMatch` lệch với tham số `any`; float64 mất chính xác (#471) | API generic (không có `any` trong đường dữ liệu); decode bằng `UseNumber` |

Mục còn mở từ saga cũng đưa vào danh mục: escape surrogate UTF-16 đơn lẻ trong JSON (lỗi 4/8) và kiểm tra tên trong `Define*`.

## Kiến trúc

```
go-flows/
  flows.go, workflow.go, activity.go, ctx.go   # API công khai (package flows)
  select.go, channel.go, timer.go, signal.go, child.go, lock.go, hooks.go, errors.go, policy.go
  engine.go, client.go, migrate.go
  internal/sync/       # port go-workflows internal/sync (future, channel, selector, waitgroup); VIẾT LẠI scheduler
  internal/command/    # port go-workflows internal/command (state machine); panic → lỗi; thêm hash arg, cancel activity
  internal/history/    # port backend/history; payload json.RawMessage; kiểm tra storable
  internal/executor/   # viết lại catch-up/replay, non-determinism → park, cache an toàn
  internal/store/      # interface Store + bộ test conformance chung
  internal/store/pg/   # Postgres mới (pgx/v5) theo thiết kế saga/internal/store
  internal/store/mem/  # store bộ nhớ
  internal/worker/     # poller, heartbeat gộp, dispatcher, LISTEN/NOTIFY
  internal/testdb/     # port nguyên từ saga/internal/testdb
  migrations/          # goose, schema `flows`
  flowtest/            # tester mới
  idem/                # port saga/idem
  analyzer/            # port go-workflows/analyzer (module riêng), đổi quy tắc sang API mới
  test/chaos/          # port saga/test/chaos
```

### API công khai (phác thảo)
```go
wf := flows.DefineWorkflow[I, O]("order", 1, func(c flows.Ctx, in I) (O, error) { ... }, opts...)
act := flows.DefineActivity[I, O, C]("charge", invoke, compensate,
        flows.WithRetry(p), flows.WithCompensatePolicy(p), flows.WithTimeout(d), flows.WithQueue(q))
// invoke: func(ctx context.Context, k flows.Key, in I) (O, C, error)
out, err := act.Run(c, in)            // = act.Start(c, in).Get(c)
f := act.Start(c, in)                 // Future[O]
flows.Go(c, func(c flows.Ctx) { ... }); flows.Select(c, flows.On(f, ...), flows.OnReceive(ch, ...), flows.Default(...))
flows.Sleep(c, d); flows.NewTimer(c, d); flows.Await[T](c, "name", timeout); flows.SignalChannel[T](c, "name")
child.Start(c, in, flows.ChildID(...)); flows.ContinueAsNew(c, in)
flows.Now(c); flows.Rand(c); flows.SideEffect[T](c, fn); flows.Lock(c, key, ttl, flows.LockWait(p))
flows.Compensate(c)   // chủ động hoàn tác; mặc định workflow lỗi → hoàn tác
eng, _ := flows.New(pool, flows.Options{InstanceID, LeaseTTL, MaxWorkers, Queues, Hooks, Observer, Retention, ...})
eng.Start(ctx); eng.Stop(ctx)
h, _ := wf.Start(ctx, eng, idemKey, in); wf.Run(...); h.Wait(ctx)
eng.Signal / SignalKey / Cancel / Terminate / RetryCompensation / PurgeRuns
flows.Migrate(ctx, pool)
```

### Schema Postgres (schema `flows`)
- **`instance`:** `id`, `instance_id`, `execution_id`, `workflow`, `version`, `parent_*`, `status` (active, completed, failed, canceled, terminated, compensating, compensated, needs_attention), `queue`, `input`/`output`/`error`, `error_kinds`, `owner_id`, `lease_version`, `locked_until`, `sticky_owner`, `sticky_until`, `ready_at`, `created_at`, `completed_at`.
  - Unique `(instance_id) WHERE status IN active-like`.
  - Partial index `(queue, ready_at) WHERE active`.
- **`history`:** PK `(instance_pk, seq)`, `type`, `schedule_id`, `name`, `arg_hash`, `payload`, `comp_data`, `uncertain`.
- **`pending_event`:** `instance_pk` FK cascade, `visible_at`. Mỗi lần insert thì cập nhật `instance.ready_at = least(...)`.
- **`activity_task`:** unique `(instance_pk, schedule_id)`, `kind` (invoke/compensate), `attempt`, `visible_at`, `owner_id`, `lease_version`, `locked_until`, FK cascade.
- **`lock`:** giống `saga.lock`.

Mọi lần ghi của worker đi qua một hàm tương đương `store.Fenced` của saga (`saga/internal/store/store.go:265`): khoá dòng `FOR NO KEY UPDATE`, kiểm tra `owner_id`/`lease_version`. Hoàn tất workflow task là **một transaction**: ghi history, xoá pending đã xử lý, tạo activity_task, timer, signal/child; nhả hoặc giữ lease; NOTIFY.

### Lớp saga trên event history
- `ActivityCompleted` lưu `comp_data`. Activity crash, timeout, panic hoặc output không lưu được thì ghi `uncertain` (vẫn được compensate, `CompensationUncertain(ctx)`).
- Khi workflow lỗi hoặc gọi `Compensate`: executor chuyển instance sang `compensating`. Engine dựng danh sách compensation **từ history**, không cần chạy lại hàm workflow. Mỗi compensation là một `activity_task kind=compensate`, chạy tuần tự theo thứ tự ngược, retry theo `CompensatePolicy`. Hết lượt thì `needs_attention`; `RetryCompensation` mở lại.
- Compensator không có trong binary này → bàn giao (giống `handBack`, `saga/engine.go:617`).
- `RunHook.RunEnded(ctx, tx, info)` chạy trong transaction kết thúc; lỗi hoặc panic thì rollback.

## Hiệu năng: CPU, GC, allocation thấp nhất có thể

Đây là yêu cầu hạng nhất, không phải bước tối ưu ở cuối. Mỗi task chạm đường nóng phải có benchmark (`-benchmem`) và không được làm tăng allocs/op.

**Mốc tham chiếu** (`saga/docs/benchmarks.md`):
- saga: 10 step no-op tốn **40 allocs/run**, 3.3 KiB/run, 2 round trip mỗi step (1 nếu `AsyncStepStart`).
- go-workflows dùng `reflect.Call`, mỗi event mã hoá một envelope JSON, scheduler tạo `time.Timer` mỗi lần chạy, và workflow task cùng activity task tách riêng.

**Mục tiêu:**

| Benchmark | Mục tiêu |
|---|---|
| `BenchmarkActivityRun` (10 activity no-op, cache nóng) | ≤ 2 round trip mỗi activity trên đường eager; ≤ 80 allocs/run (sau đó siết dần về gần 40 của saga); ≤ 8 KiB/run |
| `BenchmarkReplay` (cache lạnh, 100 event) | Không dùng `reflect` trên đường nóng; ≤ 2 allocs/event |
| `BenchmarkScheduler` (1000 coroutine, mỗi coroutine một bước) | O(số coroutine sẵn sàng); 0 alloc mỗi lần chuyển coroutine ở trạng thái ổn định |
| `BenchmarkThreeInstances` | runs/s ≥ saga trên cùng máy |

**Kỹ thuật bắt buộc:**
1. **Không dùng reflection trên đường nóng.**
   - `DefineWorkflow`/`DefineActivity` generic tạo sẵn closure đã gõ kiểu cho encode, decode và invoke. Không có `reflect.Call`, không có tham số `any`.
   - Encode qua `jsonSlot` có pool (`saga/jsonslot.go`). Có thể dùng `encoding/json/v2` khi cho cùng bytes.
2. **SQL trên `pgconn` trực tiếp** (`saga/internal/store/fast.go`):
   - statement prepare một lần cho mỗi kết nối;
   - tham số text lấy từ buffer có pool;
   - kết quả dạng binary, decode bằng tay;
   - theo dõi context một lần cho mỗi lượt thực thi.
   - Nếu pool không dùng `QueryExecModeCacheStatement` hoặc có tracer thì đi đường pgx thường.
3. **Ít round trip:**
   - Nhận workflow task và nạp event mới trong một câu lệnh (CTE ... RETURNING).
   - Hoàn tất workflow task trong một transaction gửi bằng pipeline (`pgconn` batch).
   - Hoàn tất activity và đánh dấu instance sẵn sàng trong một câu lệnh.
4. **Thực thi eager tại chỗ:**
   - Khi workflow task lập lịch activity và engine còn slot trống, `activity_task` được tạo **đã nhận sẵn** (owner và lease) ngay trong transaction đó. Activity chạy luôn, không qua poll.
   - Khi activity xong và instance đang sticky với chính engine này, chạy tiếp workflow task ngay.
   - Đường đầy đủ khi đó còn khoảng 2 round trip mỗi activity, ngang saga. Poll chỉ dành cho việc khôi phục và cho việc của instance khác.
5. **History lưu theo cột có kiểu, không dùng envelope JSON:**
   - các cột `type smallint`, `schedule_id int`, `name text`, `arg_hash int8`, `payload bytea`;
   - payload là bytes của người dùng, không base64;
   - codec dạng append vào `[]byte` tái sử dụng.
6. **Runtime tất định:**
   - Scheduler theo hàng đợi sẵn sàng, không có `time.Timer` mỗi bước. Phát hiện treo bằng một watchdog chung cho mỗi executor.
   - Channel chuyển giao coroutine được tái sử dụng.
   - Command, future và slice lấy từ arena theo execution, trả lại khi evict.
7. **Bộ nhớ và GC:**
   - Cache sticky giới hạn theo số lượng và tổng byte; evict LRU.
   - Key và ID lấy từ arena hoặc dựng không qua `fmt` (`saga/context.go` `keyArena`, `saga/runid.go`).
   - Log bằng `slog.LogAttrs`, kiểm tra level trước khi dựng attr.
   - Heartbeat gộp một câu lệnh cho mỗi engine.
8. **Đo bằng công cụ:** `pprof` (cpu, alloc), `benchstat` với `-count 6` chạy xen kẽ, `GODEBUG=gctrace=1` khi chạy tải.
   - Perf gate trong CI: `make benchgate` so median với ngân sách tuyệt đối trong `perf-budget.txt` (allocs/op giới hạn chặt). Khi review task ⚡ thì so trước/sau bằng `make benchcmp`.

## Quy trình cho mỗi task (bắt buộc)

1. Viết test trước: test tái hiện mục GW-* hoặc test hành vi. Test phải đỏ.
2. Cài đặt.
3. **Test:**
   - `go test -race ./...` trên `mem` và `pg`.
   - `go vet`, `golangci-lint`.
   - Nếu task chạm đường nóng: benchmark `-benchmem -count 6`, rồi `benchstat` so với trước.
4. **Review độc lập bằng subagent:** chỉ đưa diff và tiêu chí của task, không kèm kết luận của mình. Review kiểm tra tính đúng, bất biến (fencing, thời gian DB, recover), allocation và test có đủ không.
5. Review không đạt thì sửa và review lại. **Không đạt quá 3 lần thì dừng:** ghi lý do vào `TASKS.md`, xem lại cách thực hiện (thiết kế hoặc chia lại task), báo người dùng trước khi tiếp tục.
6. Đạt thì đánh dấu `[x]` trong `TASKS.md`, kèm số liệu benchmark nếu có, rồi commit.

## Danh sách task

Danh sách này được chép vào `TASKS.md` của repo ở T0.1. Ký hiệu ⚡ là task chạm đường nóng, phải có benchmark.

### P0 — Khởi tạo
- [ ] **T0.1** `git init`, `LICENSE` MIT, `NOTICE` (nguồn cschleiden/go-workflows), `AGENTS.md`/`CLAUDE.md` (bất biến, quy trình task, ngân sách hiệu năng), `TASKS.md`, chép `so-sanh-go-workflows.md` và bảng GW-* vào `docs/`.
- [ ] **T0.2** `Makefile` (`test`, `race`, `race10`, `lint`, `bench`, `benchcmp`, `benchgate`, `chaos`), `docker-compose.yml` (Postgres 16, fsync bật), `.golangci.yml`.
- [ ] **T0.3** Port `internal/testdb` từ saga (đổi biến môi trường sang `FLOWS_TEST_DSN`).
- [ ] **T0.4** Khung benchmark: `make bench`, `make benchcmp` (benchstat so hai lần chạy), `make benchgate` (gate theo ngân sách tuyệt đối trong `perf-budget.txt`, port từ saga).

### P1 — Runtime tất định
- [ ] **T1.1** ⚡ Port future, channel, selector, waitgroup, context từ `go-workflows/internal/sync`; bỏ `any` khỏi API nội bộ; test đơn vị gốc chạy được.
- [ ] **T1.2** ⚡ Viết lại scheduler theo hàng đợi sẵn sàng; watchdog chung; deadlock → `ErrDeadlock`; panic coroutine → lỗi có stack (GW-10, GW-16). Bench `BenchmarkScheduler`.
- [ ] **T1.3** ⚡ `internal/history`: kiểu event, codec append, `arg_hash` (FNV-1a 64 trên bytes input). Bench encode/decode.
- [ ] **T1.4** Port `internal/command`: `invalidStateTransition` → lỗi; thêm command huỷ activity; test state machine gốc.

### P2 — Store
- [ ] **T2.1** Interface `internal/store`, kiểu Task, Lease (`owner`, `version`); bộ test conformance (khung lấy từ `go-workflows/backend/test`).
- [ ] **T2.2** `internal/store/mem` qua toàn bộ conformance.
- [ ] **T2.3** `migrations/00001` (schema `flows`) cùng `Migrate` có advisory lock và goose (port `saga/migrate.go`, GW-7). Test: 3 instance chạy migrate cùng lúc.
- [ ] **T2.4** PG tạo instance: unique một phần, idempotency key, xung đột workflow (GW-1). Test tạo đồng thời.
- [ ] **T2.5** ⚡ PG nhận workflow task: `ready_at`, partial index, `ORDER BY`, lô, `lease_version++`, chỉ dùng `now()` (GW-5, GW-8). Kiểm tra bằng `EXPLAIN`.
- [ ] **T2.6** ⚡ PG hoàn tất workflow task: một transaction pipeline, được fence, unique `(instance, seq)` (GW-2). Test ABA cùng owner, owner cũ bị chặn.
- [ ] **T2.7** ⚡ PG activity_task: nhận, eager-claim, gia hạn có kiểm tra số dòng, hoàn tất cộng đánh dấu sẵn sàng trong một câu lệnh, retry bằng `visible_at` (GW-2, GW-6, GW-15).
- [ ] **T2.8** ⚡ Đường nhanh `pgconn` cho T2.5–T2.7 (port `fast.go`). Bench allocs.
- [ ] **T2.9** PG signal/cancel/terminate có khoá dòng; xen với kết thúc hoặc ContinueAsNew không mất signal (GW-3, GW-13).
- [ ] **T2.10** Heartbeat gộp có `SKIP LOCKED`, phân biệt busy và lost (port `saga/lease.go`, GW-6). Test `LockedRowDoesNotStallOtherLeases`.
- [ ] **T2.11** Retention và cascade theo lô `SKIP LOCKED` (GW-4).
- [ ] **T2.12** LISTEN/NOTIFY và poll có backoff (GW-8).

### P3 — Executor và worker
- [ ] **T3.1** ⚡ Executor replay và catch-up trên runtime P1; so khớp loại, tên, `arg_hash`, thời lượng timer; replay ngắn hơn lịch sử → non-determinism (GW-11). Bench `BenchmarkReplay`.
- [ ] **T3.2** Non-determinism, workflow chưa đăng ký, version lạ → park `needs_attention` hoặc bàn giao, không fail (GW-11, GW-12).
- [ ] **T3.3** ⚡ Cache sticky: mutex theo instance, khoá cache theo `(exec, seq, lease_version)`, evict khi có lỗi, giới hạn theo byte (GW-9).
- [ ] **T3.4** Worker/dispatcher: slot, `recover` ở mọi biên, Stop bàn giao trong một transaction.
- [ ] **T3.5** ⚡ Thực thi eager: activity chạy tại chỗ và workflow task chạy tiếp ngay. Bench round trip mỗi activity.
- [ ] **T3.6** Huỷ activity qua heartbeat và context; terminate (GW-13).

### P4 — API công khai
- [ ] **T4.1** ⚡ `DefineWorkflow`/`DefineActivity` generic, đặt tên tường minh, trùng tên thì panic, kiểm tra tên (GW-12); `jsonSlot`.
- [ ] **T4.2** `Engine` (`New`/`Start`/`Stop`/`Options`), `Handle`, `Start`/`Run`/`Wait`, idempotency (port `saga/engine.go:306`).
- [ ] **T4.3** `Ctx` API: `Go`, `Select`, `Sleep`/`NewTimer`, `Await`/`SignalChannel` FIFO (GW-14), `Now`, `Rand`, `SideEffect` (GW-18).
- [ ] **T4.4** Kiểm tra input và làm sạch dữ liệu: `checkName`, `checkStorableJSON` gồm cả surrogate đơn lẻ, `store.Text`, lỗi được tái dựng qua `error_kinds` với `errors.Is` (port saga).
- [ ] **T4.5** Retry policy phía engine, `Permanent` dùng `errors.As` (GW-15).
- [ ] **T4.6** `examples/` (đơn hàng, fan-out, signal) chạy end-to-end.

### P5 — Lớp saga
- [ ] **T5.1** `comp_data` và `uncertain` trong `ActivityCompleted`/`Failed`; `CompensationUncertain`.
- [ ] **T5.2** Pha compensating dựng từ history, thứ tự ngược, `activity_task kind=compensate`, `CompensatePolicy`.
- [ ] **T5.3** `needs_attention`, `RetryCompensation`, bàn giao khi gặp compensator lạ.
- [ ] **T5.4** `Lock`/`LockWait` gắn với lease (port `saga/lock.go`).
- [ ] **T5.5** `RunHook` chạy trong transaction kết thúc, `Observer`, port `idem/`.
- [ ] **T5.6** Port test saga: `CompensationReverseOrder`, `review_fixes_test.go`, `lock_test.go`, `async_test.go`.

### P6 — Sub-workflow và ContinueAsNew
- [ ] **T6.1** Child workflow: tạo, kết quả, huỷ theo cây, nhắm theo `instance_id`.
- [ ] **T6.2** ContinueAsNew: chuyển signal và pending một cách nguyên tử, cache evict đúng (GW-3).
- [ ] **T6.3** Compensation lồng nhau: cha hoàn tác thì child hoàn tác.

### P7 — Công cụ
- [ ] **T7.1** `flowtest`: store bộ nhớ, đồng hồ ảo tất định, mock activity, phát hiện future còn treo (GW-17). Test các kịch bản #494, #481, #436, #377.
- [ ] **T7.2** Port `analyzer` theo API mới.
- [ ] **T7.3** ⚡ OpenTelemetry tracing và metrics (tắt thì không tốn alloc).

### P8 — Chất lượng và phát hành
- [ ] **T8.1** Port `test/chaos`: SIGKILL, SIGSTOP, 3 instance, `assertOnce`.
- [ ] **T8.2** Benchmark đầy đủ so với saga và go-workflows, viết `docs/benchmarks.md`, bật perf gate trong CI.
- [ ] **T8.3** Tài liệu song ngữ: concepts, guarantees, operations, patterns.
- [ ] **T8.4** Review tổng thể toàn repo bằng subagent (đọc lạnh), sửa các phát hiện.

## File và hàm tái sử dụng

| Lấy từ | Dùng cho |
|---|---|
| `saga/internal/store/store.go` (`Fenced`, `AcquireLease`, `RenewLeases`, `ReserveRecoverable`, `Now`, `AcquireLock`) | `internal/store/pg` |
| `saga/internal/store/fast.go` (đường chạy nhanh `pgconn`) | `internal/store/pg` |
| `saga/internal/store/text.go` | `internal/store/pg` |
| `saga/lease.go` (heartbeat gộp) | `internal/worker` |
| `saga/migrate.go` | `migrate.go` |
| `saga/engine.go` (`compensate`, `handBack`, `compensationFailed`, `finish`/hooks) | lớp saga |
| `saga/panic.go`, `saga/storable.go`, `saga/errors.go`, `saga/context.go` (`recordedError`, error_kinds) | các phần tương ứng |
| `saga/lock.go`, `saga/hooks.go`, `saga/idem/` | các phần tương ứng |
| `saga/internal/testdb`, `saga/test/chaos` | test |
| `go-workflows/internal/sync/{future,channel,selector,waitgroup,context}.go` | `internal/sync` |
| `go-workflows/internal/command/*` | `internal/command` |
| `go-workflows/backend/history/*` | `internal/history` |
| `go-workflows/workflow/executor/executor.go` (cấu trúc các handler) | `internal/executor` |
| `go-workflows/backend/test/*` (khung conformance + e2e) | test conformance cho store |
| `go-workflows/analyzer` | `analyzer` |

## Verification

- `make test`: unit test và conformance của store trên cả `mem` và `pg` (Postgres qua docker-compose, mỗi test một DB riêng), chạy với `-race`.
- Mỗi mục GW-1…GW-18 có test tên `TestGW<n>_*`. Test đỏ trước khi sửa, xanh sau khi sửa.
- `go test -tags chaos ./test/chaos/...`: kill, đóng băng và tranh chấp nhiều instance; `assertOnce` (mỗi hiệu ứng xảy ra đúng một lần theo key).
- `make bench` rồi `make benchgate`: đạt bảng mục tiêu ở mục Hiệu năng; gate fail khi một số liệu vượt ngân sách trong `perf-budget.txt`. `make benchcmp` (benchstat, `-count 6`, xen kẽ) để so trước/sau.
- `pprof` cho `BenchmarkActivityRun`: không có `reflect` hay `fmt` trên đường nóng.
- Chạy `examples/` end-to-end trên Postgres: đơn hàng có compensation, signal, sub-workflow, ContinueAsNew.
- Mỗi task: test, review bằng subagent, quá 3 lần không đạt thì dừng và xem lại (ghi trong `TASKS.md`).
