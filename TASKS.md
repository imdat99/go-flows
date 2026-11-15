# TASKS

Kế hoạch gốc: quyết định, kiến trúc, schema và API phác thảo nằm trong `docs/PLAN.md`. Danh mục lỗi cần test: `docs/regressions.md`.

Ký hiệu: `[ ]` chưa làm, `[~]` đang làm hoặc làm một phần, `[x]` xong (đã qua test và review). Ghi số lần review không đạt bên dưới task (`review: 1 fail — lý do`).

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

Ký hiệu ⚡ là task chạm đường nóng, phải có benchmark.

### P0 — Khởi tạo
- [x] **T0.1** `git init`, `LICENSE` MIT, `NOTICE` (nguồn cschleiden/go-workflows), `AGENTS.md`/`CLAUDE.md` (bất biến, quy trình task, ngân sách hiệu năng), `TASKS.md`, chép `so-sanh-go-workflows.md` và bảng GW-* vào `docs/`.
- [x] **T0.2** `Makefile` (`test`, `race`, `race10`, `lint`, `bench`, `benchcmp`, `benchgate`, `chaos`), `docker-compose.yml` (Postgres 16, fsync bật), `.golangci.yml`.
- [x] **T0.3** Port `internal/testdb` từ saga (đổi biến môi trường sang `FLOWS_TEST_DSN`).
- [x] **T0.4** Khung benchmark: `make bench`, `make benchcmp` (benchstat so hai lần chạy), `make benchgate` (gate theo ngân sách tuyệt đối trong `perf-budget.txt`, port từ saga).

  - review P0: lần 1 không đạt (thiếu target `race`/`chaos`, benchgate fail khi budget rỗng, còn sót tham chiếu saga); lần 2 đạt.

### P1 — Runtime tất định
- [x] **T1.1** ⚡ Port future, channel, selector, waitgroup, context từ `go-workflows/internal/sync`; bỏ `any` khỏi API nội bộ; test đơn vị gốc chạy được.
- [x] **T1.2** ⚡ Viết lại scheduler theo hàng đợi sẵn sàng; watchdog chung; deadlock → `ErrDeadlock`; panic coroutine → lỗi có stack (GW-10, GW-16). Bench `BenchmarkScheduler`.
- [x] **T1.3** ⚡ `internal/history`: kiểu event, codec append, `arg_hash` (FNV-1a 64 trên bytes input). Bench encode/decode.
- [x] **T1.4** Port `internal/command`: `invalidStateTransition` → lỗi; thêm command huỷ activity; test state machine gốc.

  - Số liệu P1 (Xeon E5-4669 v3, 6 CPU): `BenchmarkScheduler` 0 allocs, khoảng 650 ns mỗi lần chuyển (go-workflows có timer và đi qua scheduler: đo bản đầu tiên được 1.9 µs). Ping-pong channel 1.1 µs, 0 allocs. `CommitActivity` 0 allocs. `HashArgs` 0 allocs, 1.5 GB/s.
  - Review T1.1/T1.2: lần 1 không đạt (Select mất wake-up; unbuffered send hoàn tất khi chưa có receiver; Close rò goroutine; Close một scheduler broken gây race). Lần 2 không đạt (race giữa yield và watchdog; Close không có watchdog bảo vệ). Lần 3 đạt sau khi chuyển sang trạng thái điều khiển atomic dùng CAS (`ctl`).
  - Review T1.3/T1.4: lần 1 không đạt (độ chính xác µs của timer; thiếu trạng thái `Dropped`; side effect không tới `Done`; thiếu Queue và Version). Lần 2 đạt.

### P2 — Store
- [x] **T2.1** Interface `internal/store`, kiểu Task, Lease (`owner`, `version`); bộ test conformance (khung lấy từ `go-workflows/backend/test`).
- [x] **T2.2** `internal/store/mem` qua toàn bộ conformance.
  - Review T2.1/T2.2: lần 1 không đạt (Keep không dùng được cho vòng eager; signal mất khi completion kết thúc execution; cancel mất khi ContinueAsNew; thứ tự pending; stickiness; copy phòng thủ; ...). Lần 2 đạt; các ghi chú nhỏ của lần 2 (lease của `needs_attention`, `Resume` sai execution, completion kết thúc execution còn tạo activity/timer) đã sửa. Bộ conformance có 39 case.
- [x] **T2.3** `migrations/00001` (schema `flows`) cùng `Migrate` có advisory lock và goose (port `saga/migrate.go`, GW-7). Test: 3 instance chạy migrate cùng lúc.
- [x] **T2.4** PG tạo instance: unique một phần, idempotency key, xung đột workflow (GW-1). Test tạo đồng thời.
- [x] **T2.5** ⚡ PG nhận workflow task: `ready_at`, partial index, `ORDER BY`, lô, `lease_version++`, chỉ dùng `now()` (GW-5, GW-8). Kiểm tra bằng `EXPLAIN`.
- [x] **T2.6** ⚡ PG hoàn tất workflow task: một transaction pipeline, được fence, unique `(instance, seq)` (GW-2). Test ABA cùng owner, owner cũ bị chặn.
- [x] **T2.7** ⚡ PG activity_task: nhận, eager-claim, gia hạn có kiểm tra số dòng, hoàn tất cộng đánh dấu sẵn sàng trong một câu lệnh, retry bằng `visible_at` (GW-2, GW-6, GW-15).
- [x] **T2.8** ⚡ Đường nhanh `pgconn` cho T2.5–T2.7 (port `fast.go`). Bench allocs.
- [x] **T2.9** PG signal/cancel/terminate có khoá dòng; xen với kết thúc hoặc ContinueAsNew không mất signal (GW-3, GW-13).
- [x] **T2.10** Heartbeat gộp có `SKIP LOCKED`, phân biệt busy và lost (port `saga/lease.go`, GW-6). Test `LockedRowDoesNotStallOtherLeases`.
- [x] **T2.11** Retention và cascade theo lô `SKIP LOCKED` (GW-4).
  - Review T2.3–T2.11 (Postgres store): lần 1 không đạt (signal chạy đua với ContinueAsNew bị từ chối nhầm `ErrNotActive`; activity task sống sót sau khi execution kết thúc vì SKIP LOCKED; Seq 0 lọt kiểm tra; test timer không ổn định). Lần 2 đạt; ghi chú nhỏ của lần 2 (owner khác, comment về batch) đã sửa.
  - Số round trip: claim 1, completion 1, hoàn tất activity kèm resume 1, cộng 1 lần prepare ở lần đầu mỗi kết nối dùng. Đã biết: deadlock cha/con (cha huỷ con trong lúc con kết thúc) được phát hiện sau `deadlock_timeout` rồi thử lại. `ready_at` vẫn `<= now()` khi execution đang bị lease, nên claim phải quét qua các dòng này (chuyển sang xem xét ở T2.8).
  - T2.8: đường chạy nhanh qua pgconn cho `CompleteWorkflowTask` và `CompleteActivityTask` (gồm resume). `BenchmarkEagerActivity` (phía store, 1 activity mỗi op, có fsync): alloc **311 → 51**, **20.4 → 9.4 KiB**. Độ trễ không đổi, khoảng 11.7 ms, vì bị giới hạn bởi 2 lần fsync. Với `synchronous_commit=off` còn khoảng 4.3 ms. Đường pgx vẫn được giữ làm fallback và chạy cùng bộ conformance.
  - Review T2.8: lần 1 không đạt (lost wake-up vì bỏ qua UPDATE lên dòng đang bị lease; đường pgx gửi NULL làm hỏng kiểm tra GW-3; statement prepared bị cũ; timestamp bị kẹp). Lần 2 đạt: wake luôn lấy row lock; Release và mọi chỗ tính lại `ready_at` đều chạy sau fence.
  - Hướng tối ưu tiếp theo (ghi nhận lại): giảm số commit trên đường eager (tuỳ chọn tương tự `AsyncStepStart` của saga); activity eager không cần dòng `activity_task`, phục hồi từ history thay vào đó.
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


## Quyết định (nhật ký)

- 2026-11-15: Port có chọn lọc từ go-workflows; tách hẳn upstream; chỉ Postgres + store bộ nhớ; hàng đợi riêng (không River); event history; API mới.
- 2026-11-15: Compose Postgres chạy trên cổng 5433, để chạy song song với compose của repo saga (5432).
- 2026-11-15: Perf gate trong CI là `benchgate`, so với ngân sách tuyệt đối (median của `-count 6`), thay cho so sánh tương đối bằng benchstat. Lý do: so tương đối giữa hai lần chạy CI quá nhiễu; allocs/op gần như tất định nên giới hạn chặt. Mỗi khi một thay đổi cải thiện số liệu thì hạ ngân sách xuống. `benchstat` (`make benchcmp`) dùng khi review task ⚡.
- 2026-11-15: Gói runtime tất định đặt tên `internal/coro` thay vì `internal/sync`, để không trùng tên với `sync` của thư viện chuẩn.
- 2026-11-15: `history.Event` là struct phẳng ánh xạ 1:1 với cột DB, không có codec envelope. Bỏ phần "codec append" của T1.3 vì store bind trực tiếp các field.
- 2026-11-15: Channel đánh thức mọi receiver khi có thay đổi (không dùng wakeOne), để Select không làm mất wake-up. `TrySend` chỉ thành công khi buffer còn chỗ (khác go-workflows `SendNonblocking`). Workflow dùng `Send` để rendezvous; signal dùng channel không giới hạn.
- 2026-11-15: Watchdog chung cho cả process, cộng trạng thái `ctl` atomic dùng CAS, thay cho timer theo từng lần chuyển. Sau `ErrStuck` chỉ được gọi `Close`; coroutine đang giữ quyền điều khiển sẽ dọn các coroutine còn lại.
- 2026-11-15: Contract của store: lease hết hạn nhưng chưa bị cấp lại thì vẫn hợp lệ (ngữ nghĩa saga). Completion kết thúc execution khi còn signal/cancel chưa giao bị từ chối bằng `ErrPendingEvents` (cách Temporal làm), để không bao giờ mất signal. Instance id được giữ chỗ cho tới khi retention xoá.
- 2026-11-15: sloglint chuyển sang `attr-only`, vì hiệu năng (LogAttrs).
