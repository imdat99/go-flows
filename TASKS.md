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
- [x] **T2.12** LISTEN/NOTIFY và poll có backoff (GW-8).

  - T2.12 (phía store): `store.Watcher`, kèm `pg_notify` đi qua `RETURNING` trên các đường tạo việc cho owner khác (đường eager không phát notify vì NOTIFY lấy lock toàn cục khi commit), LISTEN trên một kết nối hijack, và `Watch` cho store bộ nhớ. Poll có backoff thì làm ở T3.4 (worker). Review: lần 1 đạt; các ghi chú (hint khi release hoặc retry với delay 0, `wake` không được chặn, timeout khi đóng kết nối, test ổn định hơn, kiểm tra đường eager không phát notify) đã sửa.

### P3 — Executor và worker
- [x] **T3.1** ⚡ Executor replay và catch-up trên runtime P1; so khớp loại, tên, `arg_hash`, thời lượng timer; replay ngắn hơn lịch sử → non-determinism (GW-11). Bench `BenchmarkReplay`.
  - T3.1: `BenchmarkReplay` (cache lạnh, 302 event): **732 → 129 alloc** (chỉ còn khoảng 0.43 alloc mỗi event; mỗi command là 1 struct, hook qua interface, waiter đầu tiên lưu inline), khoảng 0.8–0.9 µs mỗi event. Review: lần 1 không đạt (`SideEffect` làm lệch thứ tự lệnh khi replay; workflow kết thúc sớm không bị phát hiện; `Execute` sau khi đã xong; executor không bị đánh dấu hỏng; `Watch` hai lần). Lần 2 đạt có điều kiện (replay chia batch giữa một task). Lần 3 đạt; hồi quy hiệu năng khi giữ phần đuôi đã sửa.
- [x] **T3.2** Non-determinism, workflow chưa đăng ký, version lạ → park `needs_attention` hoặc bàn giao, không fail (GW-11, GW-12).
- [x] **T3.3** ⚡ Cache sticky: mutex theo instance, khoá cache theo `(exec, seq, lease_version)`, evict khi có lỗi, giới hạn theo byte (GW-9).
- [x] **T3.4** Worker/dispatcher: slot, `recover` ở mọi biên, Stop bàn giao trong một transaction.
- [x] **T3.5** ⚡ Thực thi eager: activity chạy tại chỗ và workflow task chạy tiếp ngay. Bench round trip mỗi activity.
- [x] **T3.6** Huỷ activity qua heartbeat và context; terminate (GW-13).

  - T3.2–T3.6: worker (`internal/worker`). Gồm poller có backoff và hint, driver theo từng execution (vòng eager, cửa sổ eager, chuyển phần còn lại cho recorder khi nhả lease), heartbeat gộp có kiểm tra version lease, cache theo kiểu checkout, retry phía engine, huỷ activity, Terminate (store và migration `00002`), Stop êm.
    - Mục "giới hạn theo byte" của T3.3 chưa làm: cache mới giới hạn theo số lượng.
    - `BenchmarkActivityRun` (Postgres, eager, 10 activity): khoảng 1090 alloc mỗi lần chạy (đo khi async commit; số mới ở T8.2); khoảng 5.5 ms mỗi activity khi `synchronous_commit=off`. **Chưa đạt mục tiêu ≤ 80 alloc**, chuyển sang T8.2.
    - Review: lần 1 không đạt (hạn chờ của Stop làm hỏng workflow; báo cáo lease cũ huỷ nhầm lease mới; nhả lease mà không giải phóng slot; resume lỗi làm rò lease; Compensate bị bỏ; renewedAt sai; huỷ khi đang chờ retry; Terminate dễ gãy; panic). Lần 2 không đạt (attempt thành công sau hạn chờ bị bỏ). Lần 3 đạt.

### P4 — API công khai
- [x] **T4.1** ⚡ `DefineWorkflow`/`DefineActivity` generic, đặt tên tường minh, trùng tên thì panic, kiểm tra tên (GW-12); `jsonSlot`.
- [x] **T4.2** `Engine` (`New`/`Start`/`Stop`/`Options`), `Handle`, `Start`/`Run`/`Wait`, idempotency (port `saga/engine.go:306`).
- [x] **T4.3** `Ctx` API: `Go`, `Select`, `Sleep`/`NewTimer`, `Await`/`SignalChannel` FIFO (GW-14), `Now`, `Rand`, `SideEffect` (GW-18).
- [x] **T4.4** Kiểm tra input và làm sạch dữ liệu: `checkName`, `checkStorableJSON` gồm cả surrogate đơn lẻ, `store.Text`, lỗi được tái dựng qua `error_kinds` với `errors.Is` (port saga).
- [x] **T4.5** Retry policy phía engine, `Permanent` dùng `errors.As` (GW-15).
- [x] **T4.6** `examples/` (đơn hàng, fan-out, signal) chạy end-to-end.

  - T4.1–T4.6: API công khai ở package gốc (`activity.go`, `codec.go`, `ctx.go`, `engine.go`, `errors.go`, `registry.go`, `workflow.go`), cùng `examples/quickstart` (signal) và `examples/fanout` (fan-out, Select, huỷ), chạy end-to-end trên Postgres.
    - Payload lưu trong cột `bytea`, nên không cần `checkStorableJSON` (kể cả surrogate đơn lẻ); chỉ văn bản (lỗi, tên) đi qua `text.Safe`/`CheckName`.
    - Ví dụ đơn hàng có compensation chuyển sang T5.6, vì cần lớp saga.
    - Bench (`-count 6`): `BenchmarkCodec` encode 1 alloc/32 B khoảng 550 ns, decode 1 alloc/24 B khoảng 900 ns, lookup codec 0 alloc khoảng 36 ns. `BenchmarkPublicActivityRun` (Postgres, eager, 10 activity): 1461–2043 alloc/run, trung vị khoảng 1540; `BenchmarkActivityRun` (worker) đo cùng phiên: 1374–1507, trung vị khoảng 1440. Lớp typed gần như không tốn thêm. Máy ồn hơn lần đo T3 (khoảng 1090 alloc), nên số tuyệt đối thay đổi theo độ trễ của poll. Việc giảm alloc chuyển sang T8.2.
    - Review: lần 1 không đạt (`SideEffect` lệch khi replay, codec dựng mỗi lần gọi, comp data khi không có compensator, kiểm tra error kind, gán nhầm timeout, `UseNumber`, zero value, tài liệu, ví dụ fanout, test). Lần 2 đạt. Các ghi chú thấp đã sửa: zero `Ctx` panic `ErrNotInWorkflow`, `Select` không có case thì panic, `Rand` báo `ErrNonDeterministic`, decode từ chối dữ liệu thừa. Ghi chú trung bình (encode comp data lỗi sau khi activity thành công phải đánh dấu `uncertain`, và giữ lỗi gốc khi invoke lỗi) chuyển sang T5.1.

### P5 — Lớp saga
- [x] **T5.1** `comp_data` và `uncertain` trong `ActivityCompleted`/`Failed`; `CompensationUncertain`.
- [x] **T5.2** Pha compensating dựng từ history, thứ tự ngược, `activity_task kind=compensate`, `CompensatePolicy`.
- [x] **T5.3** `needs_attention`, `RetryCompensation`, bàn giao khi gặp compensator lạ.

  - T5.1–T5.3: lớp saga trên event history.
    - **Planner** `internal/compensation`: dựng chỉ từ history, không chạy code workflow.
      - Hoàn tác các activity đã xong hoặc `uncertain` và có compensator. "Có compensator" do binary chạy activity ghi lại: `comp_data` khác rỗng, tối thiểu là `"null"`. Không tra registry của binary đang lập kế hoạch, nên deploy trộn phiên bản vẫn an toàn.
      - Chạy từng cái một, theo thứ tự ngược với thứ tự kết quả, sau khi mọi activity đang chạy đã có kết quả.
      - Khi workflow kết thúc: huỷ activity, timer và child còn mở. Activity đang chờ retry thì claim được ngay, không phải chờ hết backoff.
    - **Worker:** chế độ compensating trong driver (`compensate.go`).
      - Lần đầu thì đọc history; `Executor.MayCompensate`, dùng bộ đếm không alloc, giúp bỏ qua khi không có gì để hoàn tác.
      - Ledger được giữ lại giữa các task của cùng driver.
      - Task compensation chạy eager hoặc qua poll. Binary không có compensator thì bàn giao task.
      - Kết quả là `CompensationCompleted`/`CompensationFailed`.
    - **Kết thúc:** `compensated`, hoặc `canceled` nếu workflow bị huỷ, hoặc `failed` nếu không có compensation nào chạy. Lỗi của workflow được lưu ngay khi chuyển sang `compensating`.
    - **Park:** compensation hết lượt retry thì chuyển `needs_attention` (kind `compensation_failed`). `Engine.RetryCompensation` gọi `store.ReopenCompensation` để tiếp tục.
    - **Lưu trữ:** migration `00003` (`activity_task.uncertain`).
    - **API:** `CompensationUncertain`; `ErrCompensated` và `ErrCompensationFailed` là error kind. Activity thành công nhưng comp data không encode được thì thành lỗi `uncertain` (`worker.ErrUncertain`). Activity bị huỷ trước lần chạy đầu tiên thì là lỗi chắc chắn (`certain`).
    - **Test:**
      - conformance gồm `CompensationTask`, `ActivityResultTypes`, `ReopenCompensation`, và `CancelBeforeRetry` với độ trễ 1 giờ;
      - unit test cho planner, `TestMayCompensate`, và deploy trộn phiên bản ở mức worker;
      - test engine gồm thứ tự ngược, `uncertain`, activity đang chạy, cancel, park/retry, bàn giao giữa hai engine, signal khi đang compensating, compensator panic, công việc còn mở;
      - e2e Postgres ở cả eager lẫn poll.
    - **Bench:** `BenchmarkReplay` vẫn 129 alloc (khoảng 0.82–0.89 µs/event). `BenchmarkActivityRun` khoảng 1406–1575 alloc/run, nằm trong dải nhiễu đo ở P4.
    - **Giới hạn:**
      - Workflow lỗi phải chờ mọi activity đang chạy, kể cả activity không có compensator. Activity bỏ qua context sẽ giữ execution ở `compensating` cho tới khi `Terminate`.
      - Mỗi bước compensation chạy ở driver mới phải đọc lại history (chỉ trên đường lỗi).
      - Hoàn tác child đã hoàn thành: để T6.3.
    - **Review:** lần 1 không đạt (H1 compensation bị bỏ qua khi deploy trộn; M1 activity trong backoff chặn compensation; M2 đọc lại history; L1–L5). Lần 2 đạt; ba ghi chú thấp đã sửa: tài liệu `Terminate`, lỗi của workflow sau `RetryCompensation`, comment của test.
- [x] **T5.4** `Lock`/`LockWait` gắn với lease (port `saga/lock.go`).
  - T5.4: lock là một command của workflow (`LockRequested`; TTL nằm trong payload nên replay so khớp được).
    - **Lấy lock:** store lấy lock trong transaction checkpoint (`acquireLock`, migration `00004`, bảng `flows.lock`). Kết quả là pending `LockAcquired`/`LockHeld`; driver resume ngay sau khi nhận. TTL tối thiểu bằng `LeaseTTL`. Các yêu cầu lock được sắp theo key.
    - **Giữ lock:** lock được gia hạn khi claim workflow hoặc activity của execution, và khi gia hạn lease của workflow hoặc activity (kể cả compensator). Lock hết hạn chỉ khi execution không chạy, cũng không có activity nào đang chạy. Holder đang chạy (lease còn sống, activity đang chạy, hoặc row đang bị khoá) thì không bị cướp lock.
    - **Mất lock:** khi bị cướp, store gửi `LockLost` cho holder cũ trong cùng statement. Workflow fail với `ErrLockLost` rồi compensate. `LockLost` chưa giao sẽ chặn completion kết thúc, giống signal.
    - **Nhả lock:** khi execution kết thúc, kể cả continue-as-new và terminate.
    - **API:** `flows.Lock`, `LockWait` (delay tối thiểu 10ms, được ghi vào history), `ErrLockHeld`, `ErrLockLost`, kind `invalid_argument`.
    - **Test:**
      - conformance gồm `Locks`, `LockRenewal`, `LockValidation`, `LockHeldDuringActivity`, `LockLostBlocksEnd`, `LockRace`, `LockNotTakenFromRunningHolder`;
      - `TestLockReplay`; EXPLAIN dùng `lock_execution`;
      - test engine gồm loại trừ, chờ, mất lock, tham số, activity chạy qua poll dưới lock;
      - pg e2e: 2 engine với 6 workflow, không lúc nào có quá 1 cái trong vùng găng.
    - **Bench:** alloc không đổi (`BenchmarkActivityRun` khoảng 1384–1587 alloc/run). Có lúc một run mất khoảng 1.2–1.4s, đã có từ trước (đo trên commit cũ cũng thấy). Có thể do hand-over phải chờ poll 1s. Điều tra ở T8.2.
    - **Còn mở (thấp):**
      - Lấy lock tốn 2 round trip (completion rồi resume).
      - Claim, `dropEnded` và Terminate khoá lock row theo thứ tự quét, không theo key. Khi execution giữ từ 2 lock trở lên, có thể deadlock; được retry sau `deadlock_timeout`.
      - Activity đang nằm hàng đợi hoặc chờ retry thì không tính là đang chạy: lock có thể hết hạn trước khi nó chạy (đã ghi trong tài liệu).
      - `TestLockLost` dùng sleep thật.
      - Thiếu test: holder đang ở `needs_attention`, replay của `LockWait`.
    - **Review:**
      - Lần 1 không đạt: H1, lock hết hạn khi activity không chạy eager; M1, claim và cướp lock deadlock lẫn nhau; M2, compensation; M3, `LockLost` bị bỏ khi kết thúc.
      - Lần 2 không đạt: H1-r2, claim `SKIP LOCKED` làm lock bị cướp khi holder đang chạy.
      - Lần 3 đạt.
- [x] **T5.5** `RunHook` chạy trong transaction kết thúc, `Observer`, port `idem/`.
  - T5.5: `RunHook` chạy trong transaction ghi lại việc kết thúc (completed, failed, compensated, canceled, terminated) hoặc park (`needs_attention`); continue-as-new không tính.
    - Trên pg: chỉ những completion kết thúc mới dùng transaction tường minh (`inTx`), còn lại vẫn đi fast path. Hook chạy sau fence và `noUndelivered`. Hook lỗi hoặc panic thì rollback, task được bàn giao và chạy lại sau `HandBack`. Lỗi của hook không bị hiểu nhầm thành tín hiệu của store (kiểu `hookError`).
    - Trên mem: hook nhận `tx` nil.
    - `Observer`: nhận từng attempt của activity và compensator, và các lần kết thúc đã commit (kể cả `Terminate`). Panic được recover và ghi log; khi không cấu hình thì chỉ tốn một phép so nil.
    - `idem/`: port từ saga, dùng `flows.Key`.
    - Test:
      - conformance `EndHook`, `EndHookCalls`;
      - pg `TestEndHookIsAtomic`, `TestEndHookErrorsAreNotStoreSignals`;
      - engine `TestRunHookAndObserver`, `TestPanickingObserver`, `TestRunHookFailureIsRetried`;
      - pg e2e outbox;
      - bộ test của idem.
    - Còn mở (thấp):
      - `Terminate` đọc `GetExecution` riêng để báo cho observer;
      - khi có hook, `inTx` tốn thêm một round trip `SELECT`;
      - thiếu test: hook đi cùng `ErrPendingEvents`, park do non-determinism đi tới hook ở mức engine, khẳng định 0 alloc khi không cấu hình gì.
    - Review: lần 1 không đạt (panic trong observer lọt ra ngoài; mã lỗi của hook bị hiểu nhầm). Lần 2 đạt.
- [x] **T5.6** Port test saga: `CompensationReverseOrder`, `review_fixes_test.go`, `lock_test.go`, `async_test.go`.
  - T5.6: `saga_port_test.go` port những hành vi chưa có test tương đương:
    - start đồng thời cùng một instance id;
    - retry phía engine, lỗi permanent;
    - panic trong activity và trong workflow;
    - `Await` timeout, signal tới trước `Await`, payload signal sai;
    - text lỗi không lưu được;
    - bàn giao sau Stop (mem và pg);
    - replay khác history thì park;
    - lock bị nhả khi kết thúc, được giữ khi park;
    - timeout rồi lỗi chắc chắn vẫn được compensate;
    - replay lạnh không báo sai.
  - Bảng đối chiếu saga → go-flows nằm trong review. Những phần không áp dụng cho mô hình này: run timeout, interrupt của checkpoint, `AsyncStepStart`, job đánh thức.
  - `examples/order`: saga đặt hàng có lock, compensation theo thứ tự ngược, và hoàn tiền uncertain theo key.
  - Lỗi engine tìm ra khi port test, đã sửa:
    - park do non-determinism không ghi error kind;
    - Stop để execution còn sticky tới 10s. Thêm `store.Unstick(owner)` và migration `00005` (index); không set sticky khi đang compensating;
    - **tính uncertain bị mất qua các lần retry.** Activity timeout rồi lỗi chắc chắn thì không được compensate; holder chết giữa chừng cũng vậy. Sửa: `activity_task.uncertain` cũng dùng cho invocation. `RetryActivityTask(..., uncertain)` và claim sau khi lease hết hạn đều set cờ này, và lỗi cuối cùng OR thêm nó. Huỷ trước một attempt thì chỉ uncertain nếu cờ đã được set.
  - Còn mở (thấp):
    - comp data của attempt uncertain trước đó không được giữ: compensator nhận data của attempt cuối, thường là zero; ví dụ hoàn tiền theo `Key`;
    - chưa có test cho việc bỏ sticky khi đang compensating.
  - Review: lần 1 không đạt (H1 tính uncertain qua các lần retry; M2 `Unstick` chỉ xử lý theo cache; M3–M4 độ tin cậy của test bàn giao; L5–L9). Lần 2 đạt; đã sửa M (Unstick chỉ đụng các execution còn sticky) và L2/L3/L5.

### P6 — Sub-workflow và ContinueAsNew
- [x] **T6.1** Child workflow: tạo, kết quả, huỷ theo cây, nhắm theo `instance_id`.
- [x] **T6.2** ContinueAsNew: chuyển signal và pending một cách nguyên tử, cache evict đúng (GW-3).

  - T6.1–T6.2:
    - **Huỷ theo cây:** execution kết thúc (trừ continue-as-new) hoặc bị terminate thì gửi yêu cầu huỷ cho các child còn chạy (`cancelChildrenOf`; migration `00006` thêm index `parent_id`). Child được địa chỉ hoá theo instance id.
    - **GW-3, mất tin khi continue-as-new:** signal đã giao vào channel nhưng workflow chưa đọc, và cancel đã áp dụng nhưng workflow chưa xử lý, đều bị mất khi continue-as-new. Sửa: thêm `Completion.Carry` (executor tính ở cuối task từ `sigLog`, log này được compact). Store giao chúng cho execution mới trước các pending chưa giao.
    - Gộp phần kiểm tra completion vào `store.ValidShape`; bỏ được một map/completion trên pg.
    - **Còn mở (thấp):**
      - Child được tạo trong đúng task mà cha kết thúc thì bị bỏ (giống activity).
      - Cha kết thúc đúng lúc child kết thúc thì có thể deadlock; được retry sau `deadlock_timeout` (đã ghi chú).
      - Trên mem, `cancelChildrenOf` quét toàn bộ execution.
    - **Review:** lần 1 không đạt (H1 cancel bị mất khi continue-as-new; M1 index không được dùng; M2 `sigLog` không giới hạn; M3 deadlock). Lần 2 đạt.
- [x] **T6.3** Compensation lồng nhau: cha hoàn tác thì child hoàn tác.
  - T6.3: khi cha compensate, các child đã completed cũng được compensate.
    - Child completed là một bước trong kế hoạch, xếp xen kẽ ngược thứ tự với activity của cha. Cha gửi `Completion.CompensateChildren`; store mở lại child với trạng thái `compensating`, đặt cờ `compensate_requested` (migration `00007`) và gửi `ExecutionCompensationRequested`.
    - Child compensate từ history của chính nó, đệ quy xuống cháu. Khi child kết thúc, cha nhận `CompensationCompleted` (kể cả khi child bị terminate). Child không completed thì cha được trả lời ngay.
    - Child đang chạy khi cha lỗi thì bị huỷ và được chờ. Child bị park sẽ giữ cha ở `compensating` cho tới khi `RetryCompensation` child.
    - Execution completed được mở lại thì không chờ những gì nó để lại (`settleAbandoned`).
    - Purge không xoá child của cha còn active.
    - Giới hạn (đã ghi trong tài liệu):
      - chỉ execution cuối của chuỗi continue-as-new được compensate;
      - activity không được await thì không được compensate;
      - child compensate mà không giữ lock;
      - child bỏ qua lệnh huỷ rồi completed sau đó thì không được compensate.
    - **Ghi chú nâng cấp:** drain các execution đang `compensating` trước khi deploy thay đổi này, vì bản cũ không ghi kết quả của child vào history.
    - Review: lần 1 không đạt (H1 child mở lại treo vì việc nó bỏ lại; M1 purge xoá mất child). Lần 2 đạt.

### P7 — Công cụ
- [x] **T7.1** `flowtest`: store bộ nhớ, đồng hồ ảo tất định, mock activity, phát hiện future còn treo (GW-17). Test các kịch bản #494, #481, #436, #377.
  - T7.1: package `flowtest` chạy engine thật trên store bộ nhớ với đồng hồ ảo.
    - **Thời gian ảo:** chỉ nhảy khi engine rảnh (`worker.Busy` và `mem.Store.Wake(owner, queues)`), tới mốc kế tiếp. Không có gì chạy theo đồng hồ thật, nên tránh được #494 và #481. Workflow không thể kết thúc thì trả về `ErrBlocked` ngay.
    - **Mock:** theo từng tester (`OnActivity`, `OnCompensator`), dùng chung code dựng runner với `DefineActivity`; đi qua `internal/bridge`.
    - **Future còn treo:** báo các activity và child chưa được await (#436).
    - **Tiện ích:** `SignalAfter`, `Advance`, `Settle`, `EventTypes`.
    - **API mới:** `flows.WaitGroup` (dùng lại được, `Wait` tôn trọng cancel).
    - **Test:** `TestGW17_494/481/436/377_*`, hàng đợi không ai phục vụ, thời gian đứng yên khi activity chạy, mock compensator.
    - **Bench:** `BenchmarkFanOut` 16/64/256: khoảng 5/6–10/10–18 ms, gần tuyến tính (#377).
    - **Còn mở (thấp):**
      - `Wake` quét toàn bộ execution;
      - activity chờ cancel do timer của workflow gửi thì không thể kết thúc được (đã ghi trong tài liệu, kèm `ErrNeverIdle`);
      - **T8:** test conformance pg `LockRenewal` dùng sleep thật, từng flake một lần khi máy tải nặng; cần làm chắc lại.
    - **Review:** lần 1 không đạt (H1 alloc/task trên đường nóng; H2 test #494 và #481 không tái hiện lỗi; M2 hàng đợi không ai phục vụ treo 30s; M3/M4 mock; M6 WaitGroup). Lần 2 không đạt (`Wait` bỏ qua cancel). Lần 3 đạt.
- [x] **T7.2** Port `analyzer` theo API mới.
  - T7.2: `analyzer/` (module riêng, `flowsvet`; dùng làm plugin golangci-lint hoặc `go vet -vettool`).
    - Code workflow được nhận diện theo kiểu: hàm hoặc closure có tham số đầu là `flows.Ctx`.
    - Báo:
      - lệnh `go`, `select`, channel;
      - range trên map (kể cả qua `maps.All`/`Keys`/`Values`), trên type param map;
      - `time.*` (kể cả khi dùng làm giá trị hàm);
      - `math/rand`, `crypto/rand` và method của `rand.Rand`;
      - `sync.Map.Range`, `os.Getenv`;
      - `sync.WaitGroup`/`Mutex`/`RWMutex`/`Once`/`Cond`.
    - Bỏ qua hàm truyền cho `SideEffect` (nhưng vẫn kiểm tra đối số `Ctx`), các activity, và chính package `flows`.
    - Có `LICENSE`/`NOTICE` riêng. `make test`, `lint`, `fmt`, `lint-fix` đã bao phủ module này. Chạy trên repo: sạch.
    - Còn mở (thấp): channel không gán vào biến (`foo(make(chan int))`), field kiểu channel hoặc mutex; hàm `SideEffect` để trong biến bị báo nhầm (tài liệu ghi: viết literal trực tiếp).
    - Review: lần 1 đạt; các ghi chú M1–M6 (method của rand, iterator của maps, giá trị hàm, `Args[0]` của `SideEffect`, license, test) và L1/L2/L5/L6 đã sửa.
- [x] **T7.3** ⚡ OpenTelemetry tracing và metrics (tắt thì không tốn alloc).
  - T7.3: hook `flows.Tracer` không phụ thuộc thư viện ngoài, đi qua `worker.Tracer`.
    - **Span:** một span cho mỗi attempt (context của tracer đi vào activity; cause của cancel vẫn đọc từ ctx của worker) và một span cho mỗi workflow task (kết thúc bằng status đã commit và lỗi; park = `needs_attention`). Panic của tracer được recover.
    - **Module `otelflows/` (riêng):**
      - `NewTracer` và `NewObserver`;
      - metric `flows.activity.attempts`, `flows.activity.duration` (bucket tính bằng giây), `flows.runs.ended`;
      - không dùng instance id làm thuộc tính metric.
    - **Tắt thì không tốn gì:** `-gcflags=-m` không thấy escape mới; `BenchmarkActivityRun` 1325–1575 alloc/run (trong dải nhiễu); `BenchmarkReplay` 129.
    - **Test:** `TestTracer`, `TestTracerSpansBalance` (eager và poll, retry, panic, ctx/end nil, park); test SDK của otelflows (span, quan hệ cha con, metric).
    - **Kèm theo:**
      - `make race10` có `-timeout 30m`;
      - hai test conformance lock dựa vào thời gian thật (`LockRenewal`, `LockHeldDuringActivity`) đã làm chắc lại: dùng 2×TTL và chừa 45% dư. pg 10 lần liên tiếp: sạch.
    - **Còn mở (thấp):** span không được nối với nhau qua các task (chỉ gắn với nhau bằng `flows.instance_id`); chưa có test cho attempt mất lease.
    - **Review:** lần 1 không đạt (M1 status của span chưa commit; M2 cause của cancel phụ thuộc ctx của tracer; M3 bucket; M4 test). Lần 2 đạt.

### P8 — Chất lượng và phát hành
- [x] **T8.1** Port `test/chaos`: SIGKILL, SIGSTOP, 3 instance, `assertOnce`.
  - T8.1: `test/chaos` (build tag `chaos`, `make chaos`). Test tự chạy lại chính mình thành tiến trình con, rồi bị SIGKILL hoặc SIGSTOP; engine ở tiến trình cha khôi phục. Có `Pdeathsig` để con không sống sót sau cha.
    - **A:** con bị kill giữa activity (khi đang giữ lease).
    - **B:** con bị đóng băng lâu hơn lease. Engine khác giữ task qua một cổng chặn; khi con được rã đông, nó ghi kết quả muộn và bị fence của store từ chối. Đã kiểm tra bằng mutation: bỏ predicate owner/version trong `finishTask` thì B fail.
    - **C:** 3 engine tranh nhau khôi phục 30 execution.
    - **D:** con bị kill giữa compensation: chạy lại cùng key và comp data, undo có hiệu lực đúng 1 lần, kết thúc `compensated`.
    - **E:** con bị kill khi đang giữ business lock: holder được khôi phục chạy xong vùng găng rồi bên chờ mới vào.
    - **Còn mở:**
      - fencing ở mức workflow lease trong chaos (làm được bằng kỹ thuật ngủ ngắn hơn thời gian đóng băng như B);
      - các kịch bản cha/con, continue-as-new, timer;
      - chứng minh nhiều racer thực sự tham gia ở C;
      - một lần A chạy mất 11s (vẫn đạt).
    - **Review:** lần 1 không đạt (B không kiểm tra fencing). Lần 2 không đạt (lập luận sai về heartbeat). Lần 3 đạt.
- [x] **T8.2** (bổ sung từ P3) Giảm alloc của vòng eager trên Postgres xuống gần mục tiêu ≤ 80 mỗi lần chạy 10 activity:
    - gộp các câu lệnh trong batch (fence + history + xoá pending; resume);
    - tự ghi Bind/Execute thay cho `pgconn.Batch`, vì pgconn không cho tái dùng batch;
    - theo dõi context kiểu `Interrupt` của saga;
    - tuỳ chọn một commit mỗi activity (tương tự `AsyncStepStart`).
    - **Kết quả:** `BenchmarkActivityRun` từ khoảng 1240 xuống khoảng 75 alloc mỗi workflow (−94%), cùng SQL và cùng số round trip. Đo bằng memprofile ở mỗi bước.
      - `BenchmarkPublicActivityRun`: khoảng 1330 → 140.
      - `BenchmarkEagerActivity`: 52 → 6 alloc (9.9 KiB → khoảng 780 B).
      - `BenchmarkReplay`: 129 → 30.
      - Thời gian: khoảng 150 ms mỗi workflow khi durable (trước 172 ms), khoảng 60 ms khi async (trước 80 ms).
    - **Đã làm:**
      - Fast path tự ghi Bind/Execute/Sync vào buffer có pool và đọc từng message (`ReceiveMessage`), thay cho `pgconn.Batch`.
      - Context được theo dõi một lần cho mỗi context ở store (`ctxWatch`: cancel request rồi đặt deadline cho kết nối), thay cho watch của pgconn ở mỗi lời gọi. Test `TestFastPathHonorsContext` đỏ khi chưa gửi cancel request.
      - Claim chạy trên fast path, tự giải mã mảng nhị phân (`decodeArray`, có test).
      - Task không mang input/output.
      - Execution mới không load history.
      - Benchmark có warm up (trước đó tính cả SCRAM handshake của pool).
      - Activity eager chạy trên goroutine runner được tái dùng.
      - Heartbeat lưu entry theo giá trị; `tracking` là giá trị, không còn closure.
      - Executor tái dùng completion; task được resume ghi vào 2 buffer của driver (`ActivityResult.Into`).
      - Call được cấp theo khối 8; workflow, queue, status được intern.
      - `CreateExecution` chạy trên fast path; lease trả về trong `Completion.Leases`.
      - Watcher đánh thức mọi queue một lần khi bắt đầu nghe (sửa race lúc khởi động: việc làm sẵn trước LISTEN không có hint).
    - **Đạt ≤ 80** (đợt 3, sau khi đợt 2 dừng ở khoảng 105): khoảng 75 alloc mỗi workflow (các mẫu 73–77; benchmark end-to-end chạy cố định 50 vòng mỗi mẫu).
      - Thêm `BenchmarkActivityRun20`: khoảng 110, tức khoảng 40 cố định cho mỗi workflow và khoảng 3.5 cho mỗi activity.
      - Đợt 3:
        - kết nối LISTEN chỉ theo dõi context một lần, không theo từng notification;
        - driver dùng lại timer và kênh kết quả eager qua pool;
        - option của executor dựng một lần;
        - state và scheduler chứa sẵn buffer đầu tiên và root context;
        - `coro.Canceler` thay cho closure;
        - workflow task chạy trên runner được tái dùng;
        - claim activity chạy trên fast path.
      - Không bỏ context huỷ được của mỗi attempt (2 alloc mỗi activity): `context.Cause(ctx) == ErrActivityCanceled` là API đã công bố, nên phải dùng `context.WithCancelCause` thật.
    - **Lần chạy chậm thỉnh thoảng gặp** (0.5–1.7 s): trace các lời gọi store cho thấy mọi câu lệnh chậm 50–80 ms trong khi luồng engine bình thường. Đó là I/O (fsync) của máy dev, không phải lỗi engine.
    - **Perf gate:**
      - ActivityRun ≤ 85, PublicActivityRun ≤ 155 (50 vòng mỗi mẫu, dao động ±3%).
      - EagerActivity ≤ 8 alloc và ≤ 1 KiB; Replay ≤ 34.
    - **Review lần 1 không đạt:**
      - Data race: driver đọc completion được tái dùng sau khi đã đưa executor vào cache (tái hiện bằng test bão signal; đã sửa).
      - Cancel request gửi tuần tự, mỗi cái chặn tới 5 s: nay đặt deadline trước, gửi các cancel request song song, chờ tối đa 1 s.
      - Context của ứng dụng (`CreateExecution`) giờ được theo dõi riêng cho từng lời gọi, không để lại gì.
      - Khôi phục `TestTimestampRoundTrip`; lỗi decode không còn làm đóng kết nối; sửa comment về số runner; nới budget.
- [x] **T8.2** Benchmark đầy đủ so với saga và go-workflows, viết `docs/benchmarks.md`, bật perf gate trong CI.
    - **So sánh** (10 activity no-op, Postgres cùng máy, durable commit):
      - saga: 96 ms, 63 alloc;
      - go-flows: 172 ms, khoảng 1200 alloc (80 ms khi async commit);
      - go-workflows 48e8119: thời gian chạy 2.5 s với poll mặc định, 0.77 s với poll 1 ms, đo bằng `run-ms` lấy từ history, không tính phần poll của client; 26k–150k alloc.
      - go-flows nhanh hơn go-workflows 4.5–15 lần, chậm hơn saga khoảng 1.8 lần (khoảng 9 câu lệnh mỗi activity so với 3).
    - **Vòng poll:** hint không còn reset backoff, nên `BenchmarkActivityRun` giảm từ 1397 xuống 1213 alloc (−13.6%).
      - Việc có delay mà worker tự commit (timer, retry delay) không có hint. Worker ghi thời điểm đến hạn (`internal/worker/due.go`): làm tròn lên 10 ms, bỏ trùng, chỉ giữ trong 64·PollMax.
      - Thời điểm đến hạn mới chỉ rút ngắn giấc ngủ, không claim sớm.
      - Poll theo giờ mà tìm thấy việc thì reset backoff.
      - Claim lỗi thì backoff, không quay vòng.
      - Test: `TestDelayedWorkIsNotPollMaxLate` (mem + pg), `TestTimedPollsFindingWorkDoNotBackOff`, `TestDueTimesDoNotCauseEmptyClaims`, `TestDueTimes*`. Mỗi test đỏ khi bỏ phần sửa tương ứng.
    - **Perf gate** (`perf-budget.txt`, `make benchgate`): `BenchmarkActivityRun` ≤ 1300 (trung vị 1192–1226), `BenchmarkPublicActivityRun` ≤ 1450 (1284–1357), codec 1/1/0, Replay ≤ 140, EagerActivity ≤ 56.
    - **CI** (`.github/workflows/ci.yml`): test + chaos, lint (4 module), bench + benchgate; Postgres chạy bằng `make up`; `bench.txt` luôn được upload.
    - **Module `bench/goworkflows`:** pin 48e8119; database tạo cho mỗi lần chạy được xoá sau khi chạy.
    - **Review:**
      - Lần 1 không đạt: timer trễ tới PollMax khi engine rảnh; so sánh dùng `GetWorkflowResult`; budget lỏng; CI.
      - Lần 2 không đạt: nhiều timer làm poll liên tục và bộ nhớ tăng không giới hạn; poll theo giờ tìm thấy việc vẫn backoff.
      - Lần 3 đạt. Sau đó đã sửa thêm các mục còn lại: kênh `changed` riêng để không claim rỗng, sàn của grain, lời văn.
    - **Còn mở:**
      - delay do worker khác commit, hand-back, và việc ngoài horizon vẫn có thể trễ tối đa PollMax;
      - gate trên runner CI dùng chung có thể dao động (headroom khoảng 7%).
- [x] **T8.3** Tài liệu song ngữ: concepts, guarantees, operations, patterns.
    - **File:** `README.md` (EN) và `docs/vi/README.md`; `docs/{concepts,guarantees,operations,patterns}.md` và bản tiếng Việt `docs/vi/{khai-niem,bao-dam,van-hanh,mau-thiet-ke}.md`.
    - **Kiểm tra:**
      - các snippet đã compile với API thật trong một package tạm;
      - link tương đối đều trỏ đúng;
      - test `make test` đạt.
    - **Thêm test** `GW4_PurgeKeepsActiveExecutions` (conformance, mem + pg): execution đang `compensating` hoặc `needs_attention` không bị `PurgeEnded` xoá. Nghi có lỗi nhưng kiểm tra thì không có, vì chỉ kết thúc thật mới đặt `ended_at`; test giữ lại để canh.
    - **Review:**
      - Lần 1 không đạt. H1: tài liệu nói workflow bị huỷ có thể kết thúc `compensated`, nhưng thực tế luôn là `canceled` khi trả `ErrCanceled`. Cùng lần đó có 4 mục medium: park do non-determinism, `Terminate` không compensate, hook theo từng engine, thiếu trạng thái `continued`; và 8 mục low.
      - Lần 2 đạt; 3 ghi chú low đã sửa.
    - **Chuyển sang T8.4:**
      - park do non-determinism không có cách mở lại (chỉ có `Terminate`), mà doc của `Handle.Wait` lại nói "can still resume once fixed";
      - workflow quay vòng không nhường thì bị trả lại mãi, mỗi lần để lại một goroutine quay (CPU tăng).
- [x] **T8.4** Review tổng thể toàn repo bằng subagent (đọc lạnh), sửa các phát hiện.
    - **Review lạnh:** 3 subagent đọc lạnh song song (store, runtime/worker, API công khai/công cụ). Tổng cộng 4 High, 10 Medium, khoảng 20 Low. Mỗi High/Medium có test đỏ trước khi sửa.
    - **Store:**
      - Claim workflow và activity không còn sort cả backlog: LATERAL theo từng queue, queue được khử trùng; migration `00008` đổi `execution_ready_idx` thành `(queue, ready_at, id)`; UPDATE theo `id = ANY(ARRAY(...))`.
      - Pending event đọc ngay trong câu claim (LATERAL + `array_agg` qua `pending_event_execution_idx`), vẫn một câu lệnh, atomic.
      - `PurgeEnded` giữ mọi hậu duệ của execution còn hoạt động, bằng cách đi ngược lên từ từng ứng viên (đọc cỡ số dòng xoá).
      - Test plan ở quy mô 20k dòng: `TestGW8_ClaimsReadOnlyWhatTheyClaim`.
      - Hợp đồng: lease giữ (kept lease) hết hạn mà không có event thì claim có thể trả task rỗng (pg) hoặc không trả gì (mem).
      - Fast path và đường pgx xếp câu lệnh cùng thứ tự; lock row được lấy theo thứ tự key.
      - `ReopenParked` mới.
      - Mem từ chối text NUL hoặc UTF-8 sai, giống pg.
    - **Runtime:**
      - Replay phát hiện cancel không có trong history (GW-11, `State.canceling`).
      - Loại event lạ thì trả lại (`ErrUnknownEvent`), không park.
      - Output activity không encode được tính là uncertain, nên vẫn được compensate.
      - Kênh kết quả eager chỉ tạo khi có activity eager (bớt khoảng 24 KiB mỗi task).
      - Due time không còn bị bỏ sót khi commit chậm hơn delay.
      - Slot eager không rò; cache giữ executor có `LastSeq` cao hơn; log lỗi ghi outcome lên mức error.
    - **Mục đã biết:**
      - Workflow không nhường: lần đầu trả lại; lần thứ hai ở cùng `LastSeq` thì park với `ErrStuck` (thêm `Options.StuckTimeout`).
      - `Engine.Resume` mở lại execution bị park do code sai (non-determinism hoặc stuck).
    - **API:**
      - Lỗi của workflow bị huỷ được làm sạch như lỗi fail (trước đó NUL làm execution không bao giờ kết thúc).
      - `idem`: thêm `ErrUnstorable` cho NUL; kết quả UTF-8 sai trả về giống nhau ở lần đầu và lần trùng.
      - `flowsvet` báo thêm: uuid, `os.Hostname`/`Environ`/`Getpid`/`Getwd`, `context.WithTimeout`/`WithDeadline`, method `sync` qua field, `errgroup.Go`; bỏ qua các constructor của rand.
      - `RegisterErrorKind`: từ chối sentinel không so sánh được; không còn RLock lồng nhau.
      - Kiểm tra version trước khi chuyển sang int32; kiểm tra tên queue.
      - `Handle.Wait` trên instance của workflow khác trả `ErrInstanceConflict`.
      - Sửa doc của `Cancel`/`Wait`/`PurgeEnded`; README bỏ uuid; doc của `idem.ErrAborted`.
      - otelflows: span workflow task có `flows.compensation`.
      - Tài liệu EN/VI cập nhật theo các thay đổi trên.
    - **Kiểm tra:**
      - `make test`, `make lint` (4 module), chaos và `benchgate` đều đạt.
      - `BenchmarkActivityRun` khoảng 1240 alloc (trước khoảng 1226, ±6%, trong mức nhiễu).
    - **Review các bản sửa:**
      - Lần 1 không đạt: purge quét cả bảng và chi phí bậc hai; queue lặp làm claim co lại; claim tách 2 round trip nên không atomic; một lần khựng đã park luôn.
      - Lần 2 đạt.
    - **Đĩa:** trong lúc làm, đĩa máy dev đầy 100%. Đã dọn go build cache (23 GB) và các bản sao review.
    - **Còn mở:**
      - Mục tiêu ≤ 80 alloc (thiết kế lại SQL của store).
      - ~~Đếm stuck theo từng worker: với N engine có thể tới N+1 lần thử, mỗi lần để lại goroutine.~~ Đã sửa ở T9.3.
      - ~~Chu trình deadlock hiếm (child kết thúc ↔ cha lấy lock), được Postgres phát hiện và retry.~~ Đã sửa ở T9.5.
      - Comment của migration `00005` sai (đã áp dụng nên không sửa).
      - Thỉnh thoảng có một run benchmark mất khoảng 1.5 s (có từ trước).
      - Claim lock dư tới `$3` dòng mỗi queue trong lúc chạy câu lệnh.

### P9 — Khắc phục các hạn chế về hành vi

Các mục được rút ra từ phần "Còn mở" của T8.x, xếp theo thứ tự làm. Mỗi task đi theo đúng quy trình: test trước, cài đặt, test (race, lint, bench nếu chạm đường nóng), review độc lập, rồi mới tick và commit.

- [x] **T9.1** ⚡ Poll theo thời điểm đến hạn: poller biết thời điểm sớm nhất mà việc của các queue trở nên claim được.
    - **Store:**
      - interface tuỳ chọn `store.DueClaimer` (`ClaimWorkflowTasksDue`/`ClaimActivityTasksDue`): claim trả kèm thời gian còn lại tới lần đến hạn kế tiếp, tính theo giờ của store;
      - pg chạy câu due trong cùng round trip với claim (pipeline ở fast path, batch ở đường pgx);
      - migration `00009` thêm 2 index một phần: `not_before` (hand-back) và `activity_task_delayed_idx` (task không có lease);
      - câu `ready_at` đọc tối đa 64 dòng mỗi queue và bỏ các execution đang có lease (lease giữ lại là việc của owner; lease hết hạn là việc của khôi phục). Không index `locked_until`, vì cột này đổi ở mọi lần gia hạn lease nên mỗi lần gia hạn sẽ không còn là HOT update.
    - **Báo cho engine khác:** hand-back có delay, retry delay, và các execution được nhả lúc `Stop` (`Unstick`) đều gửi notification. Timer do engine đang chạy commit thì không: execution đó sticky với chính engine ấy, vốn đã biết thời điểm đến hạn. Thử báo cho mọi timer thì engine commit tự claim rỗng thêm mỗi timer.
    - **Worker:** đưa thời điểm đến hạn của store vào `dueTimes`. Chỉ hỏi sau một lần claim không đầy.
    - **Test:**
      - `TestDelayedWorkOfAnotherWorkerIsOnTime`: A rảnh trước khi engine khác commit timer, retry và hand-back. Đỏ khoảng 1.8 s so với giới hạn 1.5 s.
      - `TestRunningActivitiesAreNotDue`: đỏ 7 claim, xanh ≤ 3.
      - `TestKeptExecutionsAreNotDue`: đỏ 8/8 claim báo due, xanh 0.
      - Conformance `DueClaims` (mem và pg).
      - `TestDueStatementsReadFewRows`: đỏ 20015 dòng khi thiếu index.
    - **Bench:** ActivityRun 76 alloc (trước 75), các rule đều đạt.
    - **Review:**
      - Lần 1 không đạt: engine đã rảnh từ trước vẫn trễ `PollMax` vì commit việc có delay không gửi notification; hạn lease của activity đang chạy bị tính là đến hạn; câu due bị lập kế hoạch lại mỗi lần và vẫn chạy khi claim đầy.
      - Lần 2 không đạt: hạn của lease giữ lại (eager) bị tính là đến hạn, số claim tăng 60%.
      - Lần 3 đạt.
    - **Còn mở (thấp):**
      - việc của engine đã crash chờ lần poll theo giờ (`PollMax`);
      - queue có hơn 64 execution giữ lease đến hạn trước timer đầu tiên thì quay về poll theo giờ;
      - timer của execution sticky với engine khác có thể làm engine rảnh claim rỗng một lần.
- [x] **T9.2** Child được tạo trong đúng task mà cha kết thúc: không bỏ âm thầm nữa. Completion kết thúc mà còn child đang được tạo thì bị từ chối (`ErrInvalidCompletion`) như với activity, hoặc báo lỗi rõ cho workflow. Test ở mức engine.
    - **Quyết định** (khác với hai hướng trên, đã cân nhắc):
      - Từ chối completion thì workflow không bao giờ kết thúc được.
      - Báo lỗi cho workflow thì vô nghĩa, vì nó đã return rồi.
      - Vì vậy:
        - khi continue-as-new, child được tạo thật và chạy tách khỏi cha, đúng như tài liệu vốn mô tả (trước đây bị bỏ, sai);
        - khi kết thúc kiểu khác, child và activity chưa await không được chạy (đúng ngữ nghĩa: child đang chạy đằng nào cũng bị huỷ khi cha kết thúc), nhưng không còn âm thầm: worker ghi warning nêu số lượng, và flowtest làm test fail (`SetDroppedHook`).
    - **Cài đặt:**
      - executor giữ lệnh child khi continue-as-new và đếm những gì bị bỏ (`DroppedAtEnd`);
      - `store.ValidShape` chỉ cho phép child trong completion kết thúc nếu là continue-as-new;
      - warning ở `driver.reportDropped`.
      - Tài liệu EN/VI và doc của `ContinueAsNew` nêu rõ từng trường hợp, và hai giới hạn: id child trùng lúc continue-as-new thì lỗi không ai thấy; child tách rời không được hoàn tác cùng chuỗi.
    - **Test:**
      - `TestChildrenStartedAtTheEnd`: child tách rời chạy tiếp sau khi chuỗi cha xong; đếm các lệnh bị bỏ; kết thúc không bỏ gì thì không có warning;
      - conformance `ChildrenWithTheEnd` (mem, pg cả hai đường);
      - flowtest `TestGW17_436_DroppedAtTheEndIsReported`.
      - Tất cả đều đỏ khi bỏ phần sửa.
      - Kèm theo, làm chắc lại 2 test conformance flaky khi máy tải nặng: `DueClaims`, `TimersBecomeVisibleAtTheirTime`.
    - **Review:** lần 1 không đạt (tài liệu khẳng định sai về flowtest, hai câu mâu thuẫn, chưa ghi quyết định); lần 2 đạt.
    - **Còn mở (thấp):** flowtest chỉ báo cho instance được truyền vào `Run`, không báo cho child của nó; hook không có recover (chỉ flowtest dùng).
- [x] **T9.3** Đếm stuck trong DB. Execution lưu `last_seq` của lần kẹt gần nhất (migration mới). Lần kẹt thứ hai ở cùng `last_seq`, trên bất kỳ engine nào, sẽ park. Test với 2 engine.
    - Migration `00010_stuck_seq.sql` thêm cột `stuck_seq` (nullable, không rewrite bảng). `Store.NoteStuck(lease, seq)`: một câu lệnh được fence (CTE `FOR NO KEY UPDATE` theo owner/lease_version), 0 dòng → `ErrLeaseLost`; trả về `again` khi giá trị cũ bằng `seq`.
    - `driver.fail` gọi `NoteStuck` thay cho map trong bộ nhớ của worker (đã xoá `stuckAgain`); lỗi → bàn giao (có log trừ `ErrLeaseLost`).
    - `ReopenParked` (Engine.Resume) xoá `stuck_seq`: lần kẹt đầu sau Resume vẫn được bàn giao như trước. Continue-as-new bắt đầu với `NULL`.
    - Test: `TestStuckIsCountedAcrossWorkers` (2 worker, đỏ: 3 lần thử), conformance `NoteStuck` (mem, pg fast/pgx, gồm cả sau Resume), `TestStuckWorkflowIsParkedAndResumed`.
    - Review 1 lần: đạt (sửa thêm: xoá đếm khi Resume, log lỗi NoteStuck).
- [x] **T9.4** Giữ lock khi activity đang chờ queue hoặc chờ retry:
    - heartbeat gia hạn lock của execution còn activity task chưa xong;
    - có giới hạn tuyệt đối để workflow bị bỏ quên không giữ lock mãi.
    Test: activity chờ retry lâu hơn TTL của lock mà lock vẫn còn; quá giới hạn thì lock hết hạn.
    - Cách làm: kiểm tra lười thay cho heartbeat (không tốn gì mỗi nhịp). Khi bên khác lấy lock đã hết `ttl`, holder vẫn giữ nếu còn activity task (trong queue, chờ retry, hoặc lease chết) và `until + hold_us > now()`. Migration `00011_lock_hold.sql` (cột `hold_us`, default hằng nên không rewrite); `store.LockRequest.Hold`; `Options.LockHold` (mặc định 1h, âm = không giữ). Cả fast path và pgx path.
    - "Giới hạn tuyệt đối" tính từ lần gia hạn cuối (`until`): workflow bị bỏ quên (không ai claim gì) hết hạn sau `ttl + LockHold`; activity còn được retry thì vẫn làm mới `until` cho tới khi hết retry policy (nó đang chạy, như trước).
    - Sửa thêm lỗi tìm thấy khi test: thực thi eager chạy cả activity thuộc queue engine không phục vụ (`WithQueue` bị bỏ qua). Nay chỉ eager với queue của engine (`slices.Contains`, 0 alloc). Test `TestEagerKeepsToItsQueues`.
    - Sửa test lỏng của T9.1 (`TestDelayedWorkOfAnotherWorkerIsOnTime`, đỏ 3/12 lần với `-race` ở HEAD): điều kiện chờ `== 1` thành `>= 1`; 12/12 xanh.
    - Test: conformance `LockKeptWhileActivityWaits` (queue, chờ retry, lease chết, quá Hold; mem, pg fast/pgx), `TestLockKeptWhileActivityWaits` (engine, mặc định và LockHold âm). Đỏ trước khi sửa.
    - Bench gate đạt: ActivityRun 76.5 allocs/op, Public 140.5, Replay 30.
    - Review 1 lần: đạt (sửa thêm tài liệu concepts/operations vi+en, kiểm tra `Hold < 0`).
    - Còn mở: kết quả activity đã xong nhưng workflow task còn chờ trong queue lâu hơn `ttl` thì lock vẫn hết hạn (không tính `pending_event`); khi nâng cấp cuốn chiếu, binary cũ lấy lại lock không ghi `hold_us`.
- [x] **T9.5** Bỏ chu trình deadlock giữa child kết thúc (`dropEnded`, `notifyParent`) và cha lấy lock (`acquireLock`): sắp lại thứ tự khoá, hoặc dùng `SKIP LOCKED` rồi thử lại. Test tái hiện tranh chấp (pg).
    - Một thứ tự khoá cho mọi transaction (doc của `lockParent`): execution row (tổ tiên trước con cháu), rồi activity task row, rồi lock row.
    - Kết thúc (complete terminal, Terminate) khoá row của cha trước (`lockParent`, trước fence), rồi `cancelChildrenOf`, `dropEnded` (pending + task), `dropLocks` (lệnh riêng), `notifyParent`. Thêm 2 lệnh trong cùng pipeline, chỉ khi kết thúc: không thêm round trip, không thêm alloc (closure không escape).
    - Test `TestEndsLockRowsInOrder` (pg; fast, pgx, hooked; complete và terminate): giữ row mà phía bên kia khoá trước, chờ end đứng chờ, kiểm tra bằng `NOWAIT` rằng end không giữ row phía kia cần. Đỏ ở HEAD với "child ends" và "parent ends"; "task" không đỏ ở HEAD (Postgres tình cờ xoá task trước) nhưng giữ thứ tự tường minh.
    - Bench gate đạt: ActivityRun 74 allocs/op, Public 142, Eager 6.
    - Review 2 lần (1 không đạt: còn chu trình ngược parent kết thúc ↔ child lấy lock; đã sửa bằng cách đổi thứ tự `cancelChildrenOf` trước `dropEnded`).
    - Còn mở: `Unstick` cập nhật nhiều execution row theo thứ tự lưu trữ (rất hiếm, Postgres phát hiện, retry).
- [x] **T9.6** `Options.ListenDSN`: kết nối `LISTEN` riêng, đi thẳng tới Postgres khi pool chạy qua PgBouncer ở transaction mode. Test: engine với pool không có `LISTEN` vẫn nhận hint qua `ListenDSN`.
    - `Options.ListenDSN` (parse bằng `pgx.ParseConfig` trong `New`, sai cú pháp → `ErrInvalidArgument`); `pg.Store.SetListenConfig`: `Watch` mở kết nối riêng (`pgx.ConnectConfig`) thay vì hijack kết nối của pool. Mật khẩu không lộ trong lỗi (pgx redact).
    - Backoff của watch được đặt lại khi Watch đã chạy lâu hơn `PollMax` (lần rớt mới không phải chờ backoff cũ).
    - Test: `TestWatchListensOnItsOwnConnection` (pool 1 kết nối bị giữ, hint vẫn tới; đỏ trước khi sửa), `TestListenDSN` (engine, DSN sai bị từ chối, session LISTEN theo application_name).
    - Tài liệu vi+en: hook của pool không áp dụng, `sslmode=verify-full`, lỗi kết nối hiện trong log.
    - Review 1 lần: đạt.
- [x] **T9.7** ⚡ Lấy lock trong 1 round trip: kết quả lock được trả về ngay trong completion, không cần resume riêng. Bench `BenchmarkEagerActivity` và một bench có lock.
    - `store.Completion.Resume`/`Resumed`: completion giữ lease (KeepFor > 0, không kết thúc) trả về task kế tiếp trong cùng pipeline (resumeExtend, resumeWorkflow, resumePending; cùng transaction nên thấy ngay câu trả lời của lock). Fast path (`resumeAfter`, `decodeResumed`), pgx path (`completeWithPgx`), mem. Driver dùng khi completion có lock; bỏ `ResumeWorkflowTask` sau đó. Hai buffer resume dùng chung qua `nextResumeBuffer`.
    - Bench `BenchmarkLockStep` (mới): 77 → 0 allocs/op, 7.4 KiB → ~140 B, 2 → 1 round trip, −10.5% thời gian (p=0.015; bị chi phối bởi commit). `BenchmarkEagerActivity` không đổi (6 allocs, ~780 B). ActivityRun 77, Public 142, Replay 30. Ngân sách: LockStep allocs ≤ 1, B/op ≤ 512.
    - Lưu ý: một lần tách hàm (gocyclo) bằng struct `resumeRows` làm tăng 1 alloc/~400 B mỗi completion; đã bisect và viết lại bằng biến cục bộ + hàm nhận giá trị.
    - Test: conformance `LockAnswerInCompletion`, `ResumeParity` (giống hệt `ResumeWorkflowTask`, LockHeld + LockAcquired + signal, bỏ timer tương lai, buffer cũ), `ResumeCompensating`, `ResumeParked`, `ResumeWithEager`; worker `TestLockAnswerComesWithTheCompletion` (0 lần `ResumeWorkflowTask`; driver cũ: 3).
    - Sửa test lỏng `TestKeptExecutionsAreNotDue` (lease của B 600ms → 2s; trượt 1 lần khi cả bộ test chạy tải nặng, cả ở HEAD).
    - Review 1 lần: đạt (sửa thêm: reset `Resumed` ở đường pgx, xoá `c.Resume` khi lỗi; gộp các probe của reviewer).
    - Còn lỏng khi máy tải nặng: `TestDelayedWorkOfAnotherWorkerIsOnTime`, `TestDelayedWorkIsNotPollMaxLate/pg` (test thời gian thực).
- [x] **T9.8** Phục vụ waiter của lock theo thứ tự FIFO: bảng hàng đợi waiter; khi nhả lock thì đánh thức waiter đầu hàng thay vì để các bên poll bằng `LockWait`. Test: N waiter nhận lock đúng thứ tự.
    - Kết quả: review lần 5 đạt (sau khi người dùng chọn làm lại ở lần 3 và chọn sửa ở lần 4). Bench gate đạt: ActivityRun 76 allocs/op, Public 140.5, Eager 6 (782 B), LockStep 0. A/B xen kẽ với HEAD trước T9.8: thời gian và allocs không đổi đáng kể; PublicActivityRun B/op +7% (2 lệnh pipelined thêm mỗi lần kết thúc).
    - Diễn biến (giữ lại để tham khảo):
    - Đã dừng theo quy tắc sau 3 lần review không đạt, báo người dùng.
    - Lần 1 (thiết kế: hàng đợi `flows.lock_waiter`, ưu tiên cho bên chờ đầu hàng tới khi lần thử kế tiếp trễ quá `ttl`, nhả lock thì bắn sớm timer chờ đặt tên `flows.lock:<key>`): không đạt. Bên chờ huỷ (context) vẫn giữ lượt nên lock rảnh bị từ chối tới hàng giờ; lỡ đánh thức khi timer chưa được tạo hoặc row của bên chờ đang bị khoá.
    - Lần 2 (sửa: request và timer chờ trong cùng một checkpoint; lượt chỉ có khi timer còn pending; wake khoá row bên chờ rồi bắn timer bằng lệnh riêng): không đạt. Bên chờ được đánh thức bị từ chối ở lần thử cuối; bên chờ được đánh thức rồi bị terminate vẫn chặn lock rảnh.
    - Lần 3 (sửa: chỉ bên chờ được một lần nhả lock đánh thức mới có quyền ưu tiên, trong một `ttl`; bên chờ chưa được đánh thức không chặn ai): không đạt. Race trên pg: request đang chờ khoá dòng lock của bên nhả thấy dòng đã bị xoá sau commit, CTE `turn` đọc snapshot cũ (chưa có `woken_until`) nên lấy mất lock trước bên chờ vừa được đánh thức. Tái hiện 3/3 (hook chặn trong lúc kết thúc).
    - Nguyên nhân gốc: quyền ưu tiên nằm ở bảng khác (`lock_waiter`) với dòng mà người tranh chấp khoá và đọc lại (`flows.lock`), nên READ COMMITTED chỉ đọc lại dòng lock, không đọc lại lượt.
    - Hướng làm lại (đã đề xuất sau lần 3, người dùng chọn): giữ lượt ngay trên dòng `flows.lock`. Khi nhả mà có bên chờ, không xoá dòng mà chuyển thành dòng giữ chỗ (cột `reserved`, `execution_id` = bên chờ, `until` = hết lượt). `acquireLock` (`cur ... FOR UPDATE`) khi đó đọc lại đúng lượt sau khi chờ. Chỉ cho lấy khi dòng rảnh, chỗ giữ là của mình, hết lượt, hoặc bên chờ không còn sống. Kèm test pg cho race này.
    - Sửa thêm cần làm: các test waiter dùng `ttl` lock riêng (dài hơn `h.TTL`) để tránh lock hết hạn khi máy tải nặng; đó là nguyên nhân của lần trượt "claimed 2 tasks" (lock hết hạn, LockLost). Không phải lỗi engine.
    - Người dùng chọn làm lại theo hướng trên. Đã làm: dòng `flows.lock` thành chỗ giữ (`reserved`, `execution_id` = bên chờ được đánh thức, `until` = hết lượt) khi nhả lock có bên chờ; `acquireLock` đọc lại lượt trên dòng sau khi chờ; chỗ giữ không được gia hạn, bị lấy lại không có LockLost. Test race (`LockWaiterRace`) đỏ trước, xanh sau; các test waiter dùng `waitTTL`. A/B xen kẽ với HEAD: không đổi thời gian và allocs; PublicActivityRun B/op +7%.
    - Lần 4: không đạt, lỗi hẹp: `dropLocksOf` chọn bên chờ có `woken_until` lớn nhất, có thể là bên chờ cũ đã rời đi (khi các lần lấy lock dùng `ttl` khác nhau) thay vì bên vừa được đánh thức; `woken_until` không được xoá khi xếp hàng lại. Cách sửa: chỉ chọn bên chờ được đánh thức trong transaction này (`woken_until = now() + ttl` của dòng lock), xoá `woken_until` khi enqueue; thêm test (ttl khác nhau, Terminate holder). Lỗi nhỏ đi kèm: bên chờ đang park (`needs_attention`) có thể được đánh thức (chỉ nên chọn `running`/`compensating`).
    - Người dùng chọn sửa rồi review lần 5. Đã sửa: `dropLocksOf` chỉ chọn bên chờ được đánh thức trong transaction này (`woken_until = now() + ttl` của dòng lock); `woken_until` bị xoá khi xếp hàng lại; bên chờ đang park không được đánh thức (pg và mem). Test thêm: `LockWaiterStaleWoken` (đỏ trên pg trước khi sửa), `LockWaiterTerminatedHolder`. Review lần 5 đạt; thêm `LockWaiterParkedSkipped`, ghi "không bị park" trong tài liệu, bỏ trường thừa ở mem.
    - Lưu ý nâng cấp: execution của code cũ đã gọi `Lock` với `LockWait` (hơn 1 lần thử) rồi phát thêm command bất kỳ sau đó (đã lấy được lock hay còn chờ) bị park `needs_attention` khi replay (không fail): timer `flows.lock:<key>` luôn chiếm một schedule id, kể cả khi lock được cấp ngay. Đã ghi trong `CHANGELOG.md` (T10.17).

### P10 — Xử lý hết các mục còn mở

Gom mọi mục "Còn mở" ở trên, xếp theo mức độ ảnh hưởng. Quy trình như cũ: test đỏ trước, cài đặt, `make test`, `make lint`, bench và benchgate với task ⚡, review độc lập; không đạt quá 3 lần thì dừng và báo người dùng. Bước đầu của mỗi task là kiểm tra lại mục đó còn đúng không (code đã đổi nhiều qua P9).

Đã xong nhờ P9, chỉ cần gạch trong các ghi chú cũ: lấy lock tốn 2 round trip (T9.7); activity chờ queue hoặc retry làm lock hết hạn (T9.4); mục tiêu ≤ 80 alloc (đang 76); child tạo ở task kết thúc bị bỏ âm thầm (T9.2).

**A. Độ trễ và khôi phục**
- [ ] **T10.1** Khôi phục nhanh sau khi engine crash. Hiện việc của engine chết chỉ được nhận ở lần poll theo giờ (`PollMax`), vì T9.1 cố ý không báo hạn lease đang chạy (heartbeat đẩy nó lên liên tục). Hướng: báo hạn của lease không còn được gia hạn (ví dụ `locked_until` cũ hơn một nửa TTL so với lần gia hạn dự kiến), hoặc ghi `renewed_at` và chỉ báo lease quá hạn gia hạn. Test: kill một engine (chaos hoặc store giả), engine kia nhận việc trong khoảng `LeaseTTL` chứ không phải `PollMax`.
- [ ] **T10.2** Hàng đợi có hơn 64 execution giữ lease đến hạn trước timer đầu tiên thì quay về poll theo giờ. Thay giới hạn cứng 64 của `nextWorkflowDue` bằng truy vấn dùng index (`min(ready_at)` trên các execution không giữ lease), giữ số dòng đọc thấp (`TestDueStatementsReadFewRows`).
- [ ] **T10.3** Timer của execution đang sticky với engine khác làm engine rảnh claim rỗng một lần: báo hạn có tính `sticky_until`. Test đếm số claim.

**B. Lock và thứ tự khoá**
- [ ] **T10.4** Thứ tự khoá dòng lock và execution còn lại:
    - `dropLocksOf` và Terminate xoá lock row theo thứ tự quét: khoá theo key trước (`ORDER BY key FOR UPDATE`), khớp thứ tự của claim và `acquireLock`;
    - `Unstick` cập nhật nhiều execution row theo thứ tự lưu trữ: khoá theo `id`;
    - claim khoá dư tới `$3` dòng lock mỗi queue: chỉ khoá lock của các execution thực sự được claim.
    Test kiểu `TestEndsLockRowsInOrder` (NOWAIT) cho từng chỗ.
- [ ] **T10.5** Lock vẫn hết hạn khi kết quả activity đã xong nhưng workflow task còn chờ trong queue lâu hơn `ttl`: tính cả `pending_event` chưa xử lý vào điều kiện giữ lock (cùng giới hạn `LockHold`). Ghi vào tài liệu nâng cấp: khi nâng cấp cuốn chiếu, binary cũ lấy lại lock không ghi `hold_us`, và execution đang ngủ trong `LockWait` sẽ bị park.

**C. Saga và executor**
- [x] **T10.6** ⚡ Cache executor giới hạn theo tổng byte (mục còn thiếu của T3.3), không chỉ theo số lượng; evict LRU khi vượt. Bench `BenchmarkActivityRun` không tăng alloc.
    - `Executor.Size()`: ước lượng rẻ (4 KiB mỗi executor + 4 KiB mỗi coroutine sống + byte payload, comp data, err của mọi event), cộng dồn khi replay hoặc ghi event, không duyệt cấu trúc mỗi task; cùng history thì cùng size (live hay replay). Cache giữ size lúc put nên tổng chính xác; evict LRU khi vượt `CacheSize` hoặc `CacheBytes`; executor lớn hơn cả giới hạn bị đóng, không cache, không evict ai.
    - Option `CacheBytes` (flows + worker), mặc định 256 MiB (`worker.DefaultCacheBytes`), `-1` = chỉ giới hạn số lượng. Tài liệu vi+en.
    - Test: `TestExecutorSizeCountsPayloads`, `TestCacheEvictsLRUBeyondBytes`, `TestCacheDropsAnExecutorLargerThanTheLimit`, `TestCacheReplaceKeepsBytes`, `TestCacheWarmCycleAllocations`.
    - Bench (n=12, máy chia sẻ với agent khác): ActivityRun 76 → 77.5 allocs/op (p=0.195, không có ý nghĩa; từng run 73–82 cả hai bên), B/op 14.59 → 14.42 KiB; Replay 30 allocs, 37.89 KiB không đổi. Gate ActivityRun: median 76 ≤ 85.
    - Review 2 lần (1 không đạt: executor quá lớn evict hết các entry khác trước khi tự bị bỏ; input activity được tính sau replay nhưng không khi chạy live). Lần 2 đạt.
    - Còn mở: ước lượng chỉ tăng, execution history rất dài (nhiều signal) có thể vượt `CacheBytes` rồi replay mỗi task (khó xảy ra với 256 MiB).
- [ ] **T10.7** Comp data của attempt uncertain trước đó bị mất: compensator nhận data của attempt cuối (thường là zero). Giữ comp data khác rỗng gần nhất qua các attempt (cột trên `activity_task`), compensator nhận nó. Test: attempt 1 trả comp data rồi timeout, attempt 2 lỗi; compensator thấy data của attempt 1.
- [x] **T10.8** Store bộ nhớ: `cancelChildrenOf` và `Wake` quét toàn bộ execution; thêm chỉ mục theo cha và theo trạng thái.
    - Hai chỉ mục trên `Store`: `runnable` (queue → execution running/compensating) và `children` (execution cha → con, mọi trạng thái). Cập nhật ở `create` (cả con và continue-as-new), mọi đổi trạng thái qua `setStatus` (hoàn tất task, mở lại child để compensate, `ReopenCompensation`, `ReopenParked`, `Terminate`) và `PurgeEnded`. Dùng cho `Wake`, `ClaimWorkflowTasks(Due)`, `cancelChildrenOf`, `compensateChildren`.
    - Test: `TestIndexedLookupsVisitMatchingSet` (đỏ trước: "ending a parent of 3 children visited 504 executions"), `TestIndexesFollowEveryChange` (so chỉ mục với quét toàn bộ sau mỗi bước), `TestClaimRepeatedQueue`.
    - Bench (0 alloc cả hai bên): Wake 10000 execution 587 µs → 408 ns; cancelChildrenOf 10000 execution 463 µs → 198 ns.
    - Review 1 lần: đạt (thêm test queue lặp).
    - Còn mở (store bộ nhớ, không phải đường nóng của pg): các hàm quét toàn bộ activity task mỗi lần hoàn tất hoặc claim (`duplicateActivity`, `cancelRequests`, `end`, `running`, `hasActivity`, claim activity theo queue); `extendLocks`/`releaseLocks` quét mọi key; purge N con của một cha tốn O(N²).
    - Lần `make test` đầu trượt 4 test lock waiter pg ("claimed 2 tasks, want 1") khi chạy song song với bench và review; chạy lại đạt. Thuộc T10.13.

**D. Round trip và quan sát**
- [x] **T10.9** `Terminate` đọc `GetExecution` riêng để báo cho observer: trả thông tin cần thiết ngay từ câu lệnh terminate. Khi có hook, `inTx` tốn thêm một round trip `SELECT`: gộp vào batch.
    - `store.Store.Terminate` trả `(EndInfo, error)`: câu lệnh terminate của pg trả execution id, instance id, workflow, version; mem trả giống. `Engine.Terminate` đưa cho observer, bỏ `GetExecution`. Khi có hook, `inTx` không gửi `SELECT` riêng: completion xếp câu đọc đó vào batch sẵn có (trước các lệnh resume), Terminate lấy từ câu lệnh terminate. Kết thúc có hook còn BEGIN, một batch, hook, COMMIT.
    - Test: `TestHookedEndsReadNothingMore` (pg, đếm câu lệnh bằng tracer pgx; đỏ trước: completion gửi thêm `SELECT instance_id, workflow, version ...`), `TestTerminateInformsObserverWithoutARead` (đỏ trước: đọc 1 lần), `TestHookedParkWithResume`, conformance `testTerminate` kiểm `EndInfo` cho pg và mem.
    - Không chạm đường nóng (đường fast completion và driver không đổi), không chạy bench.
    - Review 1 lần: đạt.
- [ ] **T10.10** Tracing: nối span qua các task của một execution (link tới span của task trước, hoặc span cha theo instance); test cho attempt mất lease.
    - Lần làm 1 (commit 50c59b4, đã revert ở 6936885): một trace cho mỗi execution, span context cha suy ra bằng hash từ instance id và execution id, không phát, đánh dấu sampled. Review độc lập 1 lần không đạt (thiết kế): cờ sampled ép buộc ghi đè sampler `ParentBased(TraceIDRatioBased)` của người dùng (giữ 100% span); trace id trùng giữa các store (instance id là khoá người dùng, execution id là bigserial từ 1); root mồ côi (Jaeger/Tempo báo thiếu root, tail sampler chờ timeout, trace kéo dài nhiều ngày); workflow bắt đầu trong request đã có trace không nối được; child không nối với trace của cha; hai nửa trace id tương quan.
    - Hướng làm lại (theo review): lúc `Start` lấy trace context của người gọi (hoặc tạo root mới), phát một span ngắn `flows.start <wf>`, lưu traceparent W3C (~55 byte) trên dòng execution (migration mới) và trả về cùng claim (vẫn một câu lệnh); task và attempt là con của span đó; child workflow nhận traceparent của cha; tôn trọng quyết định sampling thật.
- [ ] **T10.11** flowtest: báo future bị bỏ cho cả child, không chỉ instance truyền vào `Run`; hook có recover. Xem lại giới hạn activity chờ cancel do timer của workflow (`ErrNeverIdle`): sửa nếu được, nếu không thì giữ tài liệu.
    - Review 3 lần không đạt: (1) bỏ sót child của các execution trước trong chuỗi continue-as-new, đi theo `ev.Ref` của child không được tạo (`ErrInstanceExists`) nên đổ lỗi instance khác; (2) root được `Start` trước `Run` không được kiểm (cận dưới `LastID` lấy lúc `Run` bắt đầu); (3) `dropped` bị xoá hết cuối mỗi `Run` nên root khác chưa `Run` mất báo cáo; `Run` hai lần cùng instance báo trùng phần lấy từ history; `Run` với id của một child không kiểm gì.
    - Xem lại cách làm: cả ba lần đều là lỗi gán trạng thái báo cáo cho đúng `Run` khi một tester chạy nhiều `Run`, do trạng thái theo instance và bị xoá theo từng `Run`. Mô hình mới, theo execution: (a) `dropped` khoá theo execution id, giữ tới khi được báo; (b) tester giữ tập execution id đã báo, không báo lại; (c) cây của `Run(id)` = mọi execution có `InstanceID = id` (bất kể cha) cộng mọi execution có cha trong cây, theo thứ tự id; (d) chỉ kiểm execution đã kết thúc. Một quy tắc duy nhất: mỗi execution kết thúc được báo đúng một lần, ở `Run` đầu tiên mà cây của nó chứa nó.
- [ ] **T10.12** analyzer: channel không gán vào biến (`foo(make(chan int))`), field kiểu channel hoặc mutex; không báo nhầm `SideEffect` để trong biến.

**E. Test và chaos**
- [ ] **T10.13** Bỏ sleep thật trong test lock và test due: dùng đồng hồ của store (mem) hoặc chờ theo điều kiện; làm chắc `LockRenewal` (pg), `TestDelayedWorkOfAnotherWorkerIsOnTime`, `TestDelayedWorkIsNotPollMaxLate/pg`, `TestLockLost`. Chạy `make race10` để xác nhận.
- [ ] **T10.14** Test còn thiếu: holder lock đang `needs_attention`; replay của `LockWait`; hook đi cùng `ErrPendingEvents`; park do non-determinism tới hook ở mức engine; 0 alloc khi không cấu hình gì (observer, tracer, hook); bỏ sticky khi đang compensating.
- [ ] **T10.15** Chaos: fencing ở mức workflow lease (ngủ ngắn hơn thời gian đóng băng); kịch bản cha và con, continue-as-new, timer; chứng minh nhiều racer thực sự tham gia ở C.

**F. Bench, tài liệu, CI**
- [ ] **T10.16** Bench: tìm nguyên nhân run thỉnh thoảng mất khoảng 1.5 s (đã thấy liên quan fsync của máy; xác nhận bằng trace, nếu là máy thì ghi lại); xem headroom của gate trên runner CI dùng chung.
- [x] **T10.17** Tài liệu: sửa thông tin sai của comment migration `00005` trong tài liệu (không sửa migration đã áp dụng); thêm `CHANGELOG.md` với ghi chú nâng cấp (LockWait park khi replay, `hold_us` khi nâng cấp cuốn chiếu, migration 00009–00012).
    - Review 3 lần không đạt, mỗi lần tìm thêm một hành vi đúng nhưng chưa ghi của việc chạy lẫn engine cũ và mới: (1) đính chính sticky thiếu release/Unstick, lời khuyên terminate thiếu "không compensation", `hold_us` khi binary cũ lấy lại dòng, 00012 khi chạy lẫn; (2) `LockWait` lệch replay với mọi execution đã gọi nó (timer luôn chiếm schedule id), không chỉ execution đang chờ; (3) binary cũ không xoá `reserved` nên engine mới lấy được lock đang bị giữ (mất tính độc quyền).
    - Xem lại cách làm: liệt kê chính xác mọi hành vi khi chạy lẫn không hội tụ. Đổi sang một quy tắc an toàn: workflow dùng `Lock` thì không chạy lẫn engine trước T9.8 với bản mới (dừng hết engine cũ rồi mới chạy engine mới); các hiệu ứng chỉ còn là lý do minh hoạ (không đầy đủ).
    - Review lần 4 đạt (sửa thêm các điểm nhỏ: phiên bản cụ thể theo commit, index của 00009 không `CONCURRENTLY`, deadlock khi chạy lẫn trước T9.5, ngừng tạo workflow dùng `LockWait` khi drain).
- [x] **T10.18** CI trên GitHub: chưa từng chạy (chưa push; input `install-only` của `golangci-lint-action@v8` chưa kiểm chứng). **Cần người dùng cho phép push** (remote `origin` có sẵn). Trước đó kiểm tra workflow cục bộ (`actionlint`, hoặc `act` nếu có).
    - 2026-11-16: người dùng đã push; lint và bench đạt trên CI, test trượt `TestKeptExecutionsAreNotDue` ("1 of 10 workflow claims reported a due time"). Tái hiện cục bộ khi ép test và tải vào 1 core: B (CPU bị bóp) trả lại một chain; execution được nhả commit với `ready_at` = `now()` của B, sau khi transaction claim của A bắt đầu, nên được báo đến hạn sau khoảng 0.7ms. Đó là hành vi đúng (việc thật mà claim của A bỏ qua). Thử sửa engine bằng `clock_timestamp()` bị review bác: tạo lỗ lỡ đánh thức (việc đến hạn trong lúc claim chạy không được claim cũng không được báo). Đã hoàn nguyên; thêm test regression `TestDueCoversWhatTheClaimSkipped` (đỏ với `clock_timestamp()`); test worker chỉ bỏ qua hạn khi có execution được trả lại và hạn nhỏ hơn 100ms, kèm log chẩn đoán. Vẫn bắt được lỗi T9.1 (review kiểm chứng). Review 2 lần (2 không đạt), sửa theo review.
    - Kết quả: CI (`ci`) và `release` đạt trên `main` tại `76d29a4`, tag `v0.1.0` đã tạo bởi workflow release. `actionlint` v1.7.7 cục bộ: không lỗi.

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
