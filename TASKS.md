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
      - Đếm stuck theo từng worker: với N engine có thể tới N+1 lần thử, mỗi lần để lại goroutine.
      - Chu trình deadlock hiếm (child kết thúc ↔ cha lấy lock), được Postgres phát hiện và retry.
      - Comment của migration `00005` sai (đã áp dụng nên không sửa).
      - Thỉnh thoảng có một run benchmark mất khoảng 1.5 s (có từ trước).
      - Claim lock dư tới `$3` dòng mỗi queue trong lúc chạy câu lệnh.

### P9 — Khắc phục các hạn chế về hành vi

Các mục được rút ra từ phần "Còn mở" của T8.x, xếp theo thứ tự làm. Mỗi task đi theo đúng quy trình: test trước, cài đặt, test (race, lint, bench nếu chạm đường nóng), review độc lập, rồi mới tick và commit.

- [ ] **T9.1** ⚡ Poll theo thời điểm đến hạn. Poller hỏi store thời điểm sớm nhất mà việc của các queue trở nên claim được:
    - `min(ready_at)` của execution và `min(visible_at)` của activity task, gộp vào câu claim để không tốn thêm round trip;
    - poller ngủ tới đúng thời điểm đó.
    Việc do engine khác commit, hand-back và việc ngoài horizon không còn trễ tới `PollMax`. Test: timer do engine B commit được engine A (đang rảnh, `PollMax` lớn) chạy đúng hạn.
- [ ] **T9.2** Child được tạo trong đúng task mà cha kết thúc: không bỏ âm thầm nữa. Completion kết thúc mà còn child đang được tạo thì bị từ chối (`ErrInvalidCompletion`) như với activity, hoặc báo lỗi rõ cho workflow. Test ở mức engine.
- [ ] **T9.3** Đếm stuck trong DB. Execution lưu `last_seq` của lần kẹt gần nhất (migration mới). Lần kẹt thứ hai ở cùng `last_seq`, trên bất kỳ engine nào, sẽ park. Test với 2 engine.
- [ ] **T9.4** Giữ lock khi activity đang chờ queue hoặc chờ retry:
    - heartbeat gia hạn lock của execution còn activity task chưa xong;
    - có giới hạn tuyệt đối để workflow bị bỏ quên không giữ lock mãi.
    Test: activity chờ retry lâu hơn TTL của lock mà lock vẫn còn; quá giới hạn thì lock hết hạn.
- [ ] **T9.5** Bỏ chu trình deadlock giữa child kết thúc (`dropEnded`, `notifyParent`) và cha lấy lock (`acquireLock`): sắp lại thứ tự khoá, hoặc dùng `SKIP LOCKED` rồi thử lại. Test tái hiện tranh chấp (pg).
- [ ] **T9.6** `Options.ListenDSN`: kết nối `LISTEN` riêng, đi thẳng tới Postgres khi pool chạy qua PgBouncer ở transaction mode. Test: engine với pool không có `LISTEN` vẫn nhận hint qua `ListenDSN`.
- [ ] **T9.7** ⚡ Lấy lock trong 1 round trip: kết quả lock được trả về ngay trong completion, không cần resume riêng. Bench `BenchmarkEagerActivity` và một bench có lock.
- [ ] **T9.8** Phục vụ waiter của lock theo thứ tự FIFO: bảng hàng đợi waiter; khi nhả lock thì đánh thức waiter đầu hàng thay vì để các bên poll bằng `LockWait`. Test: N waiter nhận lock đúng thứ tự.

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
