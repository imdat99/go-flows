# Vận hành

[English](../operations.md)

## Cài đặt

```go
pool, err := pgxpool.New(ctx, dsn)
err = flows.Migrate(ctx, pool) // process nào cũng gọi được; advisory lock xếp hàng chúng
engine, err := flows.New(pool, flows.Options{
	Logger: slog.Default(),
	// Hook, Observer, Tracer: xem bên dưới.
})
err = engine.Start(ctx)
defer engine.Stop(shutdownCtx)
```

- **Migration.** `Migrate` tạo schema `flows` và áp dụng các migration goose
  nhúng sẵn, chỉ đi tới. Mỗi thay đổi là một file mới. `CHANGELOG.md` liệt
  kê migration của từng bản phát hành và ghi chú nâng cấp khi deploy cuốn
  chiếu. Migration đã áp dụng thì không bao giờ sửa, nên vài comment trong
  đó đã cũ; mục Corrections của `CHANGELOG.md` đính chính lại (ví dụ claim
  không xoá `sticky_owner`).
- **Index trên bảng lớn.** Migration 00013 (v0.1.1) tạo
  `execution_due_idx` không có `CONCURRENTLY`, nên chặn ghi vào
  `flows.execution` trong lúc tạo. Với bảng lớn, trước khi chạy `Migrate`,
  thêm cột được index (`ALTER TABLE flows.execution ADD COLUMN IF NOT
  EXISTS due_owner text NOT NULL DEFAULT ''`, không ghi lại bảng) rồi tạo
  index bằng `CREATE INDEX CONCURRENTLY IF NOT EXISTS` (cùng tên và định
  nghĩa trong `migrations/00013_due_owner.sql`); `Migrate` sau đó bỏ qua cả
  hai. Đặt `lock_timeout` cho lệnh `ALTER TABLE` (lệnh này giữ khoá độc
  quyền trong chốc lát). Nếu tạo index `CONCURRENTLY` thất bại hoặc bị huỷ,
  Postgres để lại một index không hợp lệ mà `IF NOT EXISTS` vẫn chấp nhận:
  kiểm `pg_index.indisvalid`, nếu là false thì `DROP INDEX CONCURRENTLY` rồi
  tạo lại. Migration mới
  tạo index trên bảng lớn dùng `-- +goose NO TRANSACTION` và
  `CONCURRENTLY`.
- **Process chỉ làm client.** Engine không được start vẫn bắt đầu workflow,
  gửi signal, huỷ, terminate và chờ kết quả được. Một process API có thể
  dùng engine như vậy mà không chạy task.
- **Queue.** `Options.Queues` chọn các queue mà engine chạy (mặc định
  `"default"`). Định tuyến activity bằng `WithQueue` và workflow bằng
  `WithWorkflowQueue`, ví dụ để dồn activity chậm vào các process riêng.
  Activity có queue mà không engine nào chạy sẽ chờ mãi: workflow của nó
  không fail.
- **Instance id của engine.** `Options.InstanceID` đặt tên engine trong
  lease và phải là duy nhất cho mỗi process. Giá trị mặc định (hostname cộng
  hậu tố ngẫu nhiên) thoả điều đó.
- **Hook ở mọi nơi.** Đặt cùng một `Options.Hook` cho mọi engine, kể cả
  engine chỉ làm client: engine chạy hook của chính nó cho các lần kết thúc
  mà nó ghi, ví dụ một lệnh `Terminate` nó thực hiện.
- **Đăng ký.** Định nghĩa workflow và activity ở cấp package, trong mọi
  binary chạy chúng. Engine chỉ chạy các version workflow và activity mà
  binary của nó định nghĩa. Những cái khác được trả lại sau một khoảng chờ,
  để engine có chúng nhận.

## Kết nối

- **LISTEN.** Mỗi engine đã start lấy **một kết nối** khỏi pool trong suốt
  vòng đời của nó, để `LISTEN`. Kết nối này rời khỏi pool (không còn tính
  vào `MaxConns`), nên mỗi engine dùng thêm một kết nối server ngoài pool:
  hãy tính nó vào `max_connections` của Postgres.
- **Task.** Task chỉ mượn kết nối trong lúc nói chuyện với store. Tối đa
  `MaxWorkflowTasks + MaxActivities` task chạy cùng lúc (mặc định 64 mỗi
  loại), nhưng phần lớn thời gian của chúng nằm trong activity của bạn,
  không phải trong database.
- **Kích thước pool.** `MaxConns` mặc định của pgxpool là
  `max(4, NumCPU)`. Hãy tăng khi nhiều activity hoàn tất cùng lúc, hoặc khi
  activity của bạn dùng chung pool.
- **Đường nhanh.** Các câu lệnh nóng dùng pgconn trực tiếp, prepare một lần
  cho mỗi kết nối. Việc này cần `QueryExecModeCacheStatement` (mặc định của
  pool) và không có `ConnConfig.Tracer`. Nếu không, engine quay về lời gọi
  pgx thông thường: vẫn đúng nhưng chậm hơn. Khi có `Hook`, các completion
  kết thúc execution cũng dùng transaction tường minh.
- **Huỷ.** Trên đường nhanh, lời gọi có context kết thúc bị cắt ngang và kết
  nối của nó bị đóng. Server cũng được gửi cancel request, để câu lệnh dừng
  lại thay vì commit sau khi bên gọi đã nhận lỗi. Lời gọi trả về khi request
  đó được trả lời, chậm nhất 1 s nếu không tới được database. Như mọi lời gọi
  bị ngắt, vẫn có thể đã commit ngay trước khi huỷ; fencing giữ cho lần thử
  lại an toàn.
- **Pooler.** Đứng sau PgBouncer ở transaction mode thì `LISTEN` không nhận
  được gì. Engine vẫn chạy nhờ poll của chính nó (thêm tới `PollMax` độ trễ
  mỗi lần chuyển giao). Đặt `Options.ListenDSN` là connection string trực
  tiếp hoặc qua session mode: engine khi đó `LISTEN` trên một kết nối riêng ở
  đó, và vẫn dùng pool cho mọi việc khác. Hook của pool không áp dụng cho kết
  nối này; đặt `sslmode=verify-full` cho kết nối trực tiếp. Kết nối lỗi hiện
  trong log là "flows: watch failed". Cũng chạy `Migrate` qua kết nối trực tiếp (nó giữ advisory lock trên
  một kết nối trong khi goose dùng các kết nối khác: pool cần
  `MaxConns >= 3`).

## Tinh chỉnh

| Tuỳ chọn | Mặc định | Tác dụng |
| --- | --- | --- |
| `LeaseTTL` | 30s | Engine đã chết giữ việc của nó bao lâu. Lease được gia hạn mỗi một phần ba thời gian này. Ngắn hơn: khôi phục nhanh hơn, gia hạn nhiều hơn. |
| `MaxWorkflowTasks`, `MaxActivities` | 64, 64 | Mức đồng thời của mỗi engine. |
| `PollMax` | 1s | Khoảng poll dài nhất khi rảnh, tối đa `LeaseTTL`/3 (khôi phục việc của engine đã crash; giá trị lớn hơn bị hạ xuống và ghi log một lần khi tạo engine). Engine rảnh nhận việc của engine đã crash trong khoảng 4/3 `LeaseTTL`, với giả định mọi engine dùng cùng `LeaseTTL`. Notification và thời điểm đến hạn đánh thức poller sớm hơn. |
| `StickyFor`, `CacheSize`, `CacheBytes` | 10s, 1000, 256 MiB | Giữ execution đã replay trong bộ nhớ, cùng với task kế tiếp của nó trên engine này. `CacheSize: -1` tắt cache (task nào cũng replay). `CacheBytes` giới hạn bộ nhớ ước tính của cache (vài KiB cho mỗi execution và mỗi coroutine còn sống, cộng payload đã mã hoá trong lịch sử của nó); vượt giới hạn nào thì evict execution dùng lâu nhất trước. `CacheBytes: -1` chỉ giới hạn theo số lượng. |
| `DisableEager` | false | Chạy mọi activity qua queue thay vì chạy ngay trong engine đã lên lịch nó. |
| `ListenDSN` | (pool) | Connection string cho kết nối `LISTEN` của engine (một kết nối), cho pool đứng sau PgBouncer ở transaction mode. |
| `LockHold` | 1h | Execution giữ lock thêm bao lâu sau `ttl` khi một activity của nó đang chờ (trong queue, chờ retry, hoặc chờ engine khác sau crash), hoặc khi task kế tiếp của nó đang chờ trong queue (đã có event, chưa được claim). Giới hạn thời gian một workflow bị bỏ quên chặn người khác; âm: lock hết hạn sau `ttl`. |
| `StuckTimeout` | 10s | Code workflow được chạy bao lâu mà không nhường. Một lần: trả lại; hai lần ở cùng chỗ, trên bất kỳ engine nào (đếm trong database): park (`ErrStuck`); `Resume` đặt lại số đếm. Tăng lên nếu code workflow thật sự cần tính toán lâu. |
| `MaxTaskClaims` | 10 | Workflow task được claim bao nhiêu lần mà không hoàn tất (mỗi lần engine của nó sập hoặc mất lease giữa chừng) trước khi execution bị park (`ErrCrashLoop`). Một task hoàn tất đặt lại số đếm; một lần trả lại (engine không định nghĩa version của workflow, lỗi tạm thời) chỉ bỏ claim của chính nó, nên các lần sập trước đó vẫn được tính. Execution đang compensating không được đếm. Âm: không giới hạn. |
| `EagerWindow` | 200ms | Execution chờ các activity eager của nó bao lâu trước khi cho signal và timer vào. |

Chi phí đo được và nguyên nhân nằm trong [benchmarks.md](../benchmarks.md).

## Dừng và deploy

- **`Stop(ctx)` dừng có trật tự.** Nó ngừng nhận việc, chờ các task đang
  chạy cho tới khi `ctx` kết thúc, rồi huỷ phần còn lại. Lease của chúng được
  nhả hoặc để hết hạn, và engine khác chạy tiếp. Các execution đang sticky
  được nhả ngay, nên engine kế tiếp không phải chờ `StickyFor`.
- **`Stop` bị giới hạn bởi `ctx`.** Khi `ctx` đã kết thúc, `Stop` chờ thêm
  tối đa 1 giây cho các task đã bị huỷ, rồi bỏ lại những task vẫn còn chạy:
  task đang kẹt vì database ngừng trả lời (failover, mạng rớt không có RST),
  và activity bỏ qua việc context bị huỷ. Trước T11.2, `Stop` chờ những
  activity như vậy vô hạn. Lease của task bị bỏ lại sẽ hết hạn và engine khác
  chạy tiếp, nên activity bỏ qua việc huỷ có thể chạy lại trong khi vẫn còn
  chạy ở process đã dừng. Các lần ghi sau khi context của task kết thúc (kết
  quả activity, nhả lease) bị giới hạn khoảng `LeaseTTL`, nên task kẹt vì
  database tự kết thúc trong khoảng đó. Tới lúc ấy nó vẫn giữ kết nối của
  pool: `pool.Close()` sau `Stop` có thể phải chờ chừng đó. Các execution
  sticky vẫn được nhả (với thời hạn riêng, tối đa 5 giây).
- **Claim lúc `Stop`.** Khi `Stop` đã bắt đầu thì không claim mới nào được
  bắt đầu nữa, trừ một claim đang được gửi đi (giới hạn bởi thời gian chờ
  bên dưới). Claim đang chạy dở có thể đã commit trong database dù bị huỷ, nên
  `Stop` cho nó chạy xong, tối đa 1 giây (hoặc tới khi `ctx` kết thúc, nếu
  sớm hơn), và các task nó claim được chạy trong lúc drain. Claim vẫn chưa
  xong sau đó (database không với tới được) bị huỷ: engine rảnh vẫn dừng
  nhanh, và `Stop` trả `nil` khi không task nào bị ngắt. Task mà claim bị huỷ
  đó có thể đã commit vẫn bị lease cho engine đã dừng tới khi hết
  `LeaseTTL`, rồi engine khác nhận.
- **Deploy cuốn chiếu là an toàn:**
  - execution có version mà binary mới không có sẽ được giao cho engine cũ;
  - activity chạy ít nhất một lần qua lúc chuyển đổi;
  - compensation chạy tiếp trên engine nào có compensator theo tên.
- **Sửa code workflow.** Tăng version và giữ định nghĩa cũ cho tới khi các
  execution của nó kết thúc. Để tìm chúng:

  ```sql
  SELECT workflow, version, status, count(*)
  FROM flows.execution
  WHERE status IN ('running', 'compensating', 'needs_attention')
  GROUP BY 1, 2, 3;
  ```

## Bảo trì Postgres

- **Autovacuum.** `flows.execution` và `flows.activity_task` được cập nhật
  ở mỗi task (lease, thời điểm sẵn sàng) và có nhiều index một phần, nên
  dead tuple tích tụ nhanh. Hãy để autovacuum theo kịp trên hai bảng này,
  ví dụ `ALTER TABLE flows.execution SET (autovacuum_vacuum_scale_factor =
  0.01, autovacuum_analyze_scale_factor = 0.02)`, tương tự với
  `flows.activity_task`; theo dõi số dead tuple và kích thước index.
- **Kích thước payload.** Engine không giới hạn kích thước input, output
  hay payload của signal, và mỗi lần replay đọc toàn bộ history của
  execution. Hãy giới hạn những gì caller gửi (`Start`, `Signal`) ở API của
  bạn, và giữ execution sống lâu ngắn lại bằng `ContinueAsNew`.

## Retention

Execution đã kết thúc cùng history của nó được giữ lại cho tới khi bị xoá.
Engine không có retention chạy nền: hãy gọi `PurgeEnded` từ một job định kỳ
trên một process.

```go
n, err := engine.PurgeEnded(ctx, 30*24*time.Hour, 1000) // kết thúc hơn 30 ngày trước, tối đa 1000 mỗi lần gọi
```

- **Cái bị xoá.** Việc xoá lan sang history, pending event, activity task và
  lock.
- **Cái được giữ.** Hậu duệ của một execution còn hoạt động (đang chạy, đang
  compensate hoặc bị park), cả child lẫn cháu, được giữ lại, vì compensation
  có thể chạm tới chúng.
- **Id.** Mặc định, instance id đã bị xoá có thể dùng để start lại. Trước
  khi bị xoá, `Workflow.Start` với id đó trả về execution cũ. Client retry
  "đặt đơn X" sau retention sẽ chạy lại workflow: khi id phải còn là khoá
  idempotency sau retention, hãy đặt `Options.KeepPurgedIDs` (trên mọi
  engine dùng chung database; trong lúc restart cuốn chiếu để bật nó, hãy
  tạm dừng job `PurgeEnded` hoặc bật trước ở engine chạy job đó, để không
  id nào bị xoá mà không có tombstone).
- **Tombstone (`Options.KeepPurgedIDs`).** Khi đó `PurgeEnded` ghi lại mỗi
  instance id mà nó xoá execution cuối cùng (id, workflow, status, thời điểm
  kết thúc và thời điểm xoá, bảng `flows.purged_instance`) trong cùng
  transaction. `Workflow.Start` với id đó lỗi `flows.ErrInstancePurged`, và
  child start với id đó cũng lỗi như vậy (lỗi của nó khớp cả
  `ErrInstanceExists`). Một Start chạy đua với việc xoá thấy hoặc execution
  hoặc tombstone, không bao giờ thấy id trống. Mỗi lần gọi `PurgeEnded` cũng
  xoá tối đa `limit` tombstone cũ hơn `KeepPurgedIDs`: id được giữ trong
  khoảng tuổi xoá cộng `KeepPurgedIDs`, rồi được giải phóng.
  `Engine.ForgetInstance(ctx, id)` xoá ngay một tombstone, cho operator thực
  sự muốn chạy lại id đó. Các execution trước của một chuỗi continue-as-new
  không để lại tombstone khi còn execution sau. Bảng giữ một dòng nhỏ cho mỗi
  id bị xoá trong cửa sổ đó: chọn cửa sổ theo thời gian client có thể retry.
  Khi tắt tuỳ chọn (mặc định) không có gì được ghi hay đọc và Start không tốn
  thêm gì; khi bật, Start và child start đọc thêm một mục index. Tombstone
  còn lại sau khi tắt tuỳ chọn ở nguyên đó cho tới khi bật lại (khi đó chúng
  hết hạn) hoặc bị xoá (`ForgetInstance`, hoặc
  `DELETE FROM flows.purged_instance`).
- **Bảng `idem`.** Xoá bằng `idem.Purge`, với độ tuổi lớn hơn hẳn khoảng
  thời gian retry và compensation dài nhất của bạn.

## Giám sát

- **Metric và trace:** `otelflows.NewObserver` và `otelflows.NewTracer`.
  - `flows.activity.attempts` và `flows.activity.duration` gắn nhãn theo
    activity, outcome (`completed`, `failed`, `retrying`) và compensation.
  - `flows.runs.ended` gắn nhãn theo workflow và status.
  - Trace đi theo execution. `Workflow.Start` phát một span ngắn
    `flows.start <workflow>`, là con của span trong context của bên gọi
    (không có thì là gốc mới). Span context của nó (26 byte) được lưu cùng
    execution (cột `trace`, migration 00015) và trả về khi claim, nên mọi
    workflow task và activity attempt, ở bất kỳ process nào, đều là con của
    nó. Span start của child workflow là con của task cha đã tạo nó.
  - Trace kéo dài bằng execution của nó. Execution chờ nhiều ngày (timer,
    signal) tạo ra trace dài ngần ấy: một số backend giới hạn thời lượng
    hoặc kích thước trace, và tail sampler bỏ cuộc khi chờ trace kết thúc.
  - Continue-as-new bắt đầu trace mới: span start của execution kế tiếp là
    gốc mới (`flows.continued=true`) có span link tới span start của
    execution trước. Workflow lặp bằng continue-as-new tạo mỗi execution
    một trace có giới hạn, nối với nhau bằng link, không bao giờ là một
    trace không có điểm kết thúc.
  - Lần start không tạo gì kết thúc với `flows.outcome=existing`:
    `Workflow.Start` lần hai cho một instance id (không phải lỗi; không lưu
    gì, execution giữ trace của nó), hoặc child có instance id đã bị dùng
    (status lỗi; child thất bại với `ErrInstanceExists`). Lần start mà
    completion bị từ chối vì có signal tới trong lúc đó kết thúc với
    `flows.outcome=rerun` (không phải lỗi): task chạy lại và start lại.
  - Việc lấy mẫu được quyết định một lần, lúc start, bởi sampler của bạn;
    quyết định lưu cùng execution áp dụng cho mọi span của nó
    (`ParentBased`). Trace state không được lưu: các entry của vendor và
    ngưỡng consistent-probability của OpenTelemetry (`ot=th`) không tới
    được các span sau.
  - Attempt hoặc task mất lease kết thúc với status lỗi và
    `flows.outcome=lease_lost`; attempt thay thế nằm trong cùng trace. Span
    có `flows.instance_id` và `flows.execution_id`.
  - Execution start trước khi nâng cấp, hoặc không có tracer, không có trace
    context đã lưu: span của chúng là span gốc.
- **Cảnh báo:**
  - có execution ở `needs_attention`;
  - số execution `compensating` tăng dần;
  - tỉ lệ `retrying` tăng ở một activity.
- **Log:** truyền `Options.Logger`. Qua `slog`, engine ghi log mức warning
  cho các lần nhận việc, ghi kết quả hoặc gia hạn thất bại. Ở mức error,
  nó ghi các lần park do non-determinism, panic, compensation task thất bại,
  và workflow task bị trả lại.

Các truy vấn hữu ích:

```sql
-- Execution bị park và lý do.
SELECT instance_id, workflow, version, err, err_kinds, created_at
FROM flows.execution WHERE status = 'needs_attention';

-- Việc đã đến hạn mà chưa ai nhận (nên luôn gần 0).
SELECT queue, count(*) FROM flows.execution
WHERE status IN ('running', 'compensating') AND ready_at <= now()
  AND (locked_until IS NULL OR locked_until < now())
  AND (not_before IS NULL OR not_before <= now())   -- đang bị trả lại một lúc
  AND (sticky_until IS NULL OR sticky_until < now()) -- dành riêng cho một engine
GROUP BY queue;

-- Activity đang chờ, theo queue.
SELECT queue, count(*) FROM flows.activity_task GROUP BY queue;
```

Schema là nội bộ và có thể đổi giữa các version. Hãy dùng nó để quan sát, và
ghi qua API của engine.

## Xử lý sự cố

| Triệu chứng | Nguyên nhân | Cách xử lý |
| --- | --- | --- |
| `needs_attention`, `err_kinds` có `compensation_failed` | Một compensator bỏ cuộc sau khi hết retry. | Sửa nguyên nhân (dữ liệu, hệ thống phía sau), rồi gọi `engine.RetryCompensation(ctx, id)`. Compensator thất bại chạy lại, rồi tới các compensator còn lại. |
| `needs_attention`, `err_kinds` có `crash_loop` (`ErrCrashLoop`) | Workflow task của nó bị claim quá `MaxTaskClaims` lần mà không hoàn tất: các engine chạy nó sập (hết bộ nhớ khi replay history lớn, `os.Exit` hoặc lỗi fatal trong code workflow) hoặc mỗi lần đều mất lease. | Tìm nguyên nhân trong log và exit của các engine (giới hạn bộ nhớ, database quá chậm so với `LeaseTTL`), sửa nó, rồi gọi `engine.Resume(ctx, id)`. |
| Activity thất bại với `ErrAttemptsExhausted` | Số attempt chạm giới hạn retry policy mà attempt cuối bị mất: mỗi lần activity làm sập process (hoặc engine mất lease). Nó không chạy lại; workflow nhận một thất bại vĩnh viễn, không chắc chắn, và compensate như thường. | Sửa activity (bộ nhớ, `os.Exit`, lỗi fatal); tăng `RetryPolicy.Attempts` nếu engine bị mất vì lý do khác (deploy giết engine giữa activity). |
| `needs_attention` do code sai (`err_kinds` có `non_deterministic` hoặc `stuck`; lỗi của `Handle.Wait` khớp `ErrNonDeterministic` hoặc `ErrStuck`) | Code đã deploy không còn khớp với history (thường do sửa workflow mà không tăng version), hoặc bị chặn mà không nhường. | Deploy code khớp với history (code cũ dưới version cũ), hoặc có nhường, rồi gọi `engine.Resume(ctx, id)`: nó replay và chạy tiếp (bị park lại nếu vẫn không khớp). Goroutine của lần chạy bị kẹt chạy tới khi process thoát: hãy khởi động lại các process. `Terminate` là lựa chọn khác; nó không chạy compensation. |
| Execution ở mãi `compensating` | Nó chờ một activity phớt lờ context, chạy trên queue không engine nào phục vụ, hoặc retry rất lâu. | Xem `flows.activity_task` của execution đó. `Terminate` kết thúc nó mà không hoàn tác. |
| Execution ở mãi `running` mà không có gì đang chạy | Nó chờ signal, timer hoặc child, đúng như thiết kế. | Gửi signal, hoặc `Cancel` nó: workflow được dọn dẹp và hoàn tác. |
| Việc chạy tiếp chậm sau crash | Lease của engine đã chết còn hiệu lực tới hết `LeaseTTL`. | Giảm `LeaseTTL` nếu thời gian khôi phục quan trọng hơn lưu lượng gia hạn. |
| Workflow lỗi `ErrLockLost` | Lock của nó hết hạn khi nó ngủ hoặc chờ lâu hơn `ttl` của lock, và execution khác đã lấy. | Chọn `ttl` lớn hơn các khoảng sleep và await dự kiến; activity chờ trong queue hoặc chờ retry, và workflow task chờ trong queue, được `LockHold` bao (tăng nó nếu chờ lâu hơn). |
