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
| `PollMax` | 1s | Khoảng poll dài nhất khi rảnh, tối đa `LeaseTTL`/3 (khôi phục việc của engine đã crash). Notification và thời điểm đến hạn đánh thức poller sớm hơn. |
| `StickyFor`, `CacheSize`, `CacheBytes` | 10s, 1000, 256 MiB | Giữ execution đã replay trong bộ nhớ, cùng với task kế tiếp của nó trên engine này. `CacheSize: -1` tắt cache (task nào cũng replay). `CacheBytes` giới hạn bộ nhớ ước tính của cache (vài KiB cho mỗi execution và mỗi coroutine còn sống, cộng payload đã mã hoá trong lịch sử của nó); vượt giới hạn nào thì evict execution dùng lâu nhất trước. `CacheBytes: -1` chỉ giới hạn theo số lượng. |
| `DisableEager` | false | Chạy mọi activity qua queue thay vì chạy ngay trong engine đã lên lịch nó. |
| `ListenDSN` | (pool) | Connection string cho kết nối `LISTEN` của engine (một kết nối), cho pool đứng sau PgBouncer ở transaction mode. |
| `LockHold` | 1h | Execution giữ lock thêm bao lâu sau `ttl` khi một activity của nó đang chờ (trong queue, chờ retry, hoặc chờ engine khác sau crash). Giới hạn thời gian một workflow bị bỏ quên chặn người khác; âm: lock hết hạn sau `ttl`. |
| `StuckTimeout` | 10s | Code workflow được chạy bao lâu mà không nhường. Một lần: trả lại; hai lần ở cùng chỗ, trên bất kỳ engine nào (đếm trong database): park (`ErrStuck`); `Resume` đặt lại số đếm. Tăng lên nếu code workflow thật sự cần tính toán lâu. |
| `EagerWindow` | 200ms | Execution chờ các activity eager của nó bao lâu trước khi cho signal và timer vào. |

Chi phí đo được và nguyên nhân nằm trong [benchmarks.md](../benchmarks.md).

## Dừng và deploy

- **`Stop(ctx)` dừng có trật tự.** Nó ngừng nhận việc, chờ các task đang
  chạy cho tới khi `ctx` kết thúc, rồi huỷ phần còn lại. Lease của chúng được
  nhả hoặc để hết hạn, và engine khác chạy tiếp. Các execution đang sticky
  được nhả ngay, nên engine kế tiếp không phải chờ `StickyFor`.
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
- **Id.** Instance id đã bị xoá có thể dùng để start lại. Trước khi bị xoá,
  `Workflow.Start` với id đó trả về execution cũ.
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
    execution (cột `trace`, migration 00016) và trả về khi claim, nên mọi
    workflow task và activity attempt, ở bất kỳ process nào, đều là con của
    nó. Span start của child workflow là con của task cha đã tạo nó;
    continue-as-new giữ nguyên trace.
  - Việc lấy mẫu được quyết định một lần, lúc start, bởi sampler của bạn;
    quyết định lưu cùng execution áp dụng cho mọi span của nó
    (`ParentBased`).
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
| `needs_attention` do code sai (`err_kinds` có `non_deterministic` hoặc `stuck`; lỗi của `Handle.Wait` khớp `ErrNonDeterministic` hoặc `ErrStuck`) | Code đã deploy không còn khớp với history (thường do sửa workflow mà không tăng version), hoặc bị chặn mà không nhường. | Deploy code khớp với history (code cũ dưới version cũ), hoặc có nhường, rồi gọi `engine.Resume(ctx, id)`: nó replay và chạy tiếp (bị park lại nếu vẫn không khớp). Goroutine của lần chạy bị kẹt chạy tới khi process thoát: hãy khởi động lại các process. `Terminate` là lựa chọn khác; nó không chạy compensation. |
| Execution ở mãi `compensating` | Nó chờ một activity phớt lờ context, chạy trên queue không engine nào phục vụ, hoặc retry rất lâu. | Xem `flows.activity_task` của execution đó. `Terminate` kết thúc nó mà không hoàn tác. |
| Execution ở mãi `running` mà không có gì đang chạy | Nó chờ signal, timer hoặc child, đúng như thiết kế. | Gửi signal, hoặc `Cancel` nó: workflow được dọn dẹp và hoàn tác. |
| Việc chạy tiếp chậm sau crash | Lease của engine đã chết còn hiệu lực tới hết `LeaseTTL`. | Giảm `LeaseTTL` nếu thời gian khôi phục quan trọng hơn lưu lượng gia hạn. |
| Workflow lỗi `ErrLockLost` | Lock của nó hết hạn khi nó ngủ hoặc chờ lâu hơn `ttl` của lock, và execution khác đã lấy. | Chọn `ttl` lớn hơn các khoảng sleep và await dự kiến; activity chờ trong queue hoặc chờ retry được `LockHold` bao (tăng nó nếu chờ lâu hơn). |
