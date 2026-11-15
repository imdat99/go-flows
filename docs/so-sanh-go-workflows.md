# So sánh với go-workflows và hướng kết hợp

> Chép từ repo saga (`github.com/imdat99/saga`, `docs/so-sanh-go-workflows.md`) ngày 2026-11-15. Trong tài liệu này, "repo này" và "ta" là engine saga. Kế hoạch đã chốt cho go-flows nằm trong `docs/PLAN.md`.

Tài liệu làm việc (2026-11-15) để chuẩn bị cho việc fork `github.com/cschleiden/go-workflows` và kết hợp với engine saga này. Các nhận định về go-workflows dựa trên việc đọc mã nguồn tại commit `48e8119` (2026-08-10) và tài liệu trong repo của nó. Những điểm ghi "chưa tái hiện" là suy luận từ code, cần test để xác nhận trước khi dựa vào.

## Tổng quan hai dự án

| | go-workflows | saga (repo này) |
| --- | --- | --- |
| Tác giả, giấy phép | cschleiden, MIT | imdat99 |
| Tuổi, cộng đồng | Từ 12/2021, khoảng 535 sao, 98 fork, 32 issue mở, còn duy trì; gần như một người duy trì chính | Mới, một người duy trì |
| Cảm hứng | Temporal, Cadence, DTFx (Azure Durable Task) | Saga orchestrator cho modular monolith nhiều instance |
| Dạng | Thư viện nhúng: worker + client + backend | Thư viện nhúng: `saga.Engine` |
| Backend | Postgres, MySQL, SQLite, Redis, bộ nhớ | Chỉ Postgres (River làm hàng đợi trong cùng DB) |
| Driver Postgres | `database/sql` + pgx stdlib | pgx/v5 trực tiếp, có đường chạy nhanh trên `pgconn` |

## Điểm tương đồng

- **Workflow là code Go thường, được replay tất định.** Cả hai chạy lại hàm workflow khi tiếp tục, trả kết quả đã ghi cho phần đã chạy, và cấm code không tất định giữa các bước.
- **Tách phần có hiệu ứng phụ.** go-workflows có *activity*, ta có *step*: được chạy ít nhất một lần, kết quả được lưu, có retry policy và lỗi vĩnh viễn (`workflow.NewPermanentError` và `saga.Permanent`).
- **Chờ đợi bền vững:** timer (`workflow.ScheduleTimer` và `saga.Sleep`), signal (`workflow.NewSignalChannel` và `saga.Await`/`Engine.Signal`).
- **Nhiều instance trên cùng DB:** nhận việc bằng `FOR UPDATE SKIP LOCKED`, khoá có hạn (`locked_until` và lease), việc của instance chết được instance khác nhận lại sau khi hết hạn.
- **Bắt panic trong code người dùng:** go-workflows bắt panic trong activity và trong workflow (`workflow.PanicError`); ta vừa thêm `ErrPanic` cho step, compensator, workflow, hook (lỗi 2 của đợt review).
- **Retention:** cả hai có cơ chế xoá instance đã xong.

## Điểm khác biệt

### Mô hình lưu trữ và thực thi

| | go-workflows | saga |
| --- | --- | --- |
| Lưu trạng thái | Event sourcing: lịch sử sự kiện của instance; mỗi workflow task nạp lịch sử (có cache "sticky" theo worker, mặc định 30 s) | Checkpoint theo step (`saga.step`, một dòng mỗi vị trí); run đọc lịch sử step khi resume |
| Đơn vị việc | Workflow task (chạy hàm workflow tới chỗ chờ) và activity task (chạy activity), hai hàng đợi | Một job River `resume_run` chạy cả run; step chạy ngay trong run |
| Chi phí mỗi bước | Một workflow task nạp lịch sử, chạy, ghi sự kiện; activity là task riêng | 2 round trip mỗi step (1 nếu `AsyncStepStart`), không có task riêng cho step |
| Workflow chạy rất lâu | `ContinueAsNew` để cắt lịch sử | Lịch sử step tăng theo số step; không có ContinueAsNew |

### Bảo đảm khi chạy nhiều instance

| | go-workflows | saga |
| --- | --- | --- |
| Fencing khi hoàn tất | `WHERE ... AND worker = $tên_worker AND locked_until IS NOT NULL`: theo tên worker, không có số phiên bản | Mọi lần ghi qua `store.Fenced` hoặc checkpoint một câu lệnh: khoá dòng run, kiểm tra `owner_id` + `lease_version` |
| Nguồn thời gian | `time.Now()` của từng worker cho `locked_until`, `sticky_until`, `visible_at` | Chỉ `now()` của Postgres (bất biến của repo) |
| Gia hạn khoá | `ExtendWorkflowTask`, `ExtendActivityTask` theo từng task | Heartbeat gộp mọi run của instance trong một câu lệnh, có `SKIP LOCKED` (lỗi 6) |
| Migration nhiều instance | Mặc định giả định một worker; nhiều worker phải tự đồng bộ | `Migrate` lấy advisory lock, chạy trên mọi instance được |
| Enqueue cùng transaction | Sự kiện và task ghi trong transaction hoàn tất task | Mọi job River tiếp tục run được chèn cùng transaction với checkpoint (`InsertTx`) |

Rủi ro của go-workflows rút ra từ code (**chưa tái hiện**):

1. **ABA trong cùng một worker.** Nếu khoá của worker A hết hạn và chính A nhận lại instance trong một goroutine khác, goroutine cũ vẫn thoả `worker = A` khi hoàn tất. `lease_version` của ta chặn đúng trường hợp này.
2. **Lệch đồng hồ giữa các máy.** Khoá có thể bị coi là hết hạn quá sớm (hai worker cùng chạy) hoặc quá muộn (instance chết lâu mới được nhận lại).

### Saga và vận hành

| | go-workflows | saga |
| --- | --- | --- |
| Compensation | Không có sẵn; tự viết trong workflow (bắt lỗi, gọi activity hoàn tác) | Compensator gắn với step, tra theo tên, dữ liệu compensation lưu kèm; chạy theo thứ tự ngược; không cần hàm workflow để compensate |
| Step không chắc đã chạy | Không có khái niệm riêng | `uncertain` (crash giữa chừng, timeout, panic, output không lưu được) vẫn được compensate, `CompensationUncertain(ctx)` |
| Compensation thất bại | Không áp dụng | Retry bền vững theo `CompensatePolicy`, rồi `needs_attention` và `RetryCompensation` |
| Versioning workflow | Không có (tác giả cố ý bỏ qua; khuyên triển khai song song) | `name@version`; instance không biết version để run cho instance khác |
| Phát hiện mất tính tất định | Theo lịch sử sự kiện | Step khác tên ở cùng vị trí, output không decode được, replay ngắn hơn lịch sử (lỗi 3) đều park `needs_attention` |
| Business lock | Không | `saga.Lock` gắn với lease của run |
| Idempotency khi khởi chạy | `InstanceID` do người gọi đặt | Idempotency key: gọi lại cùng key nhập vào cùng run, có kiểm tra xung đột workflow |
| Hook khi run kết thúc | Không | `RunHook` trong transaction cuối (dựng outbox, xem guide chương 7, 12) |
| Kiểm tra input, làm sạch lỗi | Không thấy | `ErrInvalidArgument`, `store.Text`, `ErrOutputNotStorable` (lỗi 4 và 8) |

### Tính năng và công cụ go-workflows có mà ta chưa có

- Sub-workflow (kèm huỷ, chạy trên hàng đợi riêng), signal giữa các workflow.
- Huỷ workflow và activity, dọn dẹp khi bị huỷ.
- `workflow.Select` (chờ nhiều future, channel, default) và concurrency trong workflow.
- Hàng đợi có tên, worker tách riêng cho workflow và activity.
- `ContinueAsNew`.
- Bộ công cụ test workflow và activity (`tester`, `activitytester`), analyzer kiểm tra tính tất định, web UI chẩn đoán, OpenTelemetry, context propagation, converter tuỳ biến.
- Nhiều backend (MySQL, SQLite, Redis).

## Lợi ích khi kết hợp

Mỗi bên mạnh đúng chỗ bên kia yếu:

- **go-workflows mang lại bề mặt tính năng và độ chín:** sub-workflow, huỷ, select, hàng đợi, ContinueAsNew, công cụ test, analyzer, UI, tracing, cùng nhiều năm người dùng thực tế. Ta không phải tự xây và tự tìm lỗi lại cho những phần đó.
- **Thiết kế của ta mang lại tính đúng đắn và lớp saga:**
  - fencing bằng `lease_version` thay cho tên worker (chặn ABA);
  - thời gian chỉ lấy từ DB (không phụ thuộc đồng hồ máy);
  - heartbeat gộp và không bị một dòng bận chặn cả instance;
  - migration an toàn khi nhiều instance;
  - compensation theo tên với dữ liệu lưu kèm, xử lý step không chắc chắn, `needs_attention` và `RetryCompensation`;
  - versioning, phát hiện replay ngắn hơn lịch sử;
  - business lock, hook outbox, kiểm tra input và làm sạch dữ liệu lưu.
- **Kinh nghiệm review của ta áp dụng thẳng vào fork.** Các lớp lỗi mà đợt review engine tìm ra (dữ liệu người dùng làm checkpoint fail mãi, bàn giao khi tắt, kết nối thứ hai trong transaction, đổi định dạng dữ liệu lưu mà không tương thích ngược) nên được kiểm tra trên go-workflows ngay từ đầu.

Kết quả mong muốn: một engine có mô hình và công cụ của họ Temporal, chạy như thư viện trên Postgres, với bảo đảm nhiều instance chặt hơn và saga là công dân hạng nhất.

## Kế hoạch đề xuất

Mỗi giai đoạn theo quy trình đã dùng: test tái hiện trước, sửa, review độc lập bằng subagent; 3 lần review không đạt thì dừng lại xem cách làm.

1. **Fork và mốc ban đầu.** Fork, giữ remote `upstream`; chạy toàn bộ test của họ trên Postgres; đo hiệu năng làm mốc.
2. **Xác nhận rủi ro.** Viết test tái hiện cho ABA trong cùng worker và cho lệch đồng hồ, trên Postgres với hai worker. Kết quả quyết định mức gia cố cần làm.
3. **Gia cố lõi.** Fencing theo số phiên bản, thời gian từ DB cho khoá và lịch, migration có advisory lock, gia hạn khoá không bị dòng bận chặn.
4. **Lớp saga.** Compensation gắn với activity (tra theo tên, dữ liệu lưu kèm), hoàn tác theo thứ tự ngược và bền vững, activity không chắc chắn, `needs_attention`, `RetryCompensation`, key idempotency ổn định cho từng activity.
5. **Mang sang phần đã kiểm chứng của ta.** Business lock, hook khi run kết thúc, kiểm tra input và làm sạch dữ liệu lưu, versioning.
6. **Chất lượng.** Chaos test nhiều instance (port `test/chaos`), perf gate trong CI, tài liệu song ngữ.

## Quyết định còn mở (chốt ở phiên sau)

1. **Fork ở đâu:** repo local trước (remote `upstream` trỏ về cschleiden/go-workflows, tự push sau), hay fork thẳng trên GitHub (cần `gh` đã đăng nhập, là thao tác công khai).
2. **Tên và module path:** thay thế `github.com/imdat99/saga` bằng engine mới (major version mới), giữ tên `go-workflows` để dễ so với upstream, hay tên mới.
3. **Backend:** chỉ Postgres (mọi bảo đảm làm kỹ ở một nơi, giữ backend bộ nhớ cho test) hay giữ cả MySQL, SQLite, Redis (mỗi bảo đảm phải làm lại cho từng backend).
4. **Quan hệ với upstream:** giữ khả năng kéo bản sửa từ upstream (thay đổi ít xâm lấn, nhiều lớp bọc) hay tách hẳn (tự do sửa lõi).

## Trạng thái repo saga lúc chuyển hướng

Xem mục "Engine review fixes (2026-11-15)" trong `TASKS.md`. Tóm tắt: lỗi 1, 2, 3, 5, 7 đã sửa và qua review; lỗi 4 và 8 còn một trường hợp mở (escape surrogate UTF-16 đơn lẻ trong output làm checkpoint fail); lỗi 6 đã sửa và có test, chưa review; migration `00006_error_kinds` là breaking change (không rolling deploy được qua nó).
