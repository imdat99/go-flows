# Bảo đảm

[English](../guarantees.md)

Những gì go-flows hứa, và những gì để lại cho ứng dụng. Mỗi mục đều có test
đi kèm: các mục GW-* là lỗi của go-workflows mà go-flows tái hiện và sửa
([regressions.md](../regressions.md)), còn `test/chaos` kill, đóng băng và
cho các process thật tranh nhau.

## Engine bảo đảm

**Mỗi instance id có một execution đang chạy.** Một unique index một phần
trên `instance_id` phủ các trạng thái đang hoạt động. Nhiều lệnh start đồng
thời với cùng id chỉ tạo một execution; các lệnh khác nhận handle của nó
(GW-1).

**Code workflow chạy như thể đúng một lần.** Hàm được replay, nhưng mỗi
quyết định đã ghi chỉ được đưa ra một lần: event được ghi thêm dưới lease
của execution, với `(execution, seq)` duy nhất.

**Activity chạy ít nhất một lần, và kết quả được ghi đúng một lần.** Attempt
đã mất lease (process bị tạm dừng, mất mạng hoặc chết) được chạy lại ở nơi
khác, và kết quả đến muộn của nó bị fencing từ chối (chaos test B). Dùng
`Key` để hiệu ứng xảy ra một lần (`idem.Do`).

**Fencing.** Mọi lần ghi cho một execution đang chạy hoặc một activity task
đều kiểm tra `owner_id` và `lease_version` trên dòng đã khoá:

- 0 dòng nghĩa là `ErrLeaseLost`, không bao giờ là thành công âm thầm;
- lease chỉ được cấp khi đã hết hạn, và không bao giờ cấp lại khi còn hiệu
  lực, kể cả cho cùng owner;
- mỗi lần cấp tăng version, nên một bên ghi cũ dùng lại tên owner vẫn bị từ
  chối (GW-2).

**Một transaction cho mỗi checkpoint.** Hoàn tất một workflow task là một
transaction. Nó ghi history, tiêu thụ các event đã giao, tạo activity task,
timer, child và lock. Nó cũng gia hạn hoặc nhả lease và chạy hook. Không có
gì bị ghi dở dang.

**Chỉ dùng giờ database.** Lease, timer, retry delay, TTL của lock và
retention dùng `now()` của Postgres. Đồng hồ các process lệch nhau cũng
không ảnh hưởng (GW-5). Code workflow thấy giờ của task (`flows.Now`), giống
nhau khi replay.

**Non-determinism thì park, không fail.** Replay lệch với history thì
execution bị park ở `needs_attention` (GW-11). Lệch nghĩa là khác loại lời
gọi, tên, hash tham số, thời lượng timer, hoặc replay ngắn hơn history.
Code chạy mà không nhường cũng bị park như vậy (`ErrStuck`), khi điều đó
xảy ra hai lần ở cùng chỗ. `Engine.Resume`
cho execution như vậy chạy lại khi code đã sửa được deploy. Version workflow
lạ, hoặc loại event lạ, thì được trả cho engine khác thay vì fail (GW-12).

**Panic không thoát ra ngoài.** Panic trong activity, compensator, code
workflow, coroutine và giải mã signal đều được recover và chuyển thành lỗi
(`ErrPanic`) (GW-10); activity panic được coi là không chắc chắn. Panic
trong hook làm rollback transaction của nó (việc kết thúc được thử lại
sau, hoặc lỗi được trả cho bên gọi `Terminate`). Panic trong observer và tracer được recover và ghi log.

**Signal không bị mất.** Signal gửi tới execution đang chạy được lưu trong
cùng transaction kiểm tra execution còn hoạt động. Nó được giao theo thứ tự
đến, không có giới hạn ngầm (GW-14). Nó còn nguyên qua continue-as-new, kể
cả khi chạy đua với continue-as-new (GW-3). Signal tới instance đã kết thúc
lỗi `ErrNotActive`.

**Compensation chạy tới cùng.**

- Khi execution đã compensate, các compensator chạy theo thứ tự ngược với
  thứ tự có kết quả của activity, từng cái một, mỗi cái cho tới khi thành
  công hoặc policy của nó bỏ cuộc.
- Crash thì compensation chạy tiếp từ chỗ đã dừng. Một compensator có thể
  chạy nhiều lần, nên phải idempotent; chaos test D kiểm tra việc hoàn tác
  có hiệu lực đúng một lần.
- Cancel nhận được trong lúc compensate được chấp nhận nhưng không có tác
  dụng.
- Compensator bỏ cuộc thì execution bị park (`ErrCompensationFailed`) cho
  tới khi gọi `RetryCompensation`.

**Huỷ tới được activity.** Huỷ một workflow, hoặc context của một activity,
sẽ huỷ context của attempt đang chạy thông qua heartbeat (GW-13).

**Lock độc quyền khi đang giữ.** Mỗi key chỉ một execution giữ. Execution có
lock đã hết hạn và bị lấy mất sẽ lỗi `ErrLockLost` trước khi có thể hoàn tất
như thể đã giữ lock suốt.

**Migration chạy đồng thời an toàn.** `flows.Migrate` lấy advisory lock và
áp dụng các migration goose nhúng sẵn đúng một lần (GW-7).

## Engine không bảo đảm

- **Side effect đúng một lần.** Activity chạy ít nhất một lần; khử trùng
  theo `Key` trong module thực hiện hiệu ứng (`idem`).
- **Compensator có gì để hoàn tác.** Với activity không chắc chắn
  (`CompensationUncertain`), dữ liệu compensation có thể là giá trị zero.
- **Thứ tự giữa các instance.** Execution của các instance khác nhau chạy
  đồng thời, không theo thứ tự nào. Lock cho loại trừ lẫn nhau, không xếp
  hàng: bên chờ không được phục vụ theo thứ tự.
- **Giới hạn độ trễ khi có sự cố.**
  - Việc của engine đã crash chạy tiếp khi lease của nó hết hạn
    (`LeaseTTL`, mặc định 30 s), ở lần poll kế tiếp (`PollMax`, mặc định
    1 s).
  - Việc có delay do engine khác commit có thể bị tìm thấy muộn tới
    `PollMax`. Việc đến mà không có notification cũng vậy, ví dụ sau một
    pooler ở transaction mode.
- **Tính tất định của code bạn sửa.** Đổi chuỗi lời gọi của một version
  workflow đã deploy sẽ park các execution đang chạy của nó. Hãy deploy thay
  đổi thành version mới.
- **Execution bị park tự chạy lại.** Deploy code đã sửa không tự làm execution
  bị park vì code chạy lại: hãy gọi `Engine.Resume`. Lối ra còn lại,
  `Terminate`, **không chạy compensation**: các activity đã hoàn tất giữ
  nguyên hiệu ứng.
- **Dừng code không bao giờ nhường.** Workflow bị kẹt được park, nhưng
  goroutine của nó không dừng được: nó chạy tiếp (quay vòng, với vòng lặp
  bận) cho tới khi process thoát. Hãy sửa code và khởi động lại các process.
- **Giới hạn kích thước.** Payload lưu dạng JSON trong cột `bytea`. Input,
  output và signal lớn làm tốn thời gian replay và bộ nhớ; giữ chúng ở mức
  ID.
