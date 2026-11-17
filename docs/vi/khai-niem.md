# Khái niệm

[English](../concepts.md)

go-flows là một thư viện. Ứng dụng nhúng một `Engine`, còn Postgres (schema
`flows`) là trạng thái chung duy nhất: bao nhiêu process cũng có thể chạy
engine trên cùng một database.

## Workflow, execution, instance id

Workflow là một hàm Go, định nghĩa một lần ở cấp package:

```go
var placeOrder = flows.DefineWorkflow("place-order", 1, func(c flows.Ctx, o Order) (Receipt, error) { ... })
```

- **Tên và version được ghi lại.** Mỗi execution lưu tên và version, và chỉ
  engine có đúng tên và version đó mới chạy nó. Engine thiếu version sẽ trả
  task lại để engine khác nhận, ví dụ trong lúc deploy cuốn chiếu.
- **Instance id là khoá idempotency của workflow.** `placeOrder.Start(ctx,
  engine, "order-42", o)` tạo một execution. Start lại cùng id thì nhận về
  handle của execution đó, dù nó đang chạy hay đã kết thúc, cho tới khi
  retention xoá nó. Nếu id thuộc workflow khác thì Start lỗi
  `ErrInstanceConflict`. Với `Options.KeepPurgedIDs`, id đã bị xoá giữ một
  tombstone trong khoảng thời gian đó và Start lỗi `ErrInstancePurged` thay
  vì chạy lại (vận hành: Retention).
- **Một instance có thể có nhiều execution.** `ContinueAsNew` kết thúc
  execution hiện tại và bắt đầu execution kế tiếp với cùng instance id.
  `Handle.Wait` đi theo chuỗi đó và trả kết quả của execution cuối.

Các trạng thái của execution:

| Trạng thái | Ý nghĩa |
| --- | --- |
| `running` | Code workflow đang chạy hoặc đang chờ (activity, timer, signal, child). |
| `compensating` | Workflow lỗi hoặc bị huỷ; các compensator đang chạy. |
| `needs_attention` | Bị park: code sai (replay lệch với history, `ErrNonDeterministic`; hoặc chạy mà không nhường, `ErrStuck`), hoặc một compensator thất bại sau khi hết retry (`ErrCompensationFailed`). Không có gì chạy cho tới khi người vận hành can thiệp: `Engine.Resume` sau khi deploy code đã sửa, `Engine.RetryCompensation` sau khi sửa nguyên nhân của compensation. |
| `completed` | Workflow trả về kết quả. |
| `compensated` | Workflow lỗi, và có ít nhất một compensator đã chạy (các activity đã hoàn tất được hoàn tác). Child được mở lại để phục vụ compensation của cha cũng kết thúc ở đây. |
| `failed` | Workflow lỗi và không có gì để hoàn tác. |
| `canceled` | Workflow bị huỷ và trả về lỗi huỷ; các activity đã hoàn tất (nếu có) được hoàn tác. |
| `terminated` | `Engine.Terminate` dừng nó, không hoàn tác. |
| `continued` | Workflow đã continue-as-new: execution kế tiếp của instance chạy tiếp. |

## Event history và replay

Mỗi quyết định của workflow được ghi thành một event trong `flows.history`.
Đó là: activity được lên lịch và kết quả của nó, timer bắt đầu và kích hoạt,
signal nhận được, child bắt đầu và kết quả của nó, lock được lấy.

Engine không lưu trạng thái Go của workflow. Để chạy tiếp một execution, nó
chạy lại hàm từ đầu và replay history: mỗi lời gọi đã ghi trả về kết quả đã
ghi mà không chạy thật, cho tới khi hàm tới chỗ nó đã dừng. Các lời gọi mới
được ghi lại và thực thi.

Vì vậy code workflow phải **tất định**:

- chỉ rẽ nhánh theo input và theo kết quả nhận qua `flows.Ctx`;
- dùng `flows.Now`, `flows.Rand`, `flows.SideEffect`, `flows.Sleep`,
  `flows.NewTimer` thay cho `time.Now`, số ngẫu nhiên, `time.Sleep`;
- dùng `flows.Go`, `flows.Select`, `flows.WaitGroup` và signal thay cho
  goroutine, `select`, channel và `sync`;
- không làm I/O: I/O thuộc về activity.

`flows.InstanceID` và `flows.ExecutionID` cho workflow đang chạy biết id của
chính nó, giống nhau khi replay, để đưa vào input của activity hay khoá
idempotency: continue-as-new giữ instance id và bắt đầu execution id mới.

Analyzer `flowsvet` (module `analyzer/`) báo các lỗi này.

Khi replay, mỗi lời gọi đã ghi được so với code: loại, tên, hash của tham số,
thời lượng timer. Replay kết thúc trước khi hết history cũng bị coi là lệch.
Khi lệch, execution bị **park** ở `needs_attention` với
`ErrNonDeterministic`. Nó không bao giờ bị fail, vì fail sẽ hoàn tác những
việc đã thực sự xảy ra; khi code khớp với history đã được deploy,
`Engine.Resume` cho nó chạy lại. Muốn đổi chuỗi lời gọi của workflow thì
deploy thành version mới, và giữ định nghĩa cũ cho tới khi các execution của
nó kết thúc. Code workflow chạy mà không nhường cho engine quá
`StuckTimeout` (lời gọi chặn, vòng lặp bận) được trả lại một lần (có thể chỉ
là process bị khựng), và bị park như vậy, với `ErrStuck`, khi lặp lại ở
cùng chỗ. Loại event mà binary
không biết (do version mới hơn ghi) thì execution được trả cho engine khác.

Execution đã replay được cache trong process đã chạy nó (`CacheSize`,
`CacheBytes`), và
task kế tiếp của nó ở lại process đó trong `StickyFor`. Trúng cache thì bỏ
qua replay; trượt thì replay. Kết quả như nhau.

## Activity và key

Activity là lời gọi có side effect, định nghĩa kèm compensator (tuỳ chọn):

```go
var charge = flows.DefineActivity("charge",
	func(ctx context.Context, k flows.Key, o Order) (chargeID string, comp string, err error) { ... },
	func(ctx context.Context, k flows.Key, chargeID string) error { ... }, // compensator, hoặc nil
	flows.WithRetry(flows.Exponential(5, time.Second, time.Minute)),
	flows.WithTimeout(30*time.Second))
```

- **Activity chạy ít nhất một lần.** Crash hoặc mất lease thì engine khác
  chạy lại attempt.
- **Mỗi activity có một `flows.Key`**
  (`instanceID:scheduleID:name:executionID`). Key giống nhau ở mọi attempt
  và mọi lần replay, compensator của nó cũng nhận đúng key đó, nên module
  khử trùng hiệu ứng của nó theo key (package `idem`): hiệu ứng khi đó xảy ra
  đúng một lần. Key có execution id, nên các execution của một chuỗi
  continue-as-new (schedule id bắt đầu lại từ 1) và một instance id được
  dùng lại sau khi retention xoá không bao giờ trùng key. Execution bắt đầu
  trước migration `00016` (v0.1.0) giữ dạng cũ `instanceID:scheduleID:name`
  suốt đời. Hãy coi key là chuỗi mờ, đừng tách nó ra.
- **Retry diễn ra trên activity task** (`RetryPolicy`; mặc định
  `DefaultRetry`: 5 lần, delay từ 1 s nhân đôi tới 1 phút), không nằm trong
  workflow. History chỉ ghi kết quả cuối, nên đổi policy là an toàn.
  - `flows.Permanent(err)` dừng retry.
  - `WithTimeout` giới hạn mỗi attempt.
- **Output, input và dữ liệu compensation là JSON.** Chuỗi phải là UTF-8
  hợp lệ (byte sai bị thay bằng U+FFFD): dữ liệu nhị phân hãy truyền dạng
  `[]byte`. Giữ chúng nhỏ: ID,
  không phải cả tài liệu.

**Thực thi eager.** Khi một workflow task lên lịch activity thuộc queue mà
engine chạy, và engine còn slot trống, activity task được tạo với lease thuộc sẵn engine đó, trong cùng
transaction, và chạy ngay. Khi activity xong, workflow chạy tiếp trong cùng
round trip. Poll chỉ dùng cho khôi phục và cho việc bắt đầu ở nơi khác.

## Compensation (saga)

Khi workflow trả lỗi, hoặc bị huỷ rồi trả lỗi, execution chuyển sang
`compensating`. Engine hoàn tác các activity đã hoàn tất, **cái sau cùng
trước, từng cái một**, với dữ liệu compensation mà mỗi activity đã trả và
`Key` gốc của nó:

- **Dựng từ history.** Compensation không cần hàm workflow: engine nào có
  compensator theo tên cũng chạy được.
- **Activity không chắc chắn cũng được hoàn tác.** Đó là activity crash, quá
  thời gian, panic hoặc bị huỷ giữa chừng, và có thể đã có hiệu lực.
  Compensator thấy `flows.CompensationUncertain(ctx) == true` và phải kiểm
  tra theo key xem có gì để hoàn tác không (`idem.Abort`).
- **Child cũng được hoàn tác.** Child workflow đã hoàn tất được mở lại và tự
  hoàn tác activity của nó, theo thứ tự ngược cùng với activity của cha.
- **Thất bại thì park.** Compensator retry theo `WithCompensateRetry`. Khi
  một compensator bỏ cuộc, execution bị park ở `needs_attention` với
  `ErrCompensationFailed`. `Engine.RetryCompensation` chạy tiếp sau khi đã
  sửa nguyên nhân.
- **Kết thúc thế nào.** Workflow lỗi kết thúc `compensated` nếu có
  compensator đã chạy, ngược lại là `failed`; trong trường hợp đầu, lỗi của
  `Handle.Wait` khớp `ErrCompensated`. Workflow bị huỷ luôn kết thúc
  `canceled` (xem bên dưới). `*RunError` của `Wait` cũng khớp các error kind
  đã ghi.

## Chờ: timer, signal, child

- **Timer là durable.** `flows.Sleep(c, d)` và `flows.NewTimer(c, d)` ghi
  một timer đến hạn theo giờ database. Trong lúc chờ không giữ goroutine hay
  lease; engine nào cũng kích hoạt được.
- **Signal xếp hàng theo thứ tự.** `Engine.Signal(ctx, id, name, payload)`
  lưu signal cho execution đang chạy. Workflow đọc bằng
  `flows.Await[T](c, name, timeout)` hoặc `flows.Signals[T](c, name)`, theo
  thứ tự đến, không mất cái nào. Signal gửi trước khi workflow lắng nghe vẫn
  được giữ. Signal chưa đọc chuyển sang execution kế tiếp khi
  `ContinueAsNew`.
- **Child là workflow được bắt đầu từ một workflow.** `wf.Child(c, id, in)`
  trả về `Future`. Huỷ cha thì huỷ các child đang chạy, và cha kết thúc cũng
  vậy. Huỷ context mà child được bắt đầu với nó thì huỷ child và future
  resolve ngay với `ErrCanceled`; `future.RequestCancel(c)` yêu cầu child
  huỷ và để future chờ kết quả thật của child (xem "Huỷ và terminate").
  Những gì workflow bắt đầu mà không await trong task kết thúc nó:
  - **khi kết thúc** (completed, failed, canceled): cả child lẫn activity
    đều không được chạy;
  - **khi continue-as-new:** child được tạo và chạy tiếp, tách khỏi cha,
    giống các child đã bắt đầu trước đó (không được hoàn tác cùng chuỗi);
    activity thì không được chạy.

  Mọi thứ không được chạy đều được ghi warning, và `flowtest` làm test
  fail.
- **`flows.Select` chờ case đầu tiên sẵn sàng.** Các case là future và
  signal channel, xét theo thứ tự (tất định), có thể kèm case mặc định.

## Huỷ và terminate

- **`Engine.Cancel` yêu cầu workflow dừng.** Nó huỷ context gốc của
  workflow, kéo theo context của activity, timer và child.
  - Workflow tự quyết định làm gì. Nó có thể dọn dẹp trong
    `flows.Disconnected(c)`.
  - Nếu workflow trả về lỗi huỷ (`ErrCanceled`, như các lời gọi bị huỷ trả
    về, có thể được bọc), activity được hoàn tác và nó kết thúc `canceled`;
    lỗi của `Handle.Wait` khớp `ErrCanceled`.
  - Nếu trả lỗi khác, nó kết thúc như mọi lần lỗi: `compensated` hoặc
    `failed`. Nếu trả kết quả, nó kết thúc `completed`.
- **Cha có thể yêu cầu một child huỷ mà vẫn nhận kết quả của nó.**
  `future.RequestCancel(c)` huỷ context gốc của child (child tự quyết định
  như trên: dừng, hoặc làm nốt việc trên `flows.Disconnected`), và future
  chỉ resolve khi child kết thúc: với output nếu nó vẫn hoàn tất, với lỗi
  của nó (khớp `ErrCanceled` nếu nó chấp nhận huỷ) trong các trường hợp
  khác.
  - Yêu cầu là một command ghi trong history của cha và được giao tới child
    trong cùng checkpoint; replay phải đưa ra nó ở cùng chỗ, nếu không
    execution bị park vì không tất định.
  - Child bắt đầu trong cùng task (chưa checkpoint) thì không được chạy;
    future resolve với `ErrCanceled`. Yêu cầu lặp lại, hoặc sau khi child
    đã kết thúc, không làm gì.
  - Child hoàn tất sau yêu cầu là một child đã hoàn tất: nó được hoàn tác
    nếu sau đó cha lỗi.
  - Huỷ cha (hoặc context của child) vẫn resolve future ngay với
    `ErrCanceled`.
- **`Engine.Terminate` kết thúc execution ngay**, không chạy code workflow
  hay compensation. Dùng cho execution bị kẹt.

## Lock

`flows.Lock(c, key, ttl)` lấy một business lock cho phần còn lại của
execution (`ttl` được nâng lên ít nhất bằng `LeaseTTL` của engine):

- **Nhả:** khi kết thúc, hoặc khi continue-as-new.
- **Được giữ sống khi execution làm việc:** khi nó chạy một task hoặc một
  activity của nó đang chạy.
- **Được giữ thêm tối đa `Options.LockHold` (mặc định 1h) sau `ttl`** khi một
  activity của nó đang chờ (trong queue, chờ retry, hoặc chờ engine khác sau
  crash), hoặc khi có event chờ task kế tiếp của nó (kết quả activity, signal,
  timer đã tới hạn) mà chưa engine nào claim.
- **Hết hạn sau `ttl` trong các trường hợp khác:** khi execution ngủ, chờ
  hoặc đã chết. Execution khác khi đó có thể lấy lock, và execution đầu lỗi
  `ErrLockLost` ở task kế tiếp.
- **Chờ:** `flows.LockWait(policy)` chờ lock bận thay vì lỗi `ErrLockHeld`.
  Bên chờ được xếp hàng và phục vụ theo thứ tự yêu cầu; khi lock được nhả,
  bên kế tiếp được đánh thức ngay.

## Lease và fencing

- **Owner và lease version.** Engine đang chạy task của một execution (hoặc
  một activity task) giữ lease của nó: `owner_id`, `lease_version`,
  `locked_until`.
- **Gia hạn.** Heartbeat gia hạn mọi lease của một engine mỗi `LeaseTTL/3`,
  trong một round trip.
- **Fencing.** Mọi lần ghi đều khoá dòng và kiểm tra owner cùng version.
  Engine đã mất lease (bị tạm dừng, mất kết nối mạng) không ghi được nữa: 0
  dòng nghĩa là `ErrLeaseLost`, không bao giờ là thành công âm thầm.
- **Giờ database.** Mọi phép tính lease, timer và retry dùng `now()` của
  Postgres, không bao giờ dùng đồng hồ của process.

## Hook, observer, tracing

- **`Options.Hook`** (`RunHook`) chạy trong transaction ghi lại việc kết
  thúc hoặc park. Những gì nó ghi commit cùng trạng thái, nên đây là chỗ cho
  một transactional outbox. Hook thuộc về engine ghi việc kết thúc: hãy đặt
  cùng một hook cho mọi engine, kể cả engine chỉ làm client (`Terminate` từ
  engine chỉ làm client chạy hook của chính engine đó).
- **`Options.Observer`** nhận các attempt của activity và việc kết thúc,
  sau khi chúng xảy ra, để làm metric.
- **`Options.Tracer`** bọc lần start, workflow task và attempt trong span;
  trace context lấy lúc start được lưu cùng execution, nên mọi span của nó
  nằm trong trace của bên gọi. `otelflows`
  cài đặt cả tracer lẫn observer bằng OpenTelemetry.
- Không đặt thì không tốn gì.
