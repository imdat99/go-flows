# Modular monolith trên go-flows

[English](README.md)

Bản mẫu cho một hệ thống thật: nhiều bounded context trong một binary Go,
phối hợp bằng workflow durable, deploy thành một process hoặc tách thành
nhiều process mà không sửa code. Hãy chép bố cục và các quy tắc; thay
gateway, hãng vận chuyển, nhà cung cấp và mail server giả bằng của bạn.

```sh
make up                                   # Postgres trên :5433
go run ./examples/modulith/cmd/modulith   # mọi module, HTTP API trên :8080

curl -XPOST localhost:8080/orders -d '{"id":"42","email":"a@example.com","address":"Hanoi",
  "amount":1299,"lines":[{"sku":"sku-apple","qty":2}]}'
curl localhost:8080/orders/42                                    # trạng thái, thanh toán, giao hàng
curl -XPOST localhost:8080/shipments/42/pickup -d '{"tracking":"TRK1"}'   # webhook của hãng vận chuyển
curl -XPOST localhost:8080/orders/43/cancel -d '{"reason":"changed my mind"}'
curl -XPOST localhost:8080/inventory/sku-pear/adjust -d '{"request_id":"r1","delta":5,"reason":"count"}'
```

Test: `go test -race ./examples/modulith/...` (phần lớn cần `make up`).

## Kiến trúc: ba tầng, một quy tắc phụ thuộc

```
          cmd/modulith  ──>  internal/app  (composition root)
                                │  dựng service, nối chúng vào,
                                │  migration, engine, hook, HTTP API
                 ┌──────────────┴───────────────┐
                 v                              v
      processes/  (điều phối)      ────>  modules/  (nghiệp vụ)
      code duy nhất import go-flows       orders, payments, inventory,
      workflow, activity, signal,         shipping, notifications
      queue, compensation, RunHook        không biết đến engine
                 │                              │
                 v                              v
            go-flows                 internal/platform (schema, outbox)
```

- **Module nghiệp vụ** (`modules/<tên>/`) là Go và SQL thuần: kiểu dữ
  liệu, một `Service` được dựng với dependency của nó
  (`NewService(pool, logger)`, không có state cấp package), SQL của
  repository, migration và schema riêng, và lỗi nghiệp vụ. Mỗi module tự
  phân loại lỗi của mình: `IsRejection(err)` đúng với một từ chối nghiệp
  vụ (bị từ chối thanh toán, hết hàng, xung đột...) mà retry không đổi
  được; mọi lỗi khác là tạm thời. Mọi hiệu ứng nhận một idempotency key do
  bên gọi đưa (`Authorize(ctx, key, ...)`, `Reserve(ctx, key, ...)`), lưu
  trong bảng của module, cùng các fence giúp hoàn tác theo key an toàn.
  Module không import package nào của go-flows và không import module
  khác; nó có thể phát event nghiệp vụ qua outbox dùng chung (orders làm
  vậy).
- **Process** (`processes/<tên>/`) là tầng điều phối và là code duy nhất
  import `flows`: workflow, activity, signal, queue, compensation, logic
  pivot, RunHook. Activity là một adapter mỏng: gọi service của module với
  `flows.Key` làm idempotency key và biến một từ chối thành
  `flows.Permanent` (`adapt.Error`).
- **Composition root** (`internal/app`, `cmd/modulith`) dựng các service,
  giao chúng cho các process (`Configure`), chạy migration, dựng engine và
  phục vụ HTTP API. Handler HTTP truy vấn thẳng service của module, và đi
  qua process để khởi hoặc gửi signal cho workflow.

**Quy tắc được kiểm tra.** `architecture_test.go` chạy `go list` và fail
nếu một package dưới `modules/` phụ thuộc, kể cả gián tiếp, vào bất kỳ
package nào của go-flows (`flows`, `idem`, `flowtest`, `otelflows`,
`internal/...`), vào module khác, vào `processes/` hay `internal/app`; test
của module chỉ được import thêm `internal/testdb` của repository. Nó cũng
kiểm tra `internal/platform` không dính tới engine.

## Bố cục

```
examples/modulith/
  architecture_test.go          quy tắc phụ thuộc (go list)
  cmd/modulith/main.go          flag/env, pgxpool, signal, tắt êm
  internal/app/                 composition root: service, Configure, migration, engine, hook, HTTP API
  internal/platform/
    schema/                     migration goose theo module, version trong <schema>.goose_db_version
    outbox/                     transactional outbox: Stage, Relay, Bus, LocalBus
  modules/                      nghiệp vụ: không go-flows, không module khác
    orders/                     đơn hàng, trạng thái, Event và topic (outbox)
    payments/                   authorize/void, capture/refund, theo key, có tombstone
    inventory/                  tồn kho, giữ hàng theo key (requests.go), điều chỉnh, cờ ngừng kinh doanh
    shipping/                   nhãn, quyết định lấy hàng trên dòng shipment
    notifications/              gửi tin, mỗi event id một lần
  processes/                    điều phối: nơi duy nhất import go-flows
    internal/adapt/             chỗ giữ service của một process, chuyển đổi lỗi
    fulfilment/                 saga đơn hàng, RunHook, Place/Cancel/Status         queue orders
    payment/                    activity Authorize, Capture (void, refund)          queue payments
    reservation/                child workflow Reserve (trả hàng theo tên)          queue inventory
    replenishment/              vòng lặp mỗi SKU (ContinueAsNew, Lock), Adjust      queue inventory
    shipment/                   child workflow Ship, pivot, WakeOnPickup, Cancel    queue shipping
    notify/                     event orders -> notifications, mỗi event một workflow   queue notifications
```

| Module | Schema | API của service | Process trên queue của nó |
| --- | --- | --- | --- |
| orders | `orders` | `Validate`, `Placed`, `Record`, `End`, `Get`; `Event`, `Topic*`; `ErrInvalidOrder`, `ErrOrderConflict` | fulfilment (`orders`) |
| payments | `payments` | `Authorize`, `Void`, `Capture`, `Refund`, `Find`; `ErrDeclined`, `ErrInvalidAmount`, `ErrVoided`, `ErrNotCapturable` | payment (`payments`) |
| inventory | `inventory` | `Reserve`, `Release`, `SettleReservation`, `Stock`, `Level`, `PlacePurchaseOrder`, `ApplyAdjustment`, `Applied`, `Discontinue`, `Replenishable`, `PurgeRequests`; `ErrOutOfStock`, `ErrUnknownSKU`, `ErrRequestConflict`, `ErrAborted` | reservation, replenishment (`inventory`) |
| shipping | `shipping` | `CreateLabel`, `CancelLabel`, `ConfirmPickup`, `Decide`, `MarkPickedUp`, `Find`; `ErrUnserviceable`, `ErrShipmentExists`, `ErrLabelCanceled`, `ErrPickupRefused`, `ErrNoShipment` | shipment (`shipping`) |
| notifications | `notifications` | `Deliver`, `Sent` | notify (`notifications`) |

Module nào cũng export thêm `Migrate`, `NewService` và `IsRejection`
(notifications không có từ chối nào). Process nào cũng export `Queue` và
`Configure`.

## Dependency tới activity bằng cách nào

Định nghĩa của go-flows là giá trị cấp package trong một registry toàn
cục: hàm activity không nhận tham số constructor được. Vì vậy mỗi package
process có đúng một điểm inject tường minh, và đó là state cấp package có
thể thay đổi duy nhất của ví dụ:

```go
// processes/payment
type Services struct{ Payments *payments.Service }

var deps = adapt.NewRef[Services]("payment") // con trỏ atomic, gán một lần

func Configure(s Services) error { /* kiểm tra */ deps.Set(s); return nil }

func authorize(ctx context.Context, k flows.Key, r AuthorizeRequest) (Authorization, string, error) {
	s, err := deps.Get() // adapt.ErrNotConfigured, được retry: không bao giờ compensate đơn thật
	if err != nil {
		return Authorization{}, "", err
	}
	id, err := s.Payments.Authorize(ctx, string(k), r.OrderID, r.Amount)
	if err != nil {
		return Authorization{}, "", adapt.Error(err, payments.IsRejection) // từ chối -> Permanent
	}
	return Authorization{PaymentID: id}, id, nil
}
```

`app.New` dựng năm service một lần, rồi gọi `Configure` của mọi process,
và từ chối khởi động nếu có lỗi: lỗi nối dây hỏng ngay lúc khởi động. Mọi
package process được cấu hình trong mọi process, dù module của nó có chạy
ở đó hay không, vì HTTP API của process nào cũng có thể khởi workflow
(`fulfilment.Place`, `replenishment.Adjust`). Các module nghiệp vụ nhận
dependency qua constructor và được test không cần engine.

## Một đơn hàng chạy thế nào

```
HTTP POST /orders ──> fulfilment.Place ──> workflow orders.fulfil (queue orders)
   1. orders.record                activity      orders        Orders.Record
   2. payments.authorize           activity      payments      Payments.Authorize    hoàn tác: Void
   3. inventory.reserve            child         inventory     Inventory.Reserve     hoàn tác: Release (từng dòng)
   4. payments.capture             activity      payments      Payments.Capture      hoàn tác: Refund
   5. shipping.ship                child         shipping      Shipping.CreateLabel  hoàn tác: CancelLabel
        tạo nhãn, rồi Select { signal picked-up | signal shipping.cancel | timer PickupWithin }
        rồi Shipping.Decide trên dòng shipment (quyết định đầu tiên thắng, hãng hay workflow)
        đã lấy hàng: Shipping.MarkPickedUp, retry đến khi thành công
      đơn hàng: Select { giao hàng xong | signal orders.cancel -> chuyển tiếp cho shipment }
                rồi chờ kết quả thật của shipment
 kết thúc ──> RunHook (cùng transaction): Orders.End -> orders.orders.state + event outbox
          ──> relay ──> bus ──> consumer notify ──> workflow notifications.send ──> Notifications.Deliver
```

- **Compensation đi qua các module theo tên.** Khi hãng vận chuyển không
  đến (`shipment.ErrPickupTimeout`) hoặc khách huỷ, child shipment huỷ
  nhãn của nó, rồi engine refund khoản capture, mở lại child
  `inventory.reserve` đã xong để trả lại các dòng hàng, và void
  authorization. Mỗi compensator chạy trên queue của module nó, tức là
  trên các process chạy module đó, và gọi service của module đó theo key.
  Không cần đến hàm của saga.
- **Pivot là một dòng dữ liệu, không phải một signal.** Việc hãng đã có
  kiện hàng hay chưa được module shipping quyết định một lần, trên
  `shipping.shipments`, dưới khoá dòng: dòng rời `label_created` sang
  `pickup_confirmed` hoặc `canceled`, ai đến trước thắng. Nghiệp vụ quyết
  định; engine chỉ được đánh thức.
  - Webhook của hãng (`POST /shipments/{order}/pickup`) gọi
    `shipping.Service.ConfirmPickup`, chuyển dòng sang `pickup_confirmed`;
    chỉ sau đó handler mới đánh thức workflow (`shipment.WakeOnPickup`).
    202 nghĩa là việc lấy hàng đã được ghi nhận vĩnh viễn: đơn sẽ hoàn
    tất. Nếu shipment đã bị huỷ trước (khách huỷ hoặc hết hạn), webhook
    nhận 409 `ErrPickupRefused` (trả lại kiện hàng); trước khi có nhãn,
    404 `ErrNoShipment` (thử lại). Không có chuyện nhận lấy hàng rồi bỏ
    rơi.
  - Workflow, được đánh thức bởi signal đầu tiên hoặc timer, xin kết quả
    nó muốn (`Shipping.Decide`) và nhận trạng thái của dòng. Signal chỉ
    để đánh thức: nếu signal của hãng bị mất, timer đánh thức workflow và
    dòng vẫn ghi `pickup_confirmed`.
  - Từ lúc quyết định, shipment không thể fail vào saga: activity decide
    và mark-picked-up chạy trên `flows.Disconnected` và được retry đến khi
    thành công (từng đợt attempt, ngủ durable 5 phút giữa các đợt; các
    attempt lỗi tới observer là cảnh báo). Nếu database của shipping chết
    hàng giờ sau khi lấy hàng, đơn hoàn tất muộn; nó không bao giờ bị hoàn
    tác. Bước ghi đã lấy hàng không có compensator, và `CancelLabel` chỉ
    huỷ dòng còn `label_created`.
  - Khi đơn đã kết thúc, `fulfilment.Cancel` trả về `ErrNotActive` (HTTP
    409).
- **Ai quyết định việc huỷ.** `fulfilment.Cancel` gửi signal cho đơn
  hàng. Gửi trong bước 1 đến 4 thì signal chờ trong channel và đơn tự
  dừng trước khi capture hoặc trước khi khởi shipment. Trong bước 5 chỉ
  shipment biết việc lấy hàng có đến trước hay không, nên đơn không huỷ
  child (trong go-flows, huỷ một child làm future của nó trả về
  `ErrCanceled` ngay, không có kết quả thật). Đơn chuyển tiếp lệnh huỷ cho
  shipment (một activity gọi `shipment.Cancel`, vì workflow không gửi
  signal cho workflow khác được) và chờ kết quả của shipment: bị huỷ thì
  đơn được hoàn tác; đã lấy hàng thì đơn hoàn tất và lệnh huỷ bị bỏ qua.
- **Test cho các đường này.**
  - Tầng process (flowtest, activity được mock): `TestFulfilShipped`,
    `TestFulfilCanceledByCustomer`, `TestFulfilPickupTimeout`,
    `TestFulfilOutOfStock`, `TestFulfilCancelAfterPickup` (lệnh huỷ tới
    đơn sau khi việc lấy hàng đã tới shipment),
    `TestFulfilCancelBeforeShipping`, `TestFulfilCancelDuringCapture`,
    `TestFulfilPickupRecordFailsForLong`, `TestShipPickupBeatsCancel`,
    `TestShipConfirmedPickupBeatsCancelSignal`,
    `TestShipPickupRecordedDespiteFailures`, `TestShipUnserviceable`,
    `TestReserveReleasesOnFailure`, các test vòng bổ sung hàng,
    `TestAdjustWaitsForTheLock`, `TestSendRetriesTheMailServer`. Chính các
    adapter chạy trên module thật trong `TestAdaptersCompensateByKey`,
    `TestDeclineIsPermanent` (payment) và `TestAdaptersOnTheModule`
    (reservation).
  - Nghiệp vụ (Postgres, không engine): `TestPickupAfterCancelIsRefused`,
    `TestCancelAfterPickupKeepsThePickup`, `TestDecisionRace` (hãng và
    workflow quyết định cùng lúc, 20 lần: luôn thống nhất),
    `TestLateLabelIsFenced`, `TestLateCaptureIsRefundedByVoid`,
    `TestLateAuthorizationIsFenced`, `TestSettleReservation`,
    `TestAdjustmentsByRequestID`, `TestRecordAndEnd`.
  - Test tích hợp chạy hai engine (front: orders, notifications; back:
    payments, inventory, shipping) và kiểm tra engine nào chạy workflow và
    activity nào. Nó xác nhận lấy hàng rồi huỷ qua API (đơn hoàn tất dù
    thời điểm thế nào, vì 202 của việc lấy hàng là cuối cùng), và gửi xác
    nhận lấy hàng sau khi huỷ (409).
- **`Engine.Cancel` là công cụ của người vận hành, không phải lệnh huỷ
  của khách.** Nó huỷ context của đơn: future của child shipment trả về
  `ErrCanceled` ngay, và đơn compensate mà không chờ kết quả của shipment.
  Sau khi hãng đã lấy hàng, điều đó nghĩa là hoàn tiền và trả hàng về kho
  trong khi kiện hàng vẫn được giao (bản thân shipment vẫn hoàn tất). Chỉ
  dùng cho đơn buộc phải dừng bằng mọi giá, và đối soát bằng tay.

Các tính năng khác, nằm ở đâu:

| Tính năng | Ở đâu |
| --- | --- |
| Hiệu ứng idempotent theo key của bên gọi (cột key unique, fence bằng tombstone) | `modules/payments`, `modules/shipping` |
| Hiệu ứng đúng một lần trong transaction của module, hoàn tác không chắc chắn có fence key | `modules/inventory/requests.go`, `SettleReservation` |
| Lỗi nghiệp vụ do module phân loại, adapter chuyển thành `flows.Permanent` | `modules/*/IsRejection`, `processes/internal/adapt` |
| Child workflow giữa các module, được compensate bằng cách mở lại | `processes/fulfilment`, `processes/reservation`, `processes/shipment` |
| `Select` giữa hai signal và một timer timeout; quyết định một lần trong một dòng của bảng; các bước retry đến khi thành công trên `Disconnected` | `processes/shipment`, `modules/shipping` |
| `Select` giữa child và signal; chuyển tiếp signal cho child qua một activity | `processes/fulfilment` |
| Vòng lặp dài hạn với `ContinueAsNew` và signal dừng | `processes/replenishment` (`cycle`) |
| `flows.Lock` với `LockWait` trên khoá nghiệp vụ | `processes/replenishment` (`checkAndOrder`, `adjustStock`) |
| RunHook kết thúc đơn (read model và event) trong transaction kết thúc | `processes/fulfilment` (`Hook`), `modules/orders` (`End`) |
| Transactional outbox, relay với `SKIP LOCKED`, thứ tự theo key, backoff | `internal/platform/outbox` |
| Consumer chuyển event của module này cho module khác, khử trùng lặp bằng mỗi event id một workflow | `processes/notify` |
| Queue theo module, một binary tách bằng flag | `internal/app/app.go`, `cmd/modulith/main.go` |
| Observer (log; cảnh báo khi `needs_attention`) | `internal/app/app.go` |
| Quy tắc phụ thuộc, kiểm tra bằng `go list` | `architecture_test.go` |
| Hai engine, mỗi engine một nhóm module, trên Postgres thật | `internal/app/app_test.go` |

## Quy tắc cho cả đội

### Ranh giới module

1. Mỗi module sở hữu một schema Postgres. Chỉ code của nó đọc hoặc ghi
   schema đó. Code khác gọi service của nó; không ai khác query bảng của
   nó, kể cả chỉ đọc. (Lớp HTTP ghép view của nhiều module qua service
   của chúng: `internal/app/http.go`.)
2. Module không import engine, cũng không import module khác. Dữ liệu
   qua lại giữa các module đi qua tầng process, nơi chuyển đổi (saga
   chuyển payment id từ payments sang payments, process notify biến một
   `orders.Event` thành `notifications.Message`).
3. Việc liên module phải cùng xảy ra hoặc cùng được hoàn tác là một
   process: một **activity** cho một bước đơn (payment), một **child
   workflow** khi bước đó tự có chờ đợi hoặc nhiều bước (reservation,
   shipment). Đừng gọi service của module thứ hai từ trong một activity để
   nối các hiệu ứng: engine sẽ không biết cách hoàn tác chúng.
4. Sự kiện mà module khác phản ứng theo là **event** qua outbox (orders
   sang notifications). Bên phát sở hữu kiểu event (`orders.Event`): thêm
   trường, không bao giờ đổi hoặc xoá trường.
5. Instance id là toàn cục giữa mọi workflow: thêm tiền tố module
   (`orders-42`, `shipping-42`, `inventory-reserve-42`). Mỗi process
   export một hàm dựng id của nó.
6. Error kind (`flows.RegisterErrorKind`) cũng toàn cục: thêm tiền tố
   module (`payments_declined`). Các package process đăng ký chúng, cho
   sentinel của module và của chính chúng. Chúng giữ cho `errors.Is` đúng
   với lỗi đọc lại từ history, ở bất kỳ process nào.

### Thêm một module

1. `modules/<tên>/`: kiểu dữ liệu, một `Service` với `NewService`, SQL,
   `migrations/*.sql` (được `schema.Migrate` áp vào schema `<tên>`), lỗi
   và `IsRejection`. Mọi hiệu ứng nhận một idempotency key và lưu nó (cột
   unique, hoặc `once`/`abort` như inventory); mọi thao tác hoàn tác chạy
   theo key và để lại tombstone. Test trên Postgres, không cần engine.
2. `processes/<process>/`: `Queue` (tên module), `Services`, `Configure`,
   các activity dạng adapter (`flows.Key` làm key,
   `adapt.Error(err, <tên>.IsRejection)`), các workflow, các error kind.
   Test bằng flowtest với activity được mock.
3. `internal/app`: thêm module vào `modules` (tên, queue, `Migrate`), dựng
   service của nó trong `newServices`, gọi `Configure` của process trong
   `New`, và thêm handler nếu nó có API.
4. `architecture_test.go` tự bao phủ nó, không cần sửa.

### Đổi engine

Chỉ `processes/` và `internal/app` biết go-flows. Thay engine (một thư
viện workflow khác, saga dựa trên message, job thường) là viết lại các
package process và phần nối dây; các module, bảng, migration và test của
chúng giữ nguyên. Hợp đồng của module với bất kỳ bộ điều phối nào là: một
idempotency key cho mỗi hiệu ứng, hoàn tác theo key, và `IsRejection` để
phân biệt "đừng retry" với "retry".

### Tính tất định (code workflow)

Hàm workflow được replay. Bên trong nó:

- chỉ rẽ nhánh theo input và theo kết quả các lời gọi `flows.*`;
- không I/O, không đọc đồng hồ, không ngẫu nhiên, không goroutine, không
  đọc cấu hình hay biến package có thể thay đổi, không gọi service của
  module (đó là việc của activity): đưa các giá trị đó vào input của
  workflow khi khởi động (`fulfilment.Order.PickupWithin`,
  `replenishment.State.Every`);
- dùng `flows.Now`, `flows.Sleep`, `flows.NewTimer`, `flows.Go`,
  `flows.Select`, `flows.SideEffect`.

Chạy `flowsvet` (module `analyzer/`) trong CI để bắt lỗi.

### Activity

- Chạy **ít nhất một lần**. Truyền `flows.Key` cho module làm
  idempotency key; module tạo hiệu ứng một lần cho mỗi key: cột key unique
  (`payments.payments.auth_key`) hoặc bản ghi riêng các key đã xử lý trong
  cùng transaction (`inventory.processed_requests`).
- Compensator cũng phải idempotent, và phải xử lý
  `flows.CompensationUncertain(ctx)`: activity có thể đã hoặc chưa có hiệu
  ứng. Hoàn tác theo key, và fence key để một attempt muộn không thể có
  hiệu lực sau đó (`inventory.Service.SettleReservation`, dòng tombstone
  của payments và shipping).
- Chuyển đổi các từ chối của module bằng `adapt.Error`: chúng thành
  `flows.Permanent` (không retry; saga compensate). Mọi lỗi khác được
  retry.
- Giữ input và output nhỏ (id, không phải tài liệu).

### Đăng ký trong mọi binary

Định nghĩa là giá trị cấp package trong một registry toàn cục: process
chỉ chạy những gì binary của nó định nghĩa. Build một binary import mọi
package process (`internal/app` làm việc này) và dùng nó cho mọi
deployment. Cài cùng `RunHook` trên mọi engine, kể cả engine chỉ làm
client: kết thúc được ghi bởi engine chạy task cuối cùng.

### Queue và tách deployment

Các process của mỗi module chạy workflow và activity trên queue của module
(`WithQueue`, `WithWorkflowQueue`). `-modules` chọn các queue mà process
phục vụ; process nào cũng có thể khởi workflow, gửi signal và phục vụ API,
vì những việc đó chỉ ghi vào Postgres.

```sh
# một process (monolith)
go run ./examples/modulith/cmd/modulith

# tách: cùng binary, hai deployment
go run ./examples/modulith/cmd/modulith -modules=orders,notifications -http=:8080 -id=front-1
go run ./examples/modulith/cmd/modulith -modules=payments,inventory,shipping -http= -id=back-1
```

Scale từng nhóm độc lập bằng cách chạy thêm replica. Quy tắc:

- mỗi queue phải có ít nhất một process đang chạy phục vụ: activity có
  queue không ai phục vụ sẽ chờ mãi (workflow của nó không fail);
- chuyển một module bằng cách deploy nhóm mới trước, rồi bỏ module khỏi
  nhóm cũ; việc đang dở tiếp tục trên process nào phục vụ queue đó;
- `LocalBus` trong process buộc relay vào process notifications. Với
  broker thật, relay chạy ở đâu cũng được và consumer subscribe vào
  broker;
- muốn tách một module sang database riêng về sau thì schema, migration
  và service của nó đi theo (service nhận pool kia); workflow vẫn cần
  schema `flows` dùng chung.

### Đánh version workflow

Một execution đang chạy replay bằng code đăng ký dưới tên và version của
nó. Đổi thứ tự các lời gọi `flows.*` của một version đã deploy sẽ park các
execution đang chạy (`needs_attention`, `ErrNonDeterministic`). Muốn đổi,
thêm version mới bên cạnh version cũ:

```go
var (
	// Version 1: giữ nguyên cho đến khi các execution của nó kết thúc.
	fulfilV1 = flows.DefineWorkflow("orders.fulfil", 1, fulfilOrderV1, flows.WithWorkflowQueue(Queue))
	// Version 2 thêm bước kiểm tra gian lận trước capture. Place khởi nó.
	fulfil = flows.DefineWorkflow("orders.fulfil", 2, fulfilOrder, flows.WithWorkflowQueue(Queue))
)
```

1. Chép hàm cũ thành `fulfilOrderV1` (đóng băng), sửa `fulfilOrder`, tăng
   version. Lần khởi mới dùng version 2; execution version 1 đang chạy vẫn
   replay bằng version 1.
2. Activity đổi kiểu input hoặc output thì lấy tên mới
   (`payments.capture.v2`); giữ định nghĩa cũ khi version 1 còn có thể gọi
   hoặc compensate nó. Kiểu của module dùng làm input hay output của
   activity (`shipping.Decision`) cũng vậy.
3. Timeout và phần cài đặt activity (cùng kiểu, cùng ý nghĩa) đổi thoải
   mái: chúng không nằm trong history, mọi thứ bên trong module cũng
   không. Retry policy cũng vậy, trừ một điểm: giới hạn số attempt của
   activity được ghi vào task khi nó được lên lịch, nên giới hạn mới chỉ
   áp dụng cho activity lên lịch sau khi deploy, không cho những activity
   đang chờ hay đang retry.
4. Xoá version 1 khi không còn gì chạy nó:

   ```sql
   SELECT version, status, count(*) FROM flows.execution
   WHERE workflow = 'orders.fulfil' AND status IN ('running', 'compensating', 'needs_attention')
   GROUP BY 1, 2;
   ```

5. Trong rolling deploy, engine cũ không biết version 2 sẽ trả các
   execution đó cho engine mới: deploy version mới ở mọi nơi trước khi dựa
   vào nó.

### Checklist vận hành

- `flows.Migrate` và `Migrate` của mọi module chạy lúc khởi động
  (advisory lock giúp khởi động đồng thời an toàn); pool `MaxConns >= 3`.
- `EngineID` duy nhất mỗi process (mặc định là ổn); `Stop(ctx)` có
  deadline khi nhận SIGTERM (`cmd/modulith`), ngắn hơn thời gian kill của
  orchestrator.
- Cảnh báo khi có execution `needs_attention` (observer log ở mức error),
  khi số `compensating` tăng, và khi dòng outbox có `attempts` cao.
- Job định kỳ trên một process: `engine.PurgeEnded`,
  `inventory.Service.PurgeRequests` cho `inventory.processed_requests`, xoá
  dòng outbox đã gửi lâu. Purge giải phóng các instance id nó xoá: thứ gì
  phải giữ idempotent sau đó thì được kiểm tra trong bảng của module (xem
  "Điểm vướng của API").
- Giới hạn kích thước request ở API (`maxBody`): engine lưu input và
  signal nguyên như nhận.
- Execution bị park: sửa nguyên nhân, rồi `engine.Resume` (code) hoặc
  `engine.RetryCompensation` (một compensator bỏ cuộc).
- Xem [docs/vi/van-hanh.md](../../docs/vi/van-hanh.md) về tinh chỉnh,
  pooler, autovacuum và sự cố.

## Điểm vướng của API và giới hạn đã biết

Những gì go-flows làm khó ở đây, và cách ví dụ xoay xở:

- **Định nghĩa là toàn cục, nên dependency cần chỗ giữ.** Activity là giá
  trị cấp package trong registry toàn cục và không nhận tham số
  constructor được. Mỗi package process có đúng một điểm inject tường
  minh (`Configure`, một `adapt.Ref` được composition root gán một lần):
  state cấp package có thể thay đổi duy nhất của ví dụ. Hai engine trong
  một process dùng chung định nghĩa và các chỗ giữ này (test tích hợp
  cấu hình hai lần; lần thứ hai thắng, ở đó không có gì khác đi).
- **Không có "huỷ child và nhận kết quả thật của nó".** Huỷ context của
  child làm future của nó trả về `ErrCanceled` ngay; child có thể vẫn đã
  hoàn tất (ở đây: đã được lấy hàng). Gửi lệnh huỷ cho child dưới dạng
  signal và để child quyết định (`processes/fulfilment`).
- **Không có signal từ workflow sang workflow.** Workflow không gọi được
  `Engine.Signal`. Đơn hàng chuyển tiếp lệnh huỷ bằng một activity
  (`orders.forward-cancel`), activity này cần một engine trong service của
  process và chạy ít nhất một lần (shipment bỏ qua các bản lặp).
- **Idempotency theo instance id hết hạn khi retention xoá.** `Start` với
  id đã dùng chỉ trả về execution cũ cho đến khi `PurgeEnded` xoá nó; sau
  đó cùng id sẽ khởi execution mới. Giữ idempotency lâu dài trong bảng
  của module: `fulfilment.Place` hỏi `orders.Service.Placed` (nội dung
  khác dưới id đã dùng: `ErrOrderConflict`, 409), `replenishment.Adjust`
  hỏi `inventory.Service.Applied` (`inventory.adjustments.request_id`,
  `ErrRequestConflict`), và SKU ngừng kinh doanh là một cột, nên vòng bổ
  sung hàng của nó không bị khởi lại sau purge. Hai đơn khác nhau tranh
  cùng một id mới đều nhận 202; nội dung của đơn thứ hai bị bỏ.
- **`idem` sẽ buộc nghiệp vụ vào go-flows.** Package `idem` cho hiệu ứng
  đúng một lần theo `flows.Key`, nhưng module import nó sẽ phụ thuộc
  go-flows. Module inventory giữ một bản nhỏ của riêng nó (`requests.go`:
  chiếm key, tạo hiệu ứng, lưu kết quả trong một transaction; xử lý key
  không chắc chắn bằng tombstone) trên cùng cấu trúc bảng, do migration
  đầu tiên của nó tạo.
- **flowtest không mock được child workflow.** Test `orders.fulfil` phải
  mock activity của child reservation và shipment, nên các process đó
  export chúng chỉ vì lý do này.
- **`Workflow.Start` không tham gia transaction của bên gọi được.** Dòng
  đơn hàng được activity đầu tiên của saga ghi, không phải `Place`.
- **Mỗi engine một `RunHook`, và `RunInfo` không có input hay output.**
  Composition root nối hook của các process; hook của fulfilment kết thúc
  đơn qua `orders.Service.End` (đọc email từ bảng của nó), và nhận ra lệnh
  huỷ của khách bằng cách tách `RunInfo.ErrorKinds`.
- **Signal không phải quyết định.** Signal được nhận khi execution đang
  chạy, đang compensate hay bị park, dù có được đọc hay không: workflow đã
  chọn nhánh khác sẽ không bao giờ đọc nó, còn bên gửi nhận nil. Signal
  gửi trước khi instance tồn tại thì lỗi (`ErrNotFound`). Điều gì bên gửi
  cần biết là đã được nhận (việc lấy hàng của hãng) thì được quyết định
  trong bảng của module trước (`shipping.Service.ConfirmPickup`), signal
  chỉ để đánh thức workflow.
- **Không có retry policy "mãi mãi".** Bước bắt buộc phải thành công (sau
  pivot) là một vòng lặp trong workflow quanh một retry policy hữu hạn
  với giấc ngủ durable (`untilDone` trong `processes/shipment`); các
  attempt lỗi tới observer.
  History của nó tăng vài event mỗi vòng (khoảng 600 mỗi ngày): ổn với sự
  cố kéo dài vài giờ hay vài ngày; lâu hơn thì kết thúc vòng lặp bằng
  `ContinueAsNew`, hoặc cảnh báo và sửa tay.
- **Hai khe hẹp làm sai cam kết 202 của pickup.** Nếu `CreateLabel` đã
  commit dòng rồi hết số lần thử trong khi carrier xác nhận giữa chừng,
  hoặc operator terminate shipment còn ở `label_created` trước khi xác
  nhận tới, carrier đã nhận 202 nhưng đơn hàng bị hoàn tác. Hãy theo dõi
  các shipment có dòng `pickup_confirmed` mà đơn hàng đã được bù.
- **Thứ tự outbox theo sequence, không theo commit.** Xem
  `internal/platform/outbox`: ổn với một event mỗi key mỗi transaction,
  không ổn với nhiều producer đồng thời cho cùng một key.

Giới hạn của chính ví dụ:

- Gateway, hãng vận chuyển, nhà cung cấp và mail server là bản giả nằm
  trong bảng của chính module.
- Test tích hợp chạy cả hai deployment trong một process nên `Configure`
  của các process dùng chung; điều nó chứng minh là việc tách queue (mỗi
  engine chỉ chạy activity của module mình và ghi kết thúc của workflow
  thuộc module mình).
- Test import `internal/testdb` của repository này; bản chép blueprint cần
  helper database test riêng.
