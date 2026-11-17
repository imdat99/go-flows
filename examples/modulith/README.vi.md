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

Test: `go test -race ./examples/modulith/...` (test tích hợp cần `make up`).

## Bố cục

```
examples/modulith/
  cmd/modulith/main.go          flag/env, pgxpool, signal, tắt êm
  internal/app/                 composition root: migration, Configure, engine, hook, HTTP API
  internal/platform/
    schema/                     migration goose theo module, version trong <schema>.goose_db_version
    outbox/                     transactional outbox: Stage, Relay, Bus, LocalBus
    deps/                       chỗ giữ dependency của một module (xem "Dependency")
  modules/
    orders/                     saga fulfilment, read model, RunHook, event
    payments/                   authorize/void, capture/refund (chỉ activity)
    inventory/                  Reserve (child workflow), vòng bổ sung hàng, điều chỉnh tồn kho
    shipping/                   Ship (child workflow): nhãn, signal của hãng, timeout
    notifications/              nhận event đơn hàng, mỗi event một workflow
```

Mỗi module là một package Go. Những gì được export là API công khai của
nó; mọi thứ khác, kể cả các bảng, là riêng tư.

| Module | Schema | Queue | API công khai |
| --- | --- | --- | --- |
| orders | `orders` | `orders` | `Place`, `Cancel`, `Get`, `Hook`, event `TopicCompleted`/`TopicCanceled`/`TopicFailed` + `Event` |
| payments | `payments` | `payments` | activity `Authorize`, `Capture`; `Find`, `ErrDeclined` |
| inventory | `inventory` | `inventory` | workflow `Reserve` (+ `ReserveID`), `Adjust`, `Level`, `StartReplenishment`, `StopReplenishment` |
| shipping | `shipping` | `shipping` | workflow `Ship` (+ `InstanceID`), `ConfirmPickup`, `Cancel`, `Find`, `ErrPickupTimeout`, `ErrCanceled`, `ErrPickupRefused`, `ErrNoShipment` |
| notifications | `notifications` | `notifications` | `Subscribe`, `Sent` |

Module nào cũng export thêm `Queue`, `Configure` và `Migrate`.

## Một đơn hàng chạy thế nào

```
HTTP POST /orders ──> orders.Place ──> workflow orders.fulfil (queue orders)
   1. orders.record                activity      orders
   2. payments.Authorize           activity      payments       hoàn tác: void
   3. inventory.Reserve            child         inventory      hoàn tác: release (từng dòng)
   4. payments.Capture             activity      payments       hoàn tác: refund
   5. shipping.Ship                child         shipping       hoàn tác: huỷ nhãn
        tạo nhãn, rồi Select { signal picked-up | signal shipping.cancel | timer PickupWithin }
        rồi Decide trong shipping.shipments (quyết định đầu tiên thắng, hãng hay workflow)
        đã lấy hàng: MarkPickedUp, retry đến khi thành công
      đơn hàng: Select { giao hàng xong | signal orders.cancel -> chuyển tiếp cho shipment }
                rồi chờ kết quả thật của shipment
 kết thúc ──> RunHook (cùng transaction): orders.orders.state + event outbox
          ──> relay ──> bus ──> consumer notifications ──> workflow notifications.send
```

- **Compensation đi qua các module theo tên.** Khi hãng vận chuyển không
  đến (`ErrPickupTimeout`) hoặc khách huỷ, child shipping huỷ nhãn của nó,
  rồi engine refund khoản capture, mở lại child `inventory.Reserve` đã
  xong để trả lại các dòng hàng, và void authorization. Mỗi compensator
  chạy trên queue của module nó, tức là trên các process chạy module đó.
  Không cần đến hàm `fulfil`.
- **Pivot là một dòng dữ liệu, không phải một signal.** Việc hãng đã có
  kiện hàng hay chưa được quyết định một lần, trong `shipping.shipments`,
  dưới khoá dòng: dòng rời `label_created` sang `pickup_confirmed` hoặc
  `canceled`, ai đến trước thắng.
  - Webhook của hãng (`ConfirmPickup`, `POST /shipments/{order}/pickup`)
    chuyển dòng sang `pickup_confirmed` trước khi gửi signal cho workflow.
    202 nghĩa là việc lấy hàng đã được ghi nhận vĩnh viễn: đơn sẽ hoàn
    tất. Nếu shipment đã bị huỷ trước (khách huỷ hoặc hết hạn), webhook
    nhận 409 `ErrPickupRefused` (trả lại kiện hàng); trước khi có nhãn,
    404 `ErrNoShipment` (thử lại). Không có chuyện nhận lấy hàng rồi bỏ
    rơi.
  - Workflow, được đánh thức bởi signal đầu tiên hoặc timer, xin kết quả
    nó muốn (`Decide`) và nhận trạng thái của dòng. Signal chỉ để đánh
    thức: nếu signal của hãng bị mất, timer đánh thức workflow và dòng
    vẫn ghi `pickup_confirmed`.
  - Từ lúc quyết định, shipment không thể fail vào saga: `Decide` và
    `MarkPickedUp` chạy trên `flows.Disconnected` và được retry đến khi
    thành công (từng đợt attempt, ngủ durable 5 phút giữa các đợt; các
    attempt lỗi tới observer là cảnh báo). Nếu database của shipping chết
    hàng giờ sau khi lấy hàng, đơn hoàn tất muộn; nó không bao giờ bị hoàn
    tác. `MarkPickedUp` không có compensator, và `cancelLabel` chỉ huỷ
    dòng còn `label_created`.
  - Khi đơn đã kết thúc, `orders.Cancel` trả về `ErrNotActive` (HTTP 409).
- **Ai quyết định việc huỷ.** `orders.Cancel` gửi signal cho đơn hàng.
  Gửi trong bước 1 đến 4 thì signal chờ trong channel và đơn tự dừng trước
  khi capture hoặc trước khi khởi shipment. Trong bước 5 chỉ shipment biết
  việc lấy hàng có đến trước hay không, nên đơn không huỷ child (trong
  go-flows, huỷ một child làm future của nó trả về `ErrCanceled` ngay,
  không có kết quả thật). Đơn chuyển tiếp lệnh huỷ cho shipment (một
  activity gọi `shipping.Cancel`, vì workflow không gửi signal cho workflow
  khác được) và chờ kết quả của shipment: bị huỷ thì đơn được hoàn tác;
  đã lấy hàng thì đơn hoàn tất và lệnh huỷ bị bỏ qua.
- **Test cho các đường này.**
  - flowtest: `TestFulfilCancelAfterPickup` (lệnh huỷ tới đơn sau khi việc
    lấy hàng đã tới shipment), `TestFulfilCancelBeforeShipping`,
    `TestFulfilCancelDuringCapture`, `TestFulfilPickupRecordFailsForLong`,
    `TestShipPickupBeatsCancel`, `TestShipConfirmedPickupBeatsCancelSignal`,
    `TestShipPickupRecordedDespiteFailures`.
  - Postgres: `TestPickupAfterCancelIsRefused`,
    `TestCancelAfterPickupKeepsThePickup`, và `TestDecisionRace` (hãng và
    workflow quyết định cùng lúc, 20 lần: luôn thống nhất).
  - Test tích hợp xác nhận lấy hàng rồi huỷ qua API (đơn hoàn tất dù thời
    điểm thế nào, vì 202 của việc lấy hàng là cuối cùng), và gửi xác nhận
    lấy hàng sau khi huỷ (409).
- **`Engine.Cancel` là công cụ của người vận hành, không phải lệnh huỷ
  của khách.** Nó huỷ context của đơn: future của child shipment trả về
  `ErrCanceled` ngay, và đơn compensate mà không chờ kết quả của shipment.
  Sau khi hãng đã lấy hàng, điều đó nghĩa là hoàn tiền và trả hàng về kho
  trong khi kiện hàng vẫn được giao (bản thân shipment vẫn hoàn tất). Chỉ
  dùng cho đơn buộc phải dừng bằng mọi giá, và đối soát bằng tay.

Các tính năng khác, nằm ở đâu:

| Tính năng | Ở đâu |
| --- | --- |
| Activity có compensator, idempotent theo `flows.Key` (cột key unique, fence bằng tombstone) | `payments/payments.go`, `shipping/shipping.go` |
| Hiệu ứng đúng một lần với `idem.Do`, hoàn tác không chắc chắn với `idem.Abort` | `inventory/activities.go` |
| Child workflow giữa các module, được compensate bằng cách mở lại | `orders/fulfil.go`, `inventory/workflows.go` (`Reserve`), `shipping/shipping.go` (`Ship`) |
| `Select` giữa hai signal và một timer timeout; quyết định một lần trong một dòng của bảng; các bước retry đến khi thành công trên `Disconnected` | `shipping/shipping.go` |
| `Select` giữa child và signal; chuyển tiếp signal cho child qua một activity | `orders/fulfil.go` |
| Vòng lặp dài hạn với `ContinueAsNew` và signal dừng | `inventory/workflows.go` (`replenishCycle`) |
| `flows.Lock` với `LockWait` trên khoá nghiệp vụ | `inventory/workflows.go` (`checkAndOrder`, `adjustStock`) |
| RunHook cập nhật read model và đặt event trong transaction kết thúc | `orders/hook.go` |
| Transactional outbox, relay với `SKIP LOCKED`, thứ tự theo key, backoff | `internal/platform/outbox` |
| Consumer khử trùng lặp bằng cách mỗi event id khởi một workflow | `notifications/notifications.go` |
| Queue theo module, một binary tách bằng flag | `internal/app/app.go`, `cmd/modulith/main.go` |
| Observer (log; cảnh báo khi `needs_attention`) | `internal/app/app.go` |
| Unit test bằng flowtest: đường thuận, compensation, huỷ, timeout, lock, continue-as-new | `modules/*/*_test.go` |
| Hai engine, mỗi engine một nhóm module, trên Postgres thật | `internal/app/app_test.go` |

## Quy tắc cho cả đội

### Ranh giới module

1. Mỗi module sở hữu một schema Postgres. Chỉ code của nó đọc hoặc ghi
   schema đó. Module khác gọi API Go của nó, không bao giờ query bảng của
   nó, kể cả chỉ đọc. (Lớp HTTP ghép view của nhiều module qua API của
   chúng: `internal/app/http.go`.)
2. Việc liên module phải cùng xảy ra hoặc cùng được hoàn tác thì đi qua
   workflow: gọi **activity** của module kia cho một bước đơn (payments),
   hoặc khởi **workflow của nó làm child** khi bước đó tự có chờ đợi hoặc
   nhiều bước riêng (inventory, shipping). Đừng gọi hàm Go của module khác
   từ trong một activity để nối các hiệu ứng: engine sẽ không biết cách
   hoàn tác chúng.
3. Sự kiện mà module khác phản ứng theo là **event** qua outbox (orders
   sang notifications). Bên phát sở hữu kiểu event (`orders.Event`): thêm
   trường, không bao giờ đổi hoặc xoá trường.
4. Instance id là toàn cục giữa mọi workflow: thêm tiền tố module
   (`orders-42`, `shipping-42`, `inventory-reserve-42`). Mỗi module export
   một hàm dựng id của nó.
5. Error kind (`flows.RegisterErrorKind`) cũng toàn cục: thêm tiền tố
   module (`payments_declined`). Chúng giữ cho `errors.Is` đúng với lỗi đọc
   lại từ history, ở bất kỳ process nào.

### Tính tất định (code workflow)

Hàm workflow được replay. Bên trong nó:

- chỉ rẽ nhánh theo input và theo kết quả các lời gọi `flows.*`;
- không I/O, không đọc đồng hồ, không ngẫu nhiên, không goroutine, không
  đọc cấu hình hay biến package có thể thay đổi: đưa các giá trị đó vào
  input của workflow khi khởi động (`orders.Order.PickupWithin`,
  `inventory.ReplenishState.Every`);
- dùng `flows.Now`, `flows.Sleep`, `flows.NewTimer`, `flows.Go`,
  `flows.Select`, `flows.SideEffect`.

Chạy `flowsvet` (module `analyzer/`) trong CI để bắt lỗi.

### Activity

- Chạy **ít nhất một lần**. Làm mọi hiệu ứng idempotent theo `flows.Key`:
  cột key unique (`payments.payments.auth_key`) hoặc `idem.Do` trong
  transaction của module (`inventory`).
- Compensator cũng phải idempotent, và phải xử lý
  `flows.CompensationUncertain(ctx)`: activity có thể đã hoặc chưa có hiệu
  ứng. Hoàn tác theo key, và fence key để một attempt muộn không thể có
  hiệu lực sau đó (`idem.Abort`, hoặc dòng tombstone của payments và
  shipping).
- Đánh dấu lỗi mà retry không sửa được bằng `flows.Permanent`.
- Giữ input và output nhỏ (id, không phải tài liệu).

### Đăng ký trong mọi binary

Định nghĩa là giá trị cấp package trong một registry toàn cục: process
chỉ chạy những gì binary của nó định nghĩa. Build một binary import mọi
module (`internal/app` làm việc này) và dùng nó cho mọi deployment. Cài
cùng `RunHook` trên mọi engine, kể cả engine chỉ làm client: kết thúc được
ghi bởi engine chạy task cuối cùng.

### Dependency

Activity ở cấp package nên không nhận tham số constructor. Mỗi module giữ
dependency trong một `deps.Ref`, được `Configure` gán từ composition root
trước khi engine chạy. `Configure` kiểm tra những gì nó nhận và `app.New`
từ chối khởi động nếu có lỗi: lỗi nối dây hỏng ngay lúc khởi động.
Activity vẫn chạy khi chưa cấu hình sẽ fail với `deps.ErrNotConfigured`,
lỗi được retry chứ không vĩnh viễn: một replica cấu hình sai chỉ làm chậm
việc, không làm fail (và compensate) đơn hàng thật. Cấu hình mọi module
trong mọi process: API Go của chúng (`orders.Get`, `payments.Find`...)
được HTTP API của process nào cũng dùng.

### Queue và tách deployment

Mỗi module chạy workflow và activity trên queue riêng (`WithQueue`,
`WithWorkflowQueue`). `-modules` chọn các queue mà process phục vụ; process
nào cũng có thể khởi workflow, gửi signal và phục vụ API, vì những việc đó
chỉ ghi vào Postgres.

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
  và activity của nó đi theo; workflow vẫn cần schema `flows` dùng chung,
  nên các bước khác database trở thành activity gọi API của bên kia.

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
   hoặc compensate nó.
3. Timeout và phần cài đặt activity (cùng kiểu, cùng ý nghĩa) đổi thoải
   mái: chúng không nằm trong history. Retry policy cũng vậy, trừ một
   điểm: giới hạn số attempt của activity được ghi vào task khi nó được
   lên lịch, nên giới hạn mới chỉ áp dụng cho activity lên lịch sau khi
   deploy, không cho những activity đang chờ hay đang retry.
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
- Job định kỳ trên một process: `engine.PurgeEnded`, `idem.Purge` cho
  `inventory.processed_requests`, xoá dòng outbox đã gửi lâu. Purge giải
  phóng các instance id nó xoá: thứ gì phải giữ idempotent sau đó thì được
  kiểm tra trong bảng của module (xem "Điểm vướng của API").
- Giới hạn kích thước request ở API (`maxBody`): engine lưu input và
  signal nguyên như nhận.
- Execution bị park: sửa nguyên nhân, rồi `engine.Resume` (code) hoặc
  `engine.RetryCompensation` (một compensator bỏ cuộc).
- Xem [docs/vi/van-hanh.md](../../docs/vi/van-hanh.md) về tinh chỉnh,
  pooler, autovacuum và sự cố.

## Điểm vướng của API và giới hạn đã biết

Những gì go-flows làm khó ở đây, và cách ví dụ xoay xở:

- **Không có "huỷ child và nhận kết quả thật của nó".** Huỷ context của
  child làm future của nó trả về `ErrCanceled` ngay; child có thể vẫn đã
  hoàn tất (ở đây: đã được lấy hàng). Gửi lệnh huỷ cho child dưới dạng
  signal và để child quyết định (`orders/fulfil.go`).
- **Không có signal từ workflow sang workflow.** Workflow không gọi được
  `Engine.Signal`. Đơn hàng chuyển tiếp lệnh huỷ bằng một activity
  (`orders.forward-cancel`), activity này cần một engine trong dependency
  của module và chạy ít nhất một lần (shipment bỏ qua các bản lặp).
- **Idempotency theo instance id hết hạn khi retention xoá.** `Start` với
  id đã dùng chỉ trả về execution cũ cho đến khi `PurgeEnded` xoá nó; sau
  đó cùng id sẽ khởi execution mới. Giữ idempotency lâu dài trong bảng
  của module: `orders.Place` kiểm tra `orders.orders` (nội dung khác dưới
  id đã dùng: `ErrOrderConflict`, 409), `inventory.Adjust` kiểm tra
  `inventory.adjustments.request_id` (`ErrRequestConflict`), và SKU ngừng
  kinh doanh là một cột, nên vòng bổ sung hàng của nó không bị khởi lại
  sau purge. Hai đơn khác nhau tranh cùng một id mới đều nhận 202; nội
  dung của đơn thứ hai bị bỏ.
- **`idem.CreateTable` không hợp với migration SQL.** Nó chạy DDL ngoài
  goose và lock của goose (`CREATE TABLE` đồng thời có thể lỗi trên
  `pg_type`). Module inventory tạo bảng trong migration đầu tiên:

  ```sql
  CREATE TABLE inventory.processed_requests (
  	key        text PRIMARY KEY,
  	result     jsonb,
  	aborted    boolean NOT NULL DEFAULT false,
  	created_at timestamptz NOT NULL DEFAULT now()
  );
  CREATE INDEX processed_requests_created_idx ON inventory.processed_requests (created_at);
  ```

- **Định nghĩa là toàn cục.** Activity không nhận dependency được
  (`deps.Ref`), và hai engine trong một process dùng chung chúng.
- **flowtest không mock được child workflow.** Test `orders.fulfil` phải
  mock activity của child inventory và shipping, nên các module đó export
  chúng chỉ vì lý do này.
- **`Workflow.Start` không tham gia transaction của bên gọi được.** Dòng
  đơn hàng được activity đầu tiên của workflow ghi, không phải `Place`.
- **Mỗi engine một `RunHook`, và `RunInfo` không có input hay output.**
  Composition root nối hook của các module; hook của orders đọc bảng của
  nó, và nhận ra lệnh huỷ của khách bằng cách tách `RunInfo.ErrorKinds`.
- **Signal không phải quyết định.** Signal được nhận khi execution đang
  chạy, đang compensate hay bị park, dù có được đọc hay không: workflow đã
  chọn nhánh khác sẽ không bao giờ đọc nó, còn bên gửi nhận nil. Signal
  gửi trước khi instance tồn tại thì lỗi (`ErrNotFound`). Điều gì bên gửi
  cần biết là đã được nhận (việc lấy hàng của hãng) thì được quyết định
  trong bảng của module trước (`shipping.ConfirmPickup`), signal chỉ để
  đánh thức workflow.
- **Không có retry policy "mãi mãi".** Bước bắt buộc phải thành công (sau
  pivot) là một vòng lặp trong workflow quanh một retry policy hữu hạn
  với giấc ngủ durable (`untilDone` trong `shipping/shipping.go`); các
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
  của module dùng chung; điều nó chứng minh là việc tách queue (mỗi engine
  chỉ chạy activity của module mình và ghi kết thúc của workflow thuộc
  module mình).
- Test import `internal/testdb` của repository này; bản chép blueprint cần
  helper database test riêng.
