# Mẫu thiết kế

[English](../patterns.md)

Mỗi mẫu là một đoạn code ngắn dùng API công khai. Chương trình chạy được nằm
trong [`examples/`](../../examples): `quickstart` (activity, signal),
`order` (compensation), `fanout` (đồng thời), `outbox` (phát event),
`modulith` (modular monolith ghép các mẫu trên qua năm module, tách
deployment theo queue: [README](../../examples/modulith/README.vi.md)).

## Hiệu ứng đúng một lần với `idem`

Activity chạy ít nhất một lần. Để hiệu ứng xảy ra một lần, hãy ghi nó trong
transaction của chính module, cùng với key của activity:

```go
var requests = idem.MustNew("inventory") // inventory.processed_requests

var reserve = flows.DefineActivity("inventory.reserve",
	func(ctx context.Context, k flows.Key, sku string) (string, string, error) {
		id, err := inTx(ctx, func(tx pgx.Tx) (string, error) {
			return idem.Do(ctx, tx, requests, k, func(ctx context.Context) (string, error) {
				return insertReservation(ctx, tx, sku)
			})
		})
		return id, id, err // output, dữ liệu compensation
	},
	func(ctx context.Context, k flows.Key, id string) error {
		return inTxErr(ctx, func(tx pgx.Tx) error {
			if flows.CompensationUncertain(ctx) {
				// Attempt có thể đã xảy ra hoặc chưa: chốt lại theo key.
				// Abort cũng chặn một attempt còn đang chạy ở nơi khác.
				got, done, err := idem.Abort[string](ctx, tx, requests, k)
				if err != nil || !done {
					return err // không có gì để hoàn tác
				}
				id = got
			}
			return deleteReservation(ctx, tx, id)
		})
	})
```

`inTx` và `inTxErr` là các hàm transaction của bạn. Gọi
`requests.CreateTable` trong migration của module. Xoá bảng định kỳ bằng
`idem.Purge`.

## Saga: hoàn tác khi lỗi

Workflow trả lỗi thì các activity đã hoàn tất được hoàn tác, cái sau cùng
trước:

```go
var book = flows.DefineWorkflow("trip.book", 1, func(c flows.Ctx, t Trip) (Booking, error) {
	flight, err := bookFlight.Run(c, t.Flight)
	if err != nil {
		return Booking{}, err // chưa có gì để hoàn tác
	}
	hotel, err := bookHotel.Run(c, t.Hotel)
	if err != nil {
		return Booking{}, err // cancelFlight chạy
	}
	if _, err := charge.Run(c, t.Payment); err != nil {
		return Booking{}, err // cancelHotel, rồi cancelFlight
	}
	return Booking{flight, hotel}, nil
})
```

- **Bước pivot.** Sau một bước không được hoàn tác (hàng đã giao), trả lỗi
  vẫn sẽ hoàn tác mọi thứ trước đó. Từ đó trở đi, hãy xử lý lỗi trong code
  workflow (retry tiến lên, cảnh báo, chờ signal) và trả thành công.
  Compensation bỏ qua activity không có compensator; các activity khác vẫn
  được hoàn tác.
- **Error kind.** `RegisterErrorKind("out_of_stock", ErrOutOfStock)` giữ cho
  `errors.Is(err, ErrOutOfStock)` vẫn đúng với lỗi mà `Handle.Wait` trả về,
  kể cả ở process khác.

## Chờ con người hoặc hệ thống bên ngoài

```go
approved, err := flows.Await[bool](c, "approval", 72*time.Hour)
if errors.Is(err, flows.ErrAwaitTimeout) {
	return Order{}, ErrNotApproved // hoàn tác
}
```

Bên gửi gọi `engine.Signal(ctx, orderID, "approval", true)`, từ process
nào cũng được. Signal gửi sớm được giữ cho tới khi workflow đọc.

Để chờ nhiều thứ cùng lúc, dùng timer và signal trong `Select`:

```go
reminders := flows.Signals[string](c, "reminder")
deadline := flows.NewTimer(c, 24*time.Hour)
for done := false; !done; {
	flows.Select(c,
		flows.OnSignal(reminders, func(c flows.Ctx, msg string, err error) { /* ... */ }),
		flows.OnFuture(deadline, func(c flows.Ctx, _ struct{}, err error) { done = true }),
	)
}
```

## Timeout cho một activity

`WithTimeout` giới hạn mỗi attempt. Để giới hạn cả lời gọi từ phía workflow,
tính cả retry, cho nó đua với một timer và huỷ bên thua:

```go
ac, cancel := flows.WithCancel(c)
res := slowCall.Start(ac, in)
timeout := flows.NewTimer(ac, 5*time.Minute)
var out Result
var err error
flows.Select(c,
	flows.OnFuture(res, func(c flows.Ctx, v Result, e error) { out, err = v, e }),
	flows.OnFuture(timeout, func(c flows.Ctx, _ struct{}, _ error) { err = ErrTooSlow }),
)
cancel() // huỷ bên còn đang chờ: cả context của attempt
```

## Fan-out và fan-in

```go
futures := make([]*flows.Future[Price], len(suppliers))
for i, s := range suppliers {
	futures[i] = quote.Start(c, s) // tất cả được lên lịch trong một checkpoint
}
for i, f := range futures {
	if prices[i], err = f.Get(c); err != nil {
		return nil, err // các quote đã xong được hoàn tác (nếu quote có compensator)
	}
}
```

Khi mỗi nhánh có logic riêng, dùng `flows.Go` với `flows.WaitGroup`:

```go
var wg flows.WaitGroup
for _, item := range items {
	wg.Add(1)
	flows.Go(c, func(c flows.Ctx) {
		defer wg.Done()
		// Nhiều activity cho mỗi item; coroutine là tất định.
	})
}
if err := wg.Wait(c); err != nil {
	return err
}
```

## Workflow sống lâu

Workflow lặp mãi (một subscription, một entity) làm history phình ra. Hãy
continue-as-new định kỳ:

```go
var account = flows.DefineWorkflow("account", 1, func(c flows.Ctx, st State) (State, error) {
	events := flows.Signals[Event](c, "event")
	for range 1000 {
		ev, err := events.Receive(c)
		if err != nil {
			return st, err
		}
		st = st.apply(ev)
	}
	return st, flows.ContinueAsNew(c, st) // signal chưa đọc được chuyển sang
})
```

## Child workflow

```go
shipment, err := shipWorkflow.Child(c, "ship-"+order.ID, order).Get(c)
```

- **Instance id.** Id của child là instance id riêng của nó. Hãy suy ra nó
  từ dữ liệu của cha, để replay bắt đầu đúng child đó.
- **Compensation.** Nếu sau đó cha lỗi, child đã hoàn tất được hoàn tác
  cùng.
- **Huỷ.** Huỷ cha thì huỷ child đang chạy.
- **Huỷ một child mà vẫn biết kết quả.** Huỷ context của child làm future
  resolve ngay với `ErrCanceled`, dù child vẫn có thể hoàn tất. Muốn biết
  child thực sự kết thúc thế nào thì yêu cầu huỷ:

  ```go
  ship := shipWorkflow.Child(c, "ship-"+order.ID, order)
  // ... khách huỷ đơn:
  ship.RequestCancel(c) // c.Err() của child giờ là ErrCanceled
  shipment, err := ship.Get(c)
  switch {
  case err == nil: // vẫn giao (đã qua điểm không quay lại): hoàn tiền thay vào đó
  case errors.Is(err, flows.ErrCanceled): // đã dừng và tự hoàn tác
  default: // lỗi
  }
  ```

  Trong child, làm nốt phần không dừng được trên `flows.Disconnected(c)` rồi
  trả kết quả, hoặc trả lỗi huỷ.

## Loại trừ lẫn nhau trên một business key

```go
if err := flows.Lock(c, "account:"+id, time.Minute, flows.LockWait(flows.Exponential(20, 50*time.Millisecond, 5*time.Second))); err != nil {
	return err // ErrLockHeld khi policy chờ đã hết
}
// Độc quyền cho tới khi execution kết thúc.
```

Chọn `ttl` lớn hơn khoảng thời gian dài nhất mà execution có thể sleep hoặc
await trong lúc giữ lock. Khi một activity của nó đang chờ trong queue hoặc chờ
retry, hoặc task kế tiếp của nó đang chờ trong queue (đã có kết quả activity),
lock được giữ thêm tối đa `Options.LockHold` (mặc định 1h) sau `ttl`.
Lấy nhiều lock theo một thứ tự cố định.

## Chỉ phát event khi thành công (outbox)

Đặt sẵn message trong activity, rồi phát hoặc huỷ chúng trong transaction
kết thúc:

```go
type outbox struct{}

func (outbox) RunEnded(ctx context.Context, tx pgx.Tx, r flows.RunInfo) error {
	var err error
	switch r.Status {
	case flows.StatusCompleted:
		_, err = tx.Exec(ctx, `UPDATE app.outbox SET ready = true WHERE instance_id = $1`, r.InstanceID)
	case flows.StatusNeedsAttention:
		// Park, không phải kết thúc: execution có thể vẫn compensate.
	default: // compensated, failed, canceled, terminated
		_, err = tx.Exec(ctx, `DELETE FROM app.outbox WHERE instance_id = $1`, r.InstanceID)
	}
	return err
}

engine, err := flows.New(pool, flows.Options{Hook: outbox{}})
```

Đặt hook cho mọi engine, kể cả engine chỉ làm client. Một process relay
sau đó phát các dòng đã sẵn sàng tới broker bạn chọn.
`RunEnded` cũng chạy khi park (`needs_attention`), vốn không phải kết thúc:
hãy để nguyên message.

[`examples/outbox`](../../examples/outbox) ghép tất cả lại: một activity đặt
sẵn event, chống trùng theo `Key` của nó (riêng cho mỗi execution); hook
phát hành hoặc huỷ event đó và thêm một event `workflow.ended`; một relay
phát các dòng đã sẵn sàng tới một
event bus giả bằng `FOR UPDATE SKIP LOCKED`, retry có backoff. Giao nhận
at-least-once, kèm event id để consumer chống trùng; các event đã phát hành
của cùng một key đi ra theo thứ tự id (là thứ tự đặt sẵn khi chỉ một
execution ghi key đó). Ví dụ không bao gồm ContinueAsNew.

## Kiểm thử

```go
func TestBook(t *testing.T) {
	tst := flowtest.New(t)
	flowtest.OnActivity(tst, charge, func(ctx context.Context, k flows.Key, p Payment) (string, string, error) {
		return "", "", flows.Permanent(errors.New("declined"))
	})
	_, err := flowtest.Run(tst, book, "trip-1", trip)
	if !errors.Is(err, flows.ErrCompensated) {
		t.Fatal(err)
	}
	// tst.EventTypes("trip-1") liệt kê history; tst.Advance dịch thời gian ảo.
}
```

- **Engine thật.** `flowtest` chạy engine thật trên store bộ nhớ với thời
  gian ảo. Workflow ngủ một tuần xong trong vài mili giây.
- **Workflow bị kẹt.** Workflow không bao giờ kết thúc được thì lỗi
  `flowtest.ErrBlocked`.
- **Future còn treo.** Activity và child đã bắt đầu mà không được chờ sẽ
  làm test fail, trừ khi bạn truyền `AllowPendingFutures`. Quy tắc: mỗi
  execution đã kết thúc được báo đúng một lần, ở lần `Run` đầu tiên có cây
  chứa nó. Cây của `Run(id)` gồm mọi execution của instance `id` (bất kể
  cha, mọi execution trong các chuỗi continue-as-new, kể cả khi đã start
  trước `Run`) cùng mọi execution có cha nằm trong cây. Chỉ execution đã
  kết thúc mới được kiểm tra: execution vẫn đang chạy khi `Run` trả về
  (child tách rời do continue-as-new) chờ một lần `Run` sau có cây chứa nó,
  nếu có.
- **Activity không tốn thời gian ảo.** Thời gian ảo đứng yên khi activity
  chạy. Activity chờ lệnh huỷ mà timer của workflow sẽ gửi thì không bao
  giờ thấy timer đó kích hoạt, và `Run` lỗi `flowtest.ErrNeverIdle`: hãy
  mock activity đó để nó trả về. Khi đó mock thắng trong `Select`, nên
  nhánh timer thắng activity không test được bằng `flowtest`.
