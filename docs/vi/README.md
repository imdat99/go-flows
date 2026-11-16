# go-flows

[English](../../README.md)

Workflow durable có compensation kiểu saga cho Go, nhúng trong ứng dụng và
lưu trên Postgres.

- **Workflow là hàm Go bình thường.** Chúng được replay từ event history,
  nên sống sót qua crash và deploy. Bên trong, bạn có thể chạy activity đồng
  thời, chờ timer và signal bằng `Select`, bắt đầu child workflow, và
  continue-as-new.
- **Compensation có sẵn.** Gắn compensator cho activity. Khi workflow lỗi
  hoặc bị huỷ, engine hoàn tác những gì đã xong, cái sau cùng trước, một
  cách durable, và không cần hàm workflow. Khi một compensator bỏ cuộc,
  execution được park chờ người vận hành thay vì fail.
- **Nhiều process dùng chung một database an toàn.**
  - Mọi lần ghi đều được fence bằng lease version.
  - Mọi thời điểm lấy từ đồng hồ của database.
  - Mỗi checkpoint là một transaction.
- **Nhanh ở chỗ quan trọng.** Process lên lịch activity chạy nó ngay (eager),
  khoảng 2 round trip mỗi activity. Generic có kiểu thay cho reflection.

Nó kết hợp mô hình thực thi của
[go-workflows](https://github.com/cschleiden/go-workflows) với các bảo đảm
nhiều instance và lớp saga của saga engine. Code được port ghi nguồn trong
[NOTICE](../../NOTICE). Bản so sánh dẫn tới thiết kế này nằm ở
[so-sanh-go-workflows.md](../so-sanh-go-workflows.md).

## Ví dụ

```go
var (
	charge = flows.DefineActivity("charge",
		func(ctx context.Context, k flows.Key, o Order) (string, string, error) {
			id, err := payments.Charge(ctx, string(k), o.Amount) // k: khoá idempotency
			return id, id, err                                   // output, dữ liệu compensation
		},
		func(ctx context.Context, k flows.Key, chargeID string) error {
			return payments.Refund(ctx, string(k), chargeID)
		})

	ship = flows.DefineActivity("ship", shipOrder, nil)

	placeOrder = flows.DefineWorkflow("place-order", 1, func(c flows.Ctx, o Order) (string, error) {
		if _, err := charge.Run(c, o); err != nil {
			return "", err
		}
		if _, err := flows.Await[bool](c, "approved", 24*time.Hour); err != nil {
			return "", err // tiền đã trừ được hoàn lại
		}
		return ship.Run(c, o.ID)
	})
)

func main() {
	ctx := context.Background()
	pool, _ := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	_ = flows.Migrate(ctx, pool)
	engine, _ := flows.New(pool, flows.Options{})
	_ = engine.Start(ctx)
	defer engine.Stop(ctx)

	h, _ := placeOrder.Start(ctx, engine, "order-42", Order{ID: "42", Amount: 1299}) // id là khoá idempotency
	_ = engine.Signal(ctx, "order-42", "approved", true)
	shipment, err := h.Wait(ctx)
	// ...
}
```

Bản chạy được nằm trong [`examples/`](../../examples). Chạy Postgres bằng
`make up`, rồi `go run ./examples/quickstart`.

## Tài liệu

- [Khái niệm](khai-niem.md): workflow, replay, activity, compensation, lease.
- [Bảo đảm](bao-dam.md) và giới hạn của chúng.
- [Vận hành](van-hanh.md): cài đặt, tinh chỉnh, deploy, retention, giám sát,
  xử lý sự cố.
- [Mẫu thiết kế](mau-thiet-ke.md): idempotency, saga, signal, timeout,
  fan-out, lock, outbox, kiểm thử.
- Bằng tiếng Anh: [benchmarks](../benchmarks.md) (so với saga và
  go-workflows), [regressions](../regressions.md) (các lỗi go-workflows
  GW-1 tới GW-18 mà engine này sửa).

Tài liệu tham chiếu API là tài liệu của package (`go doc
github.com/imdat99/go-flows`).

## Package và module

| Đường dẫn | Công dụng |
| --- | --- |
| `flows` (gốc) | Engine và API workflow. |
| `flowtest` | Test trên engine thật, với store bộ nhớ, thời gian ảo và activity giả lập. |
| `idem` | Hiệu ứng đúng một lần cho activity, theo `flows.Key`. |
| `otelflows` (module riêng) | Tracer và observer metric OpenTelemetry. |
| `analyzer` (module riêng) | `flowsvet`: báo code workflow không tất định, cho `go vet` hoặc golangci-lint. |

Module gốc chỉ phụ thuộc pgx và goose.

## Phát triển

```sh
make up      # Postgres 16 ở :5433 (docker compose)
make test    # test race trên store bộ nhớ và Postgres
make lint
make bench && make benchgate   # benchmark và ngân sách allocation
make chaos   # test kill/đóng băng nhiều process
```

Xem [AGENTS.md](../../AGENTS.md) cho các bất biến và quy trình, và
[TASKS.md](../../TASKS.md) cho danh sách task.

## Tình trạng

Phần mềm chưa phát hành: API và schema còn có thể thay đổi. Cần Go 1.27; đã
test với Postgres 16.

## Giấy phép

MIT. Xem [LICENSE](../../LICENSE) và [NOTICE](../../NOTICE).
