# Danh mục hồi quy GW-*

Lỗi đã xác nhận trong go-workflows (commit `48e8119`, audit 2026-11-15). Mỗi mục có test `TestGW<n>_*` trong go-flows, viết trước khi sửa (đỏ, rồi xanh).

| Mã | Lỗi (đã xác nhận trong code) | Cách xử lý trong go-flows |
|---|---|---|
| GW-1 | Instance active trùng khi tạo đồng thời (#489); signal tới copy ngẫu nhiên | Unique index một phần `(instance_id) WHERE active`; `23505` → `ErrInstanceExists` / idempotent như saga |
| GW-2 | Fencing chỉ theo tên worker; ABA cùng worker; `ExtendActivityTask` không kiểm tra số dòng; `WithWorkerName` trùng nhau là tắt fencing | `owner_id` + `lease_version` trên instance và activity_task; mọi lần ghi khoá dòng và kiểm tra; 0 dòng → `ErrLeaseLost` |
| GW-3 | Signal/cancel mất khi xen với ContinueAsNew/kết thúc (giống #493) | `INSERT … SELECT` từ dòng active `FOR SHARE`; ContinueAsNew chuyển pending signal sang execution mới trong cùng tx |
| GW-4 | Retention bỏ sót `pending_events`, `activities` → phình mãi | FK `ON DELETE CASCADE`; dọn khi kết thúc; xoá theo lô `SKIP LOCKED` |
| GW-5 | `time.Now()` của client ở ~10 chỗ (lock, sticky, timer, completed_at) | Chỉ `now()` của DB; deadline = `now() + $n * interval '1 microsecond'` |
| GW-6 | Lỗi heartbeat tạm thời → activity bị ghi thất bại vĩnh viễn | Mất lease thì bỏ, không complete; heartbeat lỗi tạm thì "busy", không phải "lost" (như saga lỗi 6) |
| GW-7 | Migration panic, `DROP TABLE IF EXISTS`, không đóng driver | goose + `pg_advisory_lock` như `saga/migrate.go`; trả lỗi; `IF NOT EXISTS` |
| GW-8 | Poll quét toàn bộ, không ORDER BY, poll 200 ms không backoff | Cột `ready_at` + partial index, `ORDER BY ready_at`, nhận theo lô, LISTEN/NOTIFY, backoff |
| GW-9 | Cache sticky giữ executor sau complete thất bại; 2 task dùng chung executor (data race) | Evict khi có lỗi; mutex theo instance; khoá cache theo `(execution_id, last_seq, lease_version)` |
| GW-10 | Panic làm sập process (coroutine timeout 40 s, signal sai kiểu, `invalidStateTransition`) | `recover` ở mọi biên; chuyển thành lỗi; deadlock → lỗi, không panic |
| GW-11 | Kiểm tra non-determinism nông; khi phát hiện thì fail vĩnh viễn; không có versioning | So khớp loại, tên, hash argument, thời lượng timer, replay ngắn hơn lịch sử; **park `needs_attention`**; `name@version` |
| GW-12 | Registry lấy tên theo reflection (trùng `pkgA.F`/`pkgB.F`, đổi tên là hỏng); workflow chưa đăng ký → fail vĩnh viễn (#464) | Tên tường minh; trùng thì panic; chưa biết version → bàn giao (hand back) như saga |
| GW-13 | Activity không bao giờ bị huỷ (#468); không có terminate (#196) | Huỷ lan tới context của activity qua heartbeat; `Terminate` |
| GW-14 | Signal đến sớm giao theo thứ tự ngược; quá 100 thì bỏ âm thầm | FIFO, không giới hạn ngầm (giới hạn rõ ràng → lỗi) |
| GW-15 | Retry chạy trong coroutine (khoảng 6 event mỗi lần, đổi `RetryOptions` là non-determinism, off-by-one, `CanRetry` không dùng `errors.As`) | Retry phía engine trên `activity_task` (`attempt`, `visible_at`); history chỉ ghi kết quả cuối; `errors.As` |
| GW-16 | Scheduler O(n) mỗi event; TTL cache 10 s nên thường replay toàn bộ | Scheduler chỉ đánh thức coroutine sẵn sàng; TTL dài hơn, giới hạn theo bộ nhớ |
| GW-17 | Tester: #494, #481, #436, #377 | Viết lại trên store bộ nhớ + đồng hồ ảo tất định |
| GW-18 | `SideEffect` nuốt lỗi convert; `ParamsMatch` lệch với tham số `any`; float64 mất chính xác (#471) | API generic (không có `any` trong đường dữ liệu); decode bằng `UseNumber` |

Mục còn mở từ saga cũng đưa vào danh mục: escape surrogate UTF-16 đơn lẻ trong JSON (lỗi 4/8) và kiểm tra tên trong `Define*`.
