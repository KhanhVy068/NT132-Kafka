# Kafka E-commerce applications

Order → Inventory → Payment → Notification, với projection Stock View riêng.

## Contract và xử lý lỗi

| Topic | Event | Key | Consumer group mặc định |
|---|---|---|---|
| orders.created | OrderCreated | order_id | inventory-group |
| inventory.results | InventoryResult | order_id | payment-group |
| payments.results | PaymentResult | order_id | inventory-group, notification-group |
| inventory.updated | StockUpdated | product_id | stock-view-group |

Mỗi event có `event_id`, `event_type`, timestamp ISO 8601 có timezone và `data`.
`StockUpdated(product_id, stock_after, version)` bắt buộc version BIGINT không âm.
Inventory seed phát version 0; reserve/release tăng version trong cùng transaction với
stock và outbox. Stock View chỉ upsert khi incoming version lớn hơn version đang lưu.
Không gán version mặc định cho event lịch sử thiếu version.

Inventory luôn tắt auto-commit và auto-offset-store. PostgreSQL commit xong mới commit
Kafka đồng bộ; commit helper phát hiện lỗi từng partition. Các topic/group lấy từ config
chung. Không đổi group/topic trên dữ liệu đang chạy nếu chưa có kế hoạch drain/replay.

InvalidEvent/JSON/UTF-8 lỗi được lưu nguyên message theo topic/partition/offset vào
`consumer_errors` trước khi commit offset. Unknown product là lỗi terminal được quarantine,
không phải reservation REJECTED do thiếu stock. Lỗi DB/Kafka/lập trình dừng consumer và giữ
offset. Không tự sửa, xóa hoặc replay hàng lỗi; operator kiểm tra và quyết định riêng.

## State và delivery semantics

Một PostgreSQL server có thể chứa bốn database/account riêng:

| Database | Account mặc định | State |
|---|---|---|
| inventory_db | inventory_user | Products, reservations, dedup, outbox, consumer_errors |
| payment_db | payment_user | Quyết định Payment bất biến theo order |
| notification_db | notification_user | Một thông báo bền vững theo order/event |
| stock_view_db | stock_view_user | Stock projection và dedup |

Payment lưu canonical decision trước publish. Kafka source chỉ commit sau delivery ACK.
Replay, kể cả đổi PAYMENT_MODE, dùng lại payload đã lưu; source mới cùng order không tạo
quyết định khác. Không có thu tiền thật trong mô phỏng này. Kafka output có thể lặp;
Inventory/Notification phải chịu được at-least-once delivery.

Notification là bản ghi in-app trong bảng `notifications`; log chỉ chẩn đoán. Không tuyên bố
exactly-once email/SMS. Thêm dịch vụ gửi ngoài cần idempotency key hoặc sender/outbox phù hợp.
Stock View không đọc Inventory database. Monitor/test runner là tiến trình riêng, có thể đọc
các DB bằng account chỉ SELECT; không cấp các credential đó cho Stock View service.

Một Inventory Outbox Publisher được enforced bằng session advisory lock trên connection riêng.
Publisher thứ hai bị từ chối; giữ session tới sau producer flush. Lock không phải Kafka fencing:
crash giữa ACK và published_at vẫn có thể tạo duplicate; version guard bảo vệ projection.
Không scale publisher tùy ý bằng SKIP LOCKED vì nó không bảo đảm thứ tự cùng product.

## Giai đoạn 1: Windows CMD, không cần Kafka/PostgreSQL

Chạy từ `kafka-application`:

```bat
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe -m pip check
.venv\Scripts\python.exe -m unittest discover -s tests -v
.venv\Scripts\python.exe -m compileall -q common config services scripts load_testing tests
git diff --check
```

Chỉ tạo venv khi chưa có. Pytest là dev tool tùy chọn; suite chính dùng unittest. Topic parser
tests dùng Git Bash ở `C:\Program Files\Git\bin\bash.exe`, hoặc biến `GIT_BASH`. Không có Bash
thì test skip, không tính PASS. Không dùng Fake để kết luận SQL/concurrency đã được kiểm chứng.

Nếu chưa có `.env`, copy `.env.example` rồi tự điền password cục bộ. Không commit `.env`.
Các biến DB của Payment/Notification mới là bắt buộc trước khi chạy service, không cần khi unit test.
Khi đổi tên account/database mặc định, truyền các tên `*_DB_NAME`/`*_DB_USER` liên quan vào container
để separation guard nhận đúng cấu hình; không truyền password của service khác.

## Giai đoạn 2: provision và khởi tạo có chủ đích

Xem [triển khai hai VM](../docs/deployment-two-vms.md). Base Compose dùng DB ngoài, không tự
provision Inventory/Payment/Notification. File local chỉ provision Stock View tùy chọn.

Sau khi DBA đã tạo database/account riêng và cho phép apply schema:

```bat
.venv\Scripts\python.exe -c "from services.inventory.database import get_connection,init_db; c=get_connection(); init_db(c); c.close()"
.venv\Scripts\python.exe -m services.payment.database
.venv\Scripts\python.exe -m services.notification.database
.venv\Scripts\python.exe -m services.stock_view.database
```

Các lệnh trên tạo bảng nếu thiếu; không tự tạo database/role và không seed. Inventory schema
thêm consumer_errors bằng additive DDL, không cần reset bảng/volume.

Data cũ đã có PaymentResult nhưng chưa có ledger cần backfill quyết định từ kết quả đã xác nhận
trước khi thay mode/replay. Không thể suy ra lịch sử từ cấu hình mới. Dùng order/run mới cho
lần nghiệm thu mới; không đổi/xóa kết quả cũ để làm test xanh.

Mỗi terminal một service:

```bat
.venv\Scripts\python.exe -m services.inventory.consumer
.venv\Scripts\python.exe -m services.inventory.producer
.venv\Scripts\python.exe -m services.payment.consumer
.venv\Scripts\python.exe -m services.notification.consumer
.venv\Scripts\python.exe -m services.stock_view.consumer
```

Docker trên Ubuntu: đặt DOCKER_*_DB_HOST thành IP VM2 truy cập được từ container hoặc DNS service
PostgreSQL cùng network. Default host.docker.internal dành cho Docker Desktop; native Linux cần
host-gateway mapping nếu chọn tên này. Bootstrap client là IP VM1:9092, không phải localhost.

## PostgreSQL integration tests

Chỉ đặt STOCK_VIEW_TEST_DSN tới database riêng có tên *_test hoặc test_*. Test chặn tên DB
ứng dụng, tạo schema UUID và dọn schema của chính test. Không tự tạo database, không reset DB dùng chung.

CMD đặt DSN cho riêng child process bằng password tương tác:

```bat
.venv\Scripts\python.exe -c "import os,subprocess,sys; from getpass import getpass; from psycopg.conninfo import make_conninfo; e=dict(os.environ); e['STOCK_VIEW_TEST_DSN']=make_conninfo(host='127.0.0.1',port=5432,dbname='stock_view_test',user='stock_view_test_user',password=getpass('Test password: '),connect_timeout=10); raise SystemExit(subprocess.call([sys.executable,'-m','unittest','tests.test_postgres','tests.test_remediation_postgres','-v'],env=e))"
```

Không có DSN: NOT RUN. Các test DB gồm SQL rollback/commit failure, duplicate/concurrent versions,
no overselling, native reserve→payment failure→release→projection, Payment mode/restart/concurrency,
Notification concurrent/restart, quarantine và singleton lock contention.

## Flash Sale

Generator async cũ vẫn dùng được; producer ACK throughput không phải Inventory throughput:

```bat
.venv\Scripts\python.exe -m load_testing.flash_sale --count 1000 --product-id P01 --csv reports\delivery.csv
```

Chỉ chạy tải thật khi môi trường test đã được phép. Runner đối soát yêu cầu một product mới
được seed version 0, projection đã nhận seed, outbox/lag toàn hệ thống test đã drain:

```bat
.venv\Scripts\python.exe -m load_testing.verify_sale --count 1000 --run-id fresh-1k --product-id FRESH-1K --initial-stock 100 --payment-mode success --output reports\fresh-1k.jsonl
```

Dùng product/run/output riêng cho 1k/10k/100k. Cấu hình Payment service mode success để nghiệm thu
capacity; --payment-mode trên runner chỉ là kỳ vọng, không đổi service. Runner đo số order Inventory
xử lý mới mỗi giây trong lúc gửi; kiểm tra cả Payment/Notification, pending outbox, từng group lag và
stock/version của projection. Ba mẫu settled liên tiếp mới PASS. Timeout/lỗi đo không coi là lag 0.
Timeout hoặc lỗi monitor yêu cầu generator ngừng enqueue, sau đó đợi flush hữu hạn; không tự bắt đầu
run mới khi các event trước còn có thể được xử lý.

Xem [test plan](../docs/kafka-testing-plan.md) và [trạng thái remediation](../docs/remediation-status.md).
