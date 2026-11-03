# NT132-Kafka — phần TV4

TV4 gồm Payment, Notification, Stock View PostgreSQL, Flash Sale Generator, Docker và test.
TV3 quản lý Order, Inventory, `inventory_db`, giữ/hoàn trả hàng và Transactional Outbox.
Stock View chỉ ghi vào **`stock_view_db` bằng tài khoản riêng**, không đọc Inventory để seed
và không thay đổi tồn kho chính. Không có SQLite trong phần TV4.

## Trạng thái tích hợp và hợp đồng cần TV3 xác nhận

**Chưa thể tuyên bố luồng Inventory → Stock View đã tích hợp hoàn chỉnh.** `products.version`
đã là BIGINT, bắt đầu 0 và tăng trong transaction giữ/hoàn trả của TV3. Tuy nhiên,
`seed_product`, `reserve_stock` và `release_stock` hiện gọi `stock_updated` mà không truyền version.
Stock View từ chối các event đó và không commit offset; không đoán version từ timestamp,
event ID, Kafka offset hoặc mặc định 0. Không sửa nghiệp vụ/schema database của TV3 trong bàn giao này.

TV3 cần xác nhận và bổ sung vào cùng transaction/outbox đang có:

`version` phải do Inventory tạo/tăng theo từng `product_id` trong cùng transaction cập nhật
tồn kho và ghi outbox; Stock View chỉ tiếp nhận version này.

- Seed sản phẩm mới: phát `StockUpdated` với **version thực tế được lưu**, hiện là 0.
- Giữ/hoàn trả: truyền `updated["version"]` từ `UPDATE ... RETURNING stock, version` vào
  `stock_updated(product_id, stock_after, version=...)`.
- Version tăng đơn điệu theo từng product, không reset khi replay/restart. Cùng version phải
  biểu diễn cùng trạng thái. Các event cũ thiếu version cần snapshot/replay được TV3 xác nhận;
  không sửa payload bằng cách tra trạng thái mới nhất rồi gán cho event cũ.
- Xác nhận topic/group mặc định bên dưới (Inventory hiện dùng hằng tên topic/group), chính sách
  replay/outbox và snapshot nhất quán. Một event ID chỉ đại diện cho một event bất biến.

Các thay đổi chung tương thích ngược: `stock_updated` nhận thêm **version tùy chọn**, chỉ đưa
vào payload khi truyền rõ ràng; `create_consumer` nhận `config_overrides` để TV4 luôn tắt
auto-commit/auto-offset-store; `commit_message` giúp TV4 phát hiện cả lỗi từng partition trả về
khi commit đồng bộ. Các lời gọi TV3 hiện tại giữ hành vi cũ; không âm thầm thêm version.

## Topic, key và offset

| Topic | Event | Key | Consumer group |
| --- | --- | --- | --- |
| `orders.created` | OrderCreated | order_id | inventory-group |
| `inventory.results` | InventoryResult | order_id | payment-group |
| `payments.results` | PaymentResult | order_id | inventory-group, notification-group |
| `inventory.updated` | StockUpdated | product_id | stock-view-group |

JSON UTF-8 dùng envelope chung `{event_id, event_type, timestamp, data}`. Timestamp ISO 8601
có múi giờ. Không tạo topic mới; phía vận hành cần chuẩn bị bốn topic này trên Broker.
Consumer TV4 manual commit đồng bộ sau xử lý thành công. Nếu validate, DB, publish hoặc commit
lỗi, vòng lặp dừng để restart/replay từ offset chưa commit, tránh commit vượt qua message lỗi.

Payment chỉ xử lý `InventoryResult.success=true`; false được bỏ qua và commit, không phát thanh toán.
`PaymentResult.data` chỉ gồm order_id và success; Inventory thực hiện hoàn trả khi thất bại.
Payment chờ delivery callback từ Broker rồi mới commit offset. SHA-256(seed, order_id) cho kết quả
ổn định qua restart; UUID5 của cặp `[order_id, InventoryResult.event_id]` giữ nguyên event ID
khi retry cùng event nguồn, cho phép `processed_events` của Inventory chống trùng. Timestamp
lấy từ InventoryResult nguồn, nên replay cùng event giữ nguyên payload. Các InventoryResult
khác nhau cho cùng order tạo PaymentResult có ID riêng và cùng kết quả khi cấu hình giữ nguyên;
Inventory còn chống hoàn trả trùng theo order/reservation. Mỗi ID đại diện một envelope bất biến.
Đây là mô phỏng, không có sổ thanh toán bền vững: **giữ cùng seed, mode, rate trên mọi instance
và trong toàn bộ thời gian có thể replay**; đổi cấu hình có thể làm kết quả khác dù event ID giữ
nguyên. Dùng order ID mới cho đợt thử cấu hình mới.

Notification log event_id, order_id, success rồi commit. Log có thể lặp khi replay vì chưa có
chống trùng bền vững. Log không thay thế trạng thái nghiệp vụ.

## Windows / PowerShell

Để chạy từng bước với PostgreSQL local trong Compose, kiểm tra tín hiệu thành công và
thử event giả lập khi chưa có Kafka, xem [hướng dẫn PowerShell](docs/WINDOWS_TV4.md).

Chạy các lệnh sau từ thư mục `kafka-application/` trong bản checkout của bạn.
Python 3.14 đã dùng để kiểm tra;
Docker cũng dùng Python 3.14.5. Không cần Kafka/PostgreSQL để chạy unit test.

```powershell
if (-not (Test-Path -LiteralPath '.\.venv\Scripts\python.exe')) {
    python -m venv .venv
}
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
if (-not (Test-Path -LiteralPath '.env')) {
    Copy-Item -LiteralPath '.env.example' -Destination '.env'
}
```

Sửa `.env` cục bộ, giữ nguyên topic/group mặc định. Ví dụ Kafka VM1 là
`KAFKA_BOOTSTRAP_SERVERS=192.168.56.10:9092`; thay bằng địa chỉ thực tế và bảo đảm
`advertised.listeners` của Broker truy cập được từ Windows/container. Đặt
`STOCK_VIEW_DB_HOST`, PORT, NAME, USER và PASSWORD của tài khoản riêng vừa tạo.
Mật khẩu trong `.env.example` được để trống; tự điền giá trị riêng, không commit `.env`.
TV4 không cần thông tin đăng nhập Inventory. Không đặt `ENABLE_AUTO_COMMIT=true` khi chạy TV3.
Biến môi trường của process được ưu tiên hơn `.env` bởi python-dotenv.

## Tạo PostgreSQL riêng cho Stock View

Một quản trị viên PostgreSQL thực hiện trước (psql, không chạy bằng service account):

```sql
CREATE ROLE stock_view_user LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
\password stock_view_user
CREATE DATABASE stock_view_db OWNER stock_view_user;
REVOKE CONNECT ON DATABASE stock_view_db FROM PUBLIC;
GRANT CONNECT ON DATABASE stock_view_db TO stock_view_user;
```

`\password` hỏi mật khẩu tương tác. Chạy phần tạo role/database **một lần** hoặc kiểm tra chúng
đã tồn tại trước; không reset, drop hay thay thế database hiện có. Tài khoản Stock View không
được cấp quyền trên `inventory_db`. Service không tạo role/database và không dùng superuser.

Khởi tạo **bảng** qua lệnh riêng, an toàn khi chạy lại, không xóa dữ liệu:

```powershell
.\.venv\Scripts\python.exe -m services.stock_view.database
```

Schema nằm tại `services/stock_view/schema.sql`: `stock_levels(product_id VARCHAR(50) PRIMARY KEY,
stock_after INTEGER, version BIGINT, updated_at TIMESTAMPTZ)` và
`processed_events(event_id VARCHAR(100) PRIMARY KEY, processed_at TIMESTAMPTZ)`.
Mỗi event dùng một top-level transaction: `INSERT processed_events ... ON CONFLICT DO NOTHING`
và upsert `WHERE EXCLUDED.version > stock_levels.version`. Event trùng không ghi lại;
version bằng/thấp được ghi nhận đã xử lý nhưng không ghi đè sản phẩm. Lỗi, kể cả lúc commit DB,
rollback toàn bộ transaction và không commit Kafka. Kết nối phải idle trước xử lý để tránh
nhầm savepoint với commit thật. Không có SELECT-version-then-UPDATE vô điều kiện.

## Seed / snapshot Stock View

Ưu tiên đọc `StockUpdated` ban đầu do Inventory phát từ outbox sau khi TV3 bổ sung version.
Ví dụ đúng hợp đồng (key Kafka là `P01`):

```json
{"event_id":"snapshot-run1-P01","event_type":"StockUpdated","timestamp":"2026-11-03T00:00:00+00:00","data":{"product_id":"P01","stock_after":100,"version":0}}
```

Version 0 chỉ hợp lệ khi **nguồn xác nhận sản phẩm đang ở version 0**. Với snapshot về sau,
dùng version thực do TV3 cung cấp. Snapshot cần nhất quán với thời điểm chụp và luồng thay đổi.
TV4 không đọc `inventory_db` để tạo snapshot. Có thể đưa từng envelope vào `inventory.updated`
bằng công cụ publish được phép của nhóm, hoặc import file JSONL UTF-8 do TV3 bàn giao:

```powershell
.\.venv\Scripts\python.exe -m services.stock_view.database --import-events .\snapshot.jsonl
```

Mỗi dòng là một event đầy đủ, có event ID và version rõ ràng; không có header CSV. Import dùng
cùng validator/transaction/upsert với consumer, mỗi dòng commit độc lập; khi lỗi có thể chạy lại.
Import là nạp projection cục bộ, không phát Kafka và không commit Kafka offset. Tiếp tục consumer
sau đó; version cũ/trùng không ghi đè snapshot. Các message legacy thiếu version vẫn cần TV3
xử lý bằng kế hoạch replay/offset được nhóm thống nhất, không tự bỏ qua.

## Chạy service trực tiếp

Mở terminal riêng cho từng consumer, sau khi khởi tạo bảng:

```powershell
.\.venv\Scripts\python.exe -m services.payment.consumer
.\.venv\Scripts\python.exe -m services.notification.consumer
.\.venv\Scripts\python.exe -m services.stock_view.consumer
```

Chế độ thanh toán: `.env` dùng `PAYMENT_MODE=random` (mặc định), `success` hoặc `failure`.
`PAYMENT_SUCCESS_RATE=0.9` trong [0,1], `PAYMENT_SEED=nt132`, `PAYMENT_DELIVERY_TIMEOUT=15`
giây hữu hạn dương. Mode success/failure ép kết quả; random dùng rate/seed. Ví dụ phiên thử:

```powershell
$env:PAYMENT_MODE = 'failure'
.\.venv\Scripts\python.exe -m services.payment.consumer
# Sau khi dừng, bỏ override để dùng lại .env:
Remove-Item Env:PAYMENT_MODE
```

Lệnh TV3 có sẵn, dùng cấu hình Inventory riêng đã được TV3 chuẩn bị:

```powershell
.\.venv\Scripts\python.exe -m services.inventory.consumer
.\.venv\Scripts\python.exe -m services.inventory.producer
.\.venv\Scripts\python.exe -m services.order.producer --product P01 --quantity 1
```

## Docker

Image cố định `python:3.14.5-slim-bookworm`, dependency ghim trong requirements; ứng dụng chạy
user không phải root. Base Compose chỉ dùng database ngoài, không tạo PostgreSQL Inventory.
`DOCKER_STOCK_VIEW_DB_HOST` / PORT tách khỏi cấu hình Windows; mặc định Docker Desktop là
`host.docker.internal:5432`. Đặt địa chỉ VM thật nếu database ở VM. Không dùng localhost để trỏ
tới container khác. Kafka dùng cùng `KAFKA_BOOTSTRAP_SERVERS` có thể cấu hình VM1.

```powershell
docker compose config --quiet
docker compose build
docker compose run --rm stock-view python -m services.stock_view.database
docker compose up -d payment notification stock-view
docker compose logs -f payment notification stock-view
docker compose --profile tools run --rm order --product P01 --quantity 1
docker compose --profile tools run --rm flash-sale --count 1000 --product P01 --quantity 1 --rate 0
```

Tùy chọn chạy các command TV3 bằng image chung (không đổi database):

```powershell
docker compose --profile inventory up -d inventory inventory-outbox
```

Đặt `DOCKER_INVENTORY_DB_HOST` / PORT và thông tin Inventory do TV3 cấp trước khi chạy profile.
Không cần bật profile này để dùng ba service TV4.

Tùy chọn PostgreSQL **chỉ cho Stock View local** qua file riêng:

```powershell
# Điền STOCK_VIEW_DB_PASSWORD và LOCAL_POSTGRES_ADMIN_PASSWORD trong .env trước.
docker compose -f docker-compose.yml -f docker-compose.local.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d stock-view-postgres
docker compose -f docker-compose.yml -f docker-compose.local.yml run --rm stock-view python -m services.stock_view.database
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d payment notification stock-view
```

Postgres `17.6-bookworm` có healthcheck và named volume; host port mặc định 5433, bind loopback.
Nếu chạy Python Windows với DB local này, đặt `STOCK_VIEW_DB_PORT=5433` (hoặc port đã chọn).
Container dùng DNS `stock-view-postgres:5432`. Bootstrap tạo tài khoản admin và tài khoản app
**riêng** bằng script `docker/init-stock-view.sh` khi volume rỗng lần đầu; service chỉ dùng tài
khoản app không có quyền superuser. Đổi mật khẩu trong `.env` không đổi role đã tồn tại trong
volume, cần quản trị viên cập nhật có chủ đích. Không xóa volume/reset database để sửa cấu hình.

## Flash Sale Generator

Reuses `services.order.producer.create_order` và serializer/client chung; gửi bất đồng bộ
bằng một vòng lặp, không tạo thread mỗi đơn. Mặc định run ID là UUID riêng cho mỗi lần chạy;
`--run-id` chủ động dùng lại sẽ tái dùng order_id và được Inventory xử lý như retry.

```powershell
.\.venv\Scripts\python.exe -m load_testing.flash_sale --count 1000 --product P01 --quantity 1 --rate 100 --csv reports\sale-1000.csv
.\.venv\Scripts\python.exe -m load_testing.flash_sale --count 10000 --product P01 --quantity 1 --rate 1000 --csv reports\sale-10000.csv
.\.venv\Scripts\python.exe -m load_testing.flash_sale --count 100000 --product P01 --quantity 1 --rate 0 --csv reports\sale-100000.csv
```

`--rate 0` không giới hạn. `--queue-timeout` mặc định 15 giây cho mỗi đơn bị đầy queue;
`--flush-timeout` mặc định 30 giây cho flush cuối. Queue hết hạn tính một lỗi gửi ngay và
dừng gửi đơn mới; đơn chưa thử được báo riêng. JSON stdout và CSV một dòng tóm tắt gồm:

- planned: số dự kiến; submitted: số produce nhận vào queue, chưa chắc Broker nhận.
- acknowledged: callback thành công; immediate_failures: lỗi enqueue; delivery_failures:
  callback thất bại; failed là tổng hai loại lỗi, không đếm trùng.
- unconfirmed: submitted chưa có callback khi hết chờ; not_submitted: chưa thử gửi.
- started_at/ended_at UTC, duration_seconds và acknowledged_per_second tính cả thời gian chờ.

Luôn `planned = acknowledged + failed + unconfirmed + not_submitted`.
Ack throughput là **tốc độ Broker xác nhận nhận đơn**, không phải tốc độ xử lý nghiệp vụ
Payment/Inventory toàn hệ thống. Exit code 1 khi có lỗi, pending, chưa gửi hoặc bị ngắt.
Kết quả chưa xác nhận có thể được Broker nhận sau deadline: giữ nguyên order ID khi điều tra/retry.
CSV trong Docker nằm trong container nếu không mount thư mục; muốn lưu bền vững, mount thư mục
output với quyền ghi cho UID 10001. Lệnh Docker mẫu trên chỉ in JSON ra stdout.

## Kiểm thử giai đoạn 1

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m compileall -q common config services load_testing tests
.\.venv\Scripts\python.exe -m pip check
```

Unit test dùng Fake/Mock cho Kafka và transaction để kiểm tra quyết định/offset/rollback;
không dùng SQLite để chứng minh SQL PostgreSQL. Test Flash Sale cả 1.000, 10.000, 100.000 đơn
bằng Fake; không coi đó là kết quả benchmark Kafka thật. Test PostgreSQL riêng tự skip nếu
không đặt `STOCK_VIEW_TEST_DSN`.

Một quản trị viên tạo **database test riêng** và user chỉ dùng để test, ví dụ:

```sql
CREATE ROLE stock_view_test_user LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
\password stock_view_test_user
CREATE DATABASE stock_view_test OWNER stock_view_test_user;
```

```powershell
# Chỉ rõ database test, KHÔNG dùng stock_view_db/inventory_db hoặc DSN phát triển.
# Điền password cục bộ; nếu chứa khoảng trắng/ký tự đặc biệt, quote theo libpq conninfo.
$env:STOCK_VIEW_TEST_DSN = 'host=localhost port=5432 dbname=stock_view_test user=stock_view_test_user password=REPLACE_LOCALLY'
.\.venv\Scripts\python.exe -m unittest tests.test_postgres -v
Remove-Item Env:STOCK_VIEW_TEST_DSN
```

Test yêu cầu tên DB rõ ràng dạng `*_test`/`test_*`, kiểm tra tách khỏi tên production và tạo
schema ngẫu nhiên riêng từng test, chỉ dọn schema test của chính nó. Không truncate/drop bảng
phát triển. Test thật kiểm chứng SQL, upsert, duplicate/concurrent processing, version bằng/thấp,
rollback cả khi commit lỗi. Component fixture dùng hai schema cô lập **trong database test** để
chạy code TV3 và Stock View, không mô phỏng quyền tách hai database production. Có test native
giữ hàng → Payment thất bại → Inventory hoàn trả để chỉ rõ event thiếu version; test projection
toàn luồng sẽ skip cho đến khi TV3 phát version thật, không tự vá event để tuyên bố tích hợp.
Test không overselling chạy riêng với Payment ép thành công, tránh việc hoàn trả làm sai số đơn giữ hàng.

## Phân biệt giai đoạn 1 / 2

Lượt kiểm tra trước commit ngày 2026-11-03 trên Windows: `python -m unittest discover -s tests -v`
chạy 64 test, **54 pass, 0 fail, 10 skip**. Cả 10 skip vì chưa cấu hình `STOCK_VIEW_TEST_DSN`;
các test bị skip không được tính là đã đạt. `compileall`, `pip check`, hai cấu hình Compose và
`git diff --check` đều qua. **PostgreSQL thực tế, Docker runtime và Kafka end-to-end chưa được
kiểm chứng** trong lượt kiểm tra này.

Giai đoạn 1 kiểm tra code trên Windows, hợp đồng, unit/Fake, syntax và Compose config. Khi chưa có
PostgreSQL test thì SQL/transaction/concurrency thật **chưa kiểm chứng**, dù unit test qua.
Docker config qua không đồng nghĩa image đã build hoặc container đã chạy.

Giai đoạn 2 cần: TV3 bổ sung version và thống nhất seed/snapshot/legacy events; chạy PostgreSQL
test; build/start Docker; kết nối Kafka VM1 thật; kiểm tra ACK/offset, retry sau crash/rebalance,
luồng hoàn trả đến projection, quyền tách database và đo tải thực. Không có kết quả VM/Broker/
benchmark thật được suy ra từ Fake. Tham khảo API chính thức:
[Psycopg transactions](https://www.psycopg.org/psycopg3/docs/basic/transactions.html),
[Confluent Kafka Python](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html).
