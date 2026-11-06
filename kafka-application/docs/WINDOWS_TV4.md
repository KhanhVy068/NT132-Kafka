# Windows CMD — kiểm tra và chạy ứng dụng

Các lệnh dưới dùng CMD, không phải PowerShell. Chạy từ repository:

```bat
cd /d E:\UIT\QTM-NT132\NT132-Kafka\kafka-application
python --version
```

## Unit/static tests không cần hạ tầng

Nếu chưa có .venv, tạo bằng `python -m venv .venv`. Sau khi được phép cài dependencies:

```bat
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe -m pip check
.venv\Scripts\python.exe -m unittest discover -s tests -v
.venv\Scripts\python.exe -m compileall -q common config services scripts load_testing tests
```

Test PostgreSQL skip khi thiếu STOCK_VIEW_TEST_DSN: ghi NOT RUN. Topic tests cần Git Bash.
StockUpdated native đã phát version; không còn skip component test vì thiếu version.

Chỉ khi chưa có .env:

```bat
if not exist .env copy .env.example .env
notepad .env
```

Điền password riêng cho bốn DB, không dùng superuser. Python Windows dùng localhost/port host;
container dùng DOCKER_*_DB_HOST/PORT. Giữ topic/group mặc định trừ khi có kế hoạch migration.

## PostgreSQL local tùy chọn — chỉ sau khi được phép chạy Docker

File docker-compose.local.yml chỉ tạo Stock View PostgreSQL. Nó không tạo DB Payment,
Notification hay Inventory. Không khởi động ba service mới mà chưa provision đủ các DB này.
Không dùng down -v, reset volume hoặc drop database.

```bat
docker compose -f docker-compose.yml -f docker-compose.local.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --wait stock-view-postgres
```

Điền STOCK_VIEW_DB_PASSWORD và LOCAL_POSTGRES_ADMIN_PASSWORD trước. Python Windows kết nối
host port LOCAL_STOCK_VIEW_DB_PORT (mặc định 5433). Container Stock View được override DNS
stock-view-postgres:5432. Volume cũ không tự đổi mật khẩu khi chỉnh .env.

Sau khi có database test riêng và được phép tạo/dọn schema test, dùng lệnh password tương tác
trong [README](../README.md#postgresql-integration-tests). Không trỏ test DSN vào DB ứng dụng.

## Chạy service và load test

Xem [README](../README.md) cho init schema, commands từng service và semantics.
Xem [triển khai hai VM](../../docs/deployment-two-vms.md) cho provision và hostname Linux.
Xem [test plan](../../docs/kafka-testing-plan.md) trước khi phát event thật.

Mọi thao tác tạo bảng/seed/publish/khởi động Docker là thao tác giai đoạn 2 có chủ đích,
không phải bước cần chạy để unit test PASS.
