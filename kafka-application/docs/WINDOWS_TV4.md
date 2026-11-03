# TV4 trên Windows: Python trực tiếp + PostgreSQL trong Docker

Các bước dưới đây dùng đúng file `docker-compose.local.yml`: chỉ PostgreSQL chạy trong
container, Python TV4 chạy trực tiếp trên Windows. Không cần Kafka cho test và demo projection.
Docker Desktop phải chạy Linux Engine trước khi đến bước PostgreSQL. Không dùng `down -v`,
`volume rm`, reset hoặc xóa database. Không cần cài `psql` trên Windows vì dùng bản trong container.

## 1. Thư mục và dependency

Mở PowerShell tại thư mục gốc bản checkout (`NT132-Kafka`), rồi chạy:

```powershell
Set-Location .\kafka-application
python --version
if (-not (Test-Path -LiteralPath '.\.venv\Scripts\python.exe')) {
    python -m venv .venv
}
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip check
```

Thành công: Python 3.14.x và `No broken requirements found.` Dependency dùng psycopg 3,
confluent-kafka và python-dotenv; không cần activate venv hoặc đổi ExecutionPolicy.
Các lệnh này chạy xong sẽ tự thoát, không tạo tiến trình nền.

## 2. Tạo `.env`, giữ nguyên tệp đã có

Vẫn trong thư mục trên:

```powershell
if (-not (Test-Path -LiteralPath '.env')) {
    Copy-Item -LiteralPath '.env.example' -Destination '.env'
}
notepad .env
```

Trong Notepad, sửa/điền các dòng sau **một lần**, không thêm các bản trùng ở cuối file:

```dotenv
STOCK_VIEW_DB_HOST=127.0.0.1
STOCK_VIEW_DB_PORT=5433
STOCK_VIEW_DB_NAME=stock_view_db
STOCK_VIEW_DB_USER=stock_view_user
STOCK_VIEW_DB_PASSWORD=REPLACE_WITH_YOUR_APP_PASSWORD
LOCAL_POSTGRES_ADMIN_PASSWORD=REPLACE_WITH_A_DIFFERENT_ADMIN_PASSWORD
LOCAL_STOCK_VIEW_DB_PORT=5433
```

Hai mật khẩu là giá trị riêng do bạn chọn; thay toàn bộ chữ `REPLACE_...`, không giữ nguyên mẫu.
Không dùng mật khẩu/tài khoản Inventory. Nếu password có `$` hoặc `#`, bọc giá trị bằng dấu
nháy đơn trong `.env` để tránh nội suy/comment của Compose; đừng dùng dấu nháy đơn trong chính
password ở ví dụ này. Đóng Notepad sau khi lưu; `.env` đã được gitignore.

- `STOCK_VIEW_DB_HOST/PORT`: Python Windows truy cập port được publish ra host.
- `STOCK_VIEW_DB_NAME/USER/PASSWORD`: database và tài khoản app riêng.
- `LOCAL_POSTGRES_ADMIN_PASSWORD`: chỉ bootstrap/quản trị container; không dùng cho service.
- `LOCAL_STOCK_VIEW_DB_PORT`: host port của container. Nếu đổi, sửa cả `STOCK_VIEW_DB_PORT`.
- `DOCKER_STOCK_VIEW_DB_HOST/PORT`: dùng trong base Compose khi DB ngoài; file local ghi đè
  bằng `stock-view-postgres:5432`, nên không đổi hai biến này cho quy trình hiện tại.
- `KAFKA_BOOTSTRAP_SERVERS`: chỉ cần IP:port VM1 thật khi chạy consumer/Flash Sale với Kafka.
  Demo dưới đây không dùng Broker. Giữ nguyên bốn `TOPIC_*`, các `GROUP_*`,
  `AUTO_OFFSET_RESET=earliest`, `ENABLE_AUTO_COMMIT=false` như `.env.example`.
- `PAYMENT_MODE=random|success|failure`, `PAYMENT_SUCCESS_RATE` trong [0,1], `PAYMENT_SEED`,
  `PAYMENT_DELIVERY_TIMEOUT` giây: giữ nhất quán khi replay/chạy nhiều instance.
- `INVENTORY_DB_*`: chưa cần điền mật khẩu khi chỉ chạy TV4; không khởi động profile Inventory.
- `STOCK_VIEW_TEST_DSN`: đặt riêng ở bước test thật, không trỏ tới database phát triển.

Biến `$env:...` đã đặt trong PowerShell được ưu tiên hơn `.env` cho cả Python và Compose.
Nếu cấu hình có vẻ không đúng, kiểm tra override trong terminal trước khi sửa file.

## 3. Khởi động PostgreSQL Stock View

Mở Docker Desktop, chờ Engine chạy. Vẫn tại thư mục `kafka-application/`:

```powershell
docker version
$composeFiles = @('-f', 'docker-compose.yml', '-f', 'docker-compose.local.yml')
docker compose @composeFiles config --quiet
docker compose @composeFiles up -d --wait --wait-timeout 120 stock-view-postgres
docker compose @composeFiles ps stock-view-postgres
```

Thành công: `docker version` có cả Client và Server; config exit code 0;
`ps` báo `healthy` và host port 5433 trỏ tới port 5432. Healthcheck dùng TCP để chờ server
chính sau bootstrap. Đây là tiến trình nền duy nhất cần thiết trong quy trình không Kafka.

Với volume mới, script `docker/init-stock-view.sh` tạo `stock_view_db` và `stock_view_user`
không có quyền superuser bằng mật khẩu trong `.env`. Không tạo `inventory_db`. Với volume đã
có, bootstrap không chạy lại: dùng thông tin đăng nhập đúng của volume đó; không reset volume
và không cho rằng đổi `.env` tự đổi mật khẩu trong PostgreSQL.

Nếu không healthy hoặc báo lỗi kết nối, xem:

```powershell
docker compose @composeFiles logs --tail 100 stock-view-postgres
```

Lệnh logs trên tự thoát; nếu thêm `-f`, Ctrl+C chỉ dừng theo dõi log. Để dừng database nền:

```powershell
docker compose @composeFiles stop stock-view-postgres
```

`stop` giữ nguyên volume và dữ liệu. Chạy lại `up -d --wait ... stock-view-postgres` để tiếp tục.
Khi mở terminal mới, định nghĩa lại `$composeFiles` như trên.

## 4. Kiểm tra đúng account/database và khởi tạo bảng

Từ cùng thư mục, khi PostgreSQL đang healthy:

```powershell
@'
from services.stock_view.database import get_connection
with get_connection() as conn:
    print(conn.execute("SELECT current_database() AS db, current_user AS account").fetchone())
    print(conn.execute("SELECT rolsuper FROM pg_roles WHERE rolname = current_user").fetchone())
'@ | .\.venv\Scripts\python.exe -
.\.venv\Scripts\python.exe -m services.stock_view.database
```

Thành công: `db=stock_view_db`, `account=stock_view_user`, `rolsuper=False`, rồi
`Stock View schema ready`. Lệnh khởi tạo bảng có thể chạy lại, giữ dữ liệu đã có.
Đây là lệnh foreground chạy xong tự thoát, không khởi động consumer và không cần Kafka.

## 5. Chạy test không cần Kafka

Cùng thư mục:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m compileall -q common config services load_testing tests
```

Thành công: suite báo `OK`; nếu chưa đặt `STOCK_VIEW_TEST_DSN`, 10 test PostgreSQL/component
sẽ skip vì chưa có cấu hình test DB. Những test Fake/Mock qua **không chứng minh SQL PostgreSQL
đã chạy đúng**. Không có tiến trình nền mới được tạo bởi các lệnh test.

Muốn chạy PostgreSQL thật, tạo database test riêng. Container PostgreSQL phải đang healthy.
Mở psql trong container:

```powershell
docker compose @composeFiles exec stock-view-postgres psql -U stock_view_admin -d postgres
```

Trong psql, kiểm tra role/database trước:

```sql
\du stock_view_test_user
\l stock_view_test
```

Nếu chúng chưa có, chạy một lần:

```sql
CREATE ROLE stock_view_test_user LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
\password stock_view_test_user
CREATE DATABASE stock_view_test OWNER stock_view_test_user;
```

`\password` hỏi hai lần password test do bạn chọn. Nếu role/database đã có, giữ chúng và dùng
thông tin đã được quản trị viên xác nhận, không ghi đè password/owner của chúng. Thoát psql:

```sql
\q
```

Trong PowerShell cùng thư mục, lấy password tương tác để không ghi vào lịch sử lệnh:

```powershell
$env:STOCK_VIEW_TEST_DSN = .\.venv\Scripts\python.exe -c "from getpass import getpass; from psycopg.conninfo import make_conninfo; print(make_conninfo(host='127.0.0.1', port=5433, dbname='stock_view_test', user='stock_view_test_user', password=getpass('Test database password: '), connect_timeout=10))"
try {
    .\.venv\Scripts\python.exe -m unittest tests.test_postgres -v
} finally {
    Remove-Item Env:STOCK_VIEW_TEST_DSN -ErrorAction SilentlyContinue
}
```

Nếu đổi host port ở bước 2, thay `port=5433` ở lệnh DSN bằng đúng giá trị đó.
Trong code TV3 hiện tại, dự kiến 9 test PostgreSQL pass và 1 component projection skip vì
StockUpdated chưa có version; đây là **kỳ vọng sau khi có DB**, không phải kết quả đã chạy.
Sau khi TV3 phát version đầy đủ, component test tương ứng tự chạy và test chứng minh legacy
contract sẽ skip. Thất bại kết nối/schema/SQL phải được xử lý, không quy thành test pass.
Test chỉ dọn schema ngẫu nhiên của chính nó trong database test, không dọn stock_view_db.

## 6. Thử Stock View bằng event giả lập, chưa có Kafka

Dùng sản phẩm demo có UUID riêng; không dùng P01 hoặc sản phẩm Inventory thật. Vẫn từ cùng
thư mục và giữ PostgreSQL chạy. Tạo JSONL qua factory chung, không fork schema:

```powershell
@'
import json
from pathlib import Path
from uuid import uuid4
from common.event_schema import stock_updated

product = "TV4-DEMO-" + uuid4().hex[:16]
seed = stock_updated(product, 100, version=0)
newer = stock_updated(product, 97, version=1)
events = [seed, newer, newer,
          stock_updated(product, 999, version=0),
          stock_updated(product, 888, version=1)]
output = Path("reports/stock-view-demo.jsonl")
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text("".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events), encoding="utf-8")
print("Demo product:", product)
print("Snapshot:", output.resolve())
'@ | .\.venv\Scripts\python.exe -
.\.venv\Scripts\python.exe -m services.stock_view.database --import-events .\reports\stock-view-demo.jsonl
```

Thành công lần đầu: `{"processed": 4, "duplicate": 1, "updated": 2}`. Seed và version 1 được
áp dụng; event trùng bị bỏ qua; version cũ/bằng không ghi đè. Xem kết quả thực trong PostgreSQL:

```powershell
@'
import json
from pathlib import Path
from services.stock_view.database import get_connection
events = [json.loads(line) for line in Path("reports/stock-view-demo.jsonl").read_text(encoding="utf-8").splitlines()]
product = events[0]["data"]["product_id"]
ids = sorted({event["event_id"] for event in events})
with get_connection() as conn:
    row = conn.execute("SELECT product_id, stock_after, version FROM stock_levels WHERE product_id = %s", (product,)).fetchone()
    count = conn.execute("SELECT count(*) AS total FROM processed_events WHERE event_id = ANY(%s)", (ids,)).fetchone()["total"]
print(row)
print("Unique processed events:", count)
assert row["stock_after"] == 97 and row["version"] == 1 and count == 4
'@ | .\.venv\Scripts\python.exe -
.\.venv\Scripts\python.exe -m services.stock_view.database --import-events .\reports\stock-view-demo.jsonl
```

Thành công: stock_after=97, version=1, 4 event ID duy nhất; replay cùng file cho
`{"processed": 0, "duplicate": 5, "updated": 0}`. Demo dùng DB thật nhưng không kiểm chứng
delivery/offset Kafka hoặc concurrency. Không xóa các dòng demo; chạy lại phần tạo file sẽ tạo
sản phẩm demo mới, nên ưu tiên replay file hiện có khi chỉ muốn thử chống trùng.
Các lệnh demo tự thoát; dữ liệu nằm trong volume. Dừng PostgreSQL bằng lệnh `stop` ở bước 3.

## 7. Khi đã có Kafka

Mở mỗi terminal tại thư mục gốc bản checkout, chuyển vào `kafka-application/`, rồi chạy
một consumer:

```powershell
Set-Location .\kafka-application
.\.venv\Scripts\python.exe -m services.payment.consumer
# Terminal khác:
.\.venv\Scripts\python.exe -m services.notification.consumer
# Terminal khác:
.\.venv\Scripts\python.exe -m services.stock_view.consumer
```

Ctrl+C dừng consumer foreground. Chỉ chạy sau khi cấu hình Broker thật và, với Stock View,
TV3 đã bàn giao StockUpdated có version cùng kế hoạch xử lý event legacy. Service chạy không
phát sinh lỗi chưa đủ chứng minh luồng nghiệp vụ thành công: phải xem ACK/log/trạng thái/offset.
