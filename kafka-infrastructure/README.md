
# Hạ tầng Kafka - NT132

Thư mục này chứa hạ tầng Kafka cho dự án thương mại điện tử NT132. TV1 phụ
trách Kafka Broker đơn, KRaft, listener, lưu trữ bền vững và mạng Docker. TV2
chịu trách nhiệm quản trị topic và Kafbat UI.

## 1. Kiến trúc (TV1 / K01)

Việc triển khai dùng một **node KRaft kết hợp** trên VM1. Container `kafka` vừa
là Broker vừa là Controller (`broker,controller`), do đó không sử dụng
ZooKeeper. Metadata của KRaft Controller và log topic Kafka đều nằm tại
`/var/lib/kafka/data`, được bảo toàn bằng named volume Docker cấu hình trong
`.env`.

```text
VM2: các service Python / Kafbat UI
              |
              | PLAINTEXT, ${KAFKA_ADVERTISED_HOST}:${KAFKA_PORT}
              v
VM1: máy chủ Docker --- công bố ${KAFKA_PORT}:9092 ---> container kafka
                                                        |- listener EXTERNAL :9092
                                                        |- listener BROKER  :19092
                                                        `- listener CONTROLLER :${KAFKA_CONTROLLER_PORT}
```

Các client kết nối ban đầu tới listener `EXTERNAL` đã được công bố. Broker trả
về đúng địa chỉ VM1 có thể truy cập thông qua `advertised.listeners`; vì vậy
`KAFKA_ADVERTISED_HOST` không được là `localhost` khi client chạy ở VM2. Điểm
cuối Controller và listener liên Broker chỉ thuộc mạng Docker. Kiến trúc này
chủ đích không có replication hoặc failover.

## 2. Điều kiện cần có

- Ubuntu Server 22.04 LTS trên VM1.
- Docker Engine và Docker Compose v2 (`docker compose version`).
- Địa chỉ IP hoặc tên DNS của VM1 mà VM2 truy cập được qua cổng Kafka đã cấu hình.
- Quy tắc tường lửa cho phép TCP `${KAFKA_PORT}` từ VM2 tới VM1.

Tệp Compose sử dụng image chính thức `apache/kafka:4.1.1` với tag cố định.
Không thay bằng `latest` khi chưa thống nhất và kiểm thử việc nâng cấp.

## 3. Cấu hình VM1

Trên VM1, tạo tệp biến môi trường cục bộ; tệp này được Git bỏ qua:

```bash
cd kafka-infrastructure
cp .env.example .env
```

Đặt `KAFKA_ADVERTISED_HOST` trong `.env` thành IP tĩnh hoặc tên DNS của VM1 mà
VM2 truy cập được. Chỉ đổi `KAFKA_PORT` khi không dùng được cổng 9092. Giữ
nguyên `KAFKA_CLUSTER_ID` mỗi khi tái sử dụng volume `kafka-data`; đổi giá trị
này yêu cầu xóa volume dữ liệu hiện có và sẽ xóa vĩnh viễn toàn bộ dữ liệu
Broker.

## 4. Khởi động, kiểm tra và dừng Kafka

```bash
# Kiểm tra cấu hình sau khi thay thế biến trước khi khởi động
docker compose config --quiet

# Khởi động một Broker + KRaft Controller
docker compose up -d

# Theo dõi log khởi động
docker compose logs -f kafka

# Kiểm tra container, health và kết nối bootstrap
bash scripts/check_broker.sh

# Dừng nhưng vẫn giữ topic và message trong named volume
docker compose down
```

**Không** dùng `docker compose down -v` trong quá trình vận hành thông thường:
`-v` xóa volume chứa log Kafka và metadata KRaft. Chỉ để đặt lại Broker trong
môi trường phát triển có thể xóa, hãy dừng nó, xóa named volume được hiển thị
bởi `docker volume ls`, rồi khởi động lại với cùng tệp `.env`.

TV2 và các service ứng dụng trên VM2 phải dùng:

```text
<KAFKA_ADVERTISED_HOST>:<KAFKA_PORT>
```

Ví dụ, với tệp mẫu, giá trị này là `192.168.56.10:9092`.

## 5. Bàn giao cho TV2

Sau khi TV1 khởi động và kiểm tra Broker, TV2 có thể chạy các script topic với
`BOOTSTRAP_SERVER=<IP-VM1>:<cổng>`. Các topic yêu cầu có hai partition và
replication factor bằng một; việc tạo topic chủ đích không thuộc cấu hình
Compose này.

## 6. Script vận hành của TV2

### 6.1. Tổng quan

Thư mục này chứa các script quản trị Kafka cho dự án thương mại điện tử NT132.

Chức năng:
- Khởi tạo topic Kafka
- Kiểm tra topic và partition
- Theo dõi consumer group và lag
- Benchmark hiệu năng Kafka Producer

### 6.2. Topic Kafka

| Topic | Số partition | Replication factor |
|---|---|---|
| orders.created | 2 | 1 |
| inventory.results | 2 | 1 |
| payments.results | 2 | 1 |
| inventory.updated | 2 | 1 |

### 6.3. Điều kiện cần có

- Kafka Broker đang chạy
- Có sẵn công cụ Kafka CLI
- Có thể kết nối mạng tới Broker
- Bash shell

Đặt BOOTSTRAP_SERVER thành địa chỉ mà môi trường chạy script có thể truy cập.

Ví dụ:

```bash
export BOOTSTRAP_SERVER=localhost:9092
```

Thay localhost bằng địa chỉ Broker thực tế khi chạy từ xa.

### 6.4. Khởi tạo topic

```bash
bash topics/init_topics.sh
```

Script tạo các topic còn thiếu và kiểm tra cấu hình partition, replication.

### 6.5. Liệt kê topic

```bash
bash scripts/list_topics.sh
```

Hiển thị:
- Tên topic
- Partition
- Thông tin replication

### 6.6. Kiểm tra Consumer Group

```bash
bash scripts/check_consumer_groups.sh
```

Hiển thị:
- Danh sách Consumer Group
- Offset hiện tại
- Offset cuối log
- Consumer lag
- Thành viên Consumer Group

### 6.7. Đo hiệu năng

Chỉ chạy trong môi trường kiểm thử Kafka độc lập.

```bash
bash scripts/benchmark_kafka.sh
```

Số lượng bản ghi kiểm thử:
- 1.000 bản ghi
- 10.000 bản ghi
- 100.000 bản ghi

Kích thước bản ghi mặc định: 256 byte.

Kết quả đo hiệu năng được lưu tại `benchmark-results/`.

Script đo hiệu năng Producer. Consumer lag cần được theo dõi riêng.

### 6.8. Tích hợp

Hạ tầng Kafka do TV1 triển khai.

Các service ứng dụng Kafka do những thành viên khác triển khai.

Địa chỉ bootstrap server thực tế và cách chạy Kafka CLI cần được thống nhất
với TV1 trước khi triển khai.

### 6.9. Trạng thái dự án

Giai đoạn 1:
- Đã chuẩn bị script
- Cần kiểm tra cú pháp Bash
- Chưa kiểm thử Kafka khi chạy thực tế

Giai đoạn 2:
- Triển khai Kafka Broker
- Khởi tạo topic
- Theo dõi Consumer Group
- Chạy đo hiệu năng
- Thu thập kết quả
