
# Kafka Produce/Consume Test

## 1. Mục tiêu

Kiểm tra Kafka Broker có thể nhận và phân phối
Message mà không cần chạy Application Services.

## 2. Điều kiện thực hiện

- Kafka Broker đang hoạt động.
- Đã khởi tạo 4 Topic.
- Có Kafka CLI.
- BOOTSTRAP_SERVER trỏ đúng Kafka Broker.

## 3. Producer Test

Chạy:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.created
```

Nhập Message mẫu:

```json
{"order_id":"ORD001","product_id":"P01","quantity":2}
```

## 4. Consumer Test

Mở terminal khác:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.created \
  --from-beginning \
  --timeout-ms 10000
```

## 5. Kết quả mong đợi

- Producer gửi Message thành công.
- Consumer đọc được Message.
- Message xuất hiện trong Kafbat UI.
- Không có lỗi kết nối Broker.

## 6. Lưu ý

Thay localhost:9092 bằng địa chỉ Kafka thực tế.

Message mẫu chỉ dùng kiểm tra Kafka CLI,
không đại diện cho Event Schema chính thức.
