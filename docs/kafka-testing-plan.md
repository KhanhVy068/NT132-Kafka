
# Kafka Testing Plan

## 1. Mục tiêu

Kiểm thử khả năng truyền Event, quan sát
Consumer Group và đánh giá hiệu năng Kafka.

## 2. UC1 - Order Processing

### Các bước

1. Khởi động Kafka Broker.
2. Khởi tạo 4 Kafka Topic.
3. Khởi động các Application Services.
4. Gửi một OrderCreated Event.
5. Quan sát orders.created.
6. Kiểm tra inventory.results.
7. Kiểm tra payments.results.
8. Kiểm tra inventory.updated.
9. Quan sát Consumer Group và Offset.

### Kết quả mong đợi

- Event xuất hiện đúng Topic.
- Consumer nhận và xử lý Event.
- Consumer Offset tiến triển.
- Consumer Lag giảm khi xử lý kịp tải.
- Trạng thái tồn kho đúng nghiệp vụ.

## 3. UC2 - Kafka Benchmark

| Test | Records |
|------|---------|
| KB01 | 1000 |
| KB02 | 10000 |
| KB03 | 100000 |

### Các chỉ số

- Throughput (records/sec)
- Throughput (MB/sec)
- Average Latency (ms)
- Maximum Latency (ms)
- Consumer Lag khi có Consumer hoạt động

### Phương pháp

1. Ghi nhận cấu hình môi trường.
2. Chạy benchmark_kafka.sh.
3. Thu thập kết quả từng mức tải.
4. Quan sát Consumer Group nếu có.
5. So sánh Throughput và Latency.

## 4. Phân biệt hai loại kiểm thử

Kafka CLI Benchmark đo hiệu năng Broker.

Flash Sale Test của TV4 đo hoạt động
của hệ thống ứng dụng thương mại điện tử.

Hai loại kiểm thử không thay thế nhau.

## 5. Trạng thái

Chưa thực thi thực tế.
Chờ triển khai Kafka Broker và Application.
