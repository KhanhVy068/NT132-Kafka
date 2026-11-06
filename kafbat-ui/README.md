# Kafbat UI — VM2

Compose dùng default network do chính Compose tạo, không còn phụ thuộc external network từ máy cũ.
Broker VM1 được truy cập qua IP/hostname EXTERNAL:9092; không cần Docker network chung giữa hai VM.

`.env.example` ghim v1.5.0 theo [release chính thức](https://github.com/kafbat/kafka-ui/releases/tag/v1.5.0).
Tag này chưa được pull/chạy trong lượt sửa. KAFKA_BOOTSTRAP_SERVER phải là địa chỉ thật của VM1.

Sau khi điền `.env` (chỉ copy khi chưa có), có thể validate:

```bash
docker compose config --quiet
```

Giai đoạn 2, khi được phép chạy:

```bash
docker compose up -d
docker compose logs --tail 100 kafbat-ui
```

Mở http://VM2_IP:8080. PASS khi UI truy vấn được Broker, bốn topic và group lag; chỉ trang web load
được chưa chứng minh kết nối Kafka. Nếu timeout, kiểm tra VM1 firewall, bootstrap, advertised listeners.
Interface quản trị chỉ dùng mạng lab nội bộ. Không có lệnh tạo external network thủ công cần chạy.
