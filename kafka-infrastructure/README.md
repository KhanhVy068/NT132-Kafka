# Kafka infrastructure — VM1

`docker-compose.yml` chạy một official `apache/kafka:4.1.2` node KRaft combined broker/controller,
với named volume `kafka_data`. Cấu hình tham khảo [Apache single-node example](https://github.com/apache/kafka/blob/4.1.2/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml).
Đây là topology lab RF=1, không có HA.

## Giai đoạn 1: chuẩn bị configuration

Copy `.env.example` thành `.env` chỉ khi chưa có. Đặt KAFKA_ADVERTISED_HOST thành IP/hostname
VM1 truy cập được từ VM2. KAFKA_CLUSTER_ID phải ổn định với volume đã format; sample ID chỉ dùng
cho cluster lab mới. Không đổi identity hoặc xóa volume để chữa startup failure.

- EXTERNAL: VM1_IP:9092, cho Windows/VM2/Kafbat/application.
- INTERNAL: kafka:19092, chỉ trong network Compose VM1.
- CONTROLLER: kafka:9093, không publish ra host.

Sau khi được phép dùng Docker CLI, CMD có thể validate mà không khởi động container:

```bat
docker compose --env-file .env.example config --quiet
```

Python unit parser tests từ `kafka-application`:

```bat
.venv\Scripts\python.exe -m unittest tests.test_topic_script -v
```

## Giai đoạn 2: deploy trên Ubuntu VM1

Chỉ chạy khi mạng/volume đã được kiểm tra và được phép triển khai:

```bash
docker compose up -d --wait --wait-timeout 180 kafka
docker compose logs --tail 100 kafka
docker compose exec -T kafka sh -c 'PATH=/opt/kafka/bin:$PATH BOOTSTRAP_SERVER=kafka:19092 bash /work/topics/init_topics.sh'
docker compose exec -T kafka /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka:19092 describe --status
```

Bốn topic orders.created, inventory.results, payments.results, inventory.updated: 2 partitions, RF=1.
Init có tính idempotent; parse giá trị chính xác, topic sai cấu hình thì exit khác 0; không tự xóa/sửa
topic. Chạy init hai lần để nghiệm thu. Mở TCP 9092 trong mạng nội bộ để VM2 kết nối;
client VM2 phải nhận metadata EXTERNAL là IP VM1, không phải localhost hay hostname container.

Các CLI scripts được mount read-only trong `/work/scripts`; chạy với PATH `/opt/kafka/bin` và
BOOTSTRAP_SERVER=kafka:19092 như init ở trên. Từ host có Kafka CLI, đặt bootstrap VM1_IP:9092.
`check_consumer_groups.sh` hiển thị offset/lag/member cho các group.

Benchmark raw payload phải dùng topic test riêng đã provision, ví dụ BENCHMARK_TOPIC=broker.benchmark;
script không còn mặc định ghi payload không đúng JSON contract vào orders.created. Benchmark Broker
không thay thế Flash Sale đối soát ứng dụng. Không tự tạo topic benchmark ở môi trường đang dùng chung.

Nghiệm thu cần health, metadata từ VM2, topic/offset và data còn nguyên sau restart. Các bước runtime
này chưa được chạy trong lượt sửa source. Xem [deploy hai VM](../docs/deployment-two-vms.md).
