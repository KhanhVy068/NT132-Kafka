# Tạo các hàm chung để khởi tạo Producer, Consumer, mã hóa và giải mã Message.
import json

from confluent_kafka import Producer, Consumer

from config.kafka_config import (
    PRODUCER_CONFIG,
    get_consumer_config
)


# 1. Tạo Kafka Producer
def create_producer():
    return Producer(PRODUCER_CONFIG.copy())


# 2. Tạo Kafka Consumer
def create_consumer(group_id):
    return Consumer(get_consumer_config(group_id))


# 3. Mã hóa Event thành JSON bytes
def serialize_event(event):
    return json.dumps(
        event,
        ensure_ascii=False
    ).encode("utf-8")


# 4. Giải mã Message nhận từ Kafka
def deserialize_event(message):
    return json.loads(
        message.value().decode("utf-8")
    )


# 5. Callback khi gửi Message
def delivery_report(err, msg):
    if err is not None:
        print(f"[FAILED] {err}")
    else:
        print(
            f"[SUCCESS] Topic={msg.topic()} "
            f"Partition={msg.partition()} "
            f"Offset={msg.offset()}"
        )


# 6. Gửi Event đến Kafka
def send_event(producer, topic, key, event):
    producer.produce(
        topic=topic,
        key=str(key).encode("utf-8"),
        value=serialize_event(event),
        callback=delivery_report
    )

    # Phục vụ callback, không chờ Broker phản hồi ngay
    producer.poll(0)
