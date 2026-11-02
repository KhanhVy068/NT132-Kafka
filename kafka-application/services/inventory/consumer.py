
import logging
import time

from confluent_kafka import KafkaException

from common.kafka_client import (
    create_consumer,
    deserialize_event,
)
from services.inventory.database import (
    get_connection,
    reserve_stock,
    release_stock,
)

logger = logging.getLogger(__name__)

TOPIC_ORDERS = "orders.created"
TOPIC_PAYMENTS = "payments.results"
GROUP_INVENTORY = "inventory-group"


def validate_event(event, topic):
    """
    Kiểm tra cấu trúc Event trước khi
    thực hiện thao tác PostgreSQL.
    """
    if not isinstance(event, dict):
        raise ValueError("Event phải là JSON object")

    event_id = event.get("event_id")
    event_type = event.get("event_type")
    data = event.get("data")

    if not isinstance(event_id, str) or not event_id.strip():
        raise ValueError("Thiếu event_id hợp lệ")

    if not isinstance(data, dict):
        raise ValueError("Thiếu data hợp lệ")

    if topic == TOPIC_ORDERS:
        if event_type != "OrderCreated":
            raise ValueError("Event không khớp orders.created")

        if (
            not isinstance(data.get("order_id"), str)
            or not data["order_id"].strip()
        ):
            raise ValueError("order_id không hợp lệ")

        if (
            not isinstance(data.get("product_id"), str)
            or not data["product_id"].strip()
        ):
            raise ValueError("product_id không hợp lệ")

        quantity = data.get("quantity")

        if type(quantity) is not int or quantity <= 0:
            raise ValueError("quantity phải là số nguyên dương")

    elif topic == TOPIC_PAYMENTS:
        if event_type != "PaymentResult":
            raise ValueError("Event không khớp payments.results")

        if (
            not isinstance(data.get("order_id"), str)
            or not data["order_id"].strip()
        ):
            raise ValueError("order_id không hợp lệ")

        if type(data.get("success")) is not bool:
            raise ValueError("success phải là boolean")

    else:
        raise ValueError(f"Topic không được hỗ trợ: {topic}")


def process_event(conn, event, topic):
    """
    Xử lý một Event trong PostgreSQL.

    - Ghi processed_events và nghiệp vụ trong
      cùng Transaction.
    - Nếu đã xử lý Event ID thì bỏ qua.
    - Không thực hiện Kafka Offset Commit ở đây.
    """
    validate_event(event, topic)

    event_id = event["event_id"]
    data = event["data"]

    with conn.transaction():
        inserted = conn.execute(
            """
            INSERT INTO processed_events (event_id)
            VALUES (%s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            (event_id,),
        ).fetchone()

        if inserted is None:
            logger.info(
                "Duplicate Event ignored: %s",
                event_id,
            )
            return {
                "processed": False,
                "duplicate": True,
            }

        if topic == TOPIC_ORDERS:
            result = reserve_stock(
                conn,
                order_id=data["order_id"],
                product_id=data["product_id"],
                quantity=data["quantity"],
            )

            logger.info(
                "Order %s: %s",
                data["order_id"],
                result["status"],
            )

        elif topic == TOPIC_PAYMENTS:
            if data["success"] is False:
                result = release_stock(
                    conn,
                    order_id=data["order_id"],
                )
                logger.info(
                    "Payment failed; release %s: %s",
                    data["order_id"],
                    result,
                )
            else:
                # Không hoàn trả khi thanh toán thành công
                result = {
                    "payment_success": True,
                    "order_id": data["order_id"],
                }

        return {
            "processed": True,
            "duplicate": False,
            "result": result,
        }

def consume_one_message(consumer, conn, message):
    """
    Xử lý một Kafka Message và commit Offset
    sau khi PostgreSQL Transaction thành công.
    """
    if message.error():
        raise KafkaException(message.error())

    topic = message.topic()

    # Hàm có sẵn trong common/kafka_client.py
    event = deserialize_event(message)

    result = process_event(
        conn=conn,
        event=event,
        topic=topic,
    )

    # Manual Commit: chỉ chạy sau khi
    # PostgreSQL đã commit thành công.
    consumer.commit(
        message=message,
        asynchronous=False,
    )

    logger.info(
        "Committed Kafka offset: topic=%s "
        "partition=%s offset=%s",
        topic,
        message.partition(),
        message.offset(),
    )

    return result


def run_consumer(
    consumer=None,
    conn=None,
    stop_after=None,
):
    """
    Chạy Inventory Consumer.

    stop_after dùng cho Unit Test, không cần
    trong môi trường chạy liên tục.
    """
    owns_consumer = consumer is None
    owns_conn = conn is None

    if owns_consumer:
        consumer = create_consumer(GROUP_INVENTORY)

    if owns_conn:
        conn = get_connection()

    count = 0

    try:
        consumer.subscribe([
            TOPIC_ORDERS,
            TOPIC_PAYMENTS,
        ])

        logger.info(
            "Inventory Consumer subscribed to %s, %s",
            TOPIC_ORDERS,
            TOPIC_PAYMENTS,
        )

        while stop_after is None or count < stop_after:
            message = consumer.poll(1.0)

            if message is None:
                continue

            consume_one_message(
                consumer,
                conn,
                message,
            )

            count += 1

    finally:
        if owns_consumer:
            consumer.close()

        if owns_conn:
            conn.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    logger.info("Starting Inventory Consumer")

    try:
        run_consumer()

    except KeyboardInterrupt:
        logger.info("Inventory Consumer stopped")

    except Exception:
        logger.exception(
            "Inventory Consumer stopped due to error. "
            "Unprocessed offset was not committed."
        )
        raise


if __name__ == "__main__":
    main()
