
import logging
import json

from confluent_kafka import KafkaException

from common.kafka_client import (
    create_consumer,
    commit_message,
    deserialize_event,
)
from services.inventory.database import (
    get_connection,
    reserve_stock,
    release_stock,
    record_message_error,
)

from config.kafka_config import GROUP_INVENTORY
from config.topics import (
    TOPIC_ORDERS,
    TOPIC_PAYMENTS_RESULTS as TOPIC_PAYMENTS,
)
from common.service_database import require_idle
from common.event_validation import (
    InvalidEvent, require_envelope, require_text, require_quantity,
)

logger = logging.getLogger(__name__)


def validate_event(event, topic):
    data = require_envelope(event)
    require_text(data.get("order_id"), "order_id", 100)
    if topic == TOPIC_ORDERS:
        if event.get("event_type") != "OrderCreated":
            raise InvalidEvent("Expected OrderCreated")
        require_text(data.get("product_id"), "product_id", 50)
        require_quantity(data.get("quantity"))
    elif topic == TOPIC_PAYMENTS:
        if event.get("event_type") != "PaymentResult":
            raise InvalidEvent("Expected PaymentResult")
        if type(data.get("success")) is not bool:
            raise InvalidEvent("success must be boolean")
    else:
        raise InvalidEvent("Unsupported topic")


def process_event(conn, event, topic):
    """
    Xử lý một Event trong PostgreSQL.

    - Ghi processed_events và nghiệp vụ trong
      cùng Transaction.
    - Nếu đã xử lý Event ID thì bỏ qua.
    - Không thực hiện Kafka Offset Commit ở đây.
    """
    require_idle(conn)
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
    if message.error():
        raise KafkaException(message.error())
    topic = message.topic()
    try:
        if message.value() is None:
            raise InvalidEvent("Tombstone is unsupported for business topics")
        event = deserialize_event(message)
        result = process_event(conn=conn, event=event, topic=topic)
    except (InvalidEvent, json.JSONDecodeError, UnicodeDecodeError) as exc:
        # process_event đã rollback trước khi tới đây.
        # record_message_error chỉ return sau top-level COMMIT thành công.
        record_message_error(conn, message, exc)
        logger.error(
            "Quarantined topic=%s partition=%s offset=%s error=%s",
            topic, message.partition(), message.offset(), type(exc).__name__,
        )
        result = {"processed": False, "quarantined": True}

    commit_message(consumer, message)
    logger.info("Committed Kafka offset: topic=%s partition=%s offset=%s",
                topic, message.partition(), message.offset())
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

    count = 0
    try:
        if owns_consumer:
            consumer = create_consumer(
                GROUP_INVENTORY,
                config_overrides={"enable.auto.commit": False, "enable.auto.offset.store": False},
            )

        if owns_conn:
            conn = get_connection()

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
        try:
            if owns_consumer and consumer is not None:
                consumer.close()
        finally:
            if owns_conn and conn is not None:
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
