import logging
from confluent_kafka import KafkaException

from common.event_validation import require_envelope, require_text
from common.kafka_client import commit_message, create_consumer, deserialize_event
from config.kafka_config import GROUP_NOTIFICATION
from config.topics import TOPIC_PAYMENTS_RESULTS
from services.notification.database import get_connection, store_notification

logger = logging.getLogger(__name__)


def process_event(event, *, conn):
    data = require_envelope(event)
    if event.get("event_type") != "PaymentResult":
        raise ValueError("Notification expects PaymentResult")
    require_text(data.get("order_id"), "order_id", 100)
    if type(data.get("success")) is not bool:
        raise ValueError("PaymentResult.success must be boolean")
    result = store_notification(conn, event)
    if result["created"]:
        logger.info("Notification stored event_id=%s order_id=%s success=%s",
                    result["event_id"], result["order_id"], result["success"])
    return result


def consume_one_message(consumer, message, *, conn):
    if message.error():
        raise KafkaException(message.error())
    if message.topic() != TOPIC_PAYMENTS_RESULTS:
        raise ValueError("Unexpected Notification topic")
    result = process_event(deserialize_event(message), conn=conn)
    commit_message(consumer, message)
    return result


def run_consumer(consumer=None, stop_after=None, conn=None):
    owns_consumer, owns_conn = consumer is None, conn is None
    try:
        if owns_conn:
            conn = get_connection()
        if owns_consumer:
            consumer = create_consumer(
                GROUP_NOTIFICATION,
                config_overrides={"enable.auto.commit": False,
                                  "enable.auto.offset.store": False},
            )
        consumer.subscribe([TOPIC_PAYMENTS_RESULTS])
        count = 0
        while stop_after is None or count < stop_after:
            message = consumer.poll(1.0)
            if message is None:
                continue
            consume_one_message(consumer, message, conn=conn)
            count += 1
    finally:
        try:
            if owns_consumer and consumer is not None:
                consumer.close()
        finally:
            if owns_conn and conn is not None:
                conn.close()


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    try:
        run_consumer()
    except KeyboardInterrupt:
        return 0
    except Exception:
        logger.exception("Notification stopped without committing failed offset")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
