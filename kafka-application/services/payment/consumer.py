"""Persist an immutable payment decision, publish it, then commit the source offset."""

import hashlib
import json
import logging

from confluent_kafka import KafkaException

from common.event_validation import require_envelope, require_text
from services.payment.database import get_connection, load_or_create_decision
from common.kafka_client import commit_message, create_consumer, create_producer, deserialize_event
from config.kafka_config import GROUP_PAYMENT
from config.topics import TOPIC_INVENTORY_RESULTS
from services.payment.producer import publish_payment
from services.payment.settings import PaymentSettings


logger = logging.getLogger(__name__)


def simulate_payment(order_id, settings):
    """Stable across processes; every instance must use the same immutable settings."""
    if not isinstance(order_id, str) or not order_id.strip():
        raise ValueError("order_id must not be empty")
    if settings.mode == "success":
        return True
    if settings.mode == "failure":
        return False
    encoded = json.dumps([settings.seed, order_id], ensure_ascii=False).encode("utf-8")
    sample = int.from_bytes(hashlib.sha256(encoded).digest()[:8], "big")
    threshold = int(settings.success_rate * (1 << 64))
    return sample < threshold


def validate_event(event):
    data = require_envelope(event)
    if event.get("event_type") != "InventoryResult":
        raise ValueError("Payment expects InventoryResult")
    require_text(data.get("order_id"), "order_id", 100)
    require_text(data.get("product_id"), "product_id", 50)
    if type(data.get("success")) is not bool:
        raise ValueError("InventoryResult.success must be boolean")


def process_event(producer, event, settings=None, *, conn):
    validate_event(event)
    settings = settings if settings is not None else PaymentSettings.from_env()
    if not event["data"]["success"]:
        return {"skipped": True, "payment_event": None}
    result = load_or_create_decision(conn, event, settings, simulate_payment)
    # Quyết định đã COMMIT. Lỗi enqueue/delivery không làm đổi quyết định này.
    publish_payment(producer, result, timeout=settings.delivery_timeout)
    logger.info("Payment delivered event_id=%s order_id=%s success=%s",
                result["event_id"], result["data"]["order_id"],
                result["data"]["success"])
    return {"skipped": False, "payment_event": result}


def consume_one_message(consumer, producer, message, settings=None, *, conn):
    if message.error():
        raise KafkaException(message.error())
    if message.topic() != TOPIC_INVENTORY_RESULTS:
        raise ValueError("Unexpected Payment topic")
    result = process_event(producer, deserialize_event(message), settings, conn=conn)
    commit_message(consumer, message)
    return result


def run_consumer(consumer=None, producer=None, settings=None, stop_after=None,
                 conn=None):
    settings = settings if settings is not None else PaymentSettings.from_env()
    owns_consumer, owns_producer, owns_conn = (
        consumer is None, producer is None, conn is None)
    try:
        if owns_conn:
            conn = get_connection()
        if owns_consumer:
            consumer = create_consumer(
                GROUP_PAYMENT,
                config_overrides={"enable.auto.commit": False,
                                  "enable.auto.offset.store": False},
            )
        if owns_producer:
            producer = create_producer()
        consumer.subscribe([TOPIC_INVENTORY_RESULTS])
        count = 0
        while stop_after is None or count < stop_after:
            message = consumer.poll(1.0)
            if message is None:
                continue
            consume_one_message(consumer, producer, message, settings, conn=conn)
            count += 1
    finally:
        try:
            if owns_consumer and consumer is not None:
                consumer.close()
        finally:
            try:
                if owns_producer and producer is not None:
                    pending = producer.flush(5)
                    if pending:
                        logger.warning("Payment stopped with %s pending messages", pending)
            finally:
                if owns_conn and conn is not None:
                    conn.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        run_consumer()
    except KeyboardInterrupt:
        logger.info("Payment stopped")
        return 0
    except Exception:
        logger.exception("Payment stopped; failed message offset was not committed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
