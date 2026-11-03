"""Log PaymentResult then commit. Logs have no durable deduplication."""

from datetime import datetime
import logging

from confluent_kafka import KafkaException

from common.kafka_client import commit_message, create_consumer, deserialize_event
from config.kafka_config import GROUP_NOTIFICATION
from config.topics import TOPIC_PAYMENTS_RESULTS


logger = logging.getLogger(__name__)


def process_event(event):
    if not isinstance(event, dict) or event.get("event_type") != "PaymentResult":
        raise ValueError("Notification expects PaymentResult")
    if not isinstance(event.get("event_id"), str) or not event["event_id"].strip():
        raise ValueError("PaymentResult requires event_id")
    timestamp = event.get("timestamp")
    if not isinstance(timestamp, str):
        raise ValueError("PaymentResult requires timestamp")
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError as exc:
        raise ValueError("PaymentResult timestamp must be ISO 8601") from exc
    if parsed.tzinfo is None:
        raise ValueError("PaymentResult timestamp must include a timezone")
    data = event.get("data")
    if not isinstance(data, dict):
        raise ValueError("PaymentResult requires data")
    if not isinstance(data.get("order_id"), str) or not data["order_id"].strip():
        raise ValueError("PaymentResult requires order_id")
    if type(data.get("success")) is not bool:
        raise ValueError("PaymentResult.success must be boolean")
    logger.info(
        "Payment notification event_id=%s order_id=%s success=%s",
        event["event_id"], data["order_id"], data["success"],
    )
    return {"event_id": event["event_id"], "order_id": data["order_id"], "success": data["success"]}


def consume_one_message(consumer, message):
    if message.error():
        raise KafkaException(message.error())
    if message.topic() != TOPIC_PAYMENTS_RESULTS:
        raise ValueError("Notification received an unexpected topic")
    result = process_event(deserialize_event(message))
    commit_message(consumer, message)
    return result


def run_consumer(consumer=None, stop_after=None):
    owns_consumer = consumer is None
    count = 0
    try:
        if owns_consumer:
            consumer = create_consumer(
                GROUP_NOTIFICATION,
                config_overrides={"enable.auto.commit": False, "enable.auto.offset.store": False},
            )
        consumer.subscribe([TOPIC_PAYMENTS_RESULTS])
        while stop_after is None or count < stop_after:
            message = consumer.poll(1.0)
            if message is None:
                continue
            consume_one_message(consumer, message)
            count += 1
    finally:
        if owns_consumer and consumer is not None:
            consumer.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        run_consumer()
    except KeyboardInterrupt:
        logger.info("Notification stopped")
        return 0
    except Exception:
        logger.exception("Notification stopped; failed message offset was not committed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
