"""InventoryResult -> deterministic simulated PaymentResult, at least once."""

from datetime import datetime
import hashlib
import json
import logging
from uuid import NAMESPACE_URL, uuid5

from confluent_kafka import KafkaException

from common.event_schema import payment_result
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
    if not isinstance(event, dict) or event.get("event_type") != "InventoryResult":
        raise ValueError("Payment expects InventoryResult")
    if not isinstance(event.get("event_id"), str) or not event["event_id"].strip():
        raise ValueError("InventoryResult requires event_id")
    timestamp = event.get("timestamp")
    if not isinstance(timestamp, str):
        raise ValueError("InventoryResult requires timestamp")
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError as exc:
        raise ValueError("InventoryResult timestamp must be ISO 8601") from exc
    if parsed.tzinfo is None:
        raise ValueError("InventoryResult timestamp must include a timezone")
    data = event.get("data")
    if not isinstance(data, dict):
        raise ValueError("InventoryResult requires data")
    if not isinstance(data.get("order_id"), str) or not data["order_id"].strip():
        raise ValueError("InventoryResult requires order_id")
    if type(data.get("success")) is not bool:
        raise ValueError("InventoryResult.success must be boolean")


def process_event(producer, event, settings=None):
    """Pure decision plus injected publisher; Kafka offsets belong to the caller."""
    validate_event(event)
    settings = settings if settings is not None else PaymentSettings.from_env()
    data = event["data"]
    if not data["success"]:
        logger.info("Inventory rejected order_id=%s; payment skipped", data["order_id"])
        return {"skipped": True, "payment_event": None}

    order_id = data["order_id"]
    result = payment_result(order_id, simulate_payment(order_id, settings))
    # Redelivery of one immutable source event must recreate the same payment event.
    # Different source events get distinct IDs, even when they refer to one order.
    # Keep settings unchanged while orders are replayable: this is a simulator, not a ledger.
    identity = json.dumps([order_id, event["event_id"]], ensure_ascii=False, separators=(",", ":"))
    result["event_id"] = str(uuid5(NAMESPACE_URL, f"NT132-Kafka/PaymentResult/v2/{identity}"))
    # Preserve the source's time so redelivery of the same InventoryResult is byte-stable.
    result["timestamp"] = event["timestamp"]
    publish_payment(producer, result, timeout=settings.delivery_timeout)
    logger.info(
        "PaymentResult delivered event_id=%s order_id=%s success=%s",
        result["event_id"], order_id, result["data"]["success"],
    )
    return {"skipped": False, "payment_event": result}


def consume_one_message(consumer, producer, message, settings=None):
    if message.error():
        raise KafkaException(message.error())
    if message.topic() != TOPIC_INVENTORY_RESULTS:
        raise ValueError("Payment received an unexpected topic")
    result = process_event(producer, deserialize_event(message), settings)
    commit_message(consumer, message)
    return result


def run_consumer(consumer=None, producer=None, settings=None, stop_after=None):
    settings = settings if settings is not None else PaymentSettings.from_env()
    owns_consumer = consumer is None
    owns_producer = producer is None
    count = 0
    try:
        if owns_consumer:
            consumer = create_consumer(
                GROUP_PAYMENT,
                config_overrides={"enable.auto.commit": False, "enable.auto.offset.store": False},
            )
        if owns_producer:
            producer = create_producer()
        consumer.subscribe([TOPIC_INVENTORY_RESULTS])
        while stop_after is None or count < stop_after:
            message = consumer.poll(1.0)
            if message is None:
                continue
            # Let errors stop the loop. Processing later offsets could skip a failed event.
            consume_one_message(consumer, producer, message, settings)
            count += 1
    finally:
        if owns_consumer and consumer is not None:
            consumer.close()
        if owns_producer and producer is not None:
            pending = producer.flush(5.0)
            if pending:
                logger.warning("Payment stopped with %s unconfirmed Kafka message(s)", pending)


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
