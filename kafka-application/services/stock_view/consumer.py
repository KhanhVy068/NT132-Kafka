"""Consume inventory.updated into the dedicated PostgreSQL Stock View."""

import logging

from confluent_kafka import KafkaException

from common.kafka_client import commit_message, create_consumer, deserialize_event
from config.kafka_config import GROUP_STOCK_VIEW
from config.topics import TOPIC_INVENTORY_UPDATED
from services.stock_view.database import get_connection, process_event


logger = logging.getLogger(__name__)


def consume_one_message(consumer, conn, message):
    if message.error():
        raise KafkaException(message.error())
    if message.topic() != TOPIC_INVENTORY_UPDATED:
        raise ValueError("Stock View accepts only inventory.updated")
    event = deserialize_event(message)
    result = process_event(conn, event)
    # process_event returns only after the database transaction has committed.
    commit_message(consumer, message)
    logger.info(
        "StockUpdated event_id=%s product_id=%s version=%s updated=%s duplicate=%s",
        event["event_id"], event["data"]["product_id"], event["data"]["version"],
        result["updated"], result["duplicate"],
    )
    return result


def run_consumer(consumer=None, conn=None, stop_after=None):
    if stop_after is not None and (type(stop_after) is not int or stop_after < 0):
        raise ValueError("stop_after must be a nonnegative integer")
    owns_consumer = consumer is None
    owns_conn = conn is None
    try:
        if owns_consumer:
            consumer = create_consumer(
                GROUP_STOCK_VIEW,
                config_overrides={"enable.auto.commit": False, "enable.auto.offset.store": False},
            )
        if owns_conn:
            conn = get_connection()
        consumer.subscribe([TOPIC_INVENTORY_UPDATED])
        count = 0
        while stop_after is None or count < stop_after:
            message = consumer.poll(1.0)
            if message is None:
                continue
            # Propagate every processing failure: advancing to a later message on
            # this partition could otherwise commit over the failed offset.
            consume_one_message(consumer, conn, message)
            count += 1
    finally:
        if owns_consumer and consumer is not None:
            consumer.close()
        if owns_conn and conn is not None:
            conn.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        run_consumer()
    except KeyboardInterrupt:
        logger.info("Stock View stopped")
    except Exception:
        logger.exception("Stock View stopped; failed Kafka offset has not been committed")
        raise


if __name__ == "__main__":
    main()
