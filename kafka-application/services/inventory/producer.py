from contextlib import contextmanager
import json
import logging
import time

from common.kafka_client import create_producer
from common.service_database import require_idle
from services.inventory.database import (
    get_connection,
    get_pending_outbox,
    mark_outbox_published,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)


@contextmanager
def publisher_lock(conn):
    """Hold the singleton lock until this dedicated connection closes.

    Use only inside the connection context in run_publisher: producer flush must
    finish before the session closes. A session lock is not a Kafka fencing token.
    """
    require_idle(conn)
    row = conn.execute(
        "SELECT pg_try_advisory_lock(%s, %s) AS acquired", (132, 1)
    ).fetchone()
    if not row["acquired"]:
        raise RuntimeError("Another Inventory Outbox Publisher is active")
    yield


def publish_one_event(producer, event, timeout=15):
    """
    Gửi 1 Event đến Kafka.

    Chỉ trả True khi Delivery Callback
    xác nhận Kafka nhận Message thành công.
    """
    result = {
        "completed": False,
        "success": False,
        "error": None,
    }

    def delivery_callback(err, msg):
        result["completed"] = True

        if err is not None:
            result["error"] = str(err)
        else:
            result["success"] = True

    payload = event["payload"]

    if isinstance(payload, str):
        payload = json.loads(payload)

    if not isinstance(payload, dict):
        raise ValueError("Outbox payload không hợp lệ")

    value = json.dumps(
        payload,
        ensure_ascii=False,
    ).encode("utf-8")

    key = str(event["message_key"]).encode("utf-8")
    topic = event["topic"]

    producer.produce(
        topic=topic,
        key=key,
        value=value,
        callback=delivery_callback,
    )

    # Kafka Producer gửi bất đồng bộ.
    # poll() xử lý Delivery Callback.
    deadline = time.monotonic() + timeout

    while not result["completed"]:
        remaining = deadline - time.monotonic()

        if remaining <= 0:
            logger.error(
                "Hết thời gian chờ Kafka: event_id=%s",
                event["event_id"],
            )
            return False

        producer.poll(min(0.5, remaining))

    if not result["success"]:
        logger.error(
            "Kafka gửi thất bại: %s",
            result["error"],
        )
        return False

    logger.info(
        "Kafka xác nhận Event %s -> %s",
        event["event_id"],
        topic,
    )
    return True


def publish_pending_events(conn, producer, limit=100):
    """
    Gửi tối đa limit Event chưa publish.

    Gửi tuần tự theo Outbox ID.
    Nếu một Event thất bại, dừng batch
    để tránh gửi Event sau vượt trước.
    """
    events = get_pending_outbox(conn, limit=limit)
    published_count = 0

    for event in events:
        try:
            success = publish_one_event(
                producer,
                event,
            )

        except Exception:
            logger.exception(
                "Lỗi gửi Event %s",
                event["event_id"],
            )
            break

        if not success:
            logger.warning(
                "Event %s vẫn giữ trong Outbox",
                event["event_id"],
            )
            break

        # Chỉ đánh dấu sau Delivery thành công.
        try:
            mark_outbox_published(
                conn,
                event["event_id"],
            )
        except Exception:
            logger.exception(
                "Kafka đã nhận nhưng DB chưa ghi "
                "published_at: event_id=%s",
                event["event_id"],
            )
            break

        published_count += 1

    return published_count


def run_publisher(interval=2):
    if interval <= 0:
        raise ValueError("interval must be positive")
    with get_connection() as conn:
        with publisher_lock(conn):
            producer = create_producer()
            try:
                while True:
                    # Cùng connection đang giữ lock, không dùng connection pool khác.
                    conn.execute("SELECT 1")
                    count = publish_pending_events(conn, producer, limit=100)
                    if count:
                        logger.info("Published %s event(s)", count)
                    else:
                        time.sleep(interval)
            except KeyboardInterrupt:
                logger.info("Stopping Inventory Publisher")
            finally:
                pending = producer.flush(10)
                if pending:
                    logger.warning("Stopped with %s unconfirmed events", pending)


def main():
    run_publisher()


if __name__ == "__main__":
    main()
