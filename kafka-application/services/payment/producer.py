"""Publish the common PaymentResult envelope and wait for broker delivery."""

import math
import time

from common.kafka_client import serialize_event
from config.topics import TOPIC_PAYMENTS_RESULTS


def publish_payment(producer, event, timeout=15.0):
    """Return only after a positive delivery callback; never infer ack from produce()."""
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (float, int))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise ValueError("timeout must be finite and positive")
    if not isinstance(event, dict) or event.get("event_type") != "PaymentResult":
        raise ValueError("Only PaymentResult can be published here")
    data = event.get("data")
    if not isinstance(data, dict):
        raise ValueError("PaymentResult requires data")
    order_id = data.get("order_id")
    if not isinstance(order_id, str) or not order_id.strip():
        raise ValueError("PaymentResult requires order_id")
    if type(data.get("success")) is not bool:
        raise ValueError("PaymentResult.success must be boolean")

    result = {"completed": False, "error": None}

    def on_delivery(error, message):
        result["completed"] = True
        result["error"] = error

    deadline = time.monotonic() + timeout
    payload = serialize_event(event)
    while True:
        if time.monotonic() >= deadline:
            raise TimeoutError("PaymentResult enqueue timed out; offset remains uncommitted")
        try:
            producer.produce(
                topic=TOPIC_PAYMENTS_RESULTS,
                key=order_id.encode("utf-8"),
                value=payload,
                callback=on_delivery,
            )
            break
        except BufferError:
            producer.poll(min(0.1, max(0, deadline - time.monotonic())))

    while not result["completed"]:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("PaymentResult delivery is unknown; offset remains uncommitted")
        producer.poll(min(0.1, remaining))

    if result["error"] is not None:
        raise RuntimeError(f"PaymentResult delivery failed: {result['error']}")
    return event
