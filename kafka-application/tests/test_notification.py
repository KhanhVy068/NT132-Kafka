from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from confluent_kafka import KafkaError, KafkaException

from common.event_schema import payment_result
from common.kafka_client import serialize_event
from config.topics import TOPIC_PAYMENTS_RESULTS
from services.notification.consumer import (
    consume_one_message as _consume_one_message, process_event as _process_event,
    run_consumer as _run_consumer,
)


_UNIT_CONN = object()


def process_event(*args, **kwargs):
    kwargs.setdefault("conn", _UNIT_CONN)
    return _process_event(*args, **kwargs)


def consume_one_message(*args, **kwargs):
    kwargs.setdefault("conn", _UNIT_CONN)
    return _consume_one_message(*args, **kwargs)


def run_consumer(*args, **kwargs):
    kwargs.setdefault("conn", _UNIT_CONN)
    return _run_consumer(*args, **kwargs)


class FakeMessage:
    def __init__(self, event):
        self.event = event

    def value(self):
        return serialize_event(self.event)

    def error(self):
        return None

    def topic(self):
        return TOPIC_PAYMENTS_RESULTS


class NotificationTests(unittest.TestCase):
    def setUp(self):
        self.rows = {}
        def store(conn, event):
            order_id = event["data"]["order_id"]
            created = order_id not in self.rows
            if created:
                self.rows[order_id] = event
            return {"event_id": event["event_id"], "order_id": order_id,
                    "success": event["data"]["success"], "created": created}
        patcher = patch("services.notification.consumer.store_notification", side_effect=store)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_logs_event_order_and_result_then_commits(self):
        event = payment_result("ORD-1", False)
        consumer = Mock()
        consumer.commit.return_value = []
        message = FakeMessage(event)
        with self.assertLogs("services.notification.consumer", level="INFO") as output:
            result = consume_one_message(consumer, message)
        self.assertIn(event["event_id"], output.output[0])
        self.assertIn("ORD-1", output.output[0])
        self.assertIn("success=False", output.output[0])
        self.assertFalse(result["success"])
        consumer.commit.assert_called_once_with(message=message, asynchronous=False)

    def test_invalid_event_is_not_committed(self):
        consumer = Mock()
        event = payment_result("ORD-1", "false")
        with self.assertRaises(ValueError):
            consume_one_message(consumer, FakeMessage(event))
        consumer.commit.assert_not_called()

    def test_processing_error_is_not_committed(self):
        consumer = Mock()
        with patch("services.notification.consumer.process_event", side_effect=RuntimeError("failed")):
            with self.assertRaises(RuntimeError):
                consume_one_message(consumer, FakeMessage(payment_result("ORD-1", True)))
        consumer.commit.assert_not_called()

    def test_replay_creates_one_notification(self):
        event = payment_result("ORD-1", True)
        with self.assertLogs("services.notification.consumer", level="INFO") as output:
            first = process_event(event)
            second = process_event(event)
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertEqual(len(self.rows), 1)
        self.assertEqual(len(output.output), 1)

    def test_partition_commit_error_stops_loop_before_later_offsets(self):
        consumer = Mock()
        event = payment_result("ORD-1", True)
        consumer.poll.side_effect = [FakeMessage(event), FakeMessage(event)]
        consumer.commit.return_value = [
            SimpleNamespace(error=KafkaError(KafkaError._STATE, "partition commit failed")),
        ]
        with self.assertRaises(KafkaException):
            run_consumer(consumer, stop_after=2)
        self.assertEqual(consumer.poll.call_count, 1)
        consumer.commit.assert_called_once()

    def test_service_manual_commit_and_subscription(self):
        consumer = Mock()
        with patch("services.notification.consumer.create_consumer", return_value=consumer) as factory:
            run_consumer(stop_after=0)
        factory.assert_called_once_with(
            "notification-group",
            config_overrides={"enable.auto.commit": False, "enable.auto.offset.store": False},
        )
        consumer.subscribe.assert_called_once_with([TOPIC_PAYMENTS_RESULTS])
        consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
