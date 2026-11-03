import json
import os
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from confluent_kafka import KafkaError, KafkaException

from common.event_schema import inventory_result, payment_result
from common.kafka_client import serialize_event
from config.topics import TOPIC_INVENTORY_RESULTS, TOPIC_PAYMENTS_RESULTS
from services.payment.consumer import (
    consume_one_message, process_event, run_consumer, simulate_payment,
)
from services.payment.producer import publish_payment
from services.payment.settings import PaymentSettings


class FakeMessage:
    def __init__(self, event):
        self.event = event

    def value(self):
        return serialize_event(self.event)

    def topic(self):
        return TOPIC_INVENTORY_RESULTS

    def error(self):
        return None


class FakeProducer:
    def __init__(self, error=None, fail_enqueue=False, full_once=False):
        self.error = error
        self.fail_enqueue = fail_enqueue
        self.full_once = full_once
        self.sent = []
        self.pending = []

    def produce(self, **kwargs):
        if self.fail_enqueue:
            raise RuntimeError("enqueue failed")
        if self.full_once:
            self.full_once = False
            raise BufferError("queue full")
        self.sent.append(kwargs)
        self.pending.append(kwargs["callback"])

    def poll(self, timeout):
        if self.pending:
            self.pending.pop(0)(self.error, None)


class PaymentTests(unittest.TestCase):
    def setUp(self):
        self.event = inventory_result("ORD-1", "P01", True)

    def test_rejected_inventory_does_not_publish_payment_but_commits(self):
        consumer, producer = Mock(), FakeProducer()
        consumer.commit.return_value = []
        event = inventory_result("ORD-1", "P01", False)
        message = FakeMessage(event)
        result = consume_one_message(consumer, producer, message, PaymentSettings())
        self.assertTrue(result["skipped"])
        self.assertEqual(producer.sent, [])
        consumer.commit.assert_called_once_with(message=message, asynchronous=False)

    def test_forced_success_and_failure(self):
        for mode, expected in [("success", True), ("failure", False)]:
            with self.subTest(mode=mode):
                producer = FakeProducer()
                result = process_event(producer, self.event, PaymentSettings(mode=mode))
                self.assertIs(result["payment_event"]["data"]["success"], expected)
                self.assertEqual(producer.sent[0]["topic"], TOPIC_PAYMENTS_RESULTS)
                self.assertEqual(producer.sent[0]["key"], b"ORD-1")
                self.assertEqual(json.loads(producer.sent[0]["value"]), result["payment_event"])

    def test_same_order_has_same_result_and_event_identity_on_replay(self):
        settings = PaymentSettings(seed="test-seed", success_rate=0.52)
        first = process_event(FakeProducer(), self.event, settings)["payment_event"]
        replay = process_event(FakeProducer(), self.event, settings)["payment_event"]
        second_source = inventory_result("ORD-1", "P01", True)
        repeated_order = process_event(FakeProducer(), second_source, settings)["payment_event"]
        self.assertEqual(first, replay)
        self.assertNotEqual(first["event_id"], repeated_order["event_id"])
        self.assertEqual(first["data"], repeated_order["data"])

    def test_distinct_source_times_keep_output_event_ids_immutable(self):
        settings = PaymentSettings(mode="failure")
        self.event["timestamp"] = "2026-11-03T00:00:00+00:00"
        next_source = inventory_result("ORD-1", "P01", True)
        next_source["timestamp"] = "2026-11-04T00:00:00+00:00"
        first = process_event(FakeProducer(), self.event, settings)["payment_event"]
        second = process_event(FakeProducer(), next_source, settings)["payment_event"]
        self.assertEqual(first["data"], second["data"])
        self.assertNotEqual(first["timestamp"], second["timestamp"])
        self.assertNotEqual(first["event_id"], second["event_id"])
        self.assertEqual(first, process_event(FakeProducer(), self.event, settings)["payment_event"])
        self.assertEqual(second, process_event(FakeProducer(), next_source, settings)["payment_event"])

    def test_result_is_stable_across_fresh_processes_and_python_hash_seeds(self):
        code = (
            "from services.payment.consumer import simulate_payment; "
            "from services.payment.settings import PaymentSettings; "
            "print([simulate_payment('ORD-'+str(i), PaymentSettings(seed='restart')) "
            "for i in range(40)])"
        )
        outputs = []
        for hash_seed in ["1", "991"]:
            env = dict(os.environ, PYTHONHASHSEED=hash_seed)
            outputs.append(subprocess.check_output([sys.executable, "-c", code], env=env))
        self.assertEqual(outputs[0], outputs[1])
        self.assertIn(b"True", outputs[0])
        self.assertIn(b"False", outputs[0])

    def test_rate_boundaries(self):
        self.assertFalse(simulate_payment("ORD-1", PaymentSettings(success_rate=0)))
        self.assertTrue(simulate_payment("ORD-1", PaymentSettings(success_rate=1)))

    def test_settings_reject_nonfinite_invalid_values(self):
        for rate in [-0.1, 1.1, float("nan"), float("inf"), True]:
            with self.subTest(rate=rate), self.assertRaises(ValueError):
                PaymentSettings(success_rate=rate)
        for timeout in [0, -1, float("inf"), float("nan"), True]:
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                PaymentSettings(delivery_timeout=timeout)
        with self.assertRaises(ValueError):
            PaymentSettings(mode="unknown")
        with self.assertRaises(ValueError):
            PaymentSettings(seed="")

    def test_env_settings(self):
        env = {
            "PAYMENT_MODE": "failure", "PAYMENT_SUCCESS_RATE": "0.25",
            "PAYMENT_SEED": "test", "PAYMENT_DELIVERY_TIMEOUT": "4",
        }
        with patch.dict(os.environ, env):
            self.assertEqual(PaymentSettings.from_env(), PaymentSettings("failure", 0.25, "test", 4))

    def test_commit_happens_only_after_delivery_callback(self):
        consumer, producer = Mock(), FakeProducer()
        consumer.commit.return_value = []
        original_poll = producer.poll

        def poll(timeout):
            consumer.commit.assert_not_called()
            original_poll(timeout)

        producer.poll = poll
        message = FakeMessage(self.event)
        consume_one_message(consumer, producer, message, PaymentSettings())
        consumer.commit.assert_called_once_with(message=message, asynchronous=False)

    def test_immediate_or_delivery_failure_does_not_commit(self):
        for producer in [FakeProducer(fail_enqueue=True), FakeProducer(error="broker error")]:
            consumer = Mock()
            with self.subTest(producer=producer), self.assertRaises(RuntimeError):
                consume_one_message(consumer, producer, FakeMessage(self.event), PaymentSettings())
            consumer.commit.assert_not_called()

    def test_delivery_timeout_does_not_commit(self):
        producer, consumer = Mock(), Mock()
        with patch("services.payment.producer.time.monotonic", side_effect=[0, 0, 2]):
            with self.assertRaises(TimeoutError):
                consume_one_message(
                    consumer, producer, FakeMessage(self.event), PaymentSettings(delivery_timeout=1),
                )
        producer.produce.assert_called_once()
        consumer.commit.assert_not_called()

    def test_queue_full_is_polled_and_retried(self):
        producer = FakeProducer(full_once=True)
        event = payment_result("ORD-1", True)
        self.assertEqual(publish_payment(producer, event), event)
        self.assertEqual(len(producer.sent), 1)

    def test_queue_full_timeout_does_not_commit(self):
        producer, consumer = Mock(), Mock()
        producer.produce.side_effect = BufferError("full")
        with patch("services.payment.producer.time.monotonic", side_effect=[0, 0, 0, 2]):
            with self.assertRaises(TimeoutError):
                consume_one_message(
                    consumer, producer, FakeMessage(self.event), PaymentSettings(delivery_timeout=1),
                )
        consumer.commit.assert_not_called()

    def test_failure_stops_loop_before_later_offsets(self):
        consumer = Mock()
        consumer.poll.side_effect = [FakeMessage(self.event), FakeMessage(self.event)]
        with self.assertRaises(RuntimeError):
            run_consumer(consumer, FakeProducer(fail_enqueue=True), PaymentSettings(), stop_after=2)
        self.assertEqual(consumer.poll.call_count, 1)
        consumer.commit.assert_not_called()

    def test_partition_commit_error_stops_loop_before_later_offsets(self):
        consumer, producer = Mock(), FakeProducer()
        consumer.poll.side_effect = [FakeMessage(self.event), FakeMessage(self.event)]
        consumer.commit.return_value = [
            SimpleNamespace(error=KafkaError(KafkaError._STATE, "partition commit failed")),
        ]
        with self.assertRaises(KafkaException):
            run_consumer(consumer, producer, PaymentSettings(), stop_after=2)
        self.assertEqual(consumer.poll.call_count, 1)
        consumer.commit.assert_called_once()
        self.assertEqual(len(producer.sent), 1)

    def test_invalid_event_does_not_commit(self):
        consumer = Mock()
        self.event["data"]["success"] = "true"
        with self.assertRaises(ValueError):
            consume_one_message(consumer, FakeProducer(), FakeMessage(self.event), PaymentSettings())
        consumer.commit.assert_not_called()

    def test_service_disables_auto_commit_and_auto_offset_store(self):
        consumer, producer = Mock(), Mock()
        producer.flush.return_value = 0
        with patch("services.payment.consumer.create_consumer", return_value=consumer) as factory:
            with patch("services.payment.consumer.create_producer", return_value=producer):
                run_consumer(settings=PaymentSettings(), stop_after=0)
        factory.assert_called_once_with(
            "payment-group",
            config_overrides={"enable.auto.commit": False, "enable.auto.offset.store": False},
        )
        consumer.subscribe.assert_called_once_with([TOPIC_INVENTORY_RESULTS])
        consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
