from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch

from confluent_kafka import KafkaError, KafkaException
from psycopg.pq import TransactionStatus

from common.event_validation import InvalidEvent
from common.event_schema import order_created, payment_result, inventory_result, stock_updated
from common.kafka_client import serialize_event
from config.topics import TOPIC_ORDERS
from services.inventory import consumer as inventory
from services.inventory import producer as outbox
from services.order.producer import create_order
from services.payment import consumer as payment
from services.payment.settings import PaymentSettings
from services.notification import consumer as notification


def message(event):
    result = Mock()
    result.error.return_value = None
    result.topic.return_value = TOPIC_ORDERS
    result.partition.return_value = 0
    result.offset.return_value = 7
    result.key.return_value = b"audit-order"
    result.value.return_value = serialize_event(event)
    return result


class InventoryOffsetTests(unittest.TestCase):
    def setUp(self):
        self.event = order_created("audit-order", "P01", 1)
        self.message = message(self.event)
        self.consumer = Mock()
        self.consumer.commit.return_value = []

    def test_forces_manual_config(self):
        with patch.object(inventory, "create_consumer") as factory:
            inventory.run_consumer(conn=Mock(), stop_after=0)
        self.assertEqual(factory.call_args.kwargs["config_overrides"], {
            "enable.auto.commit": False, "enable.auto.offset.store": False})

    def test_processing_finishes_before_kafka_commit(self):
        sequence = []
        def processed(**kwargs):
            sequence.append("database-committed")
            return {"processed": True}
        self.consumer.commit.side_effect = lambda **kw: sequence.append("kafka-commit") or []
        with patch.object(inventory, "process_event", side_effect=processed):
            inventory.consume_one_message(self.consumer, Mock(), self.message)
        self.assertEqual(sequence, ["database-committed", "kafka-commit"])

    def test_transient_failure_does_not_commit(self):
        with patch.object(inventory, "process_event", side_effect=RuntimeError("DB unavailable")):
            with self.assertRaises(RuntimeError):
                inventory.consume_one_message(self.consumer, Mock(), self.message)
        self.consumer.commit.assert_not_called()

    def test_partition_error_is_not_logged_as_committed(self):
        self.consumer.commit.return_value = [
            SimpleNamespace(error=KafkaError(KafkaError._TIMED_OUT))]
        with patch.object(inventory, "process_event", return_value={"processed": True}):
            with self.assertNoLogs(inventory.logger, level="INFO"):
                with self.assertRaises(KafkaException):
                    inventory.consume_one_message(self.consumer, Mock(), self.message)

    def test_quarantine_commits_before_offset(self):
        sequence = []
        self.consumer.commit.side_effect = lambda **kw: sequence.append("offset") or []
        with patch.object(inventory, "process_event", side_effect=InvalidEvent("unknown product")):
            with patch.object(inventory, "record_message_error",
                              side_effect=lambda *args: sequence.append("error-committed")):
                result = inventory.consume_one_message(self.consumer, Mock(), self.message)
        self.assertTrue(result["quarantined"])
        self.assertEqual(sequence, ["error-committed", "offset"])

    def test_quarantine_storage_failure_does_not_commit(self):
        with patch.object(inventory, "process_event", side_effect=InvalidEvent("bad input")):
            with patch.object(inventory, "record_message_error", side_effect=RuntimeError("DB down")):
                with self.assertRaises(RuntimeError):
                    inventory.consume_one_message(self.consumer, Mock(), self.message)
        self.consumer.commit.assert_not_called()

    def test_outer_transaction_is_rejected(self):
        conn = Mock()
        conn.info.transaction_status = TransactionStatus.INTRANS
        with self.assertRaises(RuntimeError):
            inventory.process_event(conn, self.event, TOPIC_ORDERS)
        conn.execute.assert_not_called()

    def test_programming_value_error_is_not_quarantined(self):
        with patch.object(inventory, "process_event", side_effect=ValueError("programming error")):
            with patch.object(inventory, "record_message_error") as store:
                with self.assertRaises(ValueError):
                    inventory.consume_one_message(self.consumer, Mock(), self.message)
        store.assert_not_called()
        self.consumer.commit.assert_not_called()

    def test_invalid_json_utf8_and_tombstone_are_saved_before_commit(self):
        for raw in (b"not-json", bytes([255]), None):
            with self.subTest(payload=raw):
                msg = message(self.event)
                msg.value.return_value = raw
                consumer = Mock()
                consumer.commit.return_value = []
                with patch.object(inventory, "process_event") as process:
                    with patch.object(inventory, "record_message_error") as store:
                        result = inventory.consume_one_message(consumer, Mock(), msg)
                self.assertTrue(result["quarantined"])
                process.assert_not_called()
                self.assertIs(store.call_args.args[1], msg)
                consumer.commit.assert_called_once_with(message=msg, asynchronous=False)


class ValidationTests(unittest.TestCase):
    def test_order_bounds(self):
        for product, quantity, order in [
            ("P"*51, 1, "o"), ("P01", 1, "o"*101),
            ("P01", True, "o"), ("P01", 0, "o"), ("P01", 2**31, "o"),
        ]:
            with self.subTest(product=product, quantity=quantity, order=order):
                with self.assertRaises(ValueError):
                    create_order(product, quantity, order)
        self.assertEqual(create_order("P01", 1)["event_type"], "OrderCreated")

    def test_timestamp_and_version_required(self):
        event = order_created("order", "P01", 1)
        event.pop("timestamp")
        with self.assertRaises(ValueError):
            inventory.validate_event(event, TOPIC_ORDERS)
        with self.assertRaises(TypeError):
            stock_updated("P01", 1)
        for value in [True, -1, 2**63, None]:
            with self.subTest(version=value), self.assertRaises(ValueError):
                stock_updated("P01", 1, value)
        self.assertEqual(stock_updated("P01", 10, 0)["data"]["version"], 0)


class ConfigurationTests(unittest.TestCase):
    def test_shared_configuration_in_fresh_process(self):
        env = dict(os.environ, TOPIC_ORDERS="audit.orders",
                   TOPIC_PAYMENTS_RESULTS="audit.payments",
                   TOPIC_INVENTORY_RESULTS="audit.results",
                   TOPIC_INVENTORY_UPDATED="audit.updated",
                   GROUP_INVENTORY="audit.inventory")
        code = (
            "from services.inventory import consumer as c, database as d; "
            "from services.order import producer as o; "
            "assert c.TOPIC_ORDERS == o.TOPIC_ORDERS == 'audit.orders'; "
            "assert c.TOPIC_PAYMENTS == 'audit.payments'; "
            "assert c.GROUP_INVENTORY == 'audit.inventory'; "
            "assert d.TOPIC_INVENTORY_RESULTS == 'audit.results'; "
            "assert d.TOPIC_INVENTORY_UPDATED == 'audit.updated'"
        )
        subprocess.run([sys.executable, "-B", "-c", code], env=env,
                       cwd=Path(__file__).resolve().parents[1], check=True, timeout=20)


class PublisherTests(unittest.TestCase):
    def test_second_instance_never_creates_producer(self):
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.info.transaction_status = TransactionStatus.IDLE
        conn.execute.return_value.fetchone.return_value = {"acquired": False}
        with patch.object(outbox, "get_connection", return_value=conn):
            with patch.object(outbox, "create_producer") as factory:
                with self.assertRaises(RuntimeError):
                    outbox.run_publisher()
        factory.assert_not_called()

    def test_marks_only_acked_events_and_stops_on_failure(self):
        events = [{"event_id": "one"}, {"event_id": "two"}, {"event_id": "three"}]
        with patch.object(outbox, "get_pending_outbox", return_value=events):
            with patch.object(outbox, "publish_one_event", side_effect=[True, False]) as send:
                with patch.object(outbox, "mark_outbox_published") as mark:
                    count = outbox.publish_pending_events(Mock(), Mock())
        self.assertEqual(count, 1)
        self.assertEqual(send.call_count, 2)
        self.assertEqual(mark.call_args.args[1], "one")


class DurableServiceTests(unittest.TestCase):
    def test_payment_publishes_stored_decision_despite_changed_mode(self):
        source = inventory_result("order", "P01", True)
        canonical = payment_result("order", True)
        conn, producer = Mock(), Mock()
        with patch.object(payment, "load_or_create_decision", return_value=canonical):
            with patch.object(payment, "publish_payment") as send:
                result = payment.process_event(
                    producer, source, PaymentSettings(mode="failure"), conn=conn)
        self.assertIs(result["payment_event"], canonical)
        self.assertTrue(send.call_args.args[1]["data"]["success"])

    def test_payment_db_failure_never_publishes(self):
        source = inventory_result("order", "P01", True)
        with patch.object(payment, "load_or_create_decision", side_effect=RuntimeError("DB failure")):
            with patch.object(payment, "publish_payment") as send:
                with self.assertRaises(RuntimeError):
                    payment.process_event(Mock(), source, conn=Mock())
        send.assert_not_called()

    def test_notification_uses_store_and_no_offset_on_failure(self):
        event = payment_result("order", True)
        msg = message(event)
        msg.topic.return_value = notification.TOPIC_PAYMENTS_RESULTS
        consumer = Mock()
        with patch.object(notification, "store_notification", side_effect=RuntimeError("commit failed")):
            with self.assertRaises(RuntimeError):
                notification.consume_one_message(consumer, msg, conn=Mock())
        consumer.commit.assert_not_called()

    def test_duplicate_notification_does_not_log_created_again(self):
        event = payment_result("order", True)
        result = dict(event_id=event["event_id"], order_id="order", success=True, created=False)
        with patch.object(notification, "store_notification", return_value=result):
            with self.assertNoLogs(notification.logger, level="INFO"):
                notification.process_event(event, conn=Mock())
