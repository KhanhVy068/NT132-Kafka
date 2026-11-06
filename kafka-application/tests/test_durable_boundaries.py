"""Unit checks on native DB helpers; SQL durability is tested separately on PostgreSQL."""
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch
from threading import Event

from psycopg.pq import TransactionStatus
from common.event_schema import inventory_result, payment_result
from common.service_database import connect_service
from services.payment.database import load_or_create_decision
from services.payment.consumer import process_event as process_payment
from services.payment.settings import PaymentSettings
from services.notification.database import store_notification
from load_testing.verify_sale import LagReader
from load_testing.flash_sale import run_flash_sale
from tests.test_flash_sale import Clock, FakeProducer


def idle_connection():
    conn = MagicMock()
    conn.info.transaction_status = TransactionStatus.IDLE
    return conn


class NativeDatabaseBoundaryTests(unittest.TestCase):
    def test_saved_decision_is_reused_without_simulating_again(self):
        source = inventory_result("order", "P01", True)
        canonical = payment_result("order", True)
        conn = idle_connection()
        conn.execute.return_value.fetchone.return_value = {
            "source_event_id": source["event_id"], "product_id": "P01",
            "source_event": deepcopy(source), "payment_event": canonical,
        }
        decide = Mock()
        self.assertEqual(load_or_create_decision(
            conn, source, PaymentSettings(mode="failure"), decide), canonical)
        decide.assert_not_called()
        self.assertEqual(conn.execute.call_count, 2)

    def test_native_decision_commit_failure_prevents_publish(self):
        source = inventory_result("order", "P01", True)
        conn = idle_connection()
        conn.execute.return_value.fetchone.return_value = None
        conn.transaction.return_value.__exit__.side_effect = RuntimeError("COMMIT failed")
        with patch("services.payment.consumer.publish_payment") as publish:
            with self.assertRaisesRegex(RuntimeError, "COMMIT failed"):
                process_payment(Mock(), source, PaymentSettings(mode="success"), conn=conn)
        publish.assert_not_called()

    def test_source_id_reuse_with_changed_content_is_rejected(self):
        source = inventory_result("order", "P01", True)
        old = deepcopy(source)
        old["timestamp"] = "2026-01-01T00:00:00+00:00"
        conn = idle_connection()
        conn.execute.return_value.fetchone.return_value = {
            "source_event_id": source["event_id"], "product_id": "P01",
            "source_event": old, "payment_event": payment_result("order", True),
        }
        with self.assertRaises(ValueError):
            load_or_create_decision(conn, source, PaymentSettings(), Mock())

    def test_notification_duplicate_is_not_inserted_twice(self):
        event = payment_result("order", True)
        conn = idle_connection()
        conn.execute.return_value.fetchone.return_value = None
        conn.execute.return_value.fetchall.return_value = [{
            "order_id": "order", "event_id": event["event_id"],
            "success": True, "payload": deepcopy(event),
        }]
        self.assertFalse(store_notification(conn, event)["created"])
        changed = deepcopy(event)
        changed["data"]["success"] = False
        with self.assertRaises(ValueError):
            store_notification(conn, changed)

    def test_state_database_rejects_inventory_account(self):
        with patch.dict("os.environ", {"PAYMENT_DB_USER": "inventory_user"}):
            with patch("common.service_database.psycopg.connect") as connect:
                with self.assertRaises(ValueError):
                    connect_service("PAYMENT", "payment_db", "payment_user")
        connect.assert_not_called()


class LagTests(unittest.TestCase):
    def reader(self, committed, low, high):
        kafka = Mock()
        kafka.list_topics.return_value.topics = {
            "orders.created": SimpleNamespace(error=None, partitions={0: None})}
        kafka.committed.return_value = [SimpleNamespace(
            error=None, offset=committed, partition=0)]
        kafka.get_watermark_offsets.return_value = (low, high)
        reader = LagReader.__new__(LagReader)
        reader.readers = [("inventory-group", kafka, ["orders.created"])]
        return reader, kafka

    def test_missing_commit_on_nonempty_partition_is_backlog(self):
        reader, kafka = self.reader(-1001, 0, 20)
        self.assertEqual(reader.snapshot()[0]["lag"], 20)
        kafka.subscribe.assert_not_called()
        kafka.commit.assert_not_called()

    def test_retention_gap_is_unknown_not_zero(self):
        reader, _ = self.reader(5, 10, 20)
        self.assertIsNone(reader.snapshot()[0]["lag"])

    def test_metadata_error_does_not_report_zero_lag(self):
        reader, kafka = self.reader(20, 0, 20)
        kafka.list_topics.side_effect = RuntimeError("broker unavailable")
        with self.assertRaises(RuntimeError):
            reader.snapshot()


class LoadCancellationTests(unittest.TestCase):
    def test_cancellation_before_send_keeps_all_orders_unsubmitted(self):
        stop = Event()
        stop.set()
        clock = Clock()
        producer = FakeProducer(clock)
        result = run_flash_sale(count=4, product_id="P01", run_id="cancel",
                                producer=producer, stop_event=stop,
                                monotonic=clock.monotonic, sleep=clock.sleep,
                                utc_now=clock.now, flush_timeout=0)
        self.assertEqual(result.not_submitted, 4)
        self.assertEqual(result.submitted, 0)
        self.assertTrue(result.interrupted)

    def test_cancellation_during_backpressure_preserves_accounting(self):
        stop = Event()
        clock = Clock()
        class BlockingProducer(FakeProducer):
            def poll(self, timeout):
                super().poll(timeout)
                stop.set()
        result = run_flash_sale(count=4, product_id="P01", run_id="cancel",
                                producer=BlockingProducer(clock, full=100), stop_event=stop,
                                monotonic=clock.monotonic, sleep=clock.sleep,
                                utc_now=clock.now, flush_timeout=0)
        self.assertTrue(result.interrupted)
        self.assertEqual(result.not_submitted, 4)
        self.assertEqual(result.unconfirmed + result.failed + result.acknowledged, 0)
