from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import Barrier
from unittest.mock import Mock
from uuid import uuid4

import psycopg
from psycopg import sql

from common.event_schema import inventory_result, payment_result
from common.kafka_client import serialize_event
from config.topics import TOPIC_ORDERS
from services.inventory import database as inventory_db
from services.inventory.consumer import consume_one_message, process_event as inventory_process
from services.inventory.producer import publisher_lock
from services.notification import database as notification_db
from services.payment import database as payment_db
from services.payment.consumer import simulate_payment
from services.payment.settings import PaymentSettings
from services.order.producer import create_order
from tests.test_postgres import PostgreSQLTestCase


class RemediationPostgresTests(PostgreSQLTestCase):
    def setUp(self):
        super().setUp()
        self.schemas = {}
        for kind, module in [("inventory", inventory_db),
                             ("payment", payment_db),
                             ("notification", notification_db)]:
            name = "remediation_" + uuid4().hex
            self.schemas[kind] = name
            self.admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
            self.addCleanup(self.drop_component_schema, name)
            conn = self.component_connection(kind)
            self.addCleanup(conn.close)
            setattr(self, kind, conn)
            module.init_db(conn)

    def drop_component_schema(self, name):
        self.admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name)))

    def component_connection(self, kind):
        conn = self.connect_test_database()
        conn.execute(sql.SQL("SET search_path TO {}").format(
            sql.Identifier(self.schemas[kind])))
        return conn

    def test_payment_decision_survives_restart_and_mode_change(self):
        source = inventory_result("mode-change", "P01", True)
        first = payment_db.load_or_create_decision(
            self.payment, source, PaymentSettings(mode="success"), simulate_payment)
        with self.component_connection("payment") as restarted:
            second = payment_db.load_or_create_decision(
                restarted, source, PaymentSettings(mode="failure"), simulate_payment)
        self.assertEqual(first, second)
        self.assertTrue(second["data"]["success"])
        other_source = inventory_result("mode-change", "P01", True)
        self.assertEqual(first, payment_db.load_or_create_decision(
            self.payment, other_source, PaymentSettings(mode="failure"), simulate_payment))
        changed = deepcopy(source)
        changed["timestamp"] = "2026-11-07T00:00:00+00:00"
        with self.assertRaises(ValueError):
            payment_db.load_or_create_decision(
                self.payment, changed, PaymentSettings(), simulate_payment)

    def test_payment_concurrent_order_creates_one_decision(self):
        source = inventory_result("concurrent-payment", "P01", True)
        barrier = Barrier(4)
        def worker(index):
            with self.component_connection("payment") as conn:
                barrier.wait(timeout=15)
                return payment_db.load_or_create_decision(
                    conn, source, PaymentSettings(mode="success" if index % 2 else "failure"),
                    simulate_payment)
        with ThreadPoolExecutor(max_workers=4) as pool:
            values = list(pool.map(worker, range(4)))
        self.assertTrue(all(value == values[0] for value in values))
        self.assertEqual(self.payment.execute(
            "SELECT count(*) AS n FROM payment_decisions").fetchone()["n"], 1)

    def test_notification_concurrent_and_restarted_replay(self):
        event = payment_result("notification-once", True)
        barrier = Barrier(4)
        def worker(_):
            with self.component_connection("notification") as conn:
                barrier.wait(timeout=15)
                return notification_db.store_notification(conn, event)
        with ThreadPoolExecutor(max_workers=4) as pool:
            values = list(pool.map(worker, range(4)))
        self.assertEqual(sum(value["created"] for value in values), 1)
        with self.component_connection("notification") as conn:
            self.assertFalse(notification_db.store_notification(conn, event)["created"])
        self.assertEqual(self.notification.execute(
            "SELECT count(*) AS n FROM notifications").fetchone()["n"], 1)

    def test_unknown_product_is_durably_quarantined_once(self):
        event = create_order("MISSING", 1, order_id="unknown-product")
        msg, consumer = Mock(), Mock()
        msg.error.return_value = None
        msg.topic.return_value = TOPIC_ORDERS
        msg.partition.return_value = 0
        msg.offset.return_value = 7
        msg.key.return_value = b"unknown-product"
        msg.value.return_value = serialize_event(event)
        consumer.commit.return_value = []
        self.assertTrue(consume_one_message(consumer, self.inventory, msg)["quarantined"])
        self.assertTrue(consume_one_message(consumer, self.inventory, msg)["quarantined"])
        self.assertEqual(self.inventory.execute(
            "SELECT count(*) AS n FROM consumer_errors").fetchone()["n"], 1)
        self.assertEqual(self.inventory.execute(
            "SELECT count(*) AS n FROM processed_events").fetchone()["n"], 0)
        self.assertEqual(consumer.commit.call_count, 2)

    def test_inventory_sql_failure_rolls_back_stock_version_and_outbox(self):
        inventory_db.seed_product(self.inventory, "P01", 10)
        self.inventory.execute(
            "ALTER TABLE outbox_events ADD CONSTRAINT test_fail_result "
            "CHECK (topic <> 'inventory.results')")
        event = create_order("P01", 1, order_id="rollback")
        with self.assertRaises(psycopg.errors.CheckViolation):
            inventory_process(self.inventory, event, TOPIC_ORDERS)
        self.assertEqual(self.inventory.execute(
            "SELECT stock, version FROM products WHERE product_id = 'P01'"
        ).fetchone(), {"stock": 10, "version": 0})
        self.assertEqual(self.inventory.execute(
            "SELECT count(*) AS n FROM reservations").fetchone()["n"], 0)
        self.assertEqual(self.inventory.execute(
            "SELECT count(*) AS n FROM processed_events").fetchone()["n"], 0)
        self.assertEqual(self.inventory.execute(
            "SELECT count(*) AS n FROM outbox_events").fetchone()["n"], 1)

    def test_publisher_lock_excludes_second_session(self):
        with self.component_connection("inventory") as first:
            with publisher_lock(first):
                with self.component_connection("inventory") as second:
                    with self.assertRaises(RuntimeError):
                        with publisher_lock(second):
                            self.fail("Second publisher must not acquire lock")
        with self.component_connection("inventory") as after_close:
            with publisher_lock(after_close):
                self.assertTrue(True)


    def test_payment_commit_failure_prevents_delivery_and_offset_commit(self):
        from services.payment.consumer import consume_one_message as consume_payment
        from config.topics import TOPIC_INVENTORY_RESULTS
        from tests.test_payment import FakeProducer
        self.payment.execute("CREATE TABLE allowed_orders (order_id VARCHAR(100) PRIMARY KEY)")
        self.payment.execute(
            "ALTER TABLE payment_decisions ADD CONSTRAINT deferred_order "
            "FOREIGN KEY (order_id) REFERENCES allowed_orders(order_id) "
            "DEFERRABLE INITIALLY DEFERRED")
        source = inventory_result("commit-failure", "P01", True)
        msg, consumer, producer = Mock(), Mock(), FakeProducer()
        msg.error.return_value = None
        msg.topic.return_value = TOPIC_INVENTORY_RESULTS
        msg.value.return_value = serialize_event(source)
        with self.assertRaises(psycopg.errors.ForeignKeyViolation):
            consume_payment(consumer, producer, msg, conn=self.payment)
        self.assertEqual(producer.sent, [])
        consumer.commit.assert_not_called()
        self.assertEqual(self.payment.execute(
            "SELECT count(*) AS n FROM payment_decisions").fetchone()["n"], 0)

    def test_notification_commit_failure_does_not_commit_offset(self):
        from services.notification.consumer import consume_one_message as consume_notification
        from config.topics import TOPIC_PAYMENTS_RESULTS
        self.notification.execute("CREATE TABLE allowed_orders (order_id VARCHAR(100) PRIMARY KEY)")
        self.notification.execute(
            "ALTER TABLE notifications ADD CONSTRAINT deferred_order "
            "FOREIGN KEY (order_id) REFERENCES allowed_orders(order_id) "
            "DEFERRABLE INITIALLY DEFERRED")
        msg, consumer = Mock(), Mock()
        msg.error.return_value = None
        msg.topic.return_value = TOPIC_PAYMENTS_RESULTS
        msg.value.return_value = serialize_event(payment_result("commit-failure", True))
        with self.assertRaises(psycopg.errors.ForeignKeyViolation):
            consume_notification(consumer, msg, conn=self.notification)
        consumer.commit.assert_not_called()
        self.assertEqual(self.notification.execute(
            "SELECT count(*) AS n FROM notifications").fetchone()["n"], 0)
