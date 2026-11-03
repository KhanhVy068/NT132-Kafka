"""Contract/unit tests with a transaction fake, not a PostgreSQL substitute."""

from copy import deepcopy
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from psycopg.pq import TransactionStatus

from common.event_schema import stock_updated
from config.topics import TOPIC_INVENTORY_UPDATED
from services.stock_view.consumer import consume_one_message, run_consumer
from services.stock_view.database import get_connection, import_events, process_event, validate_event


def event(version=1, stock=9, event_id="event-1", product="P01"):
    result = stock_updated(product, stock)
    result["event_id"] = event_id
    result["data"]["version"] = version
    return result


class FakeConnection:
    """Model rollback/commit boundaries; SQL behavior is tested against real PG separately."""

    def __init__(self):
        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        self.processed = set()
        self.stocks = {}
        self.calls = []
        self.commits = 0
        self.rollbacks = 0
        self.fail_upsert = False
        self.fail_commit = False

    def transaction(self):
        connection = self

        class Transaction:
            def __enter__(self):
                self.snapshot = deepcopy((connection.processed, connection.stocks))
                connection.info.transaction_status = TransactionStatus.INTRANS

            def __exit__(self, exc_type, exc, traceback):
                connection.info.transaction_status = TransactionStatus.IDLE
                if exc_type is not None or connection.fail_commit:
                    connection.processed, connection.stocks = self.snapshot
                    connection.rollbacks += 1
                    if exc_type is None:
                        raise RuntimeError("database commit failed")
                else:
                    connection.commits += 1
                return False

        return Transaction()

    def execute(self, statement, params):
        sql = " ".join(statement.split())
        self.calls.append((sql, params))
        if sql.startswith("INSERT INTO processed_events"):
            if params[0] in self.processed:
                row = None
            else:
                self.processed.add(params[0])
                row = {"event_id": params[0]}
        elif sql.startswith("INSERT INTO stock_levels"):
            if "WHERE EXCLUDED.version > stock_levels.version" not in sql:
                raise AssertionError("Projection requires a conditional SQL upsert")
            product, stock, version, updated_at = params
            previous = self.stocks.get(product)
            row = None
            if previous is None or version > previous["version"]:
                self.stocks[product] = {"stock_after": stock, "version": version, "updated_at": updated_at}
                row = {"product_id": product}
            if self.fail_upsert:
                raise RuntimeError("database upsert failed")
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")
        return SimpleNamespace(fetchone=lambda: row)


def message_for(payload):
    message = Mock()
    message.error.return_value = None
    message.topic.return_value = TOPIC_INVENTORY_UPDATED
    message.value.return_value = json.dumps(payload).encode("utf-8")
    return message


class StockViewTests(unittest.TestCase):
    def test_newer_version_updates_projection(self):
        conn = FakeConnection()
        process_event(conn, event(version=0, stock=10))
        result = process_event(conn, event(version=1, stock=8, event_id="event-2"))
        self.assertTrue(result["updated"])
        self.assertEqual(conn.stocks["P01"]["stock_after"], 8)
        self.assertEqual(conn.stocks["P01"]["version"], 1)
        self.assertEqual(conn.commits, 2)

    def test_duplicate_id_does_not_update_even_if_payload_changes(self):
        conn = FakeConnection()
        process_event(conn, event())
        result = process_event(conn, event(version=100, stock=0))
        self.assertEqual(result, {"processed": False, "duplicate": True, "updated": False})
        self.assertEqual(conn.stocks["P01"]["version"], 1)
        self.assertEqual(len(conn.calls), 3)

    def test_equal_and_lower_versions_record_event_without_overwriting(self):
        conn = FakeConnection()
        process_event(conn, event(version=3, stock=7))
        for version in (3, 2, 0):
            result = process_event(conn, event(version=version, stock=100, event_id=f"stale-{version}"))
            self.assertFalse(result["updated"])
        self.assertEqual(conn.stocks["P01"]["stock_after"], 7)
        self.assertEqual(len(conn.processed), 4)

    def test_upsert_failure_rolls_back_projection_and_event_id(self):
        conn = FakeConnection()
        process_event(conn, event())
        before = deepcopy((conn.processed, conn.stocks))
        conn.fail_upsert = True
        with self.assertRaisesRegex(RuntimeError, "upsert failed"):
            process_event(conn, event(version=2, stock=8, event_id="failing"))
        self.assertEqual((conn.processed, conn.stocks), before)
        self.assertEqual(conn.rollbacks, 1)

    def test_no_offset_commit_if_upsert_or_db_commit_fails(self):
        for failure in ("fail_upsert", "fail_commit"):
            with self.subTest(failure=failure):
                conn = FakeConnection()
                setattr(conn, failure, True)
                consumer = Mock()
                with self.assertRaises(RuntimeError):
                    consume_one_message(consumer, conn, message_for(event()))
                consumer.commit.assert_not_called()
                self.assertEqual(conn.processed, set())
                self.assertEqual(conn.stocks, {})

    def test_offset_commit_occurs_after_database_commit(self):
        conn = FakeConnection()
        consumer = Mock()
        def committed_after_database(**kwargs):
            self.assertEqual(conn.commits, 1)
            return []
        consumer.commit.side_effect = committed_after_database
        message = message_for(event())
        consume_one_message(consumer, conn, message)
        consumer.commit.assert_called_once_with(message=message, asynchronous=False)

    def test_replay_after_kafka_commit_failure_is_safe(self):
        conn = FakeConnection()
        consumer = Mock()
        consumer.commit.side_effect = RuntimeError("offset commit failed")
        message = message_for(event())
        with self.assertRaisesRegex(RuntimeError, "offset commit failed"):
            consume_one_message(consumer, conn, message)
        consumer.commit.side_effect = None
        consumer.commit.return_value = []
        result = consume_one_message(consumer, conn, message)
        self.assertTrue(result["duplicate"])
        self.assertEqual(len(conn.processed), 1)

    def test_versionless_tv3_event_is_rejected_before_database_or_offset_commit(self):
        conn = FakeConnection()
        consumer = Mock()
        legacy = stock_updated("P01", 10)
        with self.assertRaisesRegex(ValueError, "TV3 must publish"):
            consume_one_message(consumer, conn, message_for(legacy))
        self.assertEqual(conn.calls, [])
        consumer.commit.assert_not_called()

    def test_rejects_invalid_contract(self):
        invalid = [
            ("version", True), ("version", -1), ("version", 2**63),
            ("stock_after", True), ("stock_after", -1), ("stock_after", 2**31),
            ("product_id", ""), ("product_id", "P" * 51),
        ]
        for field, value in invalid:
            with self.subTest(field=field, value=value):
                payload = event()
                payload["data"][field] = value
                with self.assertRaises(ValueError):
                    validate_event(payload)
        for timestamp in (None, "2026-11-03T12:00:00", "invalid"):
            payload = event()
            payload["timestamp"] = timestamp
            with self.assertRaises(ValueError):
                validate_event(payload)

    def test_outer_transaction_is_rejected(self):
        conn = FakeConnection()
        conn.info.transaction_status = TransactionStatus.INTRANS
        with self.assertRaisesRegex(ValueError, "no outer transaction"):
            process_event(conn, event())
        self.assertEqual(conn.calls, [])

    def test_consumer_stops_at_failed_message(self):
        conn = FakeConnection()
        conn.fail_upsert = True
        consumer = Mock()
        consumer.poll.side_effect = [message_for(event()), message_for(event(event_id="later"))]
        with self.assertRaises(RuntimeError):
            run_consumer(consumer=consumer, conn=conn, stop_after=2)
        self.assertEqual(consumer.poll.call_count, 1)
        consumer.commit.assert_not_called()

    def test_manual_offset_configuration_overrides_environment_default(self):
        conn = FakeConnection()
        with patch("services.stock_view.consumer.create_consumer") as factory:
            run_consumer(conn=conn, stop_after=0)
        self.assertEqual(factory.call_args.kwargs["config_overrides"], {
            "enable.auto.commit": False, "enable.auto.offset.store": False,
        })
        factory.return_value.close.assert_called_once()

    def test_config_rejects_inventory_database_or_account(self):
        for name, value in (("STOCK_VIEW_DB_NAME", "inventory_db"), ("STOCK_VIEW_DB_USER", "inventory_user")):
            with self.subTest(name=name), patch.dict(os.environ, {name: value}), patch("services.stock_view.database.psycopg.connect") as connect:
                with self.assertRaisesRegex(ValueError, "separate from Inventory"):
                    get_connection()
                connect.assert_not_called()

    def test_connection_uses_only_stock_view_credentials(self):
        settings = {
            "STOCK_VIEW_DB_HOST": "stock-db", "STOCK_VIEW_DB_PORT": "5433",
            "STOCK_VIEW_DB_NAME": "projection", "STOCK_VIEW_DB_USER": "projection_user",
            "STOCK_VIEW_DB_PASSWORD": "unit-test-placeholder",
        }
        with patch.dict(os.environ, settings), patch("services.stock_view.database.psycopg.connect") as connect:
            get_connection()
        self.assertEqual(connect.call_args.kwargs["dbname"], "projection")
        self.assertEqual(connect.call_args.kwargs["user"], "projection_user")
        self.assertTrue(connect.call_args.kwargs["autocommit"])

    def test_snapshot_import_replays_common_events_idempotently(self):
        conn = FakeConnection()
        with TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_text(json.dumps(event(version=0, stock=10)) + "\n", encoding="utf-8")
            self.assertEqual(import_events(conn, path), {"processed": 1, "duplicate": 0, "updated": 1})
            self.assertEqual(import_events(conn, path), {"processed": 0, "duplicate": 1, "updated": 0})


if __name__ == "__main__":
    unittest.main()
