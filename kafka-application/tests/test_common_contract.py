"""Compatibility checks for the two small shared TV4 integration extensions."""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from confluent_kafka import KafkaError, KafkaException

from common.event_schema import stock_updated
from common.kafka_client import commit_message, create_consumer


class CommonContractTests(unittest.TestCase):
    def test_sync_commit_checks_partition_errors(self):
        consumer = Mock()
        message = object()
        consumer.commit.return_value = [SimpleNamespace(error=None)]
        commit_message(consumer, message)
        consumer.commit.assert_called_once_with(message=message, asynchronous=False)
        consumer.commit.return_value = [SimpleNamespace(error=KafkaError(KafkaError._TIMED_OUT))]
        with self.assertRaises(KafkaException):
            commit_message(consumer, message)

    def test_legacy_stock_factory_keeps_original_shape(self):
        event = stock_updated("P01", 10)
        self.assertEqual(event["data"], {"product_id": "P01", "stock_after": 10})
        self.assertEqual(set(event), {"event_id", "event_type", "timestamp", "data"})

    def test_explicit_version_zero_is_preserved(self):
        self.assertEqual(stock_updated("P01", 10, version=0)["data"]["version"], 0)
        self.assertEqual(stock_updated("P01", 9, version=42)["data"]["version"], 42)

    def test_invalid_version_is_rejected(self):
        for version in (None, -1, True, 1.5, "2", 2**63):
            with self.subTest(version=version), self.assertRaises(ValueError):
                stock_updated("P01", 10, version=version)

    @patch("common.kafka_client.Consumer")
    @patch("common.kafka_client.get_consumer_config")
    def test_optional_manual_commit_override_preserves_tv3_defaults(self, get_config, consumer):
        get_config.side_effect = lambda group: {"group.id": group, "enable.auto.commit": True}
        create_consumer("payment-group", config_overrides={
            "enable.auto.commit": False, "enable.auto.offset.store": False,
        })
        consumer.assert_called_with({"group.id": "payment-group", "enable.auto.commit": False,
                                     "enable.auto.offset.store": False})
        create_consumer("inventory-group")
        consumer.assert_called_with({"group.id": "inventory-group", "enable.auto.commit": True})


if __name__ == "__main__":
    unittest.main()
