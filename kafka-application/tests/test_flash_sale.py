import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest

from load_testing.flash_sale import run_flash_sale, write_summary_csv


class Clock:
    def __init__(self):
        self.value = 0.0

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        self.value += max(seconds, 0)

    def now(self):
        return datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=self.value)


class FakeProducer:
    def __init__(self, clock, outcomes=None, *, full=0, flush_callbacks=True):
        self.clock = clock
        self.outcomes = list(outcomes or [])
        self.full = full
        self.flush_callbacks = flush_callbacks
        self.messages = []
        self.callbacks = []
        self.flush_timeouts = []
        self.attempts = 0

    def produce(self, **message):
        self.attempts += 1
        if self.full:
            self.full -= 1
            raise BufferError("full")
        outcome = self.outcomes.pop(0) if self.outcomes else "success"
        if outcome == "immediate":
            raise RuntimeError("local failure")
        self.messages.append(message)
        error = RuntimeError("delivery failed") if outcome == "failure" else None
        self.callbacks.append((message["callback"], error, outcome))

    def poll(self, timeout):
        self.clock.sleep(timeout)

    def flush(self, timeout):
        self.flush_timeouts.append(timeout)
        self.clock.sleep(timeout)
        if self.flush_callbacks:
            for callback, error, outcome in self.callbacks:
                if outcome != "pending":
                    callback(error, None)
                    callback(error, None)  # Defensive duplicate callback check.
        return sum(outcome == "pending" for _, _, outcome in self.callbacks)


class FlashSaleTests(unittest.TestCase):
    def run_sale(self, producer, clock, **options):
        return run_flash_sale(
            count=options.pop("count", 4), product_id="P01", run_id="test",
            producer=producer, monotonic=clock.monotonic, sleep=clock.sleep,
            utc_now=clock.now, **options,
        )

    def assert_reconciles(self, result):
        self.assertEqual(result.planned, result.acknowledged + result.failed
                         + result.unconfirmed + result.not_submitted)

    def test_delivery_immediate_failure_and_pending_are_distinct(self):
        clock = Clock()
        producer = FakeProducer(clock, ["success", "failure", "immediate", "pending"])
        result = self.run_sale(producer, clock, flush_timeout=2)
        self.assertEqual((result.acknowledged, result.immediate_failures,
                          result.delivery_failures, result.unconfirmed), (1, 1, 1, 1))
        self.assertEqual(result.submitted, 3)
        self.assertEqual(result.failed, 2)
        self.assertEqual(result.not_submitted, 0)
        self.assertEqual(result.acknowledged_per_second, 0.5)
        self.assertEqual(producer.flush_timeouts, [2])
        self.assert_reconciles(result)

    def test_produce_success_without_callback_is_not_acknowledged(self):
        clock = Clock()
        producer = FakeProducer(clock, flush_callbacks=False)
        result = self.run_sale(producer, clock)
        self.assertEqual(result.acknowledged, 0)
        self.assertEqual(result.unconfirmed, 4)
        self.assert_reconciles(result)

    def test_queue_full_retries_without_counting_failure(self):
        clock = Clock()
        producer = FakeProducer(clock, full=2)
        result = self.run_sale(producer, clock)
        self.assertEqual(producer.attempts, 6)
        self.assertEqual(result.acknowledged, 4)
        self.assertEqual(result.failed, 0)
        self.assert_reconciles(result)

    def test_queue_timeout_stops_and_reports_unattempted_orders(self):
        clock = Clock()
        producer = FakeProducer(clock, full=1000)
        result = self.run_sale(producer, clock, queue_timeout=0.2, flush_timeout=1)
        self.assertEqual(result.immediate_failures, 1)
        self.assertEqual(result.not_submitted, 3)
        self.assertEqual(result.unconfirmed, 0)
        self.assertLessEqual(result.duration_seconds, 1.21)
        self.assert_reconciles(result)

    def test_poll_callbacks_during_backpressure_preserve_mixed_results(self):
        class PollingProducer(FakeProducer):
            def __init__(self, clock):
                super().__init__(clock, ["success", "failure", "pending"])
                self.blocked_once = set()
                self.delivered = set()

            def produce(self, **message):
                next_order = len(self.messages) + 1
                if next_order in (2, 3) and next_order not in self.blocked_once:
                    self.blocked_once.add(next_order)
                    self.attempts += 1
                    raise BufferError("in-flight queue full")
                super().produce(**message)

            def poll(self, timeout):
                super().poll(timeout)
                if timeout > 0:
                    for index, (callback, error, outcome) in enumerate(self.callbacks):
                        if index not in self.delivered and outcome != "pending":
                            self.delivered.add(index)
                            callback(error, None)
                            callback(error, None)

        clock = Clock()
        producer = PollingProducer(clock)
        result = self.run_sale(producer, clock, count=3, flush_timeout=1)
        self.assertEqual(producer.blocked_once, {2, 3})
        self.assertEqual(producer.delivered, {0, 1})
        self.assertEqual(producer.attempts, 5)
        self.assertEqual((result.submitted, result.acknowledged,
                          result.delivery_failures, result.immediate_failures,
                          result.unconfirmed, result.not_submitted), (3, 1, 1, 0, 1, 0))
        self.assert_reconciles(result)

    def test_flush_exception_after_partial_callbacks_preserves_accounting(self):
        class PartialFlushProducer(FakeProducer):
            def flush(self, timeout):
                self.flush_timeouts.append(timeout)
                self.clock.sleep(0.1)
                for callback, error, _ in self.callbacks[:2]:
                    callback(error, None)
                    callback(error, None)
                raise RuntimeError("flush failed after partial delivery")

        clock = Clock()
        producer = PartialFlushProducer(clock, ["success", "failure", "success", "pending"])
        result = self.run_sale(producer, clock, flush_timeout=2)
        self.assertEqual(producer.flush_timeouts, [2])
        self.assertEqual(result.error, "flush failed after partial delivery")
        self.assertEqual((result.submitted, result.acknowledged,
                          result.delivery_failures, result.immediate_failures,
                          result.unconfirmed, result.not_submitted), (4, 1, 1, 0, 2, 0))
        self.assertEqual(result.failed, 1)
        self.assert_reconciles(result)

    def test_order_factory_envelope_keys_and_rate(self):
        clock = Clock()
        producer = FakeProducer(clock)
        result = self.run_sale(producer, clock, rate=2, flush_timeout=0)
        self.assertAlmostEqual(result.duration_seconds, 1.5)
        events = [json.loads(message["value"].decode("utf-8"))
                  for message in producer.messages]
        self.assertEqual([event["data"]["order_id"] for event in events],
                         ["ORD-test-1", "ORD-test-2", "ORD-test-3", "ORD-test-4"])
        for message, event in zip(producer.messages, events):
            self.assertEqual(message["key"].decode("utf-8"), event["data"]["order_id"])
            self.assertEqual(event["event_type"], "OrderCreated")
            self.assertEqual(set(event), {"event_id", "event_type", "timestamp", "data"})

    def test_default_run_ids_differ(self):
        clock = Clock()
        options = dict(count=1, product_id="P01", monotonic=clock.monotonic,
                       sleep=clock.sleep, utc_now=clock.now, flush_timeout=0)
        a = run_flash_sale(producer=FakeProducer(clock), **options)
        b = run_flash_sale(producer=FakeProducer(clock), **options)
        self.assertNotEqual(a.run_id, b.run_id)

    def test_load_sizes_use_bounded_single_loop(self):
        for count in (1000, 10000, 100000):
            with self.subTest(count=count):
                clock = Clock()
                producer = FakeProducer(clock)
                result = self.run_sale(producer, clock, count=count, flush_timeout=0)
                self.assertEqual(result.acknowledged, count)
                self.assert_reconciles(result)

    def test_invalid_options_fail_before_produce(self):
        for options in ({"count": 0}, {"rate": float("nan")},
                        {"rate": -1}, {"queue_timeout": 0},
                        {"flush_timeout": float("inf")}):
            clock = Clock()
            producer = FakeProducer(clock)
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.run_sale(producer, clock, **options)
            self.assertFalse(producer.messages)

    def test_csv_contains_delivery_summary(self):
        clock = Clock()
        result = self.run_sale(FakeProducer(clock), clock)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stats.csv"
            write_summary_csv(result, path)
            with path.open(encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["acknowledged"], "4")
        self.assertEqual(rows[0]["run_id"], "test")


if __name__ == "__main__":
    unittest.main()
