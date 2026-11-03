"""Asynchronous OrderCreated delivery benchmark (not business throughput)."""

import argparse
import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import logging
import math
from pathlib import Path
import time
from uuid import uuid4

from common.kafka_client import create_producer, serialize_event
from config.topics import TOPIC_ORDERS
from services.order.producer import create_order


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FlashSaleStats:
    run_id: str
    planned: int
    submitted: int
    acknowledged: int
    immediate_failures: int
    delivery_failures: int
    failed: int
    unconfirmed: int
    not_submitted: int
    started_at: str
    ended_at: str
    duration_seconds: float
    acknowledged_per_second: float
    interrupted: bool
    error: str | None


def _validate_options(count, product_id, quantity, rate, run_id,
                      queue_timeout, flush_timeout):
    if type(count) is not int or count <= 0:
        raise ValueError("count must be a positive integer")
    # Validate through TV3's factory rather than duplicating its event schema.
    create_order(product_id, quantity, order_id="validation-only")
    if len(product_id) > 50:
        raise ValueError("product_id exceeds Inventory's 50-character limit")
    for name, value in (("rate", rate), ("queue_timeout", queue_timeout),
                        ("flush_timeout", flush_timeout)):
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{name} must be finite and non-negative")
    if queue_timeout == 0:
        raise ValueError("queue_timeout must be positive")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id must be a non-empty string")
    if len(f"ORD-{run_id}-{count}") > 100:
        raise ValueError("run_id would exceed Inventory's 100-character order_id limit")


def run_flash_sale(*, count, product_id, quantity=1, rate=0.0, run_id=None,
                   queue_timeout=15.0, flush_timeout=30.0, producer=None,
                   monotonic=time.monotonic, sleep=time.sleep,
                   utc_now=None):
    """Send without waiting per delivery; poll and flush serve callbacks.

    ``rate=0`` means unlimited. A queue timeout fails the current order and
    stops submitting more work. ``not_submitted`` then records untouched
    orders; ``unconfirmed`` records only accepted produce calls with no
    callback by the flush deadline. A successful produce call is not an ack.

    Every returned result satisfies:
    planned = acknowledged + failed + unconfirmed + not_submitted.
    Reusing an explicit run_id intentionally reuses order IDs.
    """
    if run_id is None:
        run_id = uuid4().hex
    _validate_options(count, product_id, quantity, rate, run_id,
                      queue_timeout, flush_timeout)
    if utc_now is None:
        utc_now = lambda: datetime.now(timezone.utc)
    if producer is None:
        producer = create_producer()

    started_at = utc_now().isoformat()
    started = monotonic()
    counters = {"submitted": 0, "acknowledged": 0,
                "immediate_failures": 0, "delivery_failures": 0}
    pending = set()
    interrupted = False
    error = None

    def callback_for(order_number):
        def on_delivery(delivery_error, message):
            # Guard duplicate callbacks and never count a single order twice.
            if order_number not in pending:
                return
            pending.remove(order_number)
            name = "acknowledged" if delivery_error is None else "delivery_failures"
            counters[name] += 1
            if delivery_error is not None:
                logger.debug("Delivery failed for order %s: %s",
                             order_number, delivery_error)
        return on_delivery

    try:
        for order_number in range(1, count + 1):
            if rate:
                target = started + (order_number - 1) / rate
                while monotonic() < target:
                    producer.poll(0)
                    sleep(max(0.0, min(0.05, target - monotonic())))

            event = create_order(product_id, quantity,
                                 order_id=f"ORD-{run_id}-{order_number}")
            payload = serialize_event(event)
            pending.add(order_number)
            queue_deadline = monotonic() + queue_timeout
            queue_expired = False
            while True:
                try:
                    producer.produce(
                        topic=TOPIC_ORDERS,
                        key=event["data"]["order_id"].encode("utf-8"),
                        value=payload,
                        callback=callback_for(order_number),
                    )
                except BufferError:
                    remaining = queue_deadline - monotonic()
                    if remaining <= 0:
                        pending.remove(order_number)
                        counters["immediate_failures"] += 1
                        error = "Producer queue remained full until queue_timeout"
                        queue_expired = True
                        break
                    producer.poll(min(0.1, remaining))
                except Exception as exc:
                    pending.remove(order_number)
                    counters["immediate_failures"] += 1
                    error = str(exc)
                    break
                else:
                    counters["submitted"] += 1
                    break
            producer.poll(0)
            if queue_expired:
                break
    except KeyboardInterrupt:
        interrupted = True
    except Exception as exc:
        # A poll/rate-control failure must not discard delivery accounting.
        error = str(exc)
    finally:
        # A pending entry is inserted before produce. Remove any entry whose
        # produce was interrupted/raised outside its normal error handler.
        accepted_pending = (counters["submitted"] - counters["acknowledged"]
                            - counters["delivery_failures"])
        while len(pending) > accepted_pending:
            pending.remove(max(pending))
        try:
            producer.flush(flush_timeout)
        except KeyboardInterrupt:
            interrupted = True
        except Exception as exc:
            error = str(exc)

    duration = max(0.0, monotonic() - started)
    failed = counters["immediate_failures"] + counters["delivery_failures"]
    return FlashSaleStats(
        run_id=run_id, planned=count, **counters, failed=failed,
        unconfirmed=len(pending),
        not_submitted=count - counters["submitted"] - counters["immediate_failures"],
        started_at=started_at, ended_at=utc_now().isoformat(),
        duration_seconds=duration,
        acknowledged_per_second=(counters["acknowledged"] / duration if duration else 0.0),
        interrupted=interrupted, error=error,
    )


def write_summary_csv(stats, path):
    """Write one run summary; CSV contains delivery metrics, not payments."""
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    row = asdict(stats)
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--product-id", "--product", dest="product_id", required=True)
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument("--rate", type=float, default=0.0,
                        help="Orders per second; 0 means unlimited")
    parser.add_argument("--run-id", help="Unique per run; defaults to a random UUID")
    parser.add_argument("--queue-timeout", type=float, default=15.0)
    parser.add_argument("--flush-timeout", type=float, default=30.0)
    parser.add_argument("--csv", help="Optional summary CSV output path")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    try:
        stats = run_flash_sale(
            count=args.count, product_id=args.product_id, quantity=args.quantity,
            rate=args.rate, run_id=args.run_id, queue_timeout=args.queue_timeout,
            flush_timeout=args.flush_timeout,
        )
        print(json.dumps(asdict(stats), indent=2, ensure_ascii=False))
        if args.csv:
            write_summary_csv(stats, args.csv)
        return int(bool(stats.failed or stats.unconfirmed or stats.not_submitted
                        or stats.interrupted or stats.error))
    except (ValueError, OSError) as exc:
        logger.error("Flash Sale failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
