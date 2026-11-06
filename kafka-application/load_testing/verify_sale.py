import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import json
import math
from pathlib import Path
import re
import time
from threading import Event

from confluent_kafka import Consumer, TopicPartition

from config.kafka_config import (
    BOOTSTRAP_SERVERS, GROUP_INVENTORY, GROUP_PAYMENT,
    GROUP_NOTIFICATION, GROUP_STOCK_VIEW,
)
from config.topics import (
    TOPIC_ORDERS, TOPIC_INVENTORY_RESULTS,
    TOPIC_PAYMENTS_RESULTS, TOPIC_INVENTORY_UPDATED,
)
from load_testing.flash_sale import run_flash_sale
from common.event_validation import INT_MAX
from services.order.producer import create_order
from services.inventory.database import get_connection as inventory_connection
from services.payment.database import get_connection as payment_connection
from services.notification.database import get_connection as notification_connection
from services.stock_view.database import get_connection as view_connection


class LagReader:
    def __init__(self):
        self.readers = []
        try:
            for group, topics in [
                (GROUP_INVENTORY, [TOPIC_ORDERS, TOPIC_PAYMENTS_RESULTS]),
                (GROUP_PAYMENT, [TOPIC_INVENTORY_RESULTS]),
                (GROUP_NOTIFICATION, [TOPIC_PAYMENTS_RESULTS]),
                (GROUP_STOCK_VIEW, [TOPIC_INVENTORY_UPDATED]),
            ]:
                reader = Consumer({
                    "bootstrap.servers": BOOTSTRAP_SERVERS,
                    "group.id": group,
                    "enable.auto.commit": False,
                    "enable.auto.offset.store": False,
                    "allow.auto.create.topics": False,
                })
                self.readers.append((group, reader, topics))
        except BaseException:
            self.close()
            raise

    def snapshot(self):
        rows = []
        # Không subscribe/poll/assign/commit: chỉ query metadata và offsets.
        for group, reader, topics in self.readers:
            for topic in topics:
                metadata = reader.list_topics(topic, timeout=10).topics[topic]
                if metadata.error:
                    raise RuntimeError(str(metadata.error))
                partitions = [TopicPartition(topic, p) for p in sorted(metadata.partitions)]
                for tp in reader.committed(partitions, timeout=10):
                    if tp.error:
                        raise RuntimeError(str(tp.error))
                    low, high = reader.get_watermark_offsets(tp, timeout=10, cached=False)
                    committed = tp.offset
                    if committed < 0:
                        # Empty, unused partition hợp lệ; partition có dữ liệu chưa có
                        # commit chưa thể gọi là hoàn tất, kể cả cấu hình latest.
                        lag = high if low == 0 else None
                    elif committed < low or committed > high:
                        lag = None
                    else:
                        lag = high - committed
                    rows.append({"group": group, "topic": topic,
                                 "partition": tp.partition, "low": low, "high": high,
                                 "committed": committed, "lag": lag})
        return rows

    def close(self):
        for _, reader, _ in self.readers:
            reader.close()
        self.readers.clear()


def read_snapshot(inv, pay, notify, view, product, run_id):
    prefix = f"ORD-{run_id}-%"
    # Các SELECT dùng autocommit. Có thể đọc lệch tức thời giữa DB;
    # đối soát yêu cầu ba mẫu settled liên tiếp sau khi producer kết thúc.
    counts = inv.execute(
        """
        SELECT count(*) AS processed,
          count(*) FILTER (WHERE status = 'RESERVED') AS reserved,
          count(*) FILTER (WHERE status = 'REJECTED') AS rejected,
          count(*) FILTER (WHERE status = 'RELEASED') AS released,
          count(*) FILTER (WHERE order_id NOT LIKE %s) AS foreign_orders
        FROM reservations WHERE product_id = %s
        """, (prefix, product)).fetchone()
    inventory = inv.execute(
        "SELECT stock, version FROM products WHERE product_id = %s",
        (product,)).fetchone()
    projection = view.execute(
        "SELECT stock_after, version FROM stock_levels WHERE product_id = %s",
        (product,)).fetchone()
    payments = pay.execute(
        """
        SELECT count(*) AS total,
          count(*) FILTER (WHERE payment_event->'data'->>'success' = 'true') AS success,
          count(*) FILTER (WHERE payment_event->'data'->>'success' = 'false') AS failure
        FROM payment_decisions WHERE order_id LIKE %s
        """, (prefix,)).fetchone()
    notifications = notify.execute(
        "SELECT count(*) AS total, "
        "count(*) FILTER (WHERE success) AS success, "
        "count(*) FILTER (WHERE NOT success) AS failure "
        "FROM notifications WHERE order_id LIKE %s", (prefix,)).fetchone()
    pending = inv.execute(
        "SELECT count(*) AS n FROM outbox_events WHERE published_at IS NULL"
    ).fetchone()["n"]
    errors = inv.execute("SELECT count(*) AS n FROM consumer_errors").fetchone()["n"]
    return dict(counts, inventory=inventory, projection=projection,
                payments=payments, notifications=notifications,
                pending_outbox=pending, errors=errors)


def is_settled(s, *, count, initial_stock, quantity, mode, initial_errors):
    stock, view = s["inventory"], s["projection"]
    if stock is None or view is None:
        return False
    reserved, rejected, released = s["reserved"], s["rejected"], s["released"]
    payment = s["payments"]
    notification = s["notifications"]
    checks = [
        s["processed"] == count,
        s["foreign_orders"] == 0,
        reserved + rejected + released == count,
        payment["total"] == reserved + released,
        payment["success"] == reserved,
        payment["failure"] == released,
        notification == payment,
        s["pending_outbox"] == 0,
        s["errors"] == initial_errors,
        stock["stock"] == initial_stock - quantity * reserved,
        stock["stock"] >= 0,
        stock["version"] == reserved + 2 * released,
        view["stock_after"] == stock["stock"],
        view["version"] == stock["version"],
    ]
    if mode == "success":
        checks += [released == 0,
                   reserved == min(count, initial_stock // quantity)]
    elif mode == "failure":
        checks += [reserved == 0, payment["success"] == 0]
    return all(checks)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--initial-stock", type=int, required=True)
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument("--payment-mode", choices=["success", "failure", "random"],
                        default="success")
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if (not re.fullmatch(r"[A-Za-z0-9-]{1,60}", args.run_id)
            or args.count <= 0 or args.quantity <= 0
            or not math.isfinite(args.timeout) or args.timeout <= 0
            or not 0 <= args.initial_stock <= INT_MAX
            or len(f"ORD-{args.run_id}-{args.count}") > 100):
        parser.error("Invalid run options; run ID excludes SQL LIKE wildcards")
    try:
        create_order(args.product_id, args.quantity, f"ORD-{args.run_id}-1")
    except ValueError as exc:
        parser.error(str(exc))
    output = Path(args.output)
    if output.exists():
        parser.error("Output already exists; choose a fresh run/output")
    output.parent.mkdir(parents=True, exist_ok=True)
    lag = LagReader()
    try:
        with (inventory_connection() as inv, payment_connection() as pay,
              notification_connection() as notify, view_connection() as view,
              output.open("x", encoding="utf-8") as stream):
            before = read_snapshot(inv, pay, notify, view, args.product_id, args.run_id)
            offsets = lag.snapshot()
            if (before["processed"] != 0 or before["payments"]["total"] != 0
                    or before["notifications"]["total"] != 0
                    or before["inventory"] != {"stock": args.initial_stock, "version": 0}
                    or before["projection"] != {"stock_after": args.initial_stock, "version": 0}
                    or before["pending_outbox"] != 0
                    or any(row["lag"] != 0 for row in offsets)):
                raise RuntimeError("Run requires a fresh seeded product and globally drained test system")
            started = previous_time = time.monotonic()
            previous_count = 0
            stable = 0
            # Chỉ generator ở worker thread; DB connections dùng ở main thread.
            stop = Event()
            with ThreadPoolExecutor(max_workers=1) as pool:
                try:
                    future = pool.submit(run_flash_sale, count=args.count,
                                         product_id=args.product_id, quantity=args.quantity,
                                         run_id=args.run_id, rate=0, stop_event=stop)
                    while True:
                        current = read_snapshot(inv, pay, notify, view, args.product_id, args.run_id)
                        offsets = lag.snapshot()
                        now = time.monotonic()
                        rate = ((current["processed"] - previous_count) /
                                max(now - previous_time, 1e-9))
                        record = dict(current, elapsed_seconds=now-started,
                                      inventory_orders_processed_per_second=rate, offsets=offsets)
                        stream.write(json.dumps(record) + "\n")
                        stream.flush()
                        previous_time, previous_count = now, current["processed"]
                        if future.done():
                            delivery = future.result()
                            if (delivery.acknowledged != args.count or delivery.failed
                                    or delivery.unconfirmed or delivery.not_submitted
                                    or delivery.interrupted or delivery.error):
                                raise RuntimeError("Producer delivery incomplete; do not claim business PASS")
                            valid = is_settled(
                                current, count=args.count, initial_stock=args.initial_stock,
                                quantity=args.quantity, mode=args.payment_mode,
                                initial_errors=before["errors"],
                            ) and all(row["lag"] == 0 for row in offsets)
                            stable = stable + 1 if valid else 0
                            if stable >= 3:
                                print(json.dumps({"result": "PASS", "delivery": asdict(delivery),
                                                  "settled_after_seconds": now-started,
                                                  "final": current}, indent=2))
                                return 0
                        if now - started >= args.timeout:
                            raise TimeoutError("Backlog/reconciliation did not settle before timeout")
                        time.sleep(1)
                finally:
                    stop.set()
    finally:
        lag.close()


if __name__ == "__main__":
    raise SystemExit(main())
