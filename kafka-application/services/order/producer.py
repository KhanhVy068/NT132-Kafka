
import argparse
import json
import logging
import os
import time
from uuid import uuid4

from dotenv import load_dotenv

from common.event_schema import order_created
from common.kafka_client import create_producer


load_dotenv()

logger = logging.getLogger(__name__)

TOPIC_ORDERS = os.getenv("TOPIC_ORDERS", "orders.created")


# 1. Tạo Event đơn hàng
def create_order(product_id, quantity, order_id=None):
    if not isinstance(product_id, str) or not product_id.strip():
        raise ValueError("product_id không được rỗng")

    if type(quantity) is not int or quantity <= 0:
        raise ValueError("quantity phải là số nguyên dương")

    if order_id is None:
        order_id = f"ORD-{uuid4().hex[:16]}"
    elif not isinstance(order_id, str) or not order_id.strip():
        raise ValueError("order_id không hợp lệ")

    return order_created(
        order_id=order_id,
        product_id=product_id,
        quantity=quantity,
    )


# 2. Gửi Event và chờ Kafka xác nhận
def publish_order(producer, event, timeout=15.0):
    if timeout <= 0:
        raise ValueError("timeout phải lớn hơn 0")

    if event.get("event_type") != "OrderCreated":
        raise ValueError("Chỉ hỗ trợ OrderCreated")

    data = event.get("data")
    if not isinstance(data, dict):
        raise ValueError("Event thiếu data")

    order_id = data.get("order_id")
    if not isinstance(order_id, str) or not order_id.strip():
        raise ValueError("Event thiếu order_id")

    result = {
        "completed": False,
        "success": False,
        "error": None,
        "partition": None,
        "offset": None,
    }

    def on_delivery(error, message):
        result["completed"] = True

        if error is not None:
            result["error"] = str(error)
            return

        result["success"] = True

        if message is not None:
            result["partition"] = message.partition()
            result["offset"] = message.offset()

    # Event phải giữ nguyên cấu trúc JSON chung
    payload = json.dumps(
        event,
        ensure_ascii=False,
    ).encode("utf-8")

    producer.produce(
        topic=TOPIC_ORDERS,
        key=order_id.encode("utf-8"),
        value=payload,
        callback=on_delivery,
    )

    deadline = time.monotonic() + timeout

    while not result["completed"]:
        remaining = deadline - time.monotonic()

        if remaining <= 0:
            raise TimeoutError(
                "Chưa nhận được xác nhận gửi từ Kafka. "
                "Trạng thái Event chưa xác định."
            )

        producer.poll(min(0.5, remaining))

    if not result["success"]:
        raise RuntimeError(
            f"Kafka delivery failed: {result['error']}"
        )

    return {
        "success": True,
        "event_id": event["event_id"],
        "order_id": order_id,
        "topic": TOPIC_ORDERS,
        "partition": result["partition"],
        "offset": result["offset"],
    }


# 3. Điểm vào dành cho CLI
def main():
    parser = argparse.ArgumentParser(
        description="Order Producer - Apache Kafka E-commerce"
    )

    parser.add_argument(
        "--product",
        required=True,
        help="Mã sản phẩm, ví dụ P01",
    )

    parser.add_argument(
        "--quantity",
        required=True,
        type=int,
        help="Số lượng đặt mua",
    )

    parser.add_argument(
        "--order-id",
        default=None,
        help="Mã đơn tùy chọn",
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Thời gian chờ Kafka xác nhận (giây)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    producer = None

    try:
        event = create_order(
            product_id=args.product,
            quantity=args.quantity,
            order_id=args.order_id,
        )

        print("OrderCreated:")
        print(json.dumps(event, indent=2, ensure_ascii=False))

        producer = create_producer()

        result = publish_order(
            producer,
            event,
            timeout=args.timeout,
        )

        print("Delivery result:")
        print(json.dumps(result, indent=2, ensure_ascii=False))

        return 0

    except (ValueError, RuntimeError, TimeoutError) as exc:
        logger.error("Order failed: %s", exc)
        return 1

    except Exception:
        logger.exception("Unexpected producer error")
        return 1

    finally:
        if producer is not None:
            remaining = producer.flush(5)

            if remaining:
                logger.warning(
                    "%s Kafka message(s) còn chưa hoàn tất",
                    remaining,
                )


if __name__ == "__main__":
    raise SystemExit(main())
