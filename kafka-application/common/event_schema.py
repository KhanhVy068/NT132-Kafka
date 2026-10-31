# Quy định cấu trúc các sự kiện để Producer gửi và Consumer đọc thống nhất.
from datetime import datetime, timezone
from uuid import uuid4


def create_event(event_type, data):
    """Cấu trúc chung của tất cả Event."""
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": data
    }


# 1. Order Service
def order_created(order_id, product_id, quantity):
    if quantity <= 0:
        raise ValueError("Quantity must be positive")

    return create_event("OrderCreated", {
        "order_id": order_id,
        "product_id": product_id,
        "quantity": quantity
    })


# 2. Inventory Service - kết quả giữ hàng
def inventory_result(order_id, product_id, success):
    return create_event("InventoryResult", {
        "order_id": order_id,
        "product_id": product_id,
        "success": success
    })


# 3. Inventory Service - thay đổi tồn kho
def stock_updated(product_id, stock_after):
    return create_event("StockUpdated", {
        "product_id": product_id,
        "stock_after": stock_after
    })


# 4. Payment Service
def payment_result(order_id, success):
    return create_event("PaymentResult", {
        "order_id": order_id,
        "success": success
    })
