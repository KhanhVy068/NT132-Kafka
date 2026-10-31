# Định nghĩa tên các Topic được sử dụng trong Application.
import os
from dotenv import load_dotenv

load_dotenv()

# Order Service
TOPIC_ORDERS = os.getenv(
    "TOPIC_ORDERS", "orders.created"
)

# Inventory Service
TOPIC_INVENTORY_RESULTS = os.getenv(
    "TOPIC_INVENTORY_RESULTS", "inventory.results"
)

TOPIC_INVENTORY_UPDATED = os.getenv(
    "TOPIC_INVENTORY_UPDATED", "inventory.updated"
)

# Payment Service
TOPIC_PAYMENTS_RESULTS = os.getenv(
    "TOPIC_PAYMENTS_RESULTS", "payments.results"
)
