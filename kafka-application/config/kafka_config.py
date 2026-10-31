# Đọc cấu hình kết nối Kafka từ .env để các Producer/Consumer sử dụng chung.
import os
from dotenv import load_dotenv

# Đọc file .env
load_dotenv()

# Kafka Broker
BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

# Consumer Groups: Định nghĩa Consumer Group ID cho từng nghiệp vụ
GROUP_INVENTORY = os.getenv(
    "GROUP_INVENTORY", "inventory-group"
)

GROUP_PAYMENT = os.getenv(
    "GROUP_PAYMENT", "payment-group"
)

GROUP_NOTIFICATION = os.getenv(
    "GROUP_NOTIFICATION", "notification-group"
)

GROUP_STOCK_VIEW = os.getenv(
    "GROUP_STOCK_VIEW", "stock-view-group"
)

# Consumer Settings
# Xác định vị trí đọc khi chưa có Offset hợp lệ
AUTO_OFFSET_RESET = os.getenv(
    "AUTO_OFFSET_RESET", "earliest"
)

# Cho phép bật/tắt tự động commit
ENABLE_AUTO_COMMIT = (
    os.getenv("ENABLE_AUTO_COMMIT", "false").lower() == "true"
)

# Producer Configuration: Cấu hình dùng khi tạo Kafka Producer
PRODUCER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",
}

# Consumer Configuration: Trả về cấu hình Consumer theo Group ID
def get_consumer_config(group_id):
    return {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": AUTO_OFFSET_RESET,
        "enable.auto.commit": ENABLE_AUTO_COMMIT,
    }
