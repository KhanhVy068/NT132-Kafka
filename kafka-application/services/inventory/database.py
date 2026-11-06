
import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from dotenv import load_dotenv

from common.event_schema import (
    inventory_result,
    stock_updated,
)

from config.topics import TOPIC_INVENTORY_RESULTS, TOPIC_INVENTORY_UPDATED
from common.service_database import require_idle
from common.event_validation import InvalidEvent, require_text, require_quantity, INT_MAX

load_dotenv()


def init_db(conn):
    """Tạo bảng từ schema.sql, dành cho DB chưa có dữ liệu."""
    schema_path = Path(__file__).with_name("schema.sql")
    sql = schema_path.read_text(encoding="utf-8")

    with conn.transaction():
        # Tách các câu lệnh vì schema hiện tại không chứa
        # dấu chấm phẩy trong chuỗi literal SQL.
        for statement in sql.split(";"):
            if statement.strip():
                conn.execute(statement)


def _add_outbox_event(conn, topic, key, event):
    """Ghi Event vào Outbox trong Transaction hiện tại."""
    conn.execute(
        """
        INSERT INTO outbox_events
            (event_id, topic, message_key, payload)
        VALUES (%s, %s, %s, %s)
        """,
        (
            event["event_id"],
            topic,
            str(key),
            Jsonb(event),
        ),
    )


def get_stock(conn, product_id):
    row = conn.execute(
        """
        SELECT stock
        FROM products
        WHERE product_id = %s
        """,
        (product_id,),
    ).fetchone()

    return None if row is None else row["stock"]


def get_reservation(conn, order_id):
    return conn.execute(
        """
        SELECT order_id, product_id, quantity, status
        FROM reservations
        WHERE order_id = %s
        """,
        (order_id,),
    ).fetchone()


def seed_product(conn, product_id, stock):
    """
    Thêm sản phẩm mới:
    - stock ban đầu
    - version = 0
    - ghi StockUpdated vào Outbox

    Không ghi đè sản phẩm đã tồn tại.
    """
    require_text(product_id, "product_id", 50)
    if type(stock) is not int or not 0 <= stock <= INT_MAX:
        raise InvalidEvent("stock must be a nonnegative PostgreSQL INTEGER")

    with conn.transaction():
        product = conn.execute(
            """
            INSERT INTO products (
                product_id, stock, version
            )
            VALUES (%s, %s, 0)
            ON CONFLICT (product_id) DO NOTHING
            RETURNING product_id, stock, version
            """,
            (product_id, stock),
        ).fetchone()

        if product is None:
            return False

        # Tạo Event với version ban đầu = 0
        event = stock_updated(
            product_id=product["product_id"],
            stock_after=product["stock"],
            version=product["version"],
        )

        # Ghi Outbox trong cùng Transaction
        _add_outbox_event(
            conn,
            TOPIC_INVENTORY_UPDATED,
            product_id,
            event,
        )

        return True


def _lock_order(conn, order_id):
    """
    Chỉ cho phép một transaction xử lý
    cùng order_id tại một thời điểm.

    Lock tự giải phóng khi transaction kết thúc.
    """
    conn.execute(
        """
        SELECT pg_advisory_xact_lock(
            hashtextextended(%s, 0)
        )
        """,
        (order_id,),
    )


def reserve_stock(conn, order_id, product_id, quantity):
    """
    Giữ hàng an toàn:
    - Chống xử lý cùng order_id đồng thời
    - Không cho tồn kho âm
    - Ghi Reservation và Outbox cùng Transaction
    """
    require_text(order_id, "order_id", 100)
    require_text(product_id, "product_id", 50)
    require_quantity(quantity)

    with conn.transaction():
        # 1. Khóa theo order_id trước
        _lock_order(conn, order_id)

        # 2. Chống xử lý trùng
        existing = get_reservation(conn, order_id)

        if existing is not None:
            if (
                existing["product_id"] != product_id
                or existing["quantity"] != quantity
            ):
                raise InvalidEvent(
                    "order_id đã tồn tại với dữ liệu khác"
                )

            return {
                "success": existing["status"] != "REJECTED",
                "duplicate": True,
                "status": existing["status"],
            }

        # 3. Khóa sản phẩm trước khi kiểm tra tồn kho
        product = conn.execute(
            """
            SELECT stock, version
            FROM products
            WHERE product_id = %s
            FOR UPDATE
            """,
            (product_id,),
        ).fetchone()

        if product is None:
            raise InvalidEvent("Sản phẩm không tồn tại")

        # 4. Kiểm tra đủ hàng hay không
        success = product["stock"] >= quantity
        status = "RESERVED" if success else "REJECTED"

        updated = None

        if success:
            # 5. Trừ kho trong Transaction
            updated = conn.execute(
                """
                UPDATE products
                SET stock = stock - %s,
                    version = version + 1
                WHERE product_id = %s
                  AND stock >= %s
                RETURNING stock, version
                """,
                (quantity, product_id, quantity),
            ).fetchone()

            if updated is None:
                raise RuntimeError("Giữ hàng thất bại")

        # 6. Lưu kết quả xử lý đơn
        conn.execute(
            """
            INSERT INTO reservations
                (order_id, product_id, quantity, status)
            VALUES (%s, %s, %s, %s)
            """,
            (order_id, product_id, quantity, status),
        )

        # 7. Lưu kết quả Inventory vào Outbox
        result_event = inventory_result(
            order_id, product_id, success
        )

        _add_outbox_event(
            conn,
            TOPIC_INVENTORY_RESULTS,
            order_id,
            result_event,
        )

        # 8. Nếu giữ hàng thành công thì lưu StockUpdated
        if success:
            stock_event = stock_updated(
                product_id=product_id,
                stock_after=updated["stock"],
                version=updated["version"],
            )

            _add_outbox_event(
                conn,
                TOPIC_INVENTORY_UPDATED,
                product_id,
                stock_event,
            )

        return {
            "success": success,
            "duplicate": False,
            "status": status,
            "stock_after": (
                updated["stock"] if updated else product["stock"]
            ),
        }


def release_stock(conn, order_id):
    """
    Hoàn trả hàng khi thanh toán thất bại.

    Chỉ RESERVED mới được hoàn trả.
    REJECTED và RELEASED không được cộng kho.
    """
    require_text(order_id, "order_id", 100)

    with conn.transaction():
        # 1. Khóa theo order_id
        _lock_order(conn, order_id)

        # 2. Khóa Reservation
        reservation = conn.execute(
            """
            SELECT order_id, product_id, quantity, status
            FROM reservations
            WHERE order_id = %s
            FOR UPDATE
            """,
            (order_id,),
        ).fetchone()

        if reservation is None:
            raise InvalidEvent("Không tìm thấy Reservation")

        # 3. Chống hoàn trả trùng
        if reservation["status"] != "RESERVED":
            return {
                "released": False,
                "status": reservation["status"],
            }

        # 4. Cộng lại tồn kho
        updated = conn.execute(
            """
            UPDATE products
            SET stock = stock + %s,
                version = version + 1
            WHERE product_id = %s
            RETURNING stock, version
            """,
            (
                reservation["quantity"],
                reservation["product_id"],
            ),
        ).fetchone()

        if updated is None:
            raise RuntimeError("Sản phẩm không tồn tại")

        # 5. Đổi trạng thái Reservation
        conn.execute(
            """
            UPDATE reservations
            SET status = 'RELEASED'
            WHERE order_id = %s
            """,
            (order_id,),
        )

        # 6. Ghi StockUpdated vào Outbox
        stock_event = stock_updated(
            product_id=reservation["product_id"],
            stock_after=updated["stock"],
            version=updated["version"],
        )

        _add_outbox_event(
            conn,
            TOPIC_INVENTORY_UPDATED,
            reservation["product_id"],
            stock_event,
        )

        return {
            "released": True,
            "status": "RELEASED",
            "stock_after": updated["stock"],
        }


def get_pending_outbox(conn, limit=100):
    rows = conn.execute(
        """
        SELECT id, event_id, topic, message_key, payload
        FROM outbox_events
        WHERE published_at IS NULL
        ORDER BY id ASC
        LIMIT %s
        """,
        (limit,),
    ).fetchall()

    return rows


def mark_outbox_published(conn, event_id):
    """Chỉ dùng sau khi Kafka xác nhận Delivery thành công."""
    with conn.transaction():
        conn.execute(
            """
            UPDATE outbox_events
            SET published_at = CURRENT_TIMESTAMP
            WHERE event_id = %s
              AND published_at IS NULL
            """,
            (event_id,),
        )


def get_connection():
    return psycopg.connect(
        host=os.getenv("INVENTORY_DB_HOST", "localhost"),
        port=int(os.getenv("INVENTORY_DB_PORT", "5432")),
        dbname=os.getenv("INVENTORY_DB_NAME", "inventory_db"),
        user=os.getenv("INVENTORY_DB_USER", "inventory_user"),
        password=os.environ["INVENTORY_DB_PASSWORD"],
        row_factory=dict_row,
        autocommit=True,
    )


def record_message_error(conn, message, error):
    require_idle(conn)
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO consumer_errors
                (topic, partition_id, kafka_offset, message_key, raw_payload,
                 error_class, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (topic, partition_id, kafka_offset) DO NOTHING
            """,
            (message.topic(), message.partition(), message.offset(),
             message.key(), message.value(), type(error).__name__, str(error)),
        )
