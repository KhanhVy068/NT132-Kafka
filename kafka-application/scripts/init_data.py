
import argparse
import os

from services.inventory.database import (
    get_connection,
    init_db,
    seed_product,
    get_stock,
)


INITIAL_PRODUCTS = {
    "P01": 100,
    "P02": 1000,
    "P03": 0,
}


def initialize_products(conn, products=None):
    """
    Khởi tạo sản phẩm chưa tồn tại.

    Không thay đổi tồn kho của sản phẩm đã có.
    Mỗi sản phẩm mới được ghi StockUpdated
    vào Transactional Outbox bởi seed_product().
    """
    if products is None:
        products = INITIAL_PRODUCTS

    created_count = 0
    skipped_count = 0

    for product_id, stock in products.items():
        created = seed_product(
            conn,
            product_id,
            stock,
        )

        if created:
            created_count += 1
            print(
                f"[CREATED] {product_id}: stock={stock}"
            )
        else:
            skipped_count += 1
            current_stock = get_stock(conn, product_id)
            print(
                f"[SKIPPED] {product_id}: "
                f"existing_stock={current_stock}"
            )

    return {
        "created": created_count,
        "skipped": skipped_count,
    }


def show_products(conn):
    """Hiển thị toàn bộ sản phẩm hiện tại."""
    rows = conn.execute(
        """
        SELECT product_id, stock, version
        FROM products
        ORDER BY product_id
        """
    ).fetchall()

    print("\nPRODUCTS")
    print("-" * 45)

    for row in rows:
        print(
            f"{row['product_id']:<10} "
            f"stock={row['stock']:<8} "
            f"version={row['version']}"
        )


def show_outbox_summary(conn):
    """Thống kê Event chưa gửi Kafka."""
    row = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM outbox_events
        WHERE published_at IS NULL
        """
    ).fetchone()

    print(f"\nPending Outbox Events: {row['total']}")


def main():
    parser = argparse.ArgumentParser(
        description="Initialize inventory test data"
    )

    parser.add_argument(
        "--show",
        action="store_true",
        help="Chỉ xem dữ liệu hiện có",
    )

    args = parser.parse_args()

    with get_connection() as conn:
        if args.show:
            show_products(conn)
            show_outbox_summary(conn)
            return

        init_db(conn)

        print("Initializing inventory data...\n")

        result = initialize_products(conn)

        print("\nSummary:")
        print(f"Created: {result['created']}")
        print(f"Skipped: {result['skipped']}")

        show_products(conn)
        show_outbox_summary(conn)


if __name__ == "__main__":
    main()
