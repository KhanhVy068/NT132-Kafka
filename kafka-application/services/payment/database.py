import json
from uuid import NAMESPACE_URL, uuid5

from psycopg.types.json import Jsonb

from common.event_schema import payment_result
from common.service_database import connect_service, initialize_schema, require_idle


def get_connection():
    return connect_service("PAYMENT", "payment_db", "payment_user")


def init_db(conn):
    initialize_schema(conn, __file__)


def load_or_create_decision(conn, source, settings, decide):
    require_idle(conn)
    data = source["data"]
    order_id = data["order_id"]
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                     (order_id,))
        stored = conn.execute(
            "SELECT source_event_id, product_id, source_event, payment_event "
            "FROM payment_decisions WHERE order_id = %s", (order_id,)
        ).fetchone()
        if stored is not None:
            if stored["product_id"] != data["product_id"]:
                raise ValueError("Conflicting product for the same payment order")
            if (stored["source_event_id"] == source["event_id"]
                    and stored["source_event"] != source):
                raise ValueError("Source event ID was reused with different content")
            return stored["payment_event"]

        result = payment_result(order_id, decide(order_id, settings))
        identity = json.dumps([order_id, source["event_id"]],
                              ensure_ascii=False, separators=(",", ":"))
        # Giữ thuật toán v2 hiện tại cho quyết định đầu tiên.
        result["event_id"] = str(uuid5(
            NAMESPACE_URL, f"NT132-Kafka/PaymentResult/v2/{identity}"))
        result["timestamp"] = source["timestamp"]
        conn.execute(
            """
            INSERT INTO payment_decisions
                (order_id, source_event_id, product_id, source_event, payment_event)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (order_id, source["event_id"], data["product_id"],
             Jsonb(source), Jsonb(result)),
        )
    return result


if __name__ == "__main__":
    with get_connection() as connection:
        init_db(connection)
    print("Payment schema ready")
