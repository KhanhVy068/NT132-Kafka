from psycopg.types.json import Jsonb

from common.service_database import connect_service, initialize_schema, require_idle


def get_connection():
    return connect_service("NOTIFICATION", "notification_db", "notification_user")


def init_db(conn):
    initialize_schema(conn, __file__)


def store_notification(conn, event):
    require_idle(conn)
    data = event["data"]
    with conn.transaction():
        inserted = conn.execute(
            """
            INSERT INTO notifications (order_id, event_id, success, payload)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING order_id
            """,
            (data["order_id"], event["event_id"], data["success"], Jsonb(event)),
        ).fetchone()
        if inserted is not None:
            created = True
        else:
            rows = conn.execute(
                "SELECT order_id, event_id, success, payload FROM notifications "
                "WHERE order_id = %s OR event_id = %s",
                (data["order_id"], event["event_id"]),
            ).fetchall()
            if (len(rows) != 1 or rows[0]["order_id"] != data["order_id"]
                    or rows[0]["success"] != data["success"]):
                raise ValueError("Conflicting notification for order/event")
            if (rows[0]["event_id"] == event["event_id"]
                    and rows[0]["payload"] != event):
                raise ValueError("Notification event ID reused with different content")
            created = False
    return {"event_id": event["event_id"], "order_id": data["order_id"],
            "success": data["success"], "created": created}


if __name__ == "__main__":
    with get_connection() as connection:
        init_db(connection)
    print("Notification schema ready")
