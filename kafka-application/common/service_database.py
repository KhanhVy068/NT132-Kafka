import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row

load_dotenv()


def require_idle(conn):
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise RuntimeError("A top-level transaction requires an idle connection")


def connect_service(prefix, default_database, default_user):
    name = os.getenv(f"{prefix}_DB_NAME", default_database)
    user = os.getenv(f"{prefix}_DB_USER", default_user)
    if not name.strip() or not user.strip():
        raise ValueError("Database name and user must not be empty")
    forbidden = {
        "inventory_db", "stock_view_db",
        os.getenv("INVENTORY_DB_NAME", "inventory_db"),
        os.getenv("STOCK_VIEW_DB_NAME", "stock_view_db"),
    }
    other = "NOTIFICATION" if prefix == "PAYMENT" else "PAYMENT"
    forbidden.add(os.getenv(f"{other}_DB_NAME", f"{other.lower()}_db"))
    forbidden_users = {
        "inventory_user", "stock_view_user",
        os.getenv("INVENTORY_DB_USER", "inventory_user"),
        os.getenv("STOCK_VIEW_DB_USER", "stock_view_user"),
        os.getenv(f"{other}_DB_USER", f"{other.lower()}_user"),
    }
    if name in forbidden or user in forbidden_users:
        raise ValueError("Payment/Notification require separate databases and accounts")
    return psycopg.connect(
        host=os.getenv(f"{prefix}_DB_HOST", "localhost"),
        port=int(os.getenv(f"{prefix}_DB_PORT", "5432")),
        dbname=name,
        user=user,
        password=os.environ[f"{prefix}_DB_PASSWORD"],
        row_factory=dict_row,
        connect_timeout=10,
        autocommit=True,
    )


def initialize_schema(conn, module_file):
    require_idle(conn)
    # Các schema dưới đây không chứa procedural SQL hoặc dấu ; trong literal/comment.
    source = Path(module_file).with_name("schema.sql").read_text(encoding="utf-8")
    with conn.transaction():
        for statement in source.split(";"):
            if statement.strip():
                conn.execute(statement)
