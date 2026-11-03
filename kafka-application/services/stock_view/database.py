"""PostgreSQL projection; this module never reads or connects to Inventory."""

import argparse
from datetime import datetime
import json
import os
from pathlib import Path

from dotenv import load_dotenv
import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row


load_dotenv()


def get_connection():
    """Connect using the dedicated Stock View database and account."""
    dbname = os.getenv("STOCK_VIEW_DB_NAME", "stock_view_db")
    user = os.getenv("STOCK_VIEW_DB_USER", "stock_view_user")
    inventory_names = {"inventory_db", os.getenv("INVENTORY_DB_NAME", "inventory_db")}
    inventory_users = {"inventory_user", os.getenv("INVENTORY_DB_USER", "inventory_user")}
    if dbname in inventory_names or user in inventory_users:
        raise ValueError("Stock View requires its own database and account, separate from Inventory")
    if not dbname.strip() or not user.strip():
        raise ValueError("STOCK_VIEW_DB_NAME and STOCK_VIEW_DB_USER must not be empty")
    return psycopg.connect(
        host=os.getenv("STOCK_VIEW_DB_HOST", "localhost"),
        port=int(os.getenv("STOCK_VIEW_DB_PORT", "5432")),
        dbname=dbname,
        user=user,
        password=os.environ["STOCK_VIEW_DB_PASSWORD"],
        connect_timeout=10,
        row_factory=dict_row,
        autocommit=True,
    )


def _require_idle_connection(conn):
    # A savepoint inside somebody else's open transaction is not a durable commit.
    # In particular, the Kafka adapter must never commit an offset after a savepoint.
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise ValueError("Stock View requires an idle connection, with no outer transaction")


def init_db(conn):
    """Create tables safely on repeated explicit invocations; never create a DB."""
    _require_idle_connection(conn)
    schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
    with conn.transaction():
        for statement in schema.split(";"):
            if statement.strip():
                conn.execute(statement)


def validate_event(event):
    """Require the agreed versioned contract, including an explicit version=0 seed."""
    if not isinstance(event, dict) or event.get("event_type") != "StockUpdated":
        raise ValueError("Expected a StockUpdated JSON object")
    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip() or len(event_id) > 100:
        raise ValueError("event_id must be a nonempty string of at most 100 characters")
    timestamp = event.get("timestamp")
    if not isinstance(timestamp, str):
        raise ValueError("timestamp must be an ISO 8601 string with timezone")
    try:
        updated_at = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("timestamp must be an ISO 8601 string with timezone") from exc
    if updated_at.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    data = event.get("data")
    if not isinstance(data, dict):
        raise ValueError("StockUpdated.data must be an object")
    product_id = data.get("product_id")
    if not isinstance(product_id, str) or not product_id.strip() or len(product_id) > 50:
        raise ValueError("product_id must be a nonempty string of at most 50 characters")
    stock_after = data.get("stock_after")
    if type(stock_after) is not int or not 0 <= stock_after <= 2**31 - 1:
        raise ValueError("stock_after must be a nonnegative PostgreSQL INTEGER")
    if "version" not in data:
        raise ValueError(
            "StockUpdated.data.version is required; TV3 must publish the product version "
            "from the same Inventory transaction (legacy events cannot be projected safely)"
        )
    version = data["version"]
    if type(version) is not int or not 0 <= version <= 2**63 - 1:
        raise ValueError("version must be a nonnegative PostgreSQL BIGINT")
    return updated_at


def process_event(conn, event):
    """Atomically deduplicate and apply only a strictly newer product version.

    The connection must be idle: successful return means a top-level PostgreSQL
    transaction has committed, so the caller may then commit the Kafka offset.
    """
    updated_at = validate_event(event)
    _require_idle_connection(conn)
    data = event["data"]
    with conn.transaction():
        inserted = conn.execute(
            """
            INSERT INTO processed_events (event_id)
            VALUES (%s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            (event["event_id"],),
        ).fetchone()
        if inserted is None:
            return {"processed": False, "duplicate": True, "updated": False}
        updated = conn.execute(
            """
            INSERT INTO stock_levels (product_id, stock_after, version, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE
            SET stock_after = EXCLUDED.stock_after,
                version = EXCLUDED.version,
                updated_at = EXCLUDED.updated_at
            WHERE EXCLUDED.version > stock_levels.version
            RETURNING product_id
            """,
            (data["product_id"], data["stock_after"], data["version"], updated_at),
        ).fetchone()
    return {"processed": True, "duplicate": False, "updated": updated is not None}


def import_events(conn, path):
    """Apply UTF-8 JSONL StockUpdated envelopes from an externally supplied snapshot.

    Each line commits independently and can be replayed safely after a failure.
    Source versions must be supplied explicitly by the snapshot owner.
    """
    counts = {"processed": 0, "duplicate": 0, "updated": 0}
    with Path(path).open(encoding="utf-8-sig") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
                result = process_event(conn, event)
            except (ValueError, TypeError) as exc:
                raise ValueError(f"Invalid snapshot event at line {line_number}: {exc}") from exc
            for name in counts:
                counts[name] += int(result[name])
    return counts


def main():
    parser = argparse.ArgumentParser(description="Explicitly initialize Stock View PostgreSQL tables")
    parser.add_argument("--import-events", metavar="PATH", help="Then import versioned StockUpdated JSONL")
    args = parser.parse_args()
    with get_connection() as conn:
        init_db(conn)
        print("Stock View schema ready")
        if args.import_events:
            print(json.dumps(import_events(conn, args.import_events)))


if __name__ == "__main__":
    main()
