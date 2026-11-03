-- Run explicitly with: python -m services.stock_view.database
-- This schema belongs only to stock_view_db, never inventory_db.
CREATE TABLE IF NOT EXISTS stock_levels (
    product_id VARCHAR(50) PRIMARY KEY,
    stock_after INTEGER NOT NULL CHECK (stock_after >= 0),
    version BIGINT NOT NULL CHECK (version >= 0),
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id VARCHAR(100) PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
