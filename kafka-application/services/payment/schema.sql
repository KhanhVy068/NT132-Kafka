CREATE TABLE IF NOT EXISTS payment_decisions (
    order_id VARCHAR(100) PRIMARY KEY,
    source_event_id VARCHAR(100) NOT NULL UNIQUE,
    product_id VARCHAR(50) NOT NULL,
    source_event JSONB NOT NULL,
    payment_event JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
