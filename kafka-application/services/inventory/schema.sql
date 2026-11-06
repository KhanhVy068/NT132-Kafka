
-- 1. Bảng sản phẩm và tồn kho
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    stock INTEGER NOT NULL CHECK (stock >= 0),
    version BIGINT NOT NULL DEFAULT 0
        CHECK (version >= 0)
);

-- 2. Bảng giữ hàng theo đơn
CREATE TABLE IF NOT EXISTS reservations (
    order_id VARCHAR(100) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL
        REFERENCES products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    status VARCHAR(20) NOT NULL
        CHECK (status IN (
            'RESERVED', 'RELEASED', 'REJECTED'
        )),
    created_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);

-- 3. Bảng theo dõi Event đã xử lý
CREATE TABLE IF NOT EXISTS processed_events (
    event_id VARCHAR(100) PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);

-- 4. Bảng Transactional Outbox
CREATE TABLE IF NOT EXISTS outbox_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id VARCHAR(100) UNIQUE NOT NULL,
    topic VARCHAR(100) NOT NULL,
    message_key VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);

-- Tăng tốc truy vấn Event chưa gửi
CREATE INDEX IF NOT EXISTS idx_outbox_pending
ON outbox_events (id)
WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS consumer_errors (
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL CHECK (kafka_offset >= 0),
    message_key BYTEA,
    raw_payload BYTEA,
    error_class TEXT NOT NULL,
    error_message TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (topic, partition_id, kafka_offset)
);
