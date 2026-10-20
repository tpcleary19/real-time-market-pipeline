CREATE TABLE IF NOT EXISTS pattern_alerts (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('Up', 'Down')),
    movement_percent DOUBLE PRECISION NOT NULL,
    baseline_price DOUBLE PRECISION NOT NULL CHECK (baseline_price > 0),
    price DOUBLE PRECISION NOT NULL CHECK (price > 0),
    window_start TIMESTAMPTZ NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS idx_pattern_alerts_symbol_timestamp
    ON pattern_alerts (symbol, event_timestamp DESC);
