CREATE TABLE IF NOT EXISTS trades (
    id BIGSERIAL PRIMARY KEY,
    trade_id BIGINT NOT NULL,
    symbol TEXT NOT NULL,
    price NUMERIC NOT NULL,
    size BIGINT NOT NULL,
    trade_timestamp TIMESTAMPTZ NOT NULL,
    exchange TEXT,
    tape TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_timestamp
    ON trades (symbol, trade_timestamp DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_symbol_trade_id
    ON trades (symbol, trade_id);