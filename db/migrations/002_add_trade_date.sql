-- Add a trading date to distinguish trade IDs reused
-- across different trading sessions.

ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS trade_date DATE;

-- Backfill existing records using New York time.
UPDATE trades
SET trade_date =
    (trade_timestamp AT TIME ZONE 'America/New_York')::date
WHERE trade_date IS NULL;

-- Every trade must have a trading date.
ALTER TABLE trades
    ALTER COLUMN trade_date SET NOT NULL;

-- Replace the old uniqueness rule.
DROP INDEX IF EXISTS idx_trades_symbol_trade_id;

-- Prevent duplicate trades within the same trading date.
CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_symbol_trade_id_date
    ON trades (symbol, trade_id, trade_date)
    WHERE trade_id IS NOT NULL;
