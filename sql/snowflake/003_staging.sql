-- ============================================================
-- Staging: clean, typed, deduplicated from RAW
-- ============================================================

USE DATABASE fraud_platform;
USE SCHEMA staging;
USE WAREHOUSE fraud_wh;

CREATE TABLE IF NOT EXISTS stg_volume_1m AS
SELECT
    window_start,
    window_end,
    UPPER(TRIM(symbol))                          AS symbol,
    trade_count,
    ROUND(total_notional, 8)                     AS total_notional,
    ROUND(avg_notional, 8)                       AS avg_notional,
    ROUND(max_notional, 8)                       AS max_notional,
    ROUND(vwap, 8)                               AS vwap,
    DATEDIFF('second', window_start, window_end) AS window_duration_seconds,
    date,
    hour,
    CURRENT_TIMESTAMP()                          AS loaded_at
FROM fraud_platform.raw.raw_volume_1m
WHERE symbol IS NOT NULL
  AND trade_count > 0
  AND total_notional > 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY symbol, window_start
    ORDER BY loaded_at DESC
) = 1;

CREATE TABLE IF NOT EXISTS stg_side_imbalance_1m AS
SELECT
    window_start,
    UPPER(TRIM(symbol))      AS symbol,
    is_buyer_maker,
    trade_count,
    ROUND(total_notional, 8) AS total_notional,
    date,
    hour,
    CURRENT_TIMESTAMP()      AS loaded_at
FROM fraud_platform.raw.raw_side_imbalance_1m
WHERE symbol IS NOT NULL
  AND trade_count > 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY symbol, window_start, is_buyer_maker
    ORDER BY loaded_at DESC
) = 1;