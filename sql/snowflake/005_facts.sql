-- ============================================================
-- Fact tables
-- ============================================================

USE DATABASE fraud_platform;
USE SCHEMA transformed;
USE WAREHOUSE fraud_wh;

CREATE TABLE IF NOT EXISTS fact_volume_1m (
    fact_key            NUMBER AUTOINCREMENT PRIMARY KEY,
    -- dimension keys
    symbol_key          NUMBER NOT NULL REFERENCES dim_symbol(symbol_key),
    date_key            NUMBER NOT NULL REFERENCES dim_date(date_key),
    -- degenerate dimensions
    symbol              VARCHAR(20)   NOT NULL,
    window_start        TIMESTAMP_NTZ NOT NULL,
    window_end          TIMESTAMP_NTZ NOT NULL,
    window_duration_sec NUMBER        NOT NULL,
    hour                VARCHAR(2)    NOT NULL,
    -- measures
    trade_count         BIGINT        NOT NULL,
    total_notional      NUMBER(38,8)  NOT NULL,
    avg_notional        NUMBER(38,8)  NOT NULL,
    max_notional        NUMBER(38,8)  NOT NULL,
    vwap                NUMBER(38,8)  NOT NULL,
    -- metadata
    loaded_at           TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS fact_side_imbalance_1m (
    fact_key            NUMBER AUTOINCREMENT PRIMARY KEY,
    symbol_key          NUMBER NOT NULL REFERENCES dim_symbol(symbol_key),
    date_key            NUMBER NOT NULL REFERENCES dim_date(date_key),
    symbol              VARCHAR(20)   NOT NULL,
    window_start        TIMESTAMP_NTZ NOT NULL,
    hour                VARCHAR(2)    NOT NULL,
    is_buyer_maker      BOOLEAN       NOT NULL,
    trade_count         BIGINT        NOT NULL,
    total_notional      NUMBER(38,8)  NOT NULL,
    loaded_at           TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- load fact_volume_1m from staging

ALTER TABLE fact_volume_1m ALTER COLUMN hour DROP NOT NULL;
ALTER TABLE fact_side_imbalance_1m ALTER COLUMN hour DROP NOT NULL;

INSERT INTO fact_volume_1m (
    symbol_key, date_key, symbol, window_start, window_end,
    window_duration_sec, hour, trade_count, total_notional,
    avg_notional, max_notional, vwap
)
SELECT
    ds.symbol_key,
    TO_NUMBER(TO_CHAR(s.window_start::DATE, 'YYYYMMDD')) AS date_key,
    s.symbol,
    s.window_start,
    s.window_end,
    s.window_duration_seconds,
    CAST(s.hour AS VARCHAR)                              AS hour,
    s.trade_count,
    s.total_notional,
    s.avg_notional,
    s.max_notional,
    s.vwap
FROM fraud_platform.staging.stg_volume_1m s
JOIN dim_symbol ds ON ds.symbol = s.symbol;

-- load fact_side_imbalance_1m from staging
INSERT INTO fact_side_imbalance_1m (
    symbol_key, date_key, symbol, window_start,
    hour, is_buyer_maker, trade_count, total_notional
)
SELECT
    ds.symbol_key,
    TO_NUMBER(TO_CHAR(s.window_start::DATE, 'YYYYMMDD')) AS date_key,
    s.symbol,
    s.window_start,
    s.hour,
    s.is_buyer_maker,
    s.trade_count,
    s.total_notional
FROM fraud_platform.staging.stg_side_imbalance_1m s
JOIN dim_symbol ds ON ds.symbol = s.symbol;

-- verify
SELECT 'fact_volume_1m' AS table_name, COUNT(*) AS row_count FROM fact_volume_1m
UNION ALL
SELECT 'fact_side_imbalance_1m', COUNT(*) FROM fact_side_imbalance_1m;


