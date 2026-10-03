-- ============================================================
-- Analytics views
-- ============================================================

USE DATABASE fraud_platform;
USE SCHEMA analytics;
USE WAREHOUSE fraud_wh;

-- 1. Current customer view — no history, just latest state
CREATE OR REPLACE VIEW vw_current_customers AS
SELECT
    customer_key,
    customer_id,
    external_ref,
    full_name,
    email,
    country,
    risk_tier,
    kyc_status,
    is_active,
    effective_from,
    cdc_operation
FROM fraud_platform.transformed.dim_customer
WHERE is_current = TRUE;

-- 2. Trade volume by symbol — what fraud velocity checks query
CREATE OR REPLACE VIEW vw_trade_volume AS
SELECT
    f.window_start,
    f.window_end,
    f.symbol,
    ds.base_asset,
    ds.quote_asset,
    f.trade_count,
    f.total_notional,
    f.avg_notional,
    f.max_notional,
    f.vwap,
    f.window_duration_sec,
    dd.full_date,
    dd.day_name,
    dd.is_weekend
FROM fraud_platform.transformed.fact_volume_1m f
JOIN fraud_platform.transformed.dim_symbol ds ON ds.symbol_key = f.symbol_key
JOIN fraud_platform.transformed.dim_date   dd ON dd.date_key   = f.date_key;

-- 3. Market imbalance — feeds wash-trade detection
CREATE OR REPLACE VIEW vw_market_imbalance AS
SELECT
    window_start,
    symbol,
    SUM(CASE WHEN is_buyer_maker = FALSE THEN trade_count ELSE 0 END) AS buyer_trades,
    SUM(CASE WHEN is_buyer_maker = TRUE  THEN trade_count ELSE 0 END) AS seller_trades,
    SUM(CASE WHEN is_buyer_maker = FALSE THEN total_notional ELSE 0 END) AS buyer_notional,
    SUM(CASE WHEN is_buyer_maker = TRUE  THEN total_notional ELSE 0 END) AS seller_notional,
    ROUND(
        CASE
            WHEN SUM(trade_count) = 0 THEN 0
            ELSE ABS(
                SUM(CASE WHEN is_buyer_maker = FALSE THEN trade_count ELSE 0 END) -
                SUM(CASE WHEN is_buyer_maker = TRUE  THEN trade_count ELSE 0 END)
            ) / SUM(trade_count)
        END, 4
    ) AS imbalance_ratio
FROM fraud_platform.transformed.fact_side_imbalance_1m
GROUP BY window_start, symbol;

-- 4. High risk customers — fraud rules join here
CREATE OR REPLACE VIEW vw_high_risk_customers AS
SELECT *
FROM vw_current_customers
WHERE risk_tier IN ('elevated', 'high')
  AND kyc_status = 'verified'
  AND is_active = TRUE;

-- verify
SHOW VIEWS IN SCHEMA fraud_platform.analytics;

SELECT * FROM fraud_platform.analytics.vw_trade_volume;
SELECT * FROM fraud_platform.analytics.vw_high_risk_customers;
SELECT * FROM fraud_platform.analytics.vw_market_imbalance LIMIT 5;