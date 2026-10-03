-- ============================================================
-- Fraud detection rules
-- ============================================================

USE DATABASE fraud_platform;
USE SCHEMA analytics;
USE WAREHOUSE fraud_wh;

-- Fraud alerts table
CREATE TABLE IF NOT EXISTS fraud_platform.transformed.fraud_alerts (
    alert_id        NUMBER AUTOINCREMENT PRIMARY KEY,
    rule_name       VARCHAR(100)  NOT NULL,
    symbol          VARCHAR(20)   NOT NULL,
    window_start    TIMESTAMP_NTZ NOT NULL,
    alert_value     NUMBER(38,8),
    threshold_value NUMBER(38,8),
    severity        VARCHAR(20)   NOT NULL,
    details         VARIANT,
    is_investigated BOOLEAN       NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- Rule 1: Velocity check
-- Flag symbols where trade_count spikes > 3x the average
-- ============================================================

INSERT INTO fraud_platform.transformed.fraud_alerts
    (rule_name, symbol, window_start, alert_value, threshold_value, severity, details)
SELECT
    'VELOCITY_SPIKE'                        AS rule_name,
    symbol,
    window_start,
    trade_count                             AS alert_value,
    avg_trade_count * 3                     AS threshold_value,
    CASE
        WHEN trade_count > avg_trade_count * 5 THEN 'CRITICAL'
        WHEN trade_count > avg_trade_count * 3 THEN 'HIGH'
        ELSE 'MEDIUM'
    END                                     AS severity,
    OBJECT_CONSTRUCT(
        'trade_count', trade_count,
        'avg_trade_count', avg_trade_count,
        'spike_ratio', ROUND(trade_count / NULLIF(avg_trade_count, 0), 2)
    )                                       AS details
FROM (
    SELECT
        symbol,
        window_start,
        trade_count,
        AVG(trade_count) OVER (
            PARTITION BY symbol
            ORDER BY window_start
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_trade_count
    FROM fraud_platform.analytics.vw_trade_volume
)
WHERE avg_trade_count IS NOT NULL
  AND trade_count > avg_trade_count * 3;

-- ============================================================
-- Rule 2: Amount anomaly
-- Flag windows where max_notional > 10x the average
-- ============================================================

INSERT INTO fraud_platform.transformed.fraud_alerts
    (rule_name, symbol, window_start, alert_value, threshold_value, severity, details)
SELECT
    'AMOUNT_ANOMALY'                        AS rule_name,
    symbol,
    window_start,
    max_notional                            AS alert_value,
    avg_notional * 10                       AS threshold_value,
    'HIGH'                                  AS severity,
    OBJECT_CONSTRUCT(
        'max_notional', max_notional,
        'avg_notional', avg_notional,
        'ratio', ROUND(max_notional / NULLIF(avg_notional, 0), 2)
    )                                       AS details
FROM (
    SELECT
        symbol,
        window_start,
        max_notional,
        avg_notional,
        AVG(avg_notional) OVER (
            PARTITION BY symbol
            ORDER BY window_start
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS rolling_avg_notional
    FROM fraud_platform.analytics.vw_trade_volume
)
WHERE rolling_avg_notional IS NOT NULL
  AND max_notional > rolling_avg_notional * 10;

-- ============================================================
-- Rule 3: Wash trade detection
-- Flag windows where imbalance_ratio < 0.05 (near-perfect balance)
-- and notional is significant
-- ============================================================

INSERT INTO fraud_platform.transformed.fraud_alerts
    (rule_name, symbol, window_start, alert_value, threshold_value, severity, details)
SELECT
    'WASH_TRADE_SUSPECTED'                  AS rule_name,
    i.symbol,
    i.window_start,
    i.imbalance_ratio                       AS alert_value,
    0.05                                    AS threshold_value,
    'CRITICAL'                              AS severity,
    OBJECT_CONSTRUCT(
        'buyer_trades',    i.buyer_trades,
        'seller_trades',   i.seller_trades,
        'buyer_notional',  i.buyer_notional,
        'seller_notional', i.seller_notional,
        'imbalance_ratio', i.imbalance_ratio
    )                                       AS details
FROM fraud_platform.analytics.vw_market_imbalance i
WHERE i.imbalance_ratio < 0.05
  AND (i.buyer_notional + i.seller_notional) > 1000;

-- verify
SELECT rule_name, severity, COUNT(*) AS alert_count
FROM fraud_platform.transformed.fraud_alerts
GROUP BY rule_name, severity
ORDER BY rule_name, severity;


SELECT 
    rule_name,
    symbol,
    window_start,
    alert_value,
    threshold_value,
    severity,
    details
FROM fraud_platform.transformed.fraud_alerts
ORDER BY severity, created_at;