-- ============================================================
-- Dimensions: star schema
-- ============================================================

USE DATABASE fraud_platform;
USE SCHEMA transformed;
USE WAREHOUSE fraud_wh;

-- dim_symbol: one row per trading pair
CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol_key      NUMBER AUTOINCREMENT PRIMARY KEY,
    symbol          VARCHAR(20)  NOT NULL UNIQUE,
    base_asset      VARCHAR(10)  NOT NULL,
    quote_asset     VARCHAR(10)  NOT NULL,
    exchange        VARCHAR(50)  NOT NULL DEFAULT 'binance.us',
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- dim_customer: SCD Type 2 — full history of customer changes
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key        NUMBER AUTOINCREMENT PRIMARY KEY,
    customer_id         BIGINT       NOT NULL,
    external_ref        VARCHAR(64)  NOT NULL,
    full_name           VARCHAR(200) NOT NULL,
    email               VARCHAR(200) NOT NULL,
    country             CHAR(2)      NOT NULL,
    risk_tier           VARCHAR(20)  NOT NULL,
    kyc_status          VARCHAR(20)  NOT NULL,
    is_active           BOOLEAN      NOT NULL,
    -- SCD Type 2 columns
    effective_from      TIMESTAMP_NTZ NOT NULL,
    effective_to        TIMESTAMP_NTZ,
    is_current          BOOLEAN      NOT NULL DEFAULT TRUE,
    -- CDC metadata
    cdc_operation       VARCHAR(10),
    cdc_source_ts       TIMESTAMP_NTZ,
    created_at          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- dim_account: SCD Type 2
CREATE TABLE IF NOT EXISTS dim_account (
    account_key         NUMBER AUTOINCREMENT PRIMARY KEY,
    account_id          BIGINT       NOT NULL,
    customer_id         BIGINT       NOT NULL,
    account_ref         VARCHAR(64)  NOT NULL,
    account_type        VARCHAR(20)  NOT NULL,
    base_currency       CHAR(3)      NOT NULL,
    status              VARCHAR(20)  NOT NULL,
    daily_limit         NUMBER(18,2) NOT NULL,
    -- SCD Type 2 columns
    effective_from      TIMESTAMP_NTZ NOT NULL,
    effective_to        TIMESTAMP_NTZ,
    is_current          BOOLEAN      NOT NULL DEFAULT TRUE,
    -- CDC metadata
    cdc_operation       VARCHAR(10),
    cdc_source_ts       TIMESTAMP_NTZ,
    created_at          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- dim_date: calendar dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        NUMBER PRIMARY KEY,
    full_date       DATE         NOT NULL,
    year            NUMBER       NOT NULL,
    quarter         NUMBER       NOT NULL,
    month           NUMBER       NOT NULL,
    month_name      VARCHAR(10)  NOT NULL,
    week            NUMBER       NOT NULL,
    day_of_month    NUMBER       NOT NULL,
    day_of_week     NUMBER       NOT NULL,
    day_name        VARCHAR(10)  NOT NULL,
    is_weekend      BOOLEAN      NOT NULL
);

-- seed dim_date for 2 years
TRUNCATE TABLE dim_date;

INSERT INTO dim_date
SELECT
    TO_NUMBER(TO_CHAR(d.date_val, 'YYYYMMDD'))      AS date_key,
    d.date_val                                       AS full_date,
    YEAR(d.date_val)                                 AS year,
    QUARTER(d.date_val)                              AS quarter,
    MONTH(d.date_val)                                AS month,
    TO_CHAR(d.date_val, 'MMMM')                     AS month_name,
    WEEKOFYEAR(d.date_val)                           AS week,
    DAY(d.date_val)                                  AS day_of_month,
    DAYOFWEEK(d.date_val)                            AS day_of_week,
    TO_CHAR(d.date_val, 'DY')                        AS day_name,
    CASE WHEN DAYOFWEEK(d.date_val) IN (0,6)
         THEN TRUE ELSE FALSE END                    AS is_weekend
FROM (
    SELECT DATEADD('day', SEQ4(), '2026-01-01')::DATE AS date_val
    FROM TABLE(GENERATOR(ROWCOUNT => 730))
) d;

-- seed dim_symbol from staging data
INSERT INTO dim_symbol (symbol, base_asset, quote_asset)
SELECT DISTINCT
    symbol,
    LEFT(symbol, LEN(symbol) - 4)  AS base_asset,
    RIGHT(symbol, 4)                AS quote_asset
FROM fraud_platform.staging.stg_volume_1m
WHERE symbol NOT IN (SELECT symbol FROM dim_symbol);

-- verify
SELECT 'dim_symbol'   AS tbl, COUNT(*) AS row_count FROM dim_symbol
UNION ALL
SELECT 'dim_date',   COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_account',  COUNT(*) FROM dim_account;