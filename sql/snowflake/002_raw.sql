-- ============================================================
-- RAW tables: mirror of Gold Parquet, loaded via COPY INTO
-- ============================================================

USE DATABASE fraud_platform;
USE SCHEMA raw;
USE WAREHOUSE fraud_wh;

CREATE TABLE IF NOT EXISTS raw_volume_1m (
    window_start    TIMESTAMP_NTZ,
    window_end      TIMESTAMP_NTZ,
    symbol          VARCHAR(20),
    trade_count     BIGINT,
    total_notional  NUMBER(38,18),
    avg_notional    NUMBER(38,18),
    max_notional    NUMBER(38,18),
    vwap            NUMBER(38,18),
    date            VARCHAR(10),
    hour            VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS raw_side_imbalance_1m (
    window_start    TIMESTAMP_NTZ,
    symbol          VARCHAR(20),
    is_buyer_maker  BOOLEAN,
    trade_count     BIGINT,
    total_notional  NUMBER(38,18),
    date            VARCHAR(10),
    hour            VARCHAR(2)
);

COPY INTO raw_volume_1m
FROM @s3_gold_stage/volume_1m/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.snappy\.parquet'
ON_ERROR = CONTINUE;

COPY INTO raw_side_imbalance_1m
FROM @s3_gold_stage/side_imbalance_1m/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.snappy\.parquet'
ON_ERROR = CONTINUE;