-- ============================================================
-- Snowflake setup: warehouse, database, schemas, integration
-- ============================================================

CREATE WAREHOUSE IF NOT EXISTS fraud_wh
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Fraud platform compute';

CREATE DATABASE IF NOT EXISTS fraud_platform;

CREATE SCHEMA IF NOT EXISTS fraud_platform.raw;
CREATE SCHEMA IF NOT EXISTS fraud_platform.staging;
CREATE SCHEMA IF NOT EXISTS fraud_platform.transformed;
CREATE SCHEMA IF NOT EXISTS fraud_platform.analytics;

-- ============================================================
-- S3 integration
-- ============================================================

USE DATABASE fraud_platform;
USE SCHEMA raw;
USE WAREHOUSE fraud_wh;

CREATE STORAGE INTEGRATION IF NOT EXISTS s3_fraud_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::317135986634:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://raghav-fraud-platform-s3/');

CREATE OR REPLACE STAGE fraud_platform.raw.s3_gold_stage
    STORAGE_INTEGRATION = s3_fraud_integration
    URL = 's3://raghav-fraud-platform-s3/gold/'
    FILE_FORMAT = (TYPE = PARQUET);

-- ============================================================
-- RAW tables
-- ============================================================

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

-- ============================================================
-- COPY INTO (run after new Gold data lands in S3)
-- ============================================================

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