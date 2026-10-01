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
    STORAGE_AWS_ROLE_ARN = '&AWS_ROLE_ARN'
    STORAGE_ALLOWED_LOCATIONS = ('&S3_ALLOWED_LOCATION');

CREATE OR REPLACE STAGE fraud_platform.raw.s3_gold_stage
    STORAGE_INTEGRATION = s3_fraud_integration
    URL = '&S3_ALLOWED_LOCATION'
    FILE_FORMAT = (TYPE = PARQUET);