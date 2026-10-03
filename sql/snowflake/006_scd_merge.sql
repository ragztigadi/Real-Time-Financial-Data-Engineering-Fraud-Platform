-- ============================================================
-- SCD Type 2 MERGE: customers
-- ============================================================

USE DATABASE fraud_platform;
USE SCHEMA transformed;
USE WAREHOUSE fraud_wh;

-- staging table for incoming CDC events
CREATE TABLE IF NOT EXISTS fraud_platform.staging.stg_cdc_customers (
    customer_id     BIGINT,
    external_ref    VARCHAR(64),
    full_name       VARCHAR(200),
    email           VARCHAR(200),
    country         CHAR(2),
    risk_tier       VARCHAR(20),
    kyc_status      VARCHAR(20),
    is_active       BOOLEAN,
    cdc_operation   VARCHAR(10),
    cdc_source_ts   TIMESTAMP_NTZ,
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)

-- SCD Type 2 MERGE procedure
CREATE OR REPLACE PROCEDURE fraud_platform.transformed.merge_dim_customer()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS

BEGIN
    -- Step 1: close rows where something changed
    UPDATE fraud_platform.transformed.dim_customer t
    SET
        effective_to = s.cdc_source_ts,
        is_current   = FALSE
    FROM fraud_platform.staging.stg_cdc_customers s
    WHERE t.customer_id  = s.customer_id
      AND t.is_current   = TRUE
      AND s.cdc_operation IN ('u', 'd')
      AND (
            t.risk_tier   <> s.risk_tier   OR
            t.kyc_status  <> s.kyc_status  OR
            t.email       <> s.email       OR
            t.is_active   <> s.is_active   OR
            t.full_name   <> s.full_name
          );

    -- Step 2: insert new current rows
    INSERT INTO fraud_platform.transformed.dim_customer (
        customer_id, external_ref, full_name, email,
        country, risk_tier, kyc_status, is_active,
        effective_from, effective_to, is_current,
        cdc_operation, cdc_source_ts
    )
    SELECT
        s.customer_id,
        s.external_ref,
        s.full_name,
        s.email,
        s.country,
        s.risk_tier,
        s.kyc_status,
        s.is_active,
        s.cdc_source_ts  AS effective_from,
        NULL             AS effective_to,
        TRUE             AS is_current,
        s.cdc_operation,
        s.cdc_source_ts
    FROM fraud_platform.staging.stg_cdc_customers s
    WHERE s.cdc_operation IN ('r', 'c', 'u')
      AND NOT EXISTS (
            SELECT 1
            FROM fraud_platform.transformed.dim_customer t
            WHERE t.customer_id  = s.customer_id
              AND t.is_current   = TRUE
              AND t.effective_from = s.cdc_source_ts
          );

    -- Step 3: soft-delete
    UPDATE fraud_platform.transformed.dim_customer t
    SET is_active    = FALSE,
        is_current   = FALSE,
        effective_to = s.cdc_source_ts
    FROM fraud_platform.staging.stg_cdc_customers s
    WHERE t.customer_id = s.customer_id
      AND t.is_current  = TRUE
      AND s.cdc_operation = 'd';

    -- Step 4: clear staging
    TRUNCATE TABLE fraud_platform.staging.stg_cdc_customers;

    RETURN 'merge_dim_customer completed';
END;

-- ============================================================
-- Seed from Postgres snapshot
-- ============================================================

INSERT INTO fraud_platform.staging.stg_cdc_customers (
    customer_id, external_ref, full_name, email,
    country, risk_tier, kyc_status, is_active,
    cdc_operation, cdc_source_ts
)
VALUES
    (1, 'CUST-0001', 'Ana Moreau',   'ana.moreau@example.com',   'FR', 'standard', 'verified', TRUE, 'r', '2026-09-12 04:57:51'),
    (2, 'CUST-0002', 'Idris Bello',  'idris.bello@example.com',  'NG', 'elevated', 'verified', TRUE, 'r', '2026-09-12 04:57:51'),
    (3, 'CUST-0003', 'Mei Tanaka',   'mei.tanaka@example.com',   'JP', 'high',     'verified', TRUE, 'r', '2026-09-12 04:57:51'),
    (4, 'CUST-0004', 'Sofia Duarte', 'sofia.duarte@example.com', 'BR', 'standard', 'pending',  TRUE, 'r', '2026-09-12 04:57:51'),
    (5, 'CUST-0005', 'Luca Ferrari', 'luca.ferrari@example.com', 'IT', 'high',     'verified', TRUE, 'r', '2026-09-12 04:57:51'),
    (6, 'CUST-0006', 'Priya Raman',  'priya.raman@example.com',  'IN', 'standard', 'verified', TRUE, 'r', '2026-09-12 04:57:51'),
    (7, 'CUST-0007', 'Tomas Novak',  'tomas.novak@example.com',  'CZ', 'low',      'verified', TRUE, 'r', '2026-09-12 04:57:51'),
    (8, 'CUST-0008', 'Hana Osei',    'hana.osei@example.com',    'GH', 'elevated', 'pending',  TRUE, 'r', '2026-09-12 04:57:51'),
    (9, 'CUST-0009', 'Diego Salas',  'diego.salas@example.com',  'MX', 'standard', 'verified', TRUE, 'r', '2026-09-12 04:57:51');

-- run the merge
CALL fraud_platform.transformed.merge_dim_customer();

-- verify
SELECT customer_id, full_name, risk_tier, kyc_status,
       is_current, effective_from, effective_to, cdc_operation
FROM fraud_platform.transformed.dim_customer
ORDER BY customer_id;

-- =======================================================
INSERT INTO fraud_platform.staging.stg_cdc_customers (
    customer_id, external_ref, full_name, email,
    country, risk_tier, kyc_status, is_active,
    cdc_operation, cdc_source_ts
)
VALUES (
    3, 'CUST-0003', 'Mei Tanaka', 'mei.tanaka@example.com',
    'JP', 'high', 'verified', TRUE,
    'u', '2026-09-12 05:34:34'
);

CALL fraud_platform.transformed.merge_dim_customer();

SELECT customer_id, full_name, risk_tier, is_current, 
       effective_from, effective_to, cdc_operation
FROM fraud_platform.transformed.dim_customer
WHERE customer_id = 3
ORDER BY effective_from;

-- ==========================================================
UPDATE fraud_platform.staging.stg_cdc_customers 
SET risk_tier = 'high', cdc_operation = 'u', 
    cdc_source_ts = '2026-09-12 05:34:34'
WHERE customer_id = 3;

-- ===========================================================

UPDATE fraud_platform.transformed.dim_customer
SET risk_tier = 'low'
WHERE customer_id = 3;

-- ===========================================================

INSERT INTO fraud_platform.staging.stg_cdc_customers (
    customer_id, external_ref, full_name, email,
    country, risk_tier, kyc_status, is_active,
    cdc_operation, cdc_source_ts
)
VALUES (
    3, 'CUST-0003', 'Mei Tanaka', 'mei.tanaka@example.com',
    'JP', 'high', 'verified', TRUE,
    'u', '2026-09-12 05:34:34'
);

CALL fraud_platform.transformed.merge_dim_customer();

SELECT customer_id, full_name, risk_tier, is_current,
       effective_from, effective_to, cdc_operation
FROM fraud_platform.transformed.dim_customer
WHERE customer_id = 3
ORDER BY effective_from;