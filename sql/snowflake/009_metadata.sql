USE DATABASE fraud_platform;
USE SCHEMA transformed;
USE WAREHOUSE fraud_wh;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          NUMBER AUTOINCREMENT PRIMARY KEY,
    pipeline_name   VARCHAR(100)  NOT NULL,
    stage           VARCHAR(50)   NOT NULL,
    status          VARCHAR(20)   NOT NULL,
    records_in      BIGINT,
    records_out     BIGINT,
    records_failed  BIGINT,
    started_at      TIMESTAMP_NTZ NOT NULL,
    finished_at     TIMESTAMP_NTZ,
    duration_sec    NUMBER,
    error_message   VARCHAR(2000),
    metadata        VARIANT,
    created_at      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO pipeline_runs (
    pipeline_name, stage, status, records_in, records_out,
    records_failed, started_at, finished_at, duration_sec, metadata
)
SELECT
    'binance-ingestion', 'bronze', 'SUCCESS', 1000, 1000,
    0, '2026-09-21 17:37:00'::TIMESTAMP_NTZ, '2026-09-21 17:38:00'::TIMESTAMP_NTZ, 60,
    PARSE_JSON('{"symbols": 5, "stream_types": 2}');

SELECT * FROM pipeline_runs;