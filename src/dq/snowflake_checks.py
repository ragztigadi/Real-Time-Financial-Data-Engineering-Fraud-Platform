"""DQ checks against Snowflake Silver/Staging tables."""

from __future__ import annotations

from src.dq.checks import DQFramework


def run_all_checks(framework: DQFramework) -> None:

    # 1. Null checks
    framework.run_check(
        sql="""
            SELECT COUNT(*) as failed, COUNT(*) as total
            FROM fraud_platform.staging.stg_volume_1m
            WHERE symbol IS NULL
               OR window_start IS NULL
               OR trade_count IS NULL
        """,
        check_name="NULL_CHECK",
        table_name="stg_volume_1m",
    )

    # 2. Negative values
    framework.run_check(
        sql="""
            SELECT COUNT(*) as failed, COUNT(*) as total
            FROM fraud_platform.staging.stg_volume_1m
            WHERE trade_count < 0
               OR total_notional < 0
               OR avg_notional < 0
        """,
        check_name="NEGATIVE_VALUES",
        table_name="stg_volume_1m",
    )

    # 3. Duplicate check
    framework.run_check(
        sql="""
            SELECT COUNT(*) - COUNT(DISTINCT symbol || window_start::VARCHAR) as failed,
                   COUNT(*) as total
            FROM fraud_platform.staging.stg_volume_1m
        """,
        check_name="DUPLICATE_CHECK",
        table_name="stg_volume_1m",
    )

    # 4. Referential integrity — every symbol in facts exists in dim_symbol
    framework.run_check(
        sql="""
            SELECT COUNT(*) as failed, COUNT(*) as total
            FROM fraud_platform.transformed.fact_volume_1m f
            LEFT JOIN fraud_platform.transformed.dim_symbol d
                   ON d.symbol = f.symbol
            WHERE d.symbol_key IS NULL
        """,
        check_name="REFERENTIAL_INTEGRITY",
        table_name="fact_volume_1m",
    )

    # 5. Business rule — vwap must be between min and max price range
    framework.run_check(
        sql="""
            SELECT COUNT(*) as failed, COUNT(*) as total
            FROM fraud_platform.staging.stg_volume_1m
            WHERE vwap <= 0
        """,
        check_name="BUSINESS_RULE_VWAP",
        table_name="stg_volume_1m",
    )

    # 6. Freshness check — data should not be older than 24 hours
    framework.run_check(
        sql="""
            SELECT COUNT(*) as failed, COUNT(*) as total
            FROM (
                SELECT MAX(window_start) as latest
                FROM fraud_platform.staging.stg_volume_1m
            )
            WHERE DATEDIFF('hour', latest, CURRENT_TIMESTAMP()) > 24
        """,
        check_name="FRESHNESS_CHECK",
        table_name="stg_volume_1m",
    )