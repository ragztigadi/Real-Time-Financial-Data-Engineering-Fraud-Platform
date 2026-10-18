"""Entry point for DQ checks."""

from __future__ import annotations

import logging

import snowflake.connector

from config.settings import get_settings
from src.alerts.slack import SlackAlerter
from src.common.logging_config import configure_logging, new_correlation_id
from src.dq.checks import DQFramework
from src.dq.snowflake_checks import run_all_checks

logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.service_name, settings.environment)
    new_correlation_id()

    conn = snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        schema="staging",
    )

    alerter = SlackAlerter(settings.slack_webhook_url)
    framework = DQFramework(conn=conn, alerter=alerter)

    try:
        run_all_checks(framework)
        summary = framework.summary()
        logger.info("dq run complete", extra=summary)

        if summary["failed"] > 0:
            alerter.send_dq_alert(
                check_name="DQ_SUMMARY",
                table_name="all",
                failed_count=summary["failed"],
                details=f"{summary['failed']}/{summary['total_checks']} checks failed. Pass rate: {summary['pass_rate']}",
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
