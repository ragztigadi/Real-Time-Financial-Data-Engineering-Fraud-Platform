"""Read fraud alerts from Snowflake and send to Slack."""

from __future__ import annotations

import logging

import snowflake.connector

from config.settings import get_settings
from src.alerts.slack import SlackAlerter

logger = logging.getLogger(__name__)

def run_notifier() -> None:
    settings = get_settings()
    alerter = SlackAlerter(settings.slack_webhook_url)

    conn = snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        schema="transformed",
    )

    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                alert_id,
                rule_name,
                symbol,
                window_start,
                severity,
                alert_value,
                threshold_value,
                details
            FROM fraud_platform.transformed.fraud_alerts
            WHERE is_investigated = FALSE
            ORDER BY severity DESC, created_at ASC
            LIMIT 50    
        """)

        alerts = cur.fetchall()
        sent = 0

        for row in alerts:
            alert_id, rule_name, symbol, window_start, severity, \
            alert_value, threshold_value, details = row

            success = alerter.send_fraud_alert(
                rule_name=rule_name,
                symbol=symbol,
                window_start=str(window_start),
                severity=severity,
                alert_value=float(alert_value or 0),
                threshold_value=float(threshold_value or 0),
                details=details or {},
            )

            if success:
                cur.execute("""
                    UPDATE fraud_platform.transformed.fraud_alerts
                    SET is_investigated = TRUE
                    WHERE alert_id = %s
                """, (alert_id,))
                sent += 1

        conn.commit()
        logger.info("alerts sent", extra={"sent": sent, "total": len(alerts)})

    finally:
        conn.close()

if __name__ == "__main__":
    from src.common.logging_config import configure_logging, new_correlation_id
    settings = get_settings()
    configure_logging(settings.log_level, settings.service_name, settings.environment)
    new_correlation_id()
    run_notifier()