"""Slack alert sender for fraud and DQ events."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal

import requests

logger = logging.getLogger(__name__)


class SlackAlerter:
    def __init__(self, webhook_url: str) -> None:
        if not webhook_url:
            raise ValueError("SLACK_WEBHOOK_URL is not set")
        self._url = webhook_url

    def _post(self, payload: dict) -> bool:
        try:
            response = requests.post(self._url, json=payload, timeout=5)
            response.raise_for_status()
            return True
        except requests.RequestException as exc:
            logger.error("slack alert failed", extra={"error": str(exc)})
            return False

    def send_fraud_alert(
        self,
        *,
        rule_name: str,
        symbol: str,
        window_start: str,
        severity: str,
        alert_value: float,
        threshold_value: float,
        details: dict,
    ) -> bool:
        emoji = {
            "CRITICAL": ":red_circle:",
            "HIGH": ":large_orange_circle:",
            "MEDIUM": ":large_yellow_circle:",
        }.get(severity, ":white_circle:")

        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} Fraud Alert: {rule_name}",
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Symbol:*\n{symbol}"},
                        {"type": "mrkdwn", "text": f"*Severity:*\n{severity}"},
                        {"type": "mrkdwn", "text": f"*Window:*\n{window_start}"},
                        {"type": "mrkdwn", "text": f"*Alert Value:*\n{alert_value}"},
                        {"type": "mrkdwn", "text": f"*Threshold:*\n{threshold_value}"},
                        {"type": "mrkdwn", "text": f"*Rule:*\n{rule_name}"},
                    ],
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Details:*\n```{details}```",
                    },
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"fraud-platform | {datetime.now(timezone.utc).isoformat()}",
                        }
                    ],
                },
            ]
        }
        return self._post(payload)

    def send_dq_alert(
        self,
        *,
        check_name: str,
        table_name: str,
        failed_count: int,
        details: str,
    ) -> bool:
        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f":warning: DQ Alert: {check_name}",
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Table:*\n{table_name}"},
                        {"type": "mrkdwn", "text": f"*Failed Records:*\n{failed_count}"},
                    ],
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Details:*\n{details}"},
                },
            ]
        }
        return self._post(payload)