"""Data quality checks for Silver layer data."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

logger = logging.getLogger(__name__)


@dataclass
class DQResult:
    check_name: str
    table_name: str
    passed: bool
    failed_count: int = 0
    total_count: int = 0
    details: str = ""
    checked_at: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    @property
    def pass_rate(self) -> float:
        if self.total_count == 0:
            return 1.0
        return (self.total_count - self.failed_count) / self.total_count


@dataclass
class DQCheck:
    name: str
    description: str
    fn: Callable[..., DQResult]


class DQFramework:
    def __init__(self, conn, alerter=None) -> None:
        self._conn = conn
        self._alerter = alerter
        self._results: list[DQResult] = []

    def run_check(self, sql: str, check_name: str, table_name: str) -> DQResult:
        cur = self._conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        failed_count = row[0] if row else 0
        total_count = row[1] if row and len(row) > 1 else 0

        passed = failed_count == 0
        result = DQResult(
            check_name=check_name,
            table_name=table_name,
            passed=passed,
            failed_count=failed_count,
            total_count=total_count,
            details=f"{failed_count}/{total_count} records failed",
        )
        self._results.append(result)

        if not passed:
            logger.warning(
                "dq check failed",
                extra={
                    "check": check_name,
                    "table": table_name,
                    "failed": failed_count,
                    "total": total_count,
                },
            )
            if self._alerter:
                self._alerter.send_dq_alert(
                    check_name=check_name,
                    table_name=table_name,
                    failed_count=failed_count,
                    details=result.details,
                )
        else:
            logger.info(
                "dq check passed",
                extra={"check": check_name, "table": table_name},
            )

        return result

    def summary(self) -> dict[str, Any]:
        total = len(self._results)
        passed = sum(1 for r in self._results if r.passed)
        return {
            "total_checks": total,
            "passed": passed,
            "failed": total - passed,
            "pass_rate": round(passed / total, 2) if total else 1.0,
        }