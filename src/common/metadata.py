"""Pipeline run metadata logger."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


class PipelineMetadata:
    def __init__(self, conn) -> None:
        self._conn = conn

    def log_run(
        self,
        *,
        pipeline_name: str,
        stage: str,
        status: str,
        records_in: int = 0,
        records_out: int = 0,
        records_failed: int = 0,
        started_at: datetime,
        finished_at: datetime,
        error_message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        import json

        duration = (finished_at - started_at).total_seconds()
        meta_json = json.dumps(metadata or {})

        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT INTO fraud_platform.transformed.pipeline_runs (
                pipeline_name, stage, status, records_in, records_out,
                records_failed, started_at, finished_at, duration_sec,
                error_message, metadata
            )
            SELECT %s, %s, %s, %s, %s, %s,
                   %s::TIMESTAMP_NTZ, %s::TIMESTAMP_NTZ, %s, %s,
                   PARSE_JSON(%s)
            """,
            (
                pipeline_name, stage, status, records_in, records_out,
                records_failed, str(started_at), str(finished_at),
                duration, error_message, meta_json,
            ),
        )
        self._conn.commit()
        logger.info(
            "pipeline run logged",
            extra={
                "pipeline": pipeline_name,
                "stage": stage,
                "status": status,
                "duration_sec": duration,
            },
        )