"""Append-only JSONL writer. Local stand-in for the Bronze layer."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

logger = logging.getLogger(__name__)


class JsonlWriter:
    """Writes one JSON object per line, rotating files by hour.

    Hourly rotation mirrors how the S3 Bronze layer will be partitioned, so the
    directory layout you build here is the one Spark reads later.
    """

    def __init__(self, base_dir: Path, prefix: str = "events") -> None:
        self.base_dir = Path(base_dir)
        self.prefix = prefix
        self._handle: TextIO | None = None
        self._current_hour: str | None = None
        self._written = 0

    def _hour_key(self) -> str:
        return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H")

    def _path_for(self, hour: str) -> Path:
        date_part, hour_part = hour.split("T")
        directory = self.base_dir / f"date={date_part}" / f"hour={hour_part}"
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f"{self.prefix}.jsonl"

    def _roll_if_needed(self) -> None:
        hour = self._hour_key()
        if hour != self._current_hour:
            self.close()
            path = self._path_for(hour)
            self._handle = path.open("a", encoding="utf-8")
            self._current_hour = hour
            logger.info("opened output file", extra={"path": str(path)})

    def write(self, line: str) -> None:
        self._roll_if_needed()
        assert self._handle is not None
        self._handle.write(line + "\n")
        self._written += 1

        # flush periodically -- a crash should not lose the whole buffer
        if self._written % 100 == 0:
            self._handle.flush()

    def close(self) -> None:
        if self._handle is not None:
            self._handle.flush()
            self._handle.close()
            self._handle = None