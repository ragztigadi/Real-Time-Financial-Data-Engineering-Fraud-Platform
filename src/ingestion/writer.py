from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO
import logging

logger = logging.getLogger(__name__)

class JsonWritter:
    def __init__(self, base_dir: Path, prefix: str = "Events")-> None:
        self.base_dir = Path(base_dir)
        self.prefix = prefix
        self._handle: TextIO | None = None
        self._current_hour: str | None = None
        self._written = 0

    def _hour_key(self) -> str:
        return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H")
    
    