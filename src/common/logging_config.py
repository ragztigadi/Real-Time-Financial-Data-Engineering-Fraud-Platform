"""JSON logging with a correlation ID that follows one run through the system."""

from __future__ import annotations

import json
import logging
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="-")

_RESERVED = set(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__.keys()
) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def __init__(self, service_name: str, environment: str) -> None:
        super().__init__()
        self.service_name = service_name
        self.environment = environment

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self.service_name,
            "environment": self.environment,
            "logger": record.name,
            "correlation_id": correlation_id_var.get(),
            "message": record.getMessage(),
        }

        # anything passed via extra={...} becomes a searchable field
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def configure_logging(level: str, service_name: str, environment: str) -> None:
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service_name, environment))
    root.addHandler(handler)

    logging.getLogger("websockets").setLevel(logging.WARNING)


def new_correlation_id() -> str:
    cid = str(uuid.uuid4())
    correlation_id_var.set(cid)
    return cid