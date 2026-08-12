"""The envelope every downstream stage sees. Payload is Binance's; the wrapper is ours."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

def make_event_id(*, event_type : str, dedup_key : str, schema_version:str) -> str:
    """Same trade in -> same id out. This is what makes replay safe."""

    raw = f"{event_type}|{dedup_key}|{schema_version}".encode()
    return hashlib.sha256(raw).hexdigest()

class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_type : str
    event_id : str
    schema_version : str

    source : str
    partition_key: str = Field(max_length=256)

    occurred_at : datetime
    ingested_at : datetime = Field( 
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    payload: dict[str, Any]

    @property
    def ingest_lag_ms(self) -> float:
        """Exchange-to-us latency. Your first real pipeline metric."""

        return (self.occurred_at - self.ingested_at).total_seconds()*1000