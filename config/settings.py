"""Single entry point for all configuration."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # runtime
    environment: Literal["local", "dev", "staging", "prod"] = "local"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    service_name: str = "binance-ingestion"

    # binance source
    binance_ws_url: str
    binance_symbols: str
    binance_stream_types: str = "aggTrade,bookTicker"

    # reconnect policy
    ws_ping_interval_seconds: float = Field(default=20.0, gt=0)
    ws_ping_timeout_seconds: float = Field(default=20.0, gt=0)
    ws_backoff_base_seconds: float = Field(default=1.0, gt=0)
    ws_backoff_max_seconds: float = Field(default=60.0, gt=0)

    # contracts
    schema_version: str = "1.0.0"

    # local paths
    raw_output_dir: Path = Path("data/raw")
    rejected_output_dir: Path = Path("data/rejected")
    checkpoint_dir: Path = Path("data/checkpoints")

    # kafka
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_topic_trades: str = "binance.trades.v1"
    kafka_topic_quotes: str = "binance.quotes.v1"
    kafka_topic_dlq: str = "binance.dlq.v1"

    @field_validator("binance_ws_url")
    @classmethod
    def _check_ws_url(cls, v: str) -> str:
        if not v.startswith(("ws://", "wss://")):
            raise ValueError("binance_ws_url must start with ws:// or wss://")
        return v.rstrip("/")

    @field_validator("schema_version")
    @classmethod
    def _check_semver(cls, v: str) -> str:
        parts = v.split(".")
        if len(parts) != 3 or not all(p.isdigit() for p in parts):
            raise ValueError("schema_version must be MAJOR.MINOR.PATCH")
        return v

    @property
    def symbol_list(self) -> list[str]:
        """Comma-separated env string -> clean lowercase list."""
        return [s.strip().lower() for s in self.binance_symbols.split(",") if s.strip()]

    @property
    def stream_types(self) -> list[str]:
        return [t.strip() for t in self.binance_stream_types.split(",") if t.strip()]

    @property
    def stream_names(self) -> list[str]:
        """Cartesian product: every symbol x every stream type."""
        return [
            f"{sym}@{stream_type}"
            for sym in self.symbol_list
            for stream_type in self.stream_types
        ]

    def ensure_dirs(self) -> None:
        for d in (self.raw_output_dir, self.rejected_output_dir, self.checkpoint_dir):
            d.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]