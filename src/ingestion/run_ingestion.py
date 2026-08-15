from __future__ import annotations

import logging
import asyncio

from config.settings import get_settings
from src.common.logging_config import configure_logging, new_correlation_id
from src.ingestion.api.binance_ws import BinanceWebSocketClient
from src.ingestion.models.trade import AggTrade

logger = logging.getLogger(__name__)

async def main()-> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.service_name, settings.environment)
    new_correlation_id()
    settings.ensure_dirs()

    client = BinanceWebSocketClient(
        base_url=settings.binance_ws_url,
        streams=settings.stream_names,
        ping_interval=settings.ws_ping_interval_seconds,
        ping_timeout=settings.ws_ping_timeout_seconds,
        backoff_base=settings.ws_backoff_base_seconds,
        backoff_max=settings.ws_backoff_max_seconds
    )
