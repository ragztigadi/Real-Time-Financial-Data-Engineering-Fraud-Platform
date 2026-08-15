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
    
    logger.info("starting ingestion", extra={"url":client.url})

    processed = 0
    failed = 0

    async for message in client.stream():
        payload = message.get("data", message)

        try:
            trade = AggTrade.model_validate(payload)
        except Exception as exc:
            failed += 1
            logger.warning(
                "validation failed",
                extra={"error": str(exc), "raw":payload, "failed_total":failed},
            )
            continue

        processed += 1

        if processed % 1 == 0:
            logger.info(
                "heartbeat",
                extra={
                    "processed": processed,
                    "failed": failed,
                    "last_symbol": trade.symbol,
                    "last_dedup_key": trade.dedup_key,
                    "exchange_lag_ms": trade.event_time_ms - trade.trade_time_ms,
                },
            )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("stopped by user")