from __future__ import annotations

import logging
import asyncio
from datetime import datetime, timezone

from config.settings import get_settings
from src.common.logging_config import configure_logging, new_correlation_id
from src.ingestion.api.binance_ws import BinanceWebSocketClient
from src.ingestion.models.trade import AggTrade
from src.ingestion.models.book_ticker import BookTicker
from src.ingestion.models.envelope import EventEnvelope, make_event_id
from src.ingestion.writer import JsonlWriter
from src.ingestion.dedup import  DedupCache

logger = logging.getLogger(__name__)


async def main() -> None:
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
        backoff_max=settings.ws_backoff_max_seconds,
    )

    logger.info("starting ingestion", extra={"url": client.url})

    writer = JsonlWriter(settings.raw_output_dir, prefix="binance")
    dedup = DedupCache(max_size=100_000)

    processed = 0
    failed = 0
    counts = {"aggTrade": 0, "bookTicker": 0}

    try:
        async for message in client.stream():
            stream_name = message.get("stream", "")
            payload = message.get("data", message)

            # the stream name is the routing key: "btcusdt@bookTicker"
            stream_type = stream_name.split("@")[-1] if "@" in stream_name else None

            try:
                if stream_type == "aggTrade":
                    event = AggTrade.model_validate(payload)
                elif stream_type == "bookTicker":
                    event = BookTicker.model_validate(payload)
                else:
                    raise ValueError(f"unknown stream type: {stream_name!r}")
            except Exception as exc:
                failed += 1
                logger.warning(
                    "validation failed",
                    extra={"error": str(exc), "stream": stream_name, "raw": payload},
                )
                continue

            processed += 1
            counts[stream_type] += 1

            envelope = EventEnvelope(
                event_id=make_event_id(
                    event_type=stream_type,
                    dedup_key=event.dedup_key,
                    schema_version=settings.schema_version,
                ),
                event_type=stream_type,
                schema_version=settings.schema_version,
                source="binance.us",
                partition_key=event.symbol,
                occurred_at=(
                    event.trade_time
                    if stream_type == "aggTrade"
                    else datetime.now(tz=timezone.utc)
                ),
                payload=event.model_dump(mode="json", by_alias=True),
            )
            if dedup.is_duplicate(envelope.event_id):
                continue
            writer.write(envelope.model_dump_json())

            if processed % 100 == 0:
                logger.info(
                    "heartbeat",
                    extra={
                        "processed": processed,
                        "failed": failed,
                        "agg_trades": counts["aggTrade"],
                        "book_tickers": counts["bookTicker"],
                        "duplicates": dedup.duplicates_seen,
                        "dedup_size": dedup.size,
                        "last_symbol": event.symbol,
                        "last_dedup_key": event.dedup_key,
                    },
                )
    finally:
        writer.close()
        logger.info("writer closed", extra={"processed": processed, "failed": failed})


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("stopped by user")