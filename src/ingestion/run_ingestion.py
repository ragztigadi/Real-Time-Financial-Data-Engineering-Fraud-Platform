from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from config.settings import get_settings
from src.common.logging_config import configure_logging, new_correlation_id
from src.ingestion.api.binance_ws import BinanceWebSocketClient
from src.ingestion.models.book_ticker import BookTicker
from src.ingestion.models.envelope import EventEnvelope, make_event_id
from src.ingestion.models.trade import AggTrade
from src.ingestion.producer import EventProducer

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

    producer = EventProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=settings.service_name,
    )

    topic_for = {
        "aggTrade": settings.kafka_topic_trades,
        "bookTicker": settings.kafka_topic_quotes,
    }

    logger.info("starting ingestion", extra={"url": client.url})

    processed = 0
    failed = 0
    counts = {"aggTrade": 0, "bookTicker": 0}

    try:
        async for message in client.stream():
            stream_name = message.get("stream", "")
            payload = message.get("data", message)
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
                # rejected records go to the DLQ, not to a log line that scrolls away
                producer.produce(
                    topic=settings.kafka_topic_dlq,
                    key=stream_name or "unknown",
                    value=EventEnvelope(
                        event_id=make_event_id(
                            event_type="invalid",
                            dedup_key=str(payload),
                            schema_version=settings.schema_version,
                        ),
                        event_type="invalid",
                        schema_version=settings.schema_version,
                        source="binance.us",
                        partition_key=stream_name or "unknown",
                        occurred_at=datetime.now(tz=timezone.utc),
                        payload={"error": str(exc), "raw": payload},
                    ).model_dump_json(),
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

            producer.produce(
                topic=topic_for[stream_type],
                key=envelope.partition_key,
                value=envelope.model_dump_json(),
            )

            if processed % 1000 == 0:
                logger.info(
                    "heartbeat",
                    extra={
                        "processed": processed,
                        "failed": failed,
                        "agg_trades": counts["aggTrade"],
                        "book_tickers": counts["bookTicker"],
                        "last_symbol": event.symbol,
                    },
                )
    finally:
        producer.flush()
        logger.info("ingestion stopped", extra={"processed": processed, "failed": failed})


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("stopped by user")