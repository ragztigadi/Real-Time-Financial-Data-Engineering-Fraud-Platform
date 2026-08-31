"""Kafka producer for validated event envelopes."""

from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class EventProducer:
    """Async-batching Kafka producer.

    confluent-kafka buffers internally and sends in background threads, so
    produce() does not block on the network. The cost is that delivery is only
    confirmed later, via the callback.
    """

    def __init__(self, *, bootstrap_servers: str, client_id: str) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                # durability: wait for the broker to acknowledge
                "acks": "all",
                "enable.idempotence": True,
                "retries": 5,
                # throughput: batch small messages together
                "linger.ms": 10,
                "batch.size": 65536,
                "compression.type": "lz4",
                # bound memory if the broker slows down
                "queue.buffering.max.messages": 200_000,
            }
        )
        self._delivered = 0
        self._failed = 0

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self._failed += 1
            logger.error(
                "delivery failed",
                extra={"error": str(err), "topic": msg.topic() if msg else None},
            )
        else:
            self._delivered += 1

    def produce(self, *, topic: str, key: str, value: str) -> None:
        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                on_delivery=self._on_delivery,
            )
        except BufferError:
            # local queue full -- block until the background thread drains it
            logger.warning("producer queue full, flushing")
            self._producer.flush(5)
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                on_delivery=self._on_delivery,
            )

        # serve delivery callbacks without blocking
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Block until the queue drains. Returns messages still pending."""
        remaining = self._producer.flush(timeout)
        logger.info(
            "producer flushed",
            extra={
                "delivered": self._delivered,
                "failed": self._failed,
                "still_pending": remaining,
            },
        )
        return remaining