

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator

import orjson
import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)


class BinanceWebSocketClient:
    def __init__(
        self,
        *,
        base_url: str,
        streams: list[str],
        ping_interval: float,
        ping_timeout: float,
        backoff_base: float,
        backoff_max: float,
    ) -> None:
        self.base_url = base_url
        self.streams = streams
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max

        self._reconnects = 0
        self._messages = 0

    @property
    def url(self) -> str:
        return f"{self.base_url}?streams={'/'.join(self.streams)}"

    def _backoff_delay(self, attempt: int) -> float:
        """Exponential, capped. Stops us hammering a struggling server."""
        return min(self.backoff_base * (2 ** (attempt - 1)), self.backoff_max)

    async def stream(self) -> AsyncIterator[dict]:
        """Yield raw messages forever, reconnecting as needed."""
        attempt = 0

        while True:
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                    max_queue=None,
                ) as ws:
                    logger.info(
                        "websocket connected",
                        extra={"streams": len(self.streams), "reconnects": self._reconnects},
                    )
                    attempt = 0  # reset only after a successful connect

                    async for raw in ws:
                        self._messages += 1
                        yield orjson.loads(raw)

            except ConnectionClosed as exc:
                attempt += 1
                self._reconnects += 1
                delay = self._backoff_delay(attempt)
                logger.warning(
                    "connection closed, reconnecting",
                    extra={
                        "attempt": attempt,
                        "delay_seconds": delay,
                        "code": exc.code,
                        "messages_so_far": self._messages,
                    },
                )
                await asyncio.sleep(delay)

            except asyncio.CancelledError:
                logger.info("shutdown requested", extra={"messages_total": self._messages})
                raise

            except Exception as exc:
                attempt += 1
                self._reconnects += 1
                delay = self._backoff_delay(attempt)
                logger.error(
                    "unexpected websocket error",
                    extra={"attempt": attempt, "delay_seconds": delay, "error": str(exc)},
                    exc_info=True,
                )
                await asyncio.sleep(delay)