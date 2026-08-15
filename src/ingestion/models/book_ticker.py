"""Binance bookTicker contract, schema v1.0.0.

A bookTicker is the current best bid and ask -- a quote, not a completed trade.
It fires whenever the top of the order book changes, which is far more often
than trades occur.
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BookTicker(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)

    update_id: int = Field(alias="u")
    symbol: str = Field(alias="s")
    bid_price: Decimal = Field(alias="b")
    bid_qty: Decimal = Field(alias="B")
    ask_price: Decimal = Field(alias="a")
    ask_qty: Decimal = Field(alias="A")

    @field_validator("bid_price", "bid_qty", "ask_price", "ask_qty")
    @classmethod
    def _non_negative(cls, v: Decimal) -> Decimal:
        if v < 0:
            raise ValueError("prices and quantities cannot be negative")
        return v

    @field_validator("symbol")
    @classmethod
    def _upper_symbol(cls, v: str) -> str:
        return v.upper()

    @property
    def spread(self) -> Decimal:
        """Ask minus bid. A negative spread means corrupt data."""
        return self.ask_price - self.bid_price

    @property
    def mid_price(self) -> Decimal:
        return (self.ask_price + self.bid_price) / 2

    @property
    def dedup_key(self) -> str:
        """update_id is monotonic per symbol -- same role agg_trade_id plays for trades."""
        return f"{self.symbol}:{self.update_id}"