"""Binance aggTrade contract, schema v1.0.0."""

from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, field_validator, ConfigDict, Field

class AggTrade(BaseModel):
    model_config = ConfigDict(extra='ignore', frozen=True, populate_by_name=True)
    event_type : str = Field(alias='e')
    event_time_ms : int = Field(alias='E')
    symbol: str = Field(alias = 's')
    agg_trade_id: int = Field(alias="a")
    price: Decimal = Field(alias="p")
    quantity: Decimal = Field(alias="q")
    first_trade_id: int = Field(alias="f")
    last_trade_id: int = Field(alias="l")
    trade_time_ms: int = Field(alias="T")
    is_buyer_maker: bool = Field(alias="m")
    
    # Price and Quantity Fields are must be Positive
    @field_validator("price", "quantity")
    @classmethod
    def _must_be_positive(cls, v: Decimal) -> Decimal:
        if v<=0:
            raise ValueError("price and quantity must be greater than zero")
        return v
    
    # reject anything before 2017 or absurdly far in the future
    @field_validator("event_time_ms", "trade_time_ms")
    @classmethod
    def _sane_timestamp(cls, v:int) -> int:
        if v < 1_500_000_000_000 or v > 4_000_000_000_000:
            raise ValueError(f"implausible millisecond timestamp: {v}")
        return v
    
    # Symbol validator
    @field_validator('symbol')
    @classmethod
    def _upper_symbol(cls, v:str) -> str:
        return v.upper()
    
    """Event time -- the column Spark will watermark on."""
    @property
    def trade_time(self) -> datetime:
        return datetime.fromtimestamp(self.trade_time_ms / 1000, tz=timezone.utc)
    
    # DE-DUP duplicates:
    @property
    def dedup_key(self)->str:
        return f"{self.symbol}:{self.agg_trade_id}"