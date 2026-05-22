# schemas/market.py

from pydantic import BaseModel
from datetime import datetime
from decimal import Decimal


class MarketDataBase(BaseModel):
    price: Decimal
    volume: int
    daily_change: Decimal
    daily_change_pct: float
    volatility: float | None = None
    moving_average: float | None = None


class MarketDataCreate(MarketDataBase):
    pass


class MarketDataResponse(MarketDataBase):
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True