import zoneinfo

from sqlalchemy import Column, Integer, Float, DateTime, Nullable, String, DECIMAL
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from database.base import Base


class MarketData(Base):
    __tablename__ = "market_data"

    id = Column(Integer, primary_key=True, index=True)

    ticker = Column(String(10), nullable=False, index=True)

    company = Column(String(255), nullable=False, index=True)

    price = Column(DECIMAL(10, 3), nullable=False)

    volume = Column(Integer, nullable=False)

    daily_change = Column(DECIMAL(10, 3), nullable=False)

    daily_change_pct = Column(Float, nullable=False)

    volatility = Column(Float)


    timestamp = Column(DateTime(timezone=True), default=lambda: datetime.now(
        ZoneInfo("Africa/Nairobi")))