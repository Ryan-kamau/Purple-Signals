# models/fundamentals.py

from sqlalchemy import Column, Integer, Float, DateTime
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from database.base import Base


class Fundamentals(Base):
    __tablename__ = "fundamentals"

    id = Column(Integer, primary_key=True, index=True)

    eps = Column(Float)

    pe_ratio = Column(Float)

    dividend_yield = Column(Float)

    revenue = Column(Float)

    debt_ratio = Column(Float)

    net_profit_margin = Column(Float)

    timestamp = Column(DateTime(timezone=True), 
                       default=datetime.now(ZoneInfo("Africa/Nairobi")))