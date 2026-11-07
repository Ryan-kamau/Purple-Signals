
from datetime import datetime
from zoneinfo import ZoneInfo
from database.base import Base

from sqlalchemy import Column, Integer, String, DateTime, Numeric, Float, Double, UniqueConstraint


class Fundamentals(Base):
    __tablename__ = "fundamentals"

    __table_args__ = (
        UniqueConstraint(
            "ticker",
            "report_date",
            name="uq_fundamentals_ticker_report_date",
        ),
    )

    id = Column(Integer, primary_key=True, index=True)

    ticker = Column(String(10), nullable=False, index=True)
    company = Column(String(255), nullable=False, index=True)

    report_date = Column(DateTime(timezone=True), nullable=False, index=True)

    # Financial values
    eps = Column(Numeric(12, 4))
    pe_ratio = Column(Numeric(12, 4))
    dividend_yield = Column(Numeric(12, 6))
    dividend_per_share = Column(Float)
    revenue = Column(Double)
    net_profit = Column(Double)
    debt_ratio = Column(Numeric(12, 6))
    net_profit_margin = Column(Numeric(12, 6))

    # Generate a fresh timestamp for each new object
    timestamp = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(ZoneInfo("Africa/Nairobi"))
    )
