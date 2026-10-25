# models/analysis.py

from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import (
    JSON,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    UniqueConstraint,
)

from database.base import Base


class AnalysisResult(Base):
    """
    One combined signal per (ticker, trading_date), written by SignalService.

    Existing databases need database/migrations/001_analysis_results_signal_columns.sql
    (Base.metadata.create_all never alters an existing table).
    """

    __tablename__ = "analysis_results"

    __table_args__ = (
        UniqueConstraint("ticker", "trading_date", name="uq_analysis_results_ticker_date"),
    )

    id = Column(Integer, primary_key=True, index=True)

    ticker = Column(String(20), nullable=False)

    trading_date = Column(Date, nullable=False, index=True)

    signal = Column(String(255))

    strength = Column(String(20))

    confidence_score = Column(Float)

    explanation = Column(String(1000))

    components = Column(JSON)

    # Evaluated per row (lambda); the old default ran once at import time.
    timestamp = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(ZoneInfo("Africa/Nairobi")),
    )