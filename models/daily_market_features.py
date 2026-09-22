from datetime import date, datetime
from typing import Optional

from sqlalchemy import Date, DateTime, Float, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from database.base  import Base


class DailyMarketFeatures(Base):
    __tablename__ = "daily_features"

    __table_args__ = (
        UniqueConstraint(
            "ticker",
            "trading_date",
            name="uq_daily_market_features_ticker_date"
        ),
    )

    # ─────────────────────────────────────────────
    # IDENTIFICATION
    # ─────────────────────────────────────────────

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    ticker: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        index=True
    )

    trading_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True
    )

    # ─────────────────────────────────────────────
    # PRICE / MARKET
    # ─────────────────────────────────────────────

    close_price: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    daily_return: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    volume: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    # ─────────────────────────────────────────────
    # PRICE TREND
    # ─────────────────────────────────────────────

    price_ma_5: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    price_ma_20: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    return_5d: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    return_20d: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    # ─────────────────────────────────────────────
    # VOLUME
    # ─────────────────────────────────────────────

    volume_ma_10: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    volume_ma_20: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    volume_ratio_10d: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    volume_ratio_20d: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    # ─────────────────────────────────────────────
    # VOLATILITY
    # ─────────────────────────────────────────────

    volatility_5d: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    volatility_20d: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    # ─────────────────────────────────────────────
    # NEWS SENTIMENT
    # ─────────────────────────────────────────────

    average_sentiment: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    weighted_sentiment: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    sentiment_volatility: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    # ─────────────────────────────────────────────
    # NEWS RELEVANCE / IMPACT
    # ─────────────────────────────────────────────
    average_impact_score: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    relevant_headline_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0
    )

    # ─────────────────────────────────────────────
    # TOPIC SENTIMENT
    # ─────────────────────────────────────────────

    energy_sentiment: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    electricity_sentiment: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    oil_sentiment: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    macro_sentiment: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    government_sentiment: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    # ─────────────────────────────────────────────
    # MACROECONOMIC
    # ─────────────────────────────────────────────

    inflation: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    interest_rate: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    usd_kes: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    oil_price: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    # ─────────────────────────────────────────────
    # TARGETS
    # ─────────────────────────────────────────────

    next_day_return: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    next_day_direction: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True
    )

    # ─────────────────────────────────────────────
    # TIMESTAMPS
    # ─────────────────────────────────────────────

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )