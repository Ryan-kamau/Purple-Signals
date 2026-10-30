# models/alert.py

from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import (
    JSON,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    UniqueConstraint,
)

from database.base import Base

STATUS_ACTIVE = "active"
STATUS_RESOLVED = "resolved"
ALERT_STATUSES = (STATUS_ACTIVE, STATUS_RESOLVED)


def _now() -> datetime:
    """Evaluated per row (never at import time)."""
    return datetime.now(ZoneInfo("Africa/Nairobi"))


class Alert(Base):
    """
    One lifecycle record per combined-signal classification run for a ticker.

    A ticker has at most ONE active alert. When its classification changes the
    old row is resolved (kept for history) and a new active row is created.

    One-active-per-ticker invariant (MySQL has no partial unique indexes):
    `active_ticker` equals `ticker` while status='active' and is NULL once
    resolved. A UNIQUE index on it forbids two active rows for a ticker, while
    MySQL/InnoDB permits unlimited NULLs, so resolved history is unbounded.
    The CHECK constraint keeps status and active_ticker from drifting apart.
    Requires MySQL >= 8.0.16 for CHECK enforcement (earlier versions parse and
    ignore it; the unique index still protects the invariant).

    `timestamp` = the moment this row was inserted (== first_detected_at).
    """

    __tablename__ = "alerts"

    __table_args__ = (
        UniqueConstraint("active_ticker", name="uq_alerts_active_ticker"),
        CheckConstraint("status IN ('active', 'resolved')", name="ck_alerts_status"),
        CheckConstraint(
            "(status = 'active' AND active_ticker IS NOT NULL AND active_ticker = ticker) "
            "OR (status = 'resolved' AND active_ticker IS NULL)",
            name="ck_alerts_active_ticker_consistent",
        ),
        Index("ix_alerts_ticker_status", "ticker", "status"),
        Index("ix_alerts_status_type", "status", "alert_type"),
        Index("ix_alerts_first_detected_at", "first_detected_at"),
    )

    id = Column(Integer, primary_key=True, index=True)

    alert_type = Column(String(255), nullable=False)

    message = Column(String(1000), nullable=False)

    severity = Column(String(100))

    timestamp = Column(DateTime(timezone=True), default=_now)

    # --- lifecycle / signal fields ---------------------------------------
    ticker = Column(String(20), nullable=False)

    trading_date = Column(Date, nullable=False)

    signal_strength = Column(String(20), nullable=True)

    confidence_score = Column(Float, nullable=False)

    status = Column(String(20), nullable=False, default=STATUS_ACTIVE)

    active_ticker = Column(String(20), nullable=True)

    first_detected_at = Column(DateTime(timezone=True), nullable=False, default=_now)

    last_detected_at = Column(DateTime(timezone=True), nullable=False, default=_now)

    resolved_at = Column(DateTime(timezone=True), nullable=True)

    evidence = Column(JSON, nullable=True)