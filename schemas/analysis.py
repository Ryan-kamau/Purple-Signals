# schemas/analysis.py

"""
Response schemas for GET /analysis (Master Intelligence Service).

Conventions:
  - Missing RECORD            -> section is null, status.is_available = False
  - Missing INDICATOR VALUE   -> the field is null inside a present section
  - is_stale is null (not false) whenever there is nothing to be stale
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class SourceStatus(BaseModel):
    as_of: date | None = None
    is_available: bool
    is_stale: bool | None = None


# ---------------------------------------------------------------------------
# Metadata / summaries
# ---------------------------------------------------------------------------

class PaginationMeta(BaseModel):
    limit: int
    offset: int
    total: int
    returned: int


class ResponseMetadata(BaseModel):
    generated_at: datetime
    timezone: str
    reference_date: date
    scope: Literal["nse_wide", "ticker"]
    requested_ticker: str | None = None
    pagination: PaginationMeta | None = None
    freshness_rules: dict[str, int]


class MarketSummary(BaseModel):
    """NSE-wide, independent of pagination and of the requested ticker."""

    total_securities: int
    securities_with_signals: int
    securities_without_signals: int
    bullish_count: int
    bearish_count: int
    divergence_count: int
    unusual_activity_count: int
    neutral_count: int
    fresh_signal_count: int
    stale_signal_count: int
    active_alert_count: int


class TickerSummary(BaseModel):
    ticker: str
    company_name: str | None = None
    signal: str | None = None
    signal_strength: str | None = None
    confidence_score: float | None = None
    signal_trading_date: date | None = None
    signal_is_stale: bool | None = None
    latest_features_date: date | None = None
    has_fundamentals: bool
    active_alert_count: int


# ---------------------------------------------------------------------------
# Per-security sections
# ---------------------------------------------------------------------------

class MarketFeatureSnapshot(BaseModel):
    trading_date: date
    close_price: float | None = None
    daily_return: float | None = None
    volume_ratio_10d: float | None = None
    volume_ratio_20d: float | None = None
    volatility_5d: float | None = None
    volatility_20d: float | None = None


class MarketIntelligence(BaseModel):
    latest: MarketFeatureSnapshot | None = None
    signal_date: MarketFeatureSnapshot | None = None


class SentimentSnapshot(BaseModel):
    trading_date: date
    average_sentiment: float | None = None
    weighted_sentiment: float | None = None
    sentiment_volatility: float | None = None
    relevant_headline_count: int | None = None
    average_impact_score: float | None = None


class SentimentIntelligence(BaseModel):
    latest: SentimentSnapshot | None = None
    signal_date: SentimentSnapshot | None = None


class SignalIntelligence(BaseModel):
    id: int
    signal: str | None = None
    strength: str | None = None
    confidence_score: float | None = None
    explanation: str | None = None
    components: dict[str, Any] | None = None
    trading_date: date
    timestamp: datetime | None = None


class AlertItem(BaseModel):
    """
    Core fields are typed; every other persisted Alert column (strength,
    confidence, first/last detected, evidence, ...) is passed through in
    `details` exactly as stored, so schema drift in Alert never breaks this API.
    """

    id: int | None = None
    alert_type: str | None = None
    severity: str | None = None
    status: str | None = None
    trading_date: date | None = None
    details: dict[str, Any] = {}


class FundamentalIntelligence(BaseModel):
    report_date: date
    eps: float | None = None
    pe_ratio: float | None = None
    dividend_yield: float | None = None
    dividend_per_share: float | None = None
    revenue: float | None = None
    net_profit: float | None = None
    debt_ratio: float | None = None
    net_profit_margin: float | None = None
    timestamp: datetime | None = None


class DataStatus(BaseModel):
    """
    Availability/freshness per source. Sentiment is read from the same feature
    rows as market data, so latest_features / signal_date_features cover both.
    """

    latest_features: SourceStatus
    signal_date_features: SourceStatus
    signal: SourceStatus
    fundamentals: SourceStatus
    active_alerts: SourceStatus


class SecurityIntelligence(BaseModel):
    ticker: str
    company_name: str | None = None
    market: MarketIntelligence
    sentiment: SentimentIntelligence
    signal: SignalIntelligence | None = None
    active_alerts: list[AlertItem] = []
    fundamentals: FundamentalIntelligence | None = None
    data_status: DataStatus


# ---------------------------------------------------------------------------
# Macro
# ---------------------------------------------------------------------------

class MacroIndicator(BaseModel):
    value: float | None = None
    period: str | None = None          # e.g. "July 2026" (month/year of the source row)
    as_of: date | None = None          # first day of that month
    report_date: date | None = None    # stored report_date of the source row
    trend: float | None = None         # stored trend of the SAME source row, never recomputed
    is_available: bool
    is_stale: bool | None = None


class MacroIntelligence(BaseModel):
    """Each indicator carries its own source period; they may differ."""

    inflation: MacroIndicator
    fuel_price: MacroIndicator
    cbk_rate: MacroIndicator
    usd_kes_rate: MacroIndicator
    euro_kes_rate: MacroIndicator
    pounds_kes_rate: MacroIndicator


# ---------------------------------------------------------------------------
# Top level
# ---------------------------------------------------------------------------

class AnalysisResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    metadata: ResponseMetadata
    market_summary: MarketSummary
    ticker_summary: TickerSummary | None = None
    securities: list[SecurityIntelligence]
    macroeconomics: MacroIntelligence