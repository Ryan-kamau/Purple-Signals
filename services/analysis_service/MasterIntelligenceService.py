"""
services/analysis_service/MasterIntelligenceService.py

Read-only aggregation layer behind GET /analysis.

Pipeline position:

    daily_features / analysis_results / alerts / fundamentals / macro_data
        │   (already populated by the existing pipeline)
        ▼
    MasterIntelligenceService          <- this file
        ▼
    AnalysisResponse  ->  api/analysis.py

STRICT BOUNDARY - this service only SELECTs, picks the latest applicable
persisted row, counts/groups, checks freshness and assembles. It never
recomputes a price/return/ratio/sentiment/signal, never evaluates or touches
alerts, never writes, flushes or commits.

Query plan (constant number of statements, independent of page size):

    NSE-wide  : universe count, grouped signal counts, active-alert count
    page      : 1 ticker-page query
    per page  : latest signals, latest features, signal-date features,
                latest fundamentals, latest company names, active alerts
    market    : 1 macro query (monthly table - tiny)

Read consistency: every statement runs on the one Session/transaction the
request owns and nothing commits in between, so under MySQL's default
REPEATABLE READ they all see the same snapshot (scheduler writes landing
mid-request are invisible). This is documented behaviour, not enforced here.

Assumptions:
  - Tickers are stored upper-case (MarketService upper-cases them; features
    inherit them). Only the *incoming* ticker is normalised.
  - Alert has `ticker` and `status` columns (see config.analysis_config).
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import and_, func, inspect as sa_inspect, select, tuple_, union
from sqlalchemy.orm import Session

from config import analysis_config as cfg
from models.alert_data import Alert
from models.analytics_data import AnalysisResult
from models.daily_market_features import DailyMarketFeatures as Features
from models.fundamental_data import Fundamentals
from models.macro_data import MacroData
from models.market_data import MarketData
from schemas.analysis import (
    AlertItem,
    AnalysisResponse,
    DataStatus,
    FundamentalIntelligence,
    MacroIndicator,
    MacroIntelligence,
    MarketFeatureSnapshot,
    MarketIntelligence,
    MarketSummary,
    PaginationMeta,
    ResponseMetadata,
    SecurityIntelligence,
    SentimentIntelligence,
    SentimentSnapshot,
    SignalIntelligence,
    SourceStatus,
    TickerSummary,
)

logger = logging.getLogger(__name__)

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

MONTH_ORDER: list[str] = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

# response field -> (MacroData value column, MacroData trend column | None)
MACRO_INDICATORS: dict[str, tuple[str, Optional[str]]] = {
    "inflation": ("inflation", "inflation_trend"),
    "fuel_price": ("fuel_price", "fuel_trend"),
    "cbk_rate": ("cbk_rate", None),
    "usd_kes_rate": ("usd_kes_rate", None),
    "euro_kes_rate": ("euro_kes_rate", None),
    "pounds_kes_rate": ("pounds_kes_rate", None),
}

_ALERT_CORE_FIELDS = ("id", "alert_type", "severity", "status", "trading_date")


class TickerNotFoundError(LookupError):
    """Requested ticker is not in the NSE security universe."""


class MasterIntelligenceService:
    """
    Usage:
        service = MasterIntelligenceService(db)
        service.get_analysis()                     # NSE-wide, paginated
        service.get_analysis(ticker="scom")        # one security, any valid ticker
    """

    def __init__(self, db: Session, now: Optional[datetime] = None) -> None:
        """
        Args:
            db:  Active SQLAlchemy session (injected; never committed here).
            now: Fixed "current time" - for tests. Defaults to Nairobi now.
        """
        self.db = db
        self._now = now

    # ==================================================================
    # PUBLIC API
    # ==================================================================

    def get_analysis(
        self,
        ticker: Optional[str] = None,
        limit: int = cfg.DEFAULT_LIMIT,
        offset: int = 0,
    ) -> AnalysisResponse:
        """
        Raises:
            TickerNotFoundError: ticker supplied but not in the universe.
            ValueError:          invalid pagination (NSE-wide mode only).
            sqlalchemy errors:   propagate - never turned into an empty 200.
        """
        now = self._now or datetime.now(NAIROBI_TZ)
        reference_date = now.date()

        universe = self._universe_subquery()
        market_summary = self._build_market_summary(universe, reference_date)

        requested: Optional[str] = None
        pagination: Optional[PaginationMeta] = None

        if ticker is not None:
            requested = ticker.strip().upper()
            if not self._ticker_in_universe(universe, requested):
                raise TickerNotFoundError(
                    f"Ticker '{requested}' is not in the NSE security universe."
                )
            tickers = [requested]
        else:
            if limit < 1 or limit > cfg.MAX_LIMIT:
                raise ValueError(f"limit must be between 1 and {cfg.MAX_LIMIT}, got {limit}.")
            if offset < 0:
                raise ValueError(f"offset must be >= 0, got {offset}.")
            tickers = self._page_tickers(universe, limit, offset)

        securities = self._build_securities(tickers, reference_date)

        if ticker is None:
            pagination = PaginationMeta(
                limit=limit,
                offset=offset,
                total=market_summary.total_securities,
                returned=len(securities),
            )

        return AnalysisResponse(
            metadata=ResponseMetadata(
                generated_at=now,
                timezone="Africa/Nairobi",
                reference_date=reference_date,
                scope="ticker" if ticker is not None else "nse_wide",
                requested_ticker=requested,
                pagination=pagination,
                freshness_rules=cfg.freshness_rules(),
            ),
            market_summary=market_summary,
            ticker_summary=self._build_ticker_summary(securities[0]) if ticker is not None else None,
            securities=securities,
            macroeconomics=self._build_macro(reference_date),
        )

    # ==================================================================
    # SECURITY UNIVERSE
    # ==================================================================

    @staticmethod
    def _universe_subquery():
        """Distinct, non-blank tickers present in MarketData UNION DailyMarketFeatures."""
        from_market = select(MarketData.ticker.label("ticker")).where(
            MarketData.ticker.isnot(None), MarketData.ticker != ""
        )
        from_features = select(Features.ticker.label("ticker")).where(
            Features.ticker.isnot(None), Features.ticker != ""
        )
        return union(from_market, from_features).subquery("universe")

    def _ticker_in_universe(self, universe, ticker: str) -> bool:
        if not ticker:
            return False
        found = self.db.execute(
            select(universe.c.ticker).where(universe.c.ticker == ticker).limit(1)
        ).first()
        return found is not None

    def _page_tickers(self, universe, limit: int, offset: int) -> list[str]:
        rows = self.db.execute(
            select(universe.c.ticker).order_by(universe.c.ticker).limit(limit).offset(offset)
        ).scalars().all()
        return list(rows)

    # ==================================================================
    # NSE-WIDE SUMMARY (whole universe, pagination-independent)
    # ==================================================================

    def _build_market_summary(self, universe, reference_date: date) -> MarketSummary:
        total = self.db.execute(select(func.count()).select_from(universe)).scalar_one()

        latest = (
            select(AnalysisResult.ticker.label("t"), func.max(AnalysisResult.trading_date).label("d"))
            .group_by(AnalysisResult.ticker)
            .subquery("latest_signal")
        )
        # Group by (classification, trading_date): the number of distinct signal
        # dates is tiny, so freshness is applied per date in Python without
        # loading one row per ticker.
        grouped = self.db.execute(
            select(AnalysisResult.signal, AnalysisResult.trading_date, func.count().label("n"))
            .join(
                latest,
                and_(AnalysisResult.ticker == latest.c.t, AnalysisResult.trading_date == latest.c.d),
            )
            .join(universe, universe.c.ticker == AnalysisResult.ticker)
            .group_by(AnalysisResult.signal, AnalysisResult.trading_date)
        ).all()

        by_label: dict[str, int] = {}
        with_signals = fresh = stale = 0
        for label, trading_date, n in grouped:
            by_label[label] = by_label.get(label, 0) + n
            with_signals += n
            if cfg.is_signal_stale(trading_date, reference_date):
                stale += n
            else:
                fresh += n

        active_alerts = self.db.execute(
            select(func.count())
            .select_from(Alert)
            .join(universe, universe.c.ticker == Alert.ticker)
            .where(Alert.status == cfg.ALERT_ACTIVE_STATUS)
        ).scalar_one()

        return MarketSummary(
            total_securities=total,
            securities_with_signals=with_signals,
            securities_without_signals=total - with_signals,
            bullish_count=by_label.get("bullish", 0),
            bearish_count=by_label.get("bearish", 0),
            divergence_count=by_label.get("divergence", 0),
            unusual_activity_count=by_label.get("unusual_activity", 0),
            neutral_count=by_label.get("neutral", 0),
            fresh_signal_count=fresh,
            stale_signal_count=stale,
            active_alert_count=active_alerts,
        )

    # ==================================================================
    # BULK FETCHERS (each is one statement for any number of tickers)
    # ==================================================================

    def _latest_rows(self, model, date_column, tickers: list[str]) -> dict[str, Any]:
        """
        Latest row per ticker: join against (ticker, MAX(date_column)).
        Ordered by id so a (rare) duplicate at the max date resolves
        deterministically to the highest id.
        """
        latest = (
            select(model.ticker.label("t"), func.max(date_column).label("d"))
            .where(model.ticker.in_(tickers))
            .group_by(model.ticker)
            .subquery()
        )
        rows = self.db.execute(
            select(model)
            .join(latest, and_(model.ticker == latest.c.t, date_column == latest.c.d))
            .order_by(model.id)
        ).scalars().all()
        return {row.ticker: row for row in rows}

    def _features_on_signal_dates(self, signals: dict[str, Any]) -> dict[tuple[str, date], Any]:
        pairs = [(ticker, sig.trading_date) for ticker, sig in signals.items()]
        if not pairs:
            return {}
        rows = self.db.execute(
            select(Features).where(tuple_(Features.ticker, Features.trading_date).in_(pairs))
        ).scalars().all()
        return {(row.ticker, row.trading_date): row for row in rows}

    def _active_alerts(self, tickers: list[str]) -> dict[str, list[Any]]:
        rows = self.db.execute(
            select(Alert)
            .where(Alert.ticker.in_(tickers), Alert.status == cfg.ALERT_ACTIVE_STATUS)
            .order_by(Alert.ticker, Alert.id)
        ).scalars().all()
        grouped: dict[str, list[Any]] = {}
        for row in rows:
            grouped.setdefault(row.ticker, []).append(row)
        return grouped

    # ==================================================================
    # SECURITY ASSEMBLY (no DB access below this line)
    # ==================================================================

    def _build_securities(self, tickers: list[str], reference_date: date) -> list[SecurityIntelligence]:
        if not tickers:
            return []

        signals = self._latest_rows(AnalysisResult, AnalysisResult.trading_date, tickers)
        latest_features = self._latest_rows(Features, Features.trading_date, tickers)
        fundamentals = self._latest_rows(Fundamentals, Fundamentals.report_date, tickers)
        market_rows = self._latest_rows(MarketData, MarketData.timestamp, tickers)
        signal_date_features = self._features_on_signal_dates(signals)
        alerts = self._active_alerts(tickers)

        result: list[SecurityIntelligence] = []
        for ticker in tickers:
            signal = signals.get(ticker)
            latest = latest_features.get(ticker)
            # Never substitute the latest row for a missing signal-date row.
            on_signal_date = (
                signal_date_features.get((ticker, signal.trading_date)) if signal else None
            )
            fundamental = fundamentals.get(ticker)
            ticker_alerts = alerts.get(ticker, [])

            company = (
                self._clean(getattr(market_rows.get(ticker), "company", None))
                or self._clean(getattr(fundamental, "company", None))
            )

            fundamental_date = self._as_date(fundamental.report_date) if fundamental else None

            result.append(
                SecurityIntelligence(
                    ticker=ticker,
                    company_name=company,
                    market=MarketIntelligence(
                        latest=self._market_snapshot(latest),
                        signal_date=self._market_snapshot(on_signal_date),
                    ),
                    sentiment=SentimentIntelligence(
                        latest=self._sentiment_snapshot(latest),
                        signal_date=self._sentiment_snapshot(on_signal_date),
                    ),
                    signal=self._signal(signal),
                    active_alerts=[self._alert(a) for a in ticker_alerts],
                    fundamentals=self._fundamentals(fundamental, fundamental_date),
                    data_status=DataStatus(
                        latest_features=SourceStatus(
                            as_of=latest.trading_date if latest else None,
                            is_available=latest is not None,
                            is_stale=(
                                cfg.is_market_features_stale(latest.trading_date, reference_date)
                                if latest else None
                            ),
                        ),
                        signal_date_features=SourceStatus(
                            as_of=signal.trading_date if signal else None,
                            is_available=on_signal_date is not None,
                            is_stale=None,
                        ),
                        signal=SourceStatus(
                            as_of=signal.trading_date if signal else None,
                            is_available=signal is not None,
                            is_stale=(
                                cfg.is_signal_stale(signal.trading_date, reference_date)
                                if signal else None
                            ),
                        ),
                        fundamentals=SourceStatus(
                            as_of=fundamental_date,
                            is_available=fundamental is not None,
                            is_stale=(
                                cfg.is_fundamentals_stale(fundamental_date, reference_date)
                                if fundamental_date else None
                            ),
                        ),
                        active_alerts=SourceStatus(
                            as_of=None,
                            is_available=bool(ticker_alerts),
                            is_stale=None,
                        ),
                    ),
                )
            )
        return result

    @staticmethod
    def _build_ticker_summary(security: SecurityIntelligence) -> TickerSummary:
        """Restates fields already on the security record - no new data."""
        signal = security.signal
        status = security.data_status
        return TickerSummary(
            ticker=security.ticker,
            company_name=security.company_name,
            signal=signal.signal if signal else None,
            signal_strength=signal.strength if signal else None,
            confidence_score=signal.confidence_score if signal else None,
            signal_trading_date=signal.trading_date if signal else None,
            signal_is_stale=status.signal.is_stale,
            latest_features_date=status.latest_features.as_of,
            has_fundamentals=status.fundamentals.is_available,
            active_alert_count=len(security.active_alerts),
        )

    # ------------------------------------------------------------------
    # Row -> schema mappers
    # ------------------------------------------------------------------

    def _market_snapshot(self, row) -> Optional[MarketFeatureSnapshot]:
        if row is None:
            return None
        return MarketFeatureSnapshot(
            trading_date=row.trading_date,
            close_price=self._num(row.close_price),
            daily_return=self._num(row.daily_return),
            volume_ratio_10d=self._num(row.volume_ratio_10d),
            volume_ratio_20d=self._num(row.volume_ratio_20d),
            volatility_5d=self._num(row.volatility_5d),
            volatility_20d=self._num(row.volatility_20d),
        )

    def _sentiment_snapshot(self, row) -> Optional[SentimentSnapshot]:
        if row is None:
            return None
        return SentimentSnapshot(
            trading_date=row.trading_date,
            average_sentiment=self._num(row.average_sentiment),
            weighted_sentiment=self._num(row.weighted_sentiment),
            sentiment_volatility=self._num(row.sentiment_volatility),
            relevant_headline_count=row.relevant_headline_count,
            average_impact_score=self._num(row.average_impact_score),
        )

    def _signal(self, row) -> Optional[SignalIntelligence]:
        if row is None:
            return None
        return SignalIntelligence(
            id=row.id,
            signal=row.signal,
            strength=row.strength,
            confidence_score=self._num(row.confidence_score),
            explanation=row.explanation,
            components=row.components,
            trading_date=row.trading_date,
            timestamp=row.timestamp,
        )

    def _fundamentals(self, row, report_date: Optional[date]) -> Optional[FundamentalIntelligence]:
        if row is None:
            return None
        get = lambda name: self._num(getattr(row, name, None))  # noqa: E731 - net_profit etc. may be absent on older schemas
        return FundamentalIntelligence(
            report_date=report_date,
            eps=get("eps"),
            pe_ratio=get("pe_ratio"),
            dividend_yield=get("dividend_yield"),
            dividend_per_share=get("dividend_per_share"),
            revenue=get("revenue"),
            net_profit=get("net_profit"),
            debt_ratio=get("debt_ratio"),
            net_profit_margin=get("net_profit_margin"),
            timestamp=getattr(row, "timestamp", None),
        )

    @staticmethod
    def _alert(row) -> AlertItem:
        """Core fields typed; every other stored column passed through as-is."""
        columns = [attr.key for attr in sa_inspect(type(row)).mapper.column_attrs]
        details = {
            key: MasterIntelligenceService._plain(getattr(row, key))
            for key in columns
            if key not in _ALERT_CORE_FIELDS and key != "ticker"
        }
        return AlertItem(
            id=getattr(row, "id", None),
            alert_type=getattr(row, "alert_type", None),
            severity=getattr(row, "severity", None),
            status=getattr(row, "status", None),
            trading_date=getattr(row, "trading_date", None),
            details=details,
        )

    # ==================================================================
    # MACRO (market-wide, one query)
    # ==================================================================

    def _build_macro(self, reference_date: date) -> MacroIntelligence:
        """
        Per indicator: the most recent row (by data month/year, NOT report_date,
        which is shared by every row from one KNBS PDF) where it is non-null.
        The trend is the trend stored on that SAME row, so an older trend is
        never presented as describing a newer value.

        macro_data is monthly, so loading it whole is a few dozen rows.
        """
        rows = self.db.execute(select(MacroData)).scalars().all()

        keyed: list[tuple[tuple[int, int, int], Any]] = []
        for row in rows:
            period = self._period_key(row.month, row.year)
            if period is not None:
                keyed.append(((period[0], period[1], row.id or 0), row))
        keyed.sort(key=lambda item: item[0], reverse=True)

        indicators: dict[str, MacroIndicator] = {}
        for name, (value_column, trend_column) in MACRO_INDICATORS.items():
            indicators[name] = self._macro_indicator(
                keyed, value_column, trend_column, reference_date
            )
        return MacroIntelligence(**indicators)

    def _macro_indicator(
        self,
        keyed: list[tuple[tuple[int, int, int], Any]],
        value_column: str,
        trend_column: Optional[str],
        reference_date: date,
    ) -> MacroIndicator:
        for (year, month_index, _), row in keyed:
            value = getattr(row, value_column)
            if value is None:
                continue
            month = month_index + 1
            return MacroIndicator(
                value=float(value),
                period=f"{MONTH_ORDER[month_index]} {year}",
                as_of=date(year, month, 1),
                report_date=self._as_date(row.report_date),
                trend=self._num(getattr(row, trend_column)) if trend_column else None,
                is_available=True,
                is_stale=cfg.is_macro_stale(year, month, reference_date),
            )
        return MacroIndicator(is_available=False)

    @staticmethod
    def _period_key(month: Optional[str], year: Optional[str]) -> Optional[tuple[int, int]]:
        if not month or not year:
            return None
        name = str(month).strip().capitalize()
        if name not in MONTH_ORDER:
            return None
        try:
            return int(str(year).strip()), MONTH_ORDER.index(name)
        except ValueError:
            return None

    # ==================================================================
    # SMALL HELPERS
    # ==================================================================

    @staticmethod
    def _num(value: Any) -> Optional[float]:
        """Decimal/int/float -> float, preserving None (NULL is never 0)."""
        return None if value is None else float(value)

    @staticmethod
    def _clean(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _as_date(value: Any) -> Optional[date]:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                value = value.astimezone(NAIROBI_TZ)
            return value.date()
        return value

    @staticmethod
    def _plain(value: Any) -> Any:
        return float(value) if isinstance(value, Decimal) else value