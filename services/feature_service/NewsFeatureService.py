"""
services/feature_service/news_feature_service.py

Service Layer — fills the news/sentiment-owned columns on
`daily_market_features` from stored, already-scored `Headline` rows.

Pipeline position:

    headlines (RSS/News ingestion + KeywordEngine + SentimentAnalyzer)
        │
        ▼
    NewsFeatureService              <- this file
        │
        ├── bucket headlines into (Nairobi) trading days, with an EAT
        │   market-close cutoff so nothing leaks into a day before it
        │   was actually knowable
        ├── per (ticker, trading_date): explicit-mention relevance,
        │   sentiment aggregates, weighted sentiment, volatility
        ├── per trading_date (market-wide, not ticker-filtered): topic
        │   sentiment (energy / electricity / oil / macro / government)
        │
        ▼
    daily_market_features  (UPDATE existing rows only, single commit)

Row ownership:
    MarketCalculatorService creates the (ticker, trading_date) row and
    owns the price/volume/volatility columns. This service NEVER inserts
    a row — it only updates rows that already exist. If no row exists
    yet for a targeted (ticker, trading_date), that pair is counted in
    `rows_skipped` with an explanatory error entry, never silently
    dropped. Practically: MarketCalculatorService must run first.

Relevance model (deliberately simplified — see note):
    Relevance is binary: a headline is "mentioned" for a ticker only if
    the ticker's own symbol or a derived/overridden company-name alias
    appears in its title or description. An earlier design considered a
    graduated relevance ladder (category-based partial credit), but that
    implicitly assumed KPLC's own sector (energy) and doesn't generalise
    to an arbitrary ticker with no sector data on file. Binary
    explicit-mention also matches the PRD's article-association rule
    (§5.3): explicit mention -> associate, no inferred association.

Topic sentiment scope:
    energy_sentiment / electricity_sentiment / oil_sentiment /
    macro_sentiment / government_sentiment are MARKET-WIDE per trading
    date, not filtered by per-ticker relevance — every ticker's row for
    the same trading_date gets the same values. This mirrors how the PRD
    treats macro data generally: common market-level context, not
    something computed per security.

This module deliberately does NOT:
  - Score sentiment itself (reads persisted Headline.sentiment_score
    only; never imports/loads SentimentAnalyzer/FinBERT)
  - Create daily_market_features rows
  - Touch the macroeconomic columns (inflation, interest_rate, usd_kes,
    oil_price) — those belong to a macro-fusion pass, not this service
  - Define any FastAPI routes
  - Create its own DB session (always injected, per project convention)

Known follow-ups (tracked, not fixed here):
  - `average_kplc_relevance` is a legacy column name from the KPLC-only
    phase of the project; it now holds a generic per-ticker mention
    density. Renaming it needs a migration (no Alembic in this project
    yet), so it's left as-is.
  - ALIAS_OVERRIDES is a small hand-maintained dict until a real
    company-alias table exists.
  - No enforced pipeline ordering exists yet between SentimentAnalyzer /
    MarketCalculatorService / this service — see project backlog.
"""

from __future__ import annotations

import logging
import re
import statistics
from bisect import bisect_left
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Any, Optional, Union
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from models.daily_market_features import DailyMarketFeatures
from models.headline_data import Headline
from models.market_data import MarketData

logger = logging.getLogger(__name__)

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Headlines published at/after this Nairobi hour are treated as knowable
# only from the NEXT trading day onward (rolls forward across weekends too,
# via bisect against the real trading-date list).
MARKET_CLOSE_HOUR_EAT: int = 15

# Aliases shorter than this are rejected as mention-matching candidates —
# guards against short tickers/fragments producing false-positive matches.
MIN_ALIAS_LEN: int = 3

# Extra lookback before the earliest targeted trading date, so a Saturday/
# Sunday headline that rolls forward to Monday is still captured by the
# bounded DB query.
_LOOKBACK_DAYS: int = 4

_ROUND_DP: int = 6

_CORPORATE_SUFFIX_PATTERN = re.compile(
    r"\b(PLC|Plc|Ltd\.?|Limited|Group|Holdings|Company|Co\.?|Bank)\b",
    re.IGNORECASE,
)

# Manual escape hatch for aliases a plain corporate-suffix strip won't
# produce (e.g. "Equity Group Holdings" -> "Equity", but press coverage
# says "Equity Bank"). Plain data, not per-ticker application logic —
# same spirit as a securities/company-alias table this project doesn't
# have yet. Extend freely as new tickers are onboarded.
ALIAS_OVERRIDES: dict[str, list[str]] = {
    "KPLC": ["Kenya Power"],
    "EQTY": ["Equity Bank"],
    "SCOM": ["Safaricom"],
    "KCB": ["KCB Group"],
    "COOP": ["Co-operative Bank", "Co-op Bank"],
}

# Market-wide topic buckets. Matched against the SAME `categories` /
# `keywords_detected` columns KeywordEngine already populated at
# ingestion — never re-scored here. Overlaps are fine: one headline can
# count toward more than one topic.
TOPIC_REGISTRY: dict[str, dict[str, set[str]]] = {
    "energy_sentiment": {"categories": {"energy_sector"}},
    "electricity_sentiment": {"keywords": {"electricity", "tariff", "epra", "power outage"}},
    "oil_sentiment": {"keywords": {"oil", "fuel"}},
    "macro_sentiment": {"categories": {"macro_economy"}},
    "government_sentiment": {"categories": {"kenya_policy"}},
}

# TargetSpec:
#   None       -> every existing daily_market_features row for the ticker
#   set[date]  -> exactly those trading dates (if a row exists)
#   (date, date) -> inclusive range bounds
TargetSpec = Optional[Union[set, tuple]]


# ---------------------------------------------------------------------------
# Internal result container
# ---------------------------------------------------------------------------

@dataclass
class UpdateOutcome:
    """Classifies every targeted (ticker, trading_date) pair processed in a run."""

    updated: int = 0
    skipped: int = 0
    skipped_details: list[dict[str, Any]] = field(default_factory=list)


class NewsFeatureService:
    """
    Fills the news/sentiment-owned columns of `daily_market_features`.

    Usage:
        service = NewsFeatureService(db)
        result  = service.process_all()
        result  = service.process_ticker("KPLC")
        result  = service.process_date(date(2026, 9, 22))
        result  = service.process_date_range(date(2026, 9, 1), date(2026, 9, 22))

    Every public method returns the same structured, JSON-serialisable
    dict described in `_build_result` — the same envelope shape as
    MarketCalculatorService's result.
    """

    def __init__(self, db: Session) -> None:
        """
        Args:
            db: Active SQLAlchemy session (injected by the caller — this
                service never opens, closes, or creates its own session).
        """
        self.db = db
        self._alias_cache: dict[str, list[str]] = {}

    # ==================================================================
    # PUBLIC API
    # ==================================================================

    def process_all(self) -> dict[str, Any]:
        """Update every ticker that already has at least one daily_market_features row."""
        tickers = self._get_tickers_with_feature_rows()
        logger.info("process_all: %d ticker(s) with existing feature rows", len(tickers))
        return self._run({ticker: None for ticker in tickers})

    def process_ticker(self, ticker: str) -> dict[str, Any]:
        """Update every existing daily_market_features row for a single ticker."""
        return self._run({ticker: None})

    def process_date(self, target_date: date) -> dict[str, Any]:
        """Update every ticker that has a daily_market_features row on `target_date`."""
        tickers = self._get_tickers_for_date(target_date)
        logger.info("process_date: %s — %d ticker(s) found", target_date, len(tickers))
        return self._run({ticker: {target_date} for ticker in tickers})

    def process_date_range(self, start_date: date, end_date: date) -> dict[str, Any]:
        """Update every ticker with a daily_market_features row inside [start_date, end_date]."""
        if start_date > end_date:
            return self._build_failure_result(
                tickers_processed=0,
                attempted_rows=0,
                error_message=(
                    f"start_date ({start_date}) must not be after end_date ({end_date})."
                ),
            )

        tickers = self._get_tickers_for_range(start_date, end_date)
        logger.info(
            "process_date_range: %s -> %s — %d ticker(s) found",
            start_date, end_date, len(tickers),
        )
        return self._run({ticker: (start_date, end_date) for ticker in tickers})

    # ==================================================================
    # ORCHESTRATION
    # ==================================================================

    def _run(self, ticker_specs: dict[str, TargetSpec]) -> dict[str, Any]:
        """
        Resolve targeted (ticker, trading_date) pairs against EXISTING
        daily_market_features rows only, compute news features for each,
        and persist in a single transaction.

        Headlines are loaded and bucketed by trading date ONCE per run
        and reused across every ticker (topic aggregates are computed
        once per date too) — never re-queried per ticker.
        """
        if not ticker_specs:
            return self._build_result(0, UpdateOutcome())

        trading_dates = self._get_trading_dates()
        if not trading_dates:
            logger.warning("No trading dates found in market_data — nothing to process.")
            return self._build_result(len(ticker_specs), UpdateOutcome())

        target_keys = self._resolve_target_keys(ticker_specs)
        if not target_keys:
            return self._build_result(len(ticker_specs), UpdateOutcome())

        needed_dates = {trading_date for _, trading_date in target_keys}
        day_pool = self._load_headline_buckets(trading_dates, needed_dates)
        topic_values_by_date = {
            trading_date: self._aggregate_topics(pool) for trading_date, pool in day_pool.items()
        }

        rows_by_key = self._fetch_feature_rows(target_keys)
        outcome = UpdateOutcome()

        for ticker, trading_date in sorted(target_keys):
            row = rows_by_key.get((ticker, trading_date))

            if row is None:
                outcome.skipped += 1
                outcome.skipped_details.append(
                    self._error_entry(
                        ticker, trading_date,
                        "No daily_market_features row — run MarketCalculatorService first.",
                    )
                )
                continue

            try:
                aliases = self._get_aliases(ticker)
                pool = day_pool.get(trading_date, [])
                values = {
                    **self._aggregate_ticker_day(pool, aliases),
                    **topic_values_by_date.get(trading_date, self._empty_topic_values()),
                }
            except Exception as exc:  # noqa: BLE001 — isolate per-row failures
                logger.error(
                    "Failed computing news features for %s %s: %s", ticker, trading_date, exc
                )
                outcome.skipped += 1
                outcome.skipped_details.append(
                    self._error_entry(ticker, trading_date, f"Calculation error: {exc}")
                )
                continue

            for column, value in values.items():
                setattr(row, column, value)
            outcome.updated += 1

        try:
            self.db.commit()
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.error("Failed to persist news feature batch: %s", exc)
            return self._build_failure_result(
                tickers_processed=len(ticker_specs),
                attempted_rows=len(target_keys),
                error_message=str(exc),
            )

        logger.info(
            "News feature run complete: updated=%d skipped=%d",
            outcome.updated, outcome.skipped,
        )
        return self._build_result(len(ticker_specs), outcome)

    # ==================================================================
    # TARGET RESOLUTION (update-only — driven entirely off existing rows)
    # ==================================================================

    def _resolve_target_keys(
        self, ticker_specs: dict[str, TargetSpec]
    ) -> set[tuple[str, date]]:
        """Resolve every (ticker, trading_date) pair that already has a row and is targeted."""
        keys: set[tuple[str, date]] = set()

        for ticker, spec in ticker_specs.items():
            query = select(DailyMarketFeatures.trading_date).where(
                DailyMarketFeatures.ticker == ticker
            )

            if isinstance(spec, tuple):
                start_date, end_date = spec
                query = query.where(
                    DailyMarketFeatures.trading_date >= start_date,
                    DailyMarketFeatures.trading_date <= end_date,
                )
            elif isinstance(spec, set):
                query = query.where(DailyMarketFeatures.trading_date.in_(spec))
            # spec is None -> every existing row for this ticker, no extra filter

            dates = self.db.execute(query).scalars().all()
            keys.update((ticker, trading_date) for trading_date in dates)

        return keys

    def _fetch_feature_rows(
        self, keys: set[tuple[str, date]]
    ) -> dict[tuple[str, date], DailyMarketFeatures]:
        """Batch-fetch every targeted row in one query instead of one query per key."""
        if not keys:
            return {}

        tickers = {ticker for ticker, _ in keys}
        dates = {trading_date for _, trading_date in keys}

        candidates = self.db.execute(
            select(DailyMarketFeatures).where(
                DailyMarketFeatures.ticker.in_(tickers),
                DailyMarketFeatures.trading_date.in_(dates),
            )
        ).scalars().all()

        return {
            (row.ticker, row.trading_date): row
            for row in candidates
            if (row.ticker, row.trading_date) in keys
        }

    # ==================================================================
    # TICKER / DATE DISCOVERY (against daily_market_features — update-only)
    # ==================================================================

    def _get_tickers_with_feature_rows(self) -> list[str]:
        rows = self.db.execute(select(DailyMarketFeatures.ticker).distinct()).scalars().all()
        return sorted(set(rows))

    def _get_tickers_for_date(self, target_date: date) -> list[str]:
        rows = self.db.execute(
            select(DailyMarketFeatures.ticker)
            .distinct()
            .where(DailyMarketFeatures.trading_date == target_date)
        ).scalars().all()
        return sorted(set(rows))

    def _get_tickers_for_range(self, start_date: date, end_date: date) -> list[str]:
        rows = self.db.execute(
            select(DailyMarketFeatures.ticker)
            .distinct()
            .where(
                DailyMarketFeatures.trading_date >= start_date,
                DailyMarketFeatures.trading_date <= end_date,
            )
        ).scalars().all()
        return sorted(set(rows))

    def _get_trading_dates(self) -> list[date]:
        """Trading dates come from real market_data rows, per project convention."""
        timestamps = self.db.execute(select(MarketData.timestamp)).scalars().all()
        return sorted({self._to_nairobi_date(ts) for ts in timestamps if ts is not None})

    # ==================================================================
    # HEADLINE LOADING + BUCKETING (one pass, shared across every ticker)
    # ==================================================================

    def _load_headline_buckets(
        self, trading_dates: list[date], needed_dates: set[date]
    ) -> dict[date, list[Headline]]:
        """
        Load headlines bounded to a window around `needed_dates` and bucket
        each into a trading date via `_assign_trading_date`. One bounded
        query, reused by every ticker in this run — never per-ticker.
        """
        if not needed_dates:
            return {}

        earliest = min(needed_dates) - timedelta(days=_LOOKBACK_DAYS)
        latest = max(needed_dates) + timedelta(days=1)
        start = datetime.combine(earliest, time.min, tzinfo=NAIROBI_TZ)
        end = datetime.combine(latest, time.max, tzinfo=NAIROBI_TZ)

        # COALESCE so a null published_at (rare — falls back to ingestion
        # time) is bounded in the same query instead of a second round trip.
        effective_ts = func.coalesce(Headline.published_at, Headline.timestamp)

        headlines = self.db.execute(
            select(Headline).where(effective_ts >= start, effective_ts <= end)
        ).scalars().all()

        buckets: dict[date, list[Headline]] = {trading_date: [] for trading_date in needed_dates}
        for headline in headlines:
            bucket_date = self._assign_trading_date(headline, trading_dates)
            if bucket_date in buckets:
                buckets[bucket_date].append(headline)

        logger.info(
            "Loaded %d headline(s), bucketed into %d targeted trading date(s)",
            len(headlines), len(needed_dates),
        )
        return buckets

    @staticmethod
    def _assign_trading_date(headline: Headline, trading_dates: list[date]) -> Optional[date]:
        """
        Resolve which trading date a headline is knowable by.

        Headlines at/after MARKET_CLOSE_HOUR_EAT roll forward to the next
        trading date (bisect handles weekend/holiday rollover to Monday
        automatically, since it walks the real trading-date list).
        `published_at` falling back to `timestamp` (ingestion time) can
        only delay a headline, never leak it earlier.
        """
        ts = headline.published_at or headline.timestamp
        if ts is None:
            return None

        local = ts.astimezone(NAIROBI_TZ) if ts.tzinfo else ts.replace(tzinfo=NAIROBI_TZ)
        bucket_date = (
            local.date() + timedelta(days=1)
            if local.hour >= MARKET_CLOSE_HOUR_EAT
            else local.date()
        )

        idx = bisect_left(trading_dates, bucket_date)
        return trading_dates[idx] if idx < len(trading_dates) else None

    # ==================================================================
    # RELEVANCE + TOPIC MATCHING (pure — no DB, operate on loaded objects)
    # ==================================================================

    @staticmethod
    def _is_mentioned(headline: Headline, aliases: list[str]) -> bool:
        """Explicit-mention relevance: does any alias appear as a whole word in title/description?"""
        text = f"{headline.headline or ''} {headline.description or ''}"
        return any(
            re.search(rf"\b{re.escape(alias)}\b", text, re.IGNORECASE)
            for alias in aliases
        )

    @staticmethod
    def _matches_topic(headline: Headline, topic_spec: dict[str, set[str]]) -> bool:
        """Match against KeywordEngine's already-stored categories/keywords — never re-scored."""
        categories = {
            token.strip().lower()
            for token in (headline.categories or "").split(",")
            if token.strip()
        }
        keywords = {
            token.strip().lower()
            for token in (headline.keywords_detected or "").split(",")
            if token.strip()
        }
        return bool(
            categories & topic_spec.get("categories", set())
            or keywords & topic_spec.get("keywords", set())
        )

    # ==================================================================
    # ALIAS RESOLUTION
    # ==================================================================

    def _get_aliases(self, ticker: str) -> list[str]:
        """Cached per ticker per run — one MarketData lookup, not one per trading date."""
        if ticker not in self._alias_cache:
            company_name = self._fetch_latest_company_name(ticker)
            self._alias_cache[ticker] = self._derive_aliases(ticker, company_name)
        return self._alias_cache[ticker]

    def _fetch_latest_company_name(self, ticker: str) -> Optional[str]:
        return self.db.execute(
            select(MarketData.company)
            .where(MarketData.ticker == ticker)
            .order_by(MarketData.timestamp.desc())
            .limit(1)
        ).scalar_one_or_none()

    @staticmethod
    def _derive_aliases(ticker: str, company_name: Optional[str]) -> list[str]:
        """
        Build the alias list a headline is checked against for this ticker:
        the ticker symbol itself, the full company name, a corporate-suffix-
        stripped short form, and any manual ALIAS_OVERRIDES entries.
        """
        aliases = [ticker]

        if company_name:
            aliases.append(company_name)
            stripped = _CORPORATE_SUFFIX_PATTERN.sub("", company_name).strip(" &,")
            if stripped and stripped.lower() != company_name.lower():
                aliases.append(stripped)

        aliases.extend(ALIAS_OVERRIDES.get(ticker, []))

        deduped = list(dict.fromkeys(aliases))
        return [alias for alias in deduped if len(alias) >= MIN_ALIAS_LEN]

    # ==================================================================
    # AGGREGATION
    # ==================================================================

    def _aggregate_ticker_day(
        self, pool: list[Headline], aliases: list[str]
    ) -> dict[str, Any]:
        """
        Compute the six per-(ticker, trading_date) fields from that day's
        headline pool. Missing data is always None, never 0 or a
        fabricated default — except relevant_headline_count, which is a
        genuine non-nullable count (0 is a valid, meaningful answer).
        """
        mentioned = [h for h in pool if self._is_mentioned(h, aliases)]
        scored_mentioned = [h for h in mentioned if h.sentiment_score is not None]

        relevant_count = len(mentioned)
        relevance = self._round(relevant_count / len(pool)) if pool else None
        impact = (
            self._round(statistics.fmean(h.impact_score for h in mentioned))
            if mentioned else None
        )

        if scored_mentioned:
            scores = [h.sentiment_score for h in scored_mentioned]
            average_sentiment = self._round(statistics.fmean(scores))

            total_impact = sum(h.impact_score for h in scored_mentioned)
            weighted_sentiment = (
                self._round(
                    sum(h.sentiment_score * h.impact_score for h in scored_mentioned)
                    / total_impact
                )
                if total_impact > 0 else None
            )

            # Sample standard deviation (n-1); None below n=2, per project
            # default — no population-vs-sample override was specified.
            sentiment_volatility = self._round(statistics.stdev(scores)) if len(scores) >= 2 else None
        else:
            average_sentiment = None
            weighted_sentiment = None
            sentiment_volatility = None

        return {
            "relevant_headline_count": relevant_count,
            "average_kplc_relevance": relevance,
            "average_impact_score": impact,
            "average_sentiment": average_sentiment,
            "weighted_sentiment": weighted_sentiment,
            "sentiment_volatility": sentiment_volatility,
        }

    def _aggregate_topics(self, pool: list[Headline]) -> dict[str, Optional[float]]:
        """
        Market-wide topic sentiment for one trading date — computed once
        from the FULL day's headline pool, never filtered by ticker
        relevance. Reused across every ticker's row for that date.
        """
        values: dict[str, Optional[float]] = {}

        for field_name, spec in TOPIC_REGISTRY.items():
            matched = [h for h in pool if self._matches_topic(h, spec)]
            scored = [h for h in matched if h.sentiment_score is not None]
            values[field_name] = (
                self._round(statistics.fmean(h.sentiment_score for h in scored))
                if scored else None
            )

        return values

    @staticmethod
    def _empty_topic_values() -> dict[str, Optional[float]]:
        return {field_name: None for field_name in TOPIC_REGISTRY}

    @staticmethod
    def _round(value: Optional[float]) -> Optional[float]:
        return round(value, _ROUND_DP) if value is not None else None

    # ==================================================================
    # TIMEZONE HELPERS
    # ==================================================================

    @staticmethod
    def _to_nairobi_date(timestamp: datetime) -> date:
        if timestamp.tzinfo is None:
            # Project convention: naive timestamps are already Nairobi-local.
            return timestamp.date()
        return timestamp.astimezone(NAIROBI_TZ).date()

    # ==================================================================
    # RESPONSE BUILDERS
    # ==================================================================

    @staticmethod
    def _error_entry(ticker: str, trading_date: date, message: str) -> dict[str, Any]:
        return {"ticker": ticker, "trading_date": trading_date.isoformat(), "error": message}

    @staticmethod
    def _build_result(tickers_processed: int, outcome: UpdateOutcome) -> dict[str, Any]:
        """
        Same envelope shape as MarketCalculatorService's result.
        rows_inserted is always 0 — this service is update-only and never
        creates a daily_market_features row.
        """
        rows_processed = outcome.updated + outcome.skipped
        status = "success" if not outcome.skipped_details else "partial_success"

        return {
            "status": status,
            "tickers_processed": tickers_processed,
            "rows_processed": rows_processed,
            "rows_inserted": 0,
            "rows_updated": outcome.updated,
            "rows_skipped": outcome.skipped,
            "errors": outcome.skipped_details,
        }

    @staticmethod
    def _build_failure_result(
        tickers_processed: int,
        attempted_rows: int,
        error_message: str,
    ) -> dict[str, Any]:
        """Used only when the persistence transaction itself failed, or input validation failed."""
        return {
            "status": "failed",
            "tickers_processed": tickers_processed,
            "rows_processed": attempted_rows,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_skipped": attempted_rows,
            "errors": [{"ticker": None, "trading_date": None, "error": error_message}],
        }


# ---------------------------------------------------------------------------
# CLI smoke-test:  python -m services.feature_service.news_feature_service
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    from database.session import SessionLocal

    session = SessionLocal()
    try:
        service = NewsFeatureService(session)
        result = service.process_all()
        print(json.dumps(result, indent=2, default=str))
    finally:
        session.close()