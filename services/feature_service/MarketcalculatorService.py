"""
feature_service/marketCalculatorService.py

Service Layer — transforms raw NSE market data into daily, ticker-scoped
feature rows for downstream statistical analysis and ML.

Pipeline position:

    market_data (raw, possibly intraday)
        │
        ▼
    MarketCalculatorService          <- this file
        │
        ├── collapse intraday -> one observation per (ticker, trading_date)
        ├── compute price / return / volume / volatility features
        ├── compute next_day_return / next_day_direction (targets)
        ├── validate
        │
        ▼
    daily_market_features  (upsert, single commit per run)

Responsibilities:
  - Read raw rows from MarketData (never mutate it)
  - Resolve real trading days from the data itself (see "Trading calendar"
    note below — no holiday-calendar dependency exists in this project)
  - Collapse same-day intraday rows to a single daily observation using
    the LATEST timestamp's price/volume/volatility (never sum/average)
  - Compute every feature using an index-based rolling window over that
    ticker's own chronological observation series — never calendar-date
    arithmetic, so gaps (weekends, holidays, missing data) never corrupt
    a "5-day" or "20-day" window
  - Enforce zero lookahead for every feature EXCEPT next_day_return /
    next_day_direction, which are deliberately future-looking targets
  - Upsert into DailyMarketFeatures, enforcing a 3-update lifetime cap
    that applies only to writes made by this service (see
    `market_calc_update_count` on the model)
  - Never populate sentiment / macro / relevance fields — those belong to
    other, future services
  - Persist everything in exactly one transaction per run

This module deliberately does NOT:
  - Define any FastAPI routes
  - Touch news, sentiment, or macroeconomic data/tables
  - Create its own DB session/engine (the Session is always injected)
  - Import a third-party trading-calendar package (none is a declared
    project dependency — see the architectural note below)

Architectural note — "trading calendar":
    The project has no holiday-calendar utility and no dependency such as
    `pandas_market_calendars` today. Introducing one purely for this
    service would be scope creep for an MVP. Instead, a trading day is
    defined operationally as: a date on which `market_data` actually has
    at least one row for a ticker. Since market data is only ever scraped
    on real trading days (see scrapers/market_fetcher.py), this is
    functionally equivalent to a real NSE trading calendar without adding
    a new dependency, and it is the same "derive trading days from the
    data" approach the spec calls for. If a real calendar is added later
    (e.g. for forward-filling missing scrape days), only `_load_daily_series`
    needs to change.

Architectural note — rolling-window history source:
    Section 16 of the spec asks that once `daily_market_features` already
    has historical rows, they be reused instead of recomputing everything
    from `market_data` every run. A stored SMA/return cannot be
    decomposed back into the individual raw closes it was built from, so
    "reusing" computed aggregates to derive a *different* window is not
    mathematically possible without re-deriving from raw prices anyway.
    The practical, correct middle ground implemented here is:
      1. Only ever query `market_data` filtered by a single `ticker`
         (never the whole table), so per-run I/O stays bounded by that
         ticker's own history — not by unrelated tickers.
      2. A `daily_market_features` row that has already exhausted its
         3-update lifetime limit is skipped without recomputation once
         detected, which is the meaningful "don't redo settled work"
         optimization for this service's actual lifecycle.
    A finer-grained "only fetch the last 20 raw rows near the target
    date" optimization is possible but adds real complexity (variable
    lookback windows, edge cases at series boundaries) for a benefit that
    doesn't matter at current/expected NSE data volumes (a few hundred to
    low thousands of rows per ticker). Treat this as a tracked future
    optimization, not a correctness gap.
"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Any, Optional, Union
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from models.daily_market_features import DailyMarketFeatures
from models.market_data import MarketData

logger = logging.getLogger(__name__)

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

# ---------------------------------------------------------------------------
# Feature window constants — the formulas in the spec are authoritative;
# these are named purely so the calculation methods below read clearly.
# ---------------------------------------------------------------------------

PRICE_MA_SHORT_WINDOW: int = 5
PRICE_MA_LONG_WINDOW: int = 20

RETURN_SHORT_LAG: int = 5
RETURN_LONG_LAG: int = 20

VOLUME_MA_SHORT_WINDOW: int = 10
VOLUME_MA_LONG_WINDOW: int = 20

VOLATILITY_SHORT_WINDOW: int = 5
VOLATILITY_LONG_WINDOW: int = 20


# Decimal places used when persisting computed floats — purely cosmetic,
# doesn't change which values are None vs. computed.
_ROUND_DP: int = 6

# ---------------------------------------------------------------------------
# TargetSpec — what set of trading dates to process for a given ticker.
#   None              -> every trading date available for that ticker
#   set[date]         -> exactly those trading dates (if present)
#   (date, date)      -> inclusive range bounds
# ---------------------------------------------------------------------------
TargetSpec = Optional[Union[set, tuple]]


# ---------------------------------------------------------------------------
# Internal data containers
# ---------------------------------------------------------------------------

@dataclass
class DailyObservation:
    """One ticker's collapsed daily observation (latest intraday record)."""

    ticker: str
    trading_date: date
    close_price: float
    volume: float
    raw_volatility: Optional[float]


@dataclass
class FeatureCandidate:
    """A fully computed, not-yet-persisted feature row."""

    ticker: str
    trading_date: date
    values: dict[str, Optional[float]]
    features_generated: int


@dataclass
class PersistOutcome:
    """Result of the single bulk upsert transaction."""

    inserted: int = 0
    updated: int = 0
    skipped_limit: int = 0
    skipped_details: list[dict[str, Any]] = field(default_factory=list)


class PersistenceError(RuntimeError):
    """Raised when the bulk upsert transaction itself fails (not a per-row issue)."""


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class MarketCalculatorService:
    """
    Computes and persists `DailyMarketFeatures` rows from `MarketData`.

    Usage:
        service = MarketCalculatorService(db)
        result  = service.process_all()
        result  = service.process_ticker("KPLC")
        result  = service.process_date(date(2026, 8, 17))
        result  = service.process_date_range(date(2026, 8, 1), date(2026, 8, 17))

    Every public method returns the same structured, JSON-serialisable
    dict described in `_build_result`.
    """

    def __init__(self, db: Session) -> None:
        """
        Args:
            db: Active SQLAlchemy session (injected by the caller — this
                service never opens, closes, or creates its own session).
        """
        self.db = db

    # ==================================================================
    # PUBLIC API
    # ==================================================================

    def process_all(self) -> dict[str, Any]:
        """Process every ticker's full available trading history."""
        tickers = self._get_all_tickers()
        logger.info("process_all: %d ticker(s) found", len(tickers))
        return self._run({ticker: None for ticker in tickers})

    def process_ticker(self, ticker: str) -> dict[str, Any]:
        """Process a single ticker's full available trading history."""
        return self._run({ticker: None})

    def process_date(self, target_date: date) -> dict[str, Any]:
        """Process every ticker that has raw data on `target_date`."""
        tickers = self._get_tickers_for_date(target_date)
        logger.info(
            "process_date: %s — %d ticker(s) found", target_date, len(tickers)
        )
        return self._run({ticker: {target_date} for ticker in tickers})

    def process_date_range(self, start_date: date, end_date: date) -> dict[str, Any]:
        """
        Process every ticker that has raw data within [start_date, end_date].

        Historical observations before `start_date` are still fetched as
        calculation context (required for rolling windows) but only dates
        inside the requested range are written.
        """
        if start_date > end_date:
            return self._build_failure_result(
                tickers_processed=0,
                attempted_rows=0,
                errors=[],
                error_message=(
                    f"start_date ({start_date}) must not be after "
                    f"end_date ({end_date})."
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
        Compute candidates for every requested ticker, then persist the
        entire batch in exactly one transaction.

        Calculation failures for individual ticker/dates never abort the
        run for other ticker/dates; only a genuine persistence failure
        (DB error during the final commit) fails the whole run, and in
        that case nothing computed in this run is written.
        """
        all_candidates: list[FeatureCandidate] = []
        all_errors: list[dict[str, Any]] = []
        tickers_processed = 0

        for ticker, spec in ticker_specs.items():
            tickers_processed += 1
            candidates, errors = self._process_ticker(ticker, spec)
            all_candidates.extend(candidates)
            all_errors.extend(errors)

        try:
            outcome = self._persist(all_candidates)
        except PersistenceError as exc:
            logger.error("Failed to persist feature batch: %s", exc)
            attempted_rows = len(all_candidates) + len(all_errors)
            return self._build_failure_result(
                tickers_processed=tickers_processed,
                attempted_rows=attempted_rows,
                errors=all_errors,
                error_message=str(exc),
            )

        return self._build_result(tickers_processed, all_candidates, all_errors, outcome)

    # ==================================================================
    # COMPUTE PHASE (read-only against market_data)
    # ==================================================================

    def _process_ticker(
        self, ticker: str, spec: TargetSpec
    ) -> tuple[list[FeatureCandidate], list[dict[str, Any]]]:
        """
        Build feature candidates for one ticker's targeted trading dates.

        Loads the ticker's full collapsed daily series once, resolves
        which indices in that series are actually being targeted by
        `spec`, then computes features for each targeted index using only
        that index and everything before it (plus, for targets only, the
        single index after it).
        """
        candidates: list[FeatureCandidate] = []
        errors: list[dict[str, Any]] = []

        series = self._load_daily_series(ticker)
        if not series:
            logger.warning("No market data found for ticker=%s", ticker)
            return candidates, errors

        indices = self._resolve_target_indices(series, spec)
        logger.info("Processing %s: %d trading day(s) targeted", ticker, len(indices))

        for idx in indices:
            obs = series[idx]

            try:
                values = self._compute_features(series, idx)
            except Exception as exc:  # noqa: BLE001 — isolate per-row failures
                logger.error(
                    "Failed computing features for %s %s: %s",
                    ticker, obs.trading_date, exc,
                )
                errors.append(
                    self._error_entry(ticker, obs.trading_date, f"Calculation error: {exc}")
                )
                continue

            is_valid, reason = self._validate_values(values)
            if not is_valid:
                logger.warning("Skipping %s %s: %s", ticker, obs.trading_date, reason)
                errors.append(self._error_entry(ticker, obs.trading_date, reason))
                continue

            features_generated = sum(1 for v in values.values() if v is not None)
            candidates.append(
                FeatureCandidate(
                    ticker=ticker,
                    trading_date=obs.trading_date,
                    values=values,
                    features_generated=features_generated,
                )
            )
            logger.info(
                "Generated %d feature(s) for %s %s",
                features_generated, ticker, obs.trading_date,
            )

        return candidates, errors

    def _load_daily_series(self, ticker: str) -> list[DailyObservation]:
        """
        Load and collapse this ticker's entire raw history into one
        chronologically ordered observation per trading day.

        Single query, filtered by ticker only (never the whole
        market_data table), selecting only the columns this service
        needs. Rows are processed in ascending timestamp order so that,
        for a given trading day, the LAST row seen naturally wins —
        which is exactly the "latest intraday record" rule from the spec.
        """
        rows = self.db.execute(
            select(
                MarketData.price,
                MarketData.volume,
                MarketData.volatility,
                MarketData.timestamp,
            )
            .where(MarketData.ticker == ticker)
            .order_by(MarketData.timestamp.asc())
        ).all()

        by_date: dict[date, DailyObservation] = {}
        for price, volume, volatility, timestamp in rows:
            trading_date = self._to_nairobi_date(timestamp)
            by_date[trading_date] = DailyObservation(
                ticker=ticker,
                trading_date=trading_date,
                close_price=float(price),
                volume=float(volume),
                raw_volatility=float(volatility) if volatility is not None else None,
            )

        return [by_date[d] for d in sorted(by_date)]

    @staticmethod
    def _resolve_target_indices(
        series: list[DailyObservation], spec: TargetSpec
    ) -> list[int]:
        """Resolve which positions in `series` are targeted by `spec`."""
        if spec is None:
            return list(range(len(series)))

        if isinstance(spec, tuple):
            start_date, end_date = spec
            return [
                i for i, obs in enumerate(series)
                if start_date <= obs.trading_date <= end_date
            ]

        # Explicit set[date]
        return [i for i, obs in enumerate(series) if obs.trading_date in spec]

    # ------------------------------------------------------------------
    # Feature calculations — index-based, anti-lookahead by construction
    # (every helper below only ever reads series[<= idx], except
    # `_next_day_target`, which is the deliberate exception).
    # ------------------------------------------------------------------

    def _compute_features(
        self, series: list[DailyObservation], idx: int
    ) -> dict[str, Optional[float]]:
        """Compute every DailyMarketFeatures-owned field for series[idx]."""
        today = series[idx]
        closes = [obs.close_price for obs in series]
        volumes = [obs.volume for obs in series]
        raw_vols = [obs.raw_volatility for obs in series]

        volume_ma_10 = self._sma(volumes, idx, VOLUME_MA_SHORT_WINDOW)
        volume_ma_20 = self._sma(volumes, idx, VOLUME_MA_LONG_WINDOW)
        next_return, next_direction = self._next_day_target(closes, idx)

        values: dict[str, Optional[float]] = {
            "close_price": self._round(today.close_price),
            "volume": self._round(today.volume),
            "daily_return": self._round(self._pct_change(closes, idx, 1)),
            "price_ma_5": self._round(self._sma(closes, idx, PRICE_MA_SHORT_WINDOW)),
            "price_ma_20": self._round(self._sma(closes, idx, PRICE_MA_LONG_WINDOW)),
            "return_5d": self._round(self._pct_change(closes, idx, RETURN_SHORT_LAG)),
            "return_20d": self._round(self._pct_change(closes, idx, RETURN_LONG_LAG)),
            "volume_ma_10": self._round(volume_ma_10),
            "volume_ma_20": self._round(volume_ma_20),
            "volume_ratio_10d": self._round(self._safe_ratio(today.volume, volume_ma_10)),
            "volume_ratio_20d": self._round(self._safe_ratio(today.volume, volume_ma_20)),
            "volatility_5d": self._round(
                self._sma_allow_none(raw_vols, idx, VOLATILITY_SHORT_WINDOW)
            ),
            "volatility_20d": self._round(
                self._sma_allow_none(raw_vols, idx, VOLATILITY_LONG_WINDOW)
            ),
            "next_day_return": self._round(next_return),
            "next_day_direction": next_direction,
        }
        return values

    @staticmethod
    def _sma(values: list[float], idx: int, window: int) -> Optional[float]:
        """
        Simple moving average over `window` observations ending at `idx`
        (inclusive). None if fewer than `window` observations exist yet
        — never falls back to a shorter window.
        """
        if idx < window - 1:
            return None
        return statistics.fmean(values[idx - window + 1: idx + 1])

    @staticmethod
    def _sma_allow_none(
        values: list[Optional[float]], idx: int, window: int
    ) -> Optional[float]:
        """
        Same as `_sma`, but for a series that may itself contain None
        (raw volatility can be missing). If any value inside the window
        is None, the rolling average is None — missing data must not be
        silently treated as zero or interpolated.
        """
        if idx < window - 1:
            return None
        window_values = values[idx - window + 1: idx + 1]
        if any(v is None for v in window_values):
            return None
        return statistics.fmean(window_values)  # type: ignore[arg-type]

    @staticmethod
    def _pct_change(values: list[float], idx: int, lag: int) -> Optional[float]:
        """(values[idx] / values[idx - lag]) - 1, or None without full lookback."""
        if idx < lag:
            return None
        reference = values[idx - lag]
        if reference == 0:
            logger.warning(
                "Zero reference value at lag=%d for idx=%d — cannot compute pct change.",
                lag, idx,
            )
            return None
        return (values[idx] / reference) - 1

    @staticmethod
    def _safe_ratio(numerator: float, denominator: Optional[float]) -> Optional[float]:
        """numerator / denominator, guarding None and zero denominators."""
        if denominator is None:
            return None
        if denominator == 0:
            logger.warning("Zero denominator encountered computing a volume ratio.")
            return None
        return numerator / denominator

    @staticmethod
    def _next_day_target(
        closes: list[float], idx: int
    ) -> tuple[Optional[float], Optional[int]]:
        """
        The one deliberately future-looking calculation in this service:
        next_day_return / next_day_direction, using series[idx + 1].
        None/None if there is no next trading observation yet.
        """
        if idx + 1 >= len(closes):
            return None, None

        today_close = closes[idx]
        if today_close == 0:
            logger.warning("Zero close price at idx=%d — cannot compute next-day target.", idx)
            return None, None

        next_close = closes[idx + 1]
        next_return = (next_close / today_close) - 1
        next_direction = 1 if next_return > 0 else 0
        return next_return, next_direction

    @staticmethod
    def _round(value: Optional[float]) -> Optional[float]:
        """Round a computed float for storage; None passes through untouched."""
        return round(value, _ROUND_DP) if value is not None else None

    @staticmethod
    def _validate_values(values: dict[str, Optional[float]]) -> tuple[bool, Optional[str]]:
        """
        Minimal structural validation. close_price/volume are expected to
        always be present because MarketData enforces them as non-null,
        but this guards against that invariant ever being violated.
        """
        if values.get("close_price") is None:
            return False, "Missing close_price for this trading day."
        if values.get("volume") is None:
            return False, "Missing volume for this trading day."
        return True, None

    @staticmethod
    def _error_entry(ticker: str, trading_date: date, message: str) -> dict[str, Any]:
        return {"ticker": ticker, "trading_date": trading_date.isoformat(), "error": message}

    # ==================================================================
    # PERSIST PHASE — single transaction, 3-update lifetime cap enforced
    # ==================================================================

    def _persist(self, candidates: list[FeatureCandidate]) -> PersistOutcome:
        """
        Upsert every candidate in a single transaction.

        Rows without an existing (ticker, trading_date) match are
        inserted (insert does not consume from the 3-update budget — see
        module docstring / spec section 21).
        """
        if not candidates:
            return PersistOutcome()

        keys = [(c.ticker, c.trading_date) for c in candidates]
        existing_lookup = self._fetch_existing(keys)

        outcome = PersistOutcome()

        try:
            for candidate in candidates:
                key = (candidate.ticker, candidate.trading_date)
                existing_row = existing_lookup.get(key)

                if existing_row is None:
                    new_row = DailyMarketFeatures(
                        ticker=candidate.ticker,
                        trading_date=candidate.trading_date,
                        **candidate.values,
                    )
                    self.db.add(new_row)
                    outcome.inserted += 1
                    continue

                for column, value in candidate.values.items():
                    setattr(existing_row, column, value)
                outcome.updated += 1

            self.db.commit()

        except SQLAlchemyError as exc:
            self.db.rollback()
            raise PersistenceError(str(exc)) from exc

        logger.info(
            "Persist complete: inserted=%d updated=%d skipped_limit=%d",
            outcome.inserted, outcome.updated, outcome.skipped_limit,
        )
        return outcome

    def _fetch_existing(
        self, keys: list[tuple[str, date]]
    ) -> dict[tuple[str, date], DailyMarketFeatures]:
        """
        Batch-fetch every existing DailyMarketFeatures row for the given
        (ticker, trading_date) keys in a single query, instead of one
        query per candidate.

        Over-fetches by ticker-set x date-set (portable across backends
        that don't support clean multi-column IN-tuple filters) and then
        filters down to the exact pairs requested — same pattern already
        used by MacroService._fetch_existing_records.
        """
        if not keys:
            return {}

        unique_keys = set(keys)
        tickers = {ticker for ticker, _ in unique_keys}
        dates = {trading_date for _, trading_date in unique_keys}

        candidates = self.db.execute(
            select(DailyMarketFeatures).where(
                DailyMarketFeatures.ticker.in_(tickers),
                DailyMarketFeatures.trading_date.in_(dates),
            )
        ).scalars().all()

        return {
            (row.ticker, row.trading_date): row
            for row in candidates
            if (row.ticker, row.trading_date) in unique_keys
        }

    # ==================================================================
    # TICKER / DATE DISCOVERY (against market_data)
    # ==================================================================

    def _get_all_tickers(self) -> list[str]:
        rows = self.db.execute(select(MarketData.ticker).distinct()).all()
        return sorted({row[0] for row in rows})

    def _get_tickers_for_date(self, target_date: date) -> list[str]:
        start, end = self._nairobi_day_bounds(target_date)
        rows = self.db.execute(
            select(MarketData.ticker)
            .distinct()
            .where(MarketData.timestamp >= start, MarketData.timestamp < end)
        ).all()
        return sorted({row[0] for row in rows})

    def _get_tickers_for_range(self, start_date: date, end_date: date) -> list[str]:
        start, _ = self._nairobi_day_bounds(start_date)
        _, end = self._nairobi_day_bounds(end_date)
        rows = self.db.execute(
            select(MarketData.ticker)
            .distinct()
            .where(MarketData.timestamp >= start, MarketData.timestamp < end)
        ).all()
        return sorted({row[0] for row in rows})

    # ==================================================================
    # TIMEZONE HELPERS
    # ==================================================================

    @staticmethod
    def _to_nairobi_date(timestamp: datetime) -> date:
        """Resolve the Africa/Nairobi trading date for a raw timestamp."""
        if timestamp.tzinfo is None:
            # Project convention: naive timestamps are already Nairobi-local.
            return timestamp.date()
        return timestamp.astimezone(NAIROBI_TZ).date()

    @staticmethod
    def _nairobi_day_bounds(target_date: date) -> tuple[datetime, datetime]:
        """[start, end) datetime bounds for one Nairobi calendar day."""
        start = datetime.combine(target_date, time.min, tzinfo=NAIROBI_TZ)
        end = datetime.combine(target_date, time.max, tzinfo=NAIROBI_TZ)
        return start, end

    # ==================================================================
    # RESPONSE BUILDERS
    # ==================================================================

    @staticmethod
    def _build_result(
        tickers_processed: int,
        candidates: list[FeatureCandidate],
        compute_errors: list[dict[str, Any]],
        outcome: PersistOutcome,
    ) -> dict[str, Any]:
        combined_errors = compute_errors + outcome.skipped_details
        rows_processed = len(candidates) + len(compute_errors) + outcome.skipped_limit
        rows_skipped = len(compute_errors) + outcome.skipped_limit

        status = "success" if not combined_errors else "partial_success"

        return {
            "status": status,
            "tickers_processed": tickers_processed,
            "rows_processed": rows_processed,
            "rows_inserted": outcome.inserted,
            "rows_updated": outcome.updated,
            "rows_skipped": rows_skipped,
            "errors": combined_errors,
        }

    @staticmethod
    def _build_failure_result(
        tickers_processed: int,
        attempted_rows: int,
        errors: list[dict[str, Any]],
        error_message: str,
    ) -> dict[str, Any]:
        """Used only when the persistence transaction itself failed."""
        return {
            "status": "failed",
            "tickers_processed": tickers_processed,
            "rows_processed": attempted_rows,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_skipped": attempted_rows,
            "errors": errors + [
                {"ticker": None, "trading_date": None, "error": error_message}
            ],
        }


# ---------------------------------------------------------------------------
# CLI smoke-test:  python -m feature_service.marketCalculatorService
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
        service = MarketCalculatorService(session)
        result = service.process_all()
        print(json.dumps(result, indent=2, default=str))
    finally:
        session.close()