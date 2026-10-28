"""
services/signal_service/SignalService.py

Service Layer — turns daily_features rows into one combined signal per
(ticker, trading_date) and upserts it into analysis_results.

Pipeline position:

    daily_features (market + news features, already computed)
        │
        ▼
    SignalService                    <- this file
        │
        ├── per ticker: load its full daily_features series (ascending)
        ├── per targeted day: intelligence.signal_engine.evaluate()
        │       previous 20 sessions' volume = strictly earlier rows only
        │
        ▼
    analysis_results  (upsert on (ticker, trading_date), single commit per run)

Rules:
  - Read-only against daily_features; never creates or edits feature rows.
  - Generic over tickers: no company-specific code. The NSE registry
    (config/nse_companies.py) is used only to REPORT coverage, never to drop
    a ticker that has feature rows.
  - Days without 20 prior sessions are skipped (cold start), not stored as
    "neutral". They are counted in rows_skipped without an error entry.
  - Idempotent: re-running recomputes and overwrites the same rows.
  - Session injected; this class never opens/closes one.

Run order: market features -> news features -> signals.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional, Union
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from config.nse_companies import NSE_COMPANIES
from config.signal_config import VOLUME_LOOKBACK
from intelligence.signal_engine import SignalResult, evaluate
from models.analytics_data import AnalysisResult
from models.daily_market_features import DailyMarketFeatures

logger = logging.getLogger(__name__)

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

# None -> every day for the ticker; set[date] -> those days; (start, end) -> inclusive range.
TargetSpec = Optional[Union[set, tuple]]


@dataclass
class _Candidate:
    ticker: str
    trading_date: date
    result: SignalResult


class SignalService:
    """
    Usage:
        service = SignalService(db)
        service.process_all()
        service.process_ticker("KCB")
        service.process_date(date(2026, 10, 20))
        service.process_date_range(date(2026, 10, 1), date(2026, 10, 20))
        service.get_signals(ticker="KCB", limit=10)
    """

    def __init__(self, db: Session) -> None:
        self.db = db

    # ==================================================================
    # PUBLIC — write
    # ==================================================================

    def process_all(self) -> dict[str, Any]:
        tickers = self._tickers_with_features()
        logger.info("process_all: %d ticker(s) with feature rows", len(tickers))
        result = self._run({ticker: None for ticker in tickers})
        result["registry_coverage"] = self._registry_coverage(set(tickers))
        return result

    def process_ticker(self, ticker: str) -> dict[str, Any]:
        return self._run({(ticker or "").strip().upper(): None})

    def process_date(self, target_date: date) -> dict[str, Any]:
        rows = self.db.execute(
            select(DailyMarketFeatures.ticker)
            .distinct()
            .where(DailyMarketFeatures.trading_date == target_date)
        ).scalars().all()
        return self._run({ticker: {target_date} for ticker in sorted(set(rows))})

    def process_date_range(self, start_date: date, end_date: date) -> dict[str, Any]:
        if start_date > end_date:
            return self._failure(
                0, 0, f"start_date ({start_date}) must not be after end_date ({end_date})."
            )
        rows = self.db.execute(
            select(DailyMarketFeatures.ticker)
            .distinct()
            .where(
                DailyMarketFeatures.trading_date >= start_date,
                DailyMarketFeatures.trading_date <= end_date,
            )
        ).scalars().all()
        return self._run({ticker: (start_date, end_date) for ticker in sorted(set(rows))})

    # ==================================================================
    # PUBLIC — read
    # ==================================================================

    def get_signals(
        self,
        ticker: Optional[str] = None,
        signal: Optional[str] = None,
        trading_date: Optional[date] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[AnalysisResult]:
        """Newest first. All filters optional and combined with AND."""
        stmt = select(AnalysisResult)
        if ticker and ticker.strip():
            stmt = stmt.where(AnalysisResult.ticker == ticker.strip().upper())
        if signal and signal.strip():
            stmt = stmt.where(AnalysisResult.signal == signal.strip().lower())
        if trading_date is not None:
            stmt = stmt.where(AnalysisResult.trading_date == trading_date)

        stmt = (
            stmt.order_by(AnalysisResult.trading_date.desc(), AnalysisResult.ticker.asc())
            .offset(offset)
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())

    # ==================================================================
    # ORCHESTRATION
    # ==================================================================

    def _run(self, ticker_specs: dict[str, TargetSpec]) -> dict[str, Any]:
        candidates: list[_Candidate] = []
        errors: list[dict[str, Any]] = []
        skipped_history = 0

        for ticker, spec in ticker_specs.items():
            ticker_candidates, ticker_errors, ticker_skipped = self._evaluate_ticker(ticker, spec)
            candidates.extend(ticker_candidates)
            errors.extend(ticker_errors)
            skipped_history += ticker_skipped

        try:
            inserted, updated = self._persist(candidates)
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.error("Failed to persist signals: %s", exc)
            return self._failure(
                len(ticker_specs),
                len(candidates) + skipped_history + len(errors),
                str(exc),
                errors,
            )

        rows_skipped = skipped_history + len(errors)
        logger.info(
            "Signals done: inserted=%d updated=%d skipped=%d (cold start=%d, errors=%d)",
            inserted, updated, rows_skipped, skipped_history, len(errors),
        )

        return {
            "status": "success" if not errors else "partial_success",
            "tickers_processed": len(ticker_specs),
            "rows_processed": inserted + updated + rows_skipped,
            "rows_inserted": inserted,
            "rows_updated": updated,
            "rows_skipped": rows_skipped,
            "errors": errors,
        }

    def _evaluate_ticker(
        self, ticker: str, spec: TargetSpec
    ) -> tuple[list[_Candidate], list[dict[str, Any]], int]:
        rows = self.db.execute(
            select(
                DailyMarketFeatures.trading_date,
                DailyMarketFeatures.daily_return,
                DailyMarketFeatures.volume,
                DailyMarketFeatures.weighted_sentiment,
                DailyMarketFeatures.average_sentiment,
                DailyMarketFeatures.relevant_headline_count,
            )
            .where(DailyMarketFeatures.ticker == ticker)
            .order_by(DailyMarketFeatures.trading_date.asc())
        ).all()

        candidates: list[_Candidate] = []
        errors: list[dict[str, Any]] = []
        skipped = 0

        if not rows:
            return candidates, errors, skipped

        volumes = [row.volume for row in rows]

        for idx, row in enumerate(rows):
            if not self._is_targeted(row.trading_date, spec):
                continue

            try:
                result = evaluate(
                    daily_return=row.daily_return,
                    volume=row.volume,
                    # strictly earlier sessions only -> no lookahead
                    prior_volumes=volumes[max(0, idx - VOLUME_LOOKBACK):idx],
                    weighted_sentiment=row.weighted_sentiment,
                    average_sentiment=row.average_sentiment,
                    headline_count=row.relevant_headline_count,
                )
            except Exception as exc:  # noqa: BLE001 — isolate per-row failures
                logger.error("Signal failed for %s %s: %s", ticker, row.trading_date, exc)
                errors.append(self._error(ticker, row.trading_date, f"Calculation error: {exc}"))
                continue

            if result is None:
                skipped += 1
                continue

            candidates.append(_Candidate(ticker, row.trading_date, result))

        return candidates, errors, skipped

    @staticmethod
    def _is_targeted(trading_date: date, spec: TargetSpec) -> bool:
        if spec is None:
            return True
        if isinstance(spec, tuple):
            start_date, end_date = spec
            return start_date <= trading_date <= end_date
        return trading_date in spec

    # ==================================================================
    # PERSISTENCE (single transaction, upsert)
    # ==================================================================

    def _persist(self, candidates: list[_Candidate]) -> tuple[int, int]:
        if not candidates:
            return 0, 0

        existing = self._fetch_existing(candidates)
        now = datetime.now(NAIROBI_TZ)
        inserted = updated = 0

        for c in candidates:
            fields = {
                "signal": c.result.signal,
                "strength": c.result.strength,
                "confidence_score": c.result.confidence_score,
                "explanation": c.result.explanation,
                "components": c.result.components,
                "timestamp": now,
            }
            row = existing.get((c.ticker, c.trading_date))

            if row is None:
                self.db.add(AnalysisResult(ticker=c.ticker, trading_date=c.trading_date, **fields))
                inserted += 1
            else:
                for column, value in fields.items():
                    setattr(row, column, value)
                updated += 1

        self.db.commit()
        return inserted, updated

    def _fetch_existing(
        self, candidates: list[_Candidate]
    ) -> dict[tuple[str, date], AnalysisResult]:
        wanted = {(c.ticker, c.trading_date) for c in candidates}
        rows = self.db.execute(
            select(AnalysisResult).where(
                AnalysisResult.ticker.in_({t for t, _ in wanted}),
                AnalysisResult.trading_date.in_({d for _, d in wanted}),
            )
        ).scalars().all()
        return {(r.ticker, r.trading_date): r for r in rows if (r.ticker, r.trading_date) in wanted}

    # ==================================================================
    # DISCOVERY / COVERAGE
    # ==================================================================

    def _tickers_with_features(self) -> list[str]:
        rows = self.db.execute(select(DailyMarketFeatures.ticker).distinct()).scalars().all()
        return sorted(set(rows))

    @staticmethod
    def _registry_coverage(tickers: set[str]) -> dict[str, Any]:
        """Which NSE registry companies have / lack feature rows (reporting only)."""
        registry = set(NSE_COMPANIES)
        return {
            "registry_total": len(registry),
            "with_signals": len(registry & tickers),
            "registry_without_features": sorted(registry - tickers),
            "features_not_in_registry": sorted(tickers - registry),
        }

    # ==================================================================
    # RESPONSE BUILDERS
    # ==================================================================

    @staticmethod
    def _error(ticker: str, trading_date: date, message: str) -> dict[str, Any]:
        return {"ticker": ticker, "trading_date": trading_date.isoformat(), "error": message}

    @staticmethod
    def _failure(
        tickers_processed: int,
        attempted_rows: int,
        message: str,
        errors: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        return {
            "status": "failed",
            "tickers_processed": tickers_processed,
            "rows_processed": attempted_rows,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_skipped": attempted_rows,
            "errors": (errors or []) + [{"ticker": None, "trading_date": None, "error": message}],
        }


# ---------------------------------------------------------------------------
# CLI smoke-test:  python -m services.signal_service.SignalService
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
        print(json.dumps(SignalService(session).process_all(), indent=2, default=str))
    finally:
        session.close()