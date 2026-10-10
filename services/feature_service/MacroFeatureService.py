"""
services/feature_service/macro_features_service.py

Service Layer — fills the macro columns of `daily_features` from `macro_data`.

Pipeline position:

    macro_data (monthly, market-level)
        │
        ▼
    MacroFeaturesService             <- this file
        │
        ├── resolve the latest usable value per indicator
        ├── pick target daily_features rows (latest date, or backfill)
        ├── fill NULL macro columns only
        │
        ▼
    daily_features  (UPDATE only, single commit per run)

Rules (decided with the project owner):
  - Macro data is market-level: the same values apply to every ticker.
  - "Latest" is defined by the DATA month/year of a macro_data row, NOT by
    report_date (report_date is shared by every row from one KNBS PDF, is
    only set when NULL, and its model default is evaluated at import time).
  - Per indicator: use the newest macro row where that indicator is
    non-null. If the newest row has a NULL for an indicator, fall back to
    the most recent earlier row that has a value.
  - Only NULL macro columns on daily_features are filled. Existing values
    are never overwritten, and None is never written.
  - This service NEVER creates daily_features rows. Row ownership belongs
    to MarketCalculatorService (see PRD, "Row ownership").
  - Default run fills each ticker's latest trading_date row. With
    backfill=True, every row that still has a NULL macro column is filled.

Known limitations (recorded, not silently fixed):
  - oil_price is populated from macro_data.fuel_price (KNBS diesel,
    KSh/litre). It is a PROXY, not crude/Brent. Replace when a real oil
    source exists.
  - backfill=True writes TODAY's macro values onto historical rows. That
    leaks the future into any backtest/ML training use of those rows. The
    default mode, run daily, is effectively point-in-time.
  - Macro data is monthly and published with a lag, so values repeat
    across many trading days.
  - No staleness limit: a fallback value may come from an old month. The
    month used for each indicator is returned in `macro_sources`.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from models.daily_market_features import DailyMarketFeatures
from models.macro_data import MacroData

logger = logging.getLogger(__name__)

MONTH_ORDER: list[str] = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

# macro_data column -> daily_features column.
# NOTE: fuel_price -> oil_price is a diesel PROXY (see module docstring).
COLUMN_MAP: dict[str, str] = {
    "inflation": "inflation",
    "cbk_rate": "interest_rate",
    "usd_kes_rate": "usd_kes",
    "fuel_price": "oil_price",
}


class MacroFeaturesService:
    """
    Fills macro columns of DailyMarketFeatures from MacroData.

    Usage:
        service = MacroFeaturesService(db)
        result  = service.fill()                 # latest date per ticker
        result  = service.fill(backfill=True)    # every row with NULL macro
    """

    def __init__(self, db: Session) -> None:
        """
        Args:
            db: Active SQLAlchemy session (injected; never created here).
        """
        self.db = db

    # ==================================================================
    # PUBLIC API
    # ==================================================================

    def fill(self, backfill: bool = False) -> dict[str, Any]:
        """
        Fill NULL macro columns on daily_features rows.

        Args:
            backfill: False (default) -> only each ticker's latest
                      trading_date row. True -> every row that has at least
                      one NULL macro column.

        Returns:
            {
                "status": "success" | "partial_success" | "failed",
                "message": str,
                "backfill": bool,
                "macro_sources": {daily_col: {"value": float, "period": str}},
                "rows_examined": int,
                "rows_updated": int,
                "rows_unchanged": int,
                "columns_filled": {daily_col: int},
                "errors": list[str],
            }
        """
        errors: list[str] = []

        try:
            macro_values = self._resolve_macro_values()
        except SQLAlchemyError as exc:
            logger.error("Failed reading macro_data: %s", exc)
            return self._failure(backfill, f"Database error reading macro_data: {exc}")

        if macro_values is None:
            return self._failure(backfill, "macro_data is empty — nothing to fill from.")

        for daily_col in COLUMN_MAP.values():
            if daily_col not in macro_values:
                errors.append(f"No non-null macro value found for '{daily_col}'.")

        try:
            targets = self._load_targets(backfill)
        except SQLAlchemyError as exc:
            logger.error("Failed reading daily_features: %s", exc)
            return self._failure(backfill, f"Database error reading daily_features: {exc}")

        rows_updated = 0
        columns_filled = {daily_col: 0 for daily_col in COLUMN_MAP.values()}

        try:
            for row in targets:
                changed = False
                for daily_col, source in macro_values.items():
                    if getattr(row, daily_col) is None:
                        setattr(row, daily_col, source["value"])
                        columns_filled[daily_col] += 1
                        changed = True
                if changed:
                    rows_updated += 1

            self.db.commit()

        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.error("Failed persisting macro features: %s", exc)
            return self._failure(backfill, f"Database error persisting macro features: {exc}")

        rows_examined = len(targets)
        status = "success" if not errors else "partial_success"

        logger.info(
            "Macro features filled: backfill=%s examined=%d updated=%d filled=%s",
            backfill, rows_examined, rows_updated, columns_filled,
        )

        return {
            "status": status,
            "message": f"Macro features processed for {rows_examined} row(s).",
            "backfill": backfill,
            "macro_sources": macro_values,
            "rows_examined": rows_examined,
            "rows_updated": rows_updated,
            "rows_unchanged": rows_examined - rows_updated,
            "columns_filled": columns_filled,
            "errors": errors,
        }

    # ==================================================================
    # MACRO RESOLUTION
    # ==================================================================

    def _resolve_macro_values(self) -> Optional[dict[str, dict[str, Any]]]:
        """
        Resolve the latest usable value for each indicator.

        Rows are ordered newest-first by (year, month) of the DATA they
        describe. For each indicator, the first row with a non-null value
        wins, so a NULL in the newest row falls back to an earlier row.

        Returns:
            None if macro_data has no usable rows. Otherwise
            {daily_col: {"value": float, "period": "March 2026"}} for every
            indicator that has at least one non-null value.
        """
        rows = self.db.execute(select(MacroData)).scalars().all()

        keyed: list[tuple[tuple[int, int], MacroData]] = []
        for row in rows:
            key = self._period_key(row.month, row.year)
            if key is None:
                logger.debug("Skipping macro row id=%s: unusable month/year.", row.id)
                continue
            keyed.append((key, row))

        if not keyed:
            return None

        keyed.sort(key=lambda item: item[0], reverse=True)

        resolved: dict[str, dict[str, Any]] = {}
        for macro_col, daily_col in COLUMN_MAP.items():
            for _key, row in keyed:
                value = getattr(row, macro_col)
                if value is not None:
                    resolved[daily_col] = {
                        "value": float(value),
                        "period": f"{row.month} {row.year}",
                    }
                    break

        return resolved

    @staticmethod
    def _period_key(month: Optional[str], year: Optional[str]) -> Optional[tuple[int, int]]:
        """(year, month_index) sort key, or None if month/year are unusable."""
        if not month or not year:
            return None
        normalized = str(month).strip().capitalize()
        if normalized not in MONTH_ORDER:
            return None
        try:
            return int(str(year).strip()), MONTH_ORDER.index(normalized)
        except ValueError:
            return None

    # ==================================================================
    # TARGET SELECTION
    # ==================================================================

    def _load_targets(self, backfill: bool) -> list[DailyMarketFeatures]:
        """
        Load the daily_features rows to fill, in a single query.

        backfill=False: each ticker's latest trading_date row.
        backfill=True:  every row with at least one NULL macro column.
        """
        if backfill:
            stmt = select(DailyMarketFeatures).where(
                or_(*[
                    getattr(DailyMarketFeatures, daily_col).is_(None)
                    for daily_col in COLUMN_MAP.values()
                ])
            )
        else:
            latest = (
                select(
                    DailyMarketFeatures.ticker.label("ticker"),
                    func.max(DailyMarketFeatures.trading_date).label("max_date"),
                )
                .group_by(DailyMarketFeatures.ticker)
                .subquery()
            )
            stmt = select(DailyMarketFeatures).join(
                latest,
                and_(
                    DailyMarketFeatures.ticker == latest.c.ticker,
                    DailyMarketFeatures.trading_date == latest.c.max_date,
                ),
            )

        return list(self.db.execute(stmt).scalars().all())

    # ==================================================================
    # RESPONSE BUILDERS
    # ==================================================================

    @staticmethod
    def _failure(backfill: bool, message: str) -> dict[str, Any]:
        return {
            "status": "failed",
            "message": message,
            "backfill": backfill,
            "macro_sources": {},
            "rows_examined": 0,
            "rows_updated": 0,
            "rows_unchanged": 0,
            "columns_filled": {daily_col: 0 for daily_col in COLUMN_MAP.values()},
            "errors": [message],
        }


# ---------------------------------------------------------------------------
# CLI smoke-test:  python -m services.feature_service.macro_features_service
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
        print(json.dumps(MacroFeaturesService(session).fill(), indent=2, default=str))
    finally:
        session.close()