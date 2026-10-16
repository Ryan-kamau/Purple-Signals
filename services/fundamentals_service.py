"""
services/fundamentals_service.py

Service Layer — owns ALL business logic for fundamentals CSV ingestion and
for reading stored fundamentals.

Pipeline position:

    POST /fundamentals/ingest (upload -> temp file)
        │
        ▼
    FundamentalsService.ingest_csv(path)        <- this file
        │
        ├── scrapers.fundamentals_csv   (parse only)
        ├── config.nse_companies        (ticker registry + units multiplier)
        ├── market_data                 (closing price for P/E and yield)
        │
        ▼
    parse -> validate -> normalise -> calculate -> upsert -> single commit -> envelope
        │
        ▼
    fundamentals table ONLY (never daily_features)

Rules (mirrors MacroService):
  - Natural key is (ticker, report_date).
  - New key            -> insert.
  - Existing key       -> fill ONLY columns that are currently NULL.
  - Bad rows are skipped and reported per row; good rows are still saved.
  - Ratios supplied in the CSV take precedence over calculated ones.
  - Session is injected; this class never opens/closes one.

No FastAPI, no HTTP concerns. Every public method returns a JSON-serialisable dict.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from config.nse_companies import get_company
from models.fundamental_data import Fundamentals
from models.market_data import MarketData
from scrapers.fundamentals_csv import (
    OPTIONAL_COLUMNS,
    REQUIRED_COLUMNS,
    CSVParseError,
    parse_fundamentals_csv,
)

logger = logging.getLogger(__name__)

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

DEFAULT_LIMIT: int = 10
DEFAULT_OFFSET: int = 0

# Required numeric CSV columns (everything required except ticker/report_date).
_REQUIRED_NUMERIC: tuple[str, ...] = tuple(
    c for c in REQUIRED_COLUMNS if c not in ("ticker", "report_date")
)

# Money columns scaled by the company's units_multiplier. eps and
# dividend_per_share are per-share values and are never scaled.
_MONEY_FIELDS: tuple[str, ...] = (
    "revenue",
    "net_profit",
    "shareholders_equity",
    "total_liabilities",
)

# Columns persisted on the fundamentals row. These are also the only columns
# an existing row may have back-filled (when currently NULL).
_PERSISTED_COLUMNS: tuple[str, ...] = (
    "eps",
    "pe_ratio",
    "dividend_yield",
    "revenue",
    "net_profit",
    "dividend_per_share",
    "debt_ratio",
    "net_profit_margin",
)

_ROUND_DP: dict[str, int] = {
    "pe_ratio": 4,
    "dividend_yield": 6,
    "debt_ratio": 4,
    "net_profit_margin": 6,
}


# ---------------------------------------------------------------------------
# Internal containers
# ---------------------------------------------------------------------------

@dataclass
class _Candidate:
    """A fully validated + calculated row, ready to persist."""

    row: int
    ticker: str
    company: str
    report_date: datetime
    values: dict[str, Optional[float]]


@dataclass
class _PersistStats:
    inserted: list[Fundamentals] = field(default_factory=list)
    updated: list[Fundamentals] = field(default_factory=list)
    unchanged: list[Fundamentals] = field(default_factory=list)

    @property
    def records(self) -> list[Fundamentals]:
        return self.inserted + self.updated + self.unchanged


class FundamentalsService:
    """
    Ingest fundamentals from CSV and query stored fundamentals.

    Usage:
        service = FundamentalsService(db)
        result  = service.ingest_csv("/tmp/fundamentals.csv")
        latest  = service.get_latest("KPLC")
        history = service.get_history(ticker="KPLC", limit=5)
    """

    def __init__(self, db: Session) -> None:
        self.db = db

    # ==================================================================
    # PUBLIC — ingestion
    # ==================================================================

    def ingest_csv(self, path: str) -> dict[str, Any]:
        """
        Run the full CSV ingestion pipeline.

        Returns:
            Ingestion envelope — see `_ingest_response`. status is:
              "success"         every row saved/unchanged, no row errors
              "partial_success" some rows rejected, the rest handled
              "failed"          file-level error, no valid rows, or DB failure
        """
        logger.info("Fundamentals CSV ingestion started: %s", path)

        try:
            raw_rows, parse_errors = parse_fundamentals_csv(path)
        except CSVParseError as exc:
            logger.error("CSV parse failed: %s", exc)
            return self._ingest_response("failed", str(exc))

        errors: list[dict[str, Any]] = [
            self._issue(item["row"], None, None, "error", item["error"])
            for item in parse_errors
        ]
        warnings: list[dict[str, Any]] = []

        if not raw_rows and not errors:
            return self._ingest_response("failed", "CSV contains no data rows.")

        candidates: list[_Candidate] = []
        seen_keys: set[tuple[str, date]] = set()
        price_cache: dict[tuple[str, date], Optional[float]] = {}

        try:
            for raw in raw_rows:
                row_no = raw["row"]
                values = raw["values"]

                normalised, error = self._normalise_row(values)
                if error is not None:
                    errors.append(
                        self._issue(
                            row_no, values.get("ticker") or None,
                            values.get("report_date") or None, "error", error,
                        )
                    )
                    continue

                key = (normalised["ticker"], normalised["report_date"].date())
                if key in seen_keys:
                    errors.append(
                        self._issue(
                            row_no, normalised["ticker"], key[1].isoformat(), "error",
                            "Duplicate (ticker, report_date) earlier in this CSV.",
                        )
                    )
                    continue
                seen_keys.add(key)

                persisted_values, row_warnings = self._calculate(normalised, price_cache)
                for message in row_warnings:
                    warnings.append(
                        self._issue(row_no, normalised["ticker"], key[1].isoformat(), "warning", message)
                    )

                candidates.append(
                    _Candidate(
                        row=row_no,
                        ticker=normalised["ticker"],
                        company=normalised["company"],
                        report_date=normalised["report_date"],
                        values=persisted_values,
                    )
                )

            if not candidates:
                return self._ingest_response(
                    "failed", "No valid rows to ingest.", errors=errors, warnings=warnings
                )

            stats = self._persist(candidates)

        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.error("Fundamentals DB transaction rolled back: %s", exc)
            return self._ingest_response(
                "failed", "Database transaction rolled back.", errors=errors, warnings=warnings
            )

        if errors:
            status = "partial_success"
            message = f"{len(errors)} row(s) rejected; remaining rows processed."
        else:
            status = "success"
            message = "Fundamentals ingested successfully."

        logger.info(
            "Fundamentals ingestion done: inserted=%d updated=%d unchanged=%d rejected=%d",
            len(stats.inserted), len(stats.updated), len(stats.unchanged), len(errors),
        )
        return self._ingest_response(status, message, stats=stats, errors=errors, warnings=warnings)

    # ==================================================================
    # PUBLIC — queries
    # ==================================================================

    def get_latest(self, ticker: str) -> dict[str, Any]:
        """
        Latest fundamentals row (by report_date) for `ticker`, enriched with
        the current market price and a live P/E (price now / stored EPS).
        """
        symbol = (ticker or "").strip().upper()
        if not symbol:
            return self._read_failure("ticker is required.")

        try:
            record = self.db.execute(
                select(Fundamentals)
                .where(Fundamentals.ticker == symbol)
                .order_by(Fundamentals.report_date.desc(), Fundamentals.id.desc())
                .limit(1)
            ).scalar_one_or_none()

            if record is None:
                return self._read_response(
                    f"No fundamentals found for {symbol}.", data=[], total=0
                )

            price = self._latest_price(symbol)
        except SQLAlchemyError as exc:
            return self._handle_db_error(exc, "get_latest")

        item = self._serialize(record)
        item["current_market_price"] = price
        item["current_pe_ratio"] = (
            round(price / record.eps, 4)
            if price is not None and record.eps is not None and record.eps > 0
            else None
        )

        return self._read_response(
            f"Latest fundamentals for {symbol} retrieved successfully.",
            data=[item], total=1,
        )

    def get_history(
        self,
        ticker: Optional[str] = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
    ) -> dict[str, Any]:
        """Paginated fundamentals history, newest report first. ticker optional."""
        if limit < 1:
            return self._read_failure(f"limit must be >= 1, got {limit}.", limit, offset)
        if offset < 0:
            return self._read_failure(f"offset must be >= 0, got {offset}.", limit, offset)

        symbol = (ticker or "").strip().upper() or None

        try:
            base = select(Fundamentals)
            count_stmt = select(func.count(Fundamentals.id))
            if symbol:
                base = base.where(Fundamentals.ticker == symbol)
                count_stmt = count_stmt.where(Fundamentals.ticker == symbol)

            total = self.db.execute(count_stmt).scalar_one()
            records = self.db.execute(
                base.order_by(Fundamentals.report_date.desc(), Fundamentals.id.desc())
                .offset(offset)
                .limit(limit)
            ).scalars().all()
        except SQLAlchemyError as exc:
            return self._handle_db_error(exc, "get_history")

        return self._read_response(
            "Fundamentals history retrieved successfully.",
            data=[self._serialize(r) for r in records],
            total=total, limit=limit, offset=offset,
        )

    # ==================================================================
    # PRIVATE — validation / normalisation
    # ==================================================================

    def _normalise_row(
        self, values: dict[str, str]
    ) -> tuple[Optional[dict[str, Any]], Optional[str]]:
        """
        Validate one raw row and convert it to typed values.

        Returns (normalised, None) or (None, error_message).
        Money columns are scaled by the company's units_multiplier here.
        """
        ticker = (values.get("ticker") or "").strip().upper()
        if not ticker:
            return None, "ticker is required."

        company_cfg = get_company(ticker)
        if company_cfg is None:
            return None, f"Ticker '{ticker}' is not in the company registry (config/nse_companies.py)."

        date_text = (values.get("report_date") or "").strip()
        if not date_text:
            return None, "report_date is required."
        try:
            report_date = datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=NAIROBI_TZ)
        except ValueError:
            return None, f"report_date '{date_text}' is not in YYYY-MM-DD format."

        numbers: dict[str, Optional[float]] = {}

        for column in _REQUIRED_NUMERIC:
            text = (values.get(column) or "").strip()
            if not text:
                return None, f"{column} is required."
            try:
                numbers[column] = self._to_float(text)
            except ValueError:
                return None, f"{column} '{text}' is not a valid number."

        for column in OPTIONAL_COLUMNS:
            text = (values.get(column) or "").strip()
            if not text:
                numbers[column] = None
                continue
            try:
                numbers[column] = self._to_float(text)
            except ValueError:
                return None, f"{column} '{text}' is not a valid number."

        multiplier = float(company_cfg["units_multiplier"])
        for column in _MONEY_FIELDS:
            numbers[column] = numbers[column] * multiplier  # type: ignore[operator]

        return (
            {
                "ticker": ticker,
                "company": company_cfg["company"],
                "report_date": report_date,
                **numbers,
            },
            None,
        )

    @staticmethod
    def _to_float(text: str) -> float:
        """Parse '1,234.5' -> 1234.5. Raises ValueError for non-finite/invalid."""
        value = float(text.replace(",", "").strip())
        if not math.isfinite(value):
            raise ValueError("non-finite number")
        return value

    # ==================================================================
    # PRIVATE — calculation
    # ==================================================================

    def _calculate(
        self,
        n: dict[str, Any],
        price_cache: dict[tuple[str, date], Optional[float]],
    ) -> tuple[dict[str, Optional[float]], list[str]]:
        """
        Resolve the final persisted values for one row.

        Ratios supplied in the CSV win. Otherwise:
          pe_ratio          = price / eps                  (price present, eps > 0)
          dividend_yield    = dividend_per_share / price   (price > 0)
          net_profit_margin = net_profit / revenue         (revenue != 0)
          debt_ratio        = total_liabilities / shareholders_equity (equity != 0)

        Ratios are plain fractions (0.05 == 5%), matching the previous PDF pipeline.
        Anything that cannot be calculated stays None and yields a warning.
        """
        warnings: list[str] = []

        pe = n["pe_ratio"]
        dy = n["dividend_yield"]
        npm = n["net_profit_margin"]
        debt = n["debt_ratio"]

        needs_price = [name for name, val in (("pe_ratio", pe), ("dividend_yield", dy)) if val is None]
        price: Optional[float] = None
        if needs_price:
            price = self._closing_price(n["ticker"], n["report_date"], price_cache)
            if price is None:
                warnings.append(
                    "No market_data price on or before report_date; "
                    f"{', '.join(needs_price)} left NULL."
                )

        if pe is None and price is not None:
            if n["eps"] > 0:
                pe = price / n["eps"]
            else:
                warnings.append("eps <= 0; pe_ratio left NULL.")

        if dy is None and price is not None:
            if price > 0:
                dy = n["dividend_per_share"] / price
            else:
                warnings.append("price <= 0; dividend_yield left NULL.")

        if npm is None:
            if n["revenue"] != 0:
                npm = n["net_profit"] / n["revenue"]
            else:
                warnings.append("revenue is 0; net_profit_margin left NULL.")

        if debt is None:
            if n["shareholders_equity"] != 0:
                debt = n["total_liabilities"] / n["shareholders_equity"]
            else:
                warnings.append("shareholders_equity is 0; debt_ratio left NULL.")

        resolved = {
            "eps": n["eps"],
            "pe_ratio": pe,
            "dividend_yield": dy,
            "revenue": n["revenue"],
            "net_profit": n["net_profit"],
            "dividend_per_share": n["dividend_per_share"],
            "debt_ratio": debt,
            "net_profit_margin": npm,
        }
        for column, places in _ROUND_DP.items():
            if resolved[column] is not None:
                resolved[column] = round(resolved[column], places)

        return resolved, warnings

    def _closing_price(
        self,
        ticker: str,
        report_date: datetime,
        cache: dict[tuple[str, date], Optional[float]],
    ) -> Optional[float]:
        """
        Closing price on or just before report_date: the latest market_data row
        with timestamp earlier than midnight following report_date (Nairobi).
        Cached per (ticker, date) for the duration of one run.
        """
        key = (ticker, report_date.date())
        if key in cache:
            return cache[key]

        cutoff = report_date + timedelta(days=1)
        price = self.db.execute(
            select(MarketData.price)
            .where(MarketData.ticker == ticker, MarketData.timestamp < cutoff)
            .order_by(MarketData.timestamp.desc())
            .limit(1)
        ).scalar_one_or_none()

        cache[key] = float(price) if price is not None else None
        return cache[key]

    def _latest_price(self, ticker: str) -> Optional[float]:
        price = self.db.execute(
            select(MarketData.price)
            .where(MarketData.ticker == ticker)
            .order_by(MarketData.timestamp.desc())
            .limit(1)
        ).scalar_one_or_none()
        return float(price) if price is not None else None

    # ==================================================================
    # PRIVATE — persistence (upsert, single commit)
    # ==================================================================

    def _persist(self, candidates: list[_Candidate]) -> _PersistStats:
        """
        Upsert candidates in ONE transaction.

        Existing rows are batch-fetched once. An existing row only has
        currently-NULL columns filled; non-NULL values are never overwritten.
        """
        stats = _PersistStats()
        existing = self._fetch_existing(candidates)

        for c in candidates:
            row = existing.get((c.ticker, c.report_date.date()))

            if row is None:
                new_row = Fundamentals(
                    ticker=c.ticker,
                    company=c.company,
                    report_date=c.report_date,
                    **c.values,
                )
                self.db.add(new_row)
                stats.inserted.append(new_row)
                continue

            changed = False
            for column in _PERSISTED_COLUMNS:
                new_value = c.values.get(column)
                if getattr(row, column) is None and new_value is not None:
                    setattr(row, column, new_value)
                    changed = True

            (stats.updated if changed else stats.unchanged).append(row)

        self.db.flush()
        self.db.commit()
        for record in stats.records:
            self.db.refresh(record)

        return stats

    def _fetch_existing(
        self, candidates: list[_Candidate]
    ) -> dict[tuple[str, date], Fundamentals]:
        """
        One query for every existing row that could match: ticker-set x date
        window, then filtered to exact (ticker, date) keys in memory. Matching
        on the DATE (not the raw datetime) is robust to timezone/naive storage.
        """
        tickers = {c.ticker for c in candidates}
        start = min(c.report_date for c in candidates)
        end = max(c.report_date for c in candidates) + timedelta(days=1)
        wanted = {(c.ticker, c.report_date.date()) for c in candidates}

        rows = self.db.execute(
            select(Fundamentals).where(
                Fundamentals.ticker.in_(tickers),
                Fundamentals.report_date >= start,
                Fundamentals.report_date < end,
            )
        ).scalars().all()

        lookup: dict[tuple[str, date], Fundamentals] = {}
        for row in rows:
            key = (row.ticker, self._as_date(row.report_date))
            if key in wanted:
                lookup[key] = row
        return lookup

    @staticmethod
    def _as_date(value: Any) -> date:
        return value.date() if isinstance(value, datetime) else value

    # ==================================================================
    # PRIVATE — serialisation / response builders
    # ==================================================================

    @staticmethod
    def _serialize(record: Fundamentals) -> dict[str, Any]:
        return {
            "id": record.id,
            "ticker": record.ticker,
            "company": record.company,
            "report_date": record.report_date.isoformat() if record.report_date else None,
            "eps": record.eps,
            "pe_ratio": record.pe_ratio,
            "dividend_yield": record.dividend_yield,
            "revenue": record.revenue,
            "net_profit": record.net_profit,
            "dividend_per_share": record.dividend_per_share,
            "debt_ratio": record.debt_ratio,
            "net_profit_margin": record.net_profit_margin,
            "timestamp": record.timestamp.isoformat() if record.timestamp else None,
        }

    @staticmethod
    def _issue(
        row: Optional[int],
        ticker: Optional[str],
        report_date: Optional[str],
        kind: str,
        text: str,
    ) -> dict[str, Any]:
        """kind is 'error' or 'warning' — it names the text key."""
        return {"row": row, "ticker": ticker, "report_date": report_date, kind: text}

    def _ingest_response(
        self,
        status: str,
        message: str,
        stats: Optional[_PersistStats] = None,
        errors: Optional[list[dict[str, Any]]] = None,
        warnings: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        errors = errors or []
        inserted = len(stats.inserted) if stats else 0
        updated = len(stats.updated) if stats else 0
        unchanged = len(stats.unchanged) if stats else 0

        return {
            "status": status,
            "message": message,
            "rows_saved": inserted + updated,
            "rows_inserted": inserted,
            "rows_updated": updated,
            "rows_unchanged": unchanged,
            "rows_skipped": len(errors),
            "errors": errors,
            "warnings": warnings or [],
            "data": [self._serialize(r) for r in stats.records] if stats else [],
        }

    @staticmethod
    def _read_response(
        message: str,
        data: list[dict[str, Any]],
        total: Optional[int] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> dict[str, Any]:
        return {
            "status": "success",
            "message": message,
            "count": len(data),
            "total": total if total is not None else len(data),
            "limit": limit if limit is not None else len(data),
            "offset": offset if offset is not None else 0,
            "data": data,
        }

    @staticmethod
    def _read_failure(
        message: str, limit: int = DEFAULT_LIMIT, offset: int = DEFAULT_OFFSET
    ) -> dict[str, Any]:
        return {
            "status": "failed",
            "message": message,
            "count": 0,
            "total": 0,
            "limit": limit,
            "offset": offset,
            "data": [],
        }

    def _handle_db_error(self, exc: SQLAlchemyError, context: str) -> dict[str, Any]:
        logger.error("%s: database error: %s", context, exc)
        return self._read_failure("A database error occurred while processing the query.")