"""
services/macro_query_service.py

Service Layer — owns ALL read/query business logic for macroeconomic data.

Pipeline position:

    FastAPI Route
        │
        ▼
    MacroQueryService             <- this file
        │
        ▼
    SQLAlchemy Session  ->  MacroData table  ->  MySQL

Responsibilities:
  - Query, filter, sort, paginate, and aggregate MacroData rows
  - Serialize ORM objects into JSON-serialisable dictionaries
  - Compute read-time statistics and comparisons (min/max/avg, period deltas)
  - Return structured, consistent response envelopes to callers
  - Never touch the ingestion pipeline (see services/macro_service.py)

This module is intentionally the mirror image of MacroService: where
MacroService owns "extract -> normalize -> upsert", this service owns
"query -> filter -> sort -> paginate -> serialize". Both share the same
registry-driven philosophy so a new macro indicator (e.g. money supply)
requires exactly one change in each file's registry — no orchestration
code changes anywhere else.

This file is NOT:
  - A FastAPI router (no HTTP concerns, no HTTPException, no request bodies)
  - A repository (business logic — filtering, stats, comparisons — lives here)
  - A writer (read-only; never inserts, updates, or deletes)

Every public method returns a JSON-serialisable dict. No SQLAlchemy model
instances ever cross the public API boundary.
"""

import logging
from datetime import datetime
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import Column
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Query, Session

from models.macro_data import MacroData

logger = logging.getLogger(__name__)

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

# Canonical month ordering — mirrors MacroService.MONTH_ORDER. Used to walk
# to adjacent months (e.g. get_last_n_months) without depending on
# report_date, since report_date may be absent on hand-seeded rows.
MONTH_ORDER: list[str] = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

# Default pagination — every collection-returning public method uses these
# unless the caller overrides them.
DEFAULT_LIMIT: int = 10
DEFAULT_OFFSET: int = 0


class MacroQueryService:
    """
    Read-only query service for stored MacroData records.

    Encapsulation notes (mirrors MacroService):
      - `_db` is injected, never created internally — keeps this class
        trivially testable (pass a fake/in-memory Session).
      - `_registry` (INDICATOR_MAP) is built once per instance and is the
        single source of truth for "which indicator maps to which
        MacroData column(s)". Adding a new indicator means adding one
        registry entry — no changes to the query engine or the thin
        wrapper methods that use it.
      - The private query engine (_build_query -> _apply_filters ->
        _apply_sorting -> _apply_pagination -> _execute_query ->
        _serialize -> response builder) is the only place that touches
        SQLAlchemy. Every public method configures the engine; none of
        them write raw queries of their own.

    Example:
        service = MacroQueryService(db)
        result  = service.get_latest()
        history = service.get_inflation_history(limit=6)
    """

    def __init__(self, db: Session) -> None:
        """
        Args:
            db: Active SQLAlchemy session (injected via FastAPI Depends).
                This service never opens or closes its own session.
        """
        self._db = db
        self._registry = self._build_registry()

    # ------------------------------------------------------------------
    # Registry — the only place that knows "indicator -> MacroData column(s)"
    # ------------------------------------------------------------------

    def _build_registry(self) -> dict[str, dict[str, Any]]:
        """
        Build the indicator registry (INDICATOR_MAP).

        Each entry maps a short indicator key to the list of MacroData
        column names that belong to it. This is the single source of
        truth consumed by every indicator-scoped method
        (_get_indicator_history, _build_statistics, _validate_indicator).

        Extending the pipeline (e.g. adding a "reserves" indicator) means
        adding one more entry here — no other method needs to change.

        Returns:
            Dict of indicator key -> {"columns": [column_name, ...]}.
        """
        return {
            "inflation": {"columns": ["inflation"]},
            "fuel": {"columns": ["fuel_price"]},
            "cbr": {"columns": ["cbk_rate"]},
            "exchange": {
                "columns": ["usd_kes_rate", "euro_kes_rate", "pounds_kes_rate"],
            },
        }

    # ==================================================================
    # SNAPSHOT QUERIES — return one logical macro record
    # ==================================================================

    def get_latest(self) -> dict[str, Any]:
        """
        Return the single most recent MacroData record.

        "Most recent" is resolved chronologically via report_date, falling
        back to the highest (year, month) pair when report_date is absent.

        Returns:
            Success envelope with `data` containing zero or one serialized
            record.
        """
        record = self._get_latest_record()

        if record is None:
            return self._success_response(
                message="No macro data available.",
                data=[],
                total=0,
            )

        return self._success_response(
            message="Latest macro record retrieved successfully.",
            data=[self._serialize(record)],
            total=1,
        )

    def get_macro_snapshot(self) -> dict[str, Any]:
        """
        Alias for get_latest(), phrased for dashboard/snapshot callers.

        Kept as a distinct public method (rather than reusing get_latest()
        under a different name at the router layer) so the API surface
        reads naturally for a "current macro snapshot" widget.

        Returns:
            Same envelope shape as get_latest().
        """
        return self.get_latest()

    def get_by_month(self, month: str, year: str) -> dict[str, Any]:
        """
        Return the MacroData record for a specific (month, year).

        Args:
            month: Month name, e.g. "March". Matched case-insensitively.
            year:  Year as a string, e.g. "2026".

        Returns:
            Success envelope with `data` containing zero or one serialized
            record. Returns a failure envelope only on a database error —
            an unmatched month/year is a valid empty result, not a failure.
        """
        normalized_month = self._normalize_month(month)

        if normalized_month not in MONTH_ORDER:
            return self._failure_response(
                message=f"'{month}' is not a recognised month name."
            )

        try:
            record = (
                self._db.query(MacroData)
                .filter(
                    MacroData.month == normalized_month,
                    MacroData.year == str(year).strip(),
                )
                .first()
            )
        except SQLAlchemyError as exc:
            return self._handle_db_error(exc, context="get_by_month")

        if record is None:
            return self._success_response(
                message=f"No macro record found for {normalized_month} {year}.",
                data=[],
                total=0,
            )

        return self._success_response(
            message=f"Macro record for {normalized_month} {year} retrieved successfully.",
            data=[self._serialize(record)],
            total=1,
        )

    def get_by_report_date(self, report_date: datetime) -> dict[str, Any]:
        """
        Return the MacroData record matching an exact report_date.

        Args:
            report_date: Timezone-aware (or naive) datetime to match.
                         Naive datetimes are assumed to be Africa/Nairobi.

        Returns:
            Success envelope with `data` containing zero or one serialized
            record.
        """
        resolved_date = self._ensure_timezone(report_date)

        try:
            record = (
                self._db.query(MacroData)
                .filter(MacroData.report_date == resolved_date)
                .first()
            )
        except SQLAlchemyError as exc:
            return self._handle_db_error(exc, context="get_by_report_date")

        if record is None:
            return self._success_response(
                message=f"No macro record found for report_date={resolved_date}.",
                data=[],
                total=0,
            )

        return self._success_response(
            message="Macro record retrieved successfully.",
            data=[self._serialize(record)],
            total=1,
        )

    # ==================================================================
    # HISTORICAL QUERIES — return collections
    # ==================================================================

    def get_history(
        self, limit: int = DEFAULT_LIMIT, offset: int = DEFAULT_OFFSET
    ) -> dict[str, Any]:
        """
        Return a paginated, chronologically sorted history of all records.

        Args:
            limit:  Maximum records to return. Defaults to 10.
            offset: Number of records to skip. Defaults to 0.

        Returns:
            Success envelope with paginated `data`, or a failure envelope
            on invalid pagination / database error.
        """
        return self._run_collection_query(
            message="Macro history retrieved successfully.",
            filters=[],
            limit=limit,
            offset=offset,
        )

    def get_last_n_months(self, n: int) -> dict[str, Any]:
        """
        Return the most recent `n` months of macro data, oldest first.

        Args:
            n: Number of trailing months to return. Must be >= 1.

        Returns:
            Success envelope with up to `n` records in chronological order,
            or a failure envelope if `n` is invalid.
        """
        if n < 1:
            return self._failure_response(message=f"n must be >= 1, got {n}.")

        try:
            # Pull the most recent `n` rows in descending order, then
            # reverse in Python so the response reads oldest -> newest —
            # consistent with every other historical method.
            query = self._build_query()
            query = self._apply_sorting(query, order="desc")
            recent_records = query.limit(n).all()
        except SQLAlchemyError as exc:
            return self._handle_db_error(exc, context="get_last_n_months")

        chronological = list(reversed(recent_records))

        return self._success_response(
            message=f"Last {n} month(s) of macro data retrieved successfully.",
            data=[self._serialize(r) for r in chronological],
            total=len(chronological),
            limit=n,
            offset=0,
        )

    def get_year(
        self, year: str, limit: int = DEFAULT_LIMIT, offset: int = DEFAULT_OFFSET
    ) -> dict[str, Any]:
        """
        Return all macro records for a specific year, paginated.

        Args:
            year:   Year as a string, e.g. "2026".
            limit:  Maximum records to return. Defaults to 10.
            offset: Number of records to skip. Defaults to 0.

        Returns:
            Success envelope with matching records in chronological order.
        """
        conditions = [MacroData.year == str(year).strip()]

        return self._run_collection_query(
            message=f"Macro records for {year} retrieved successfully.",
            filters=conditions,
            limit=limit,
            offset=offset,
        )

    def get_latest_year(self) -> dict[str, Any]:
        """
        Return every macro record for the most recent year present in the
        database, in chronological order.

        Returns:
            Success envelope with the latest year's records, or an empty
            result if the table has no data.
        """
        latest_record = self._get_latest_record()

        if latest_record is None or latest_record.year is None:
            return self._success_response(
                message="No macro data available.",
                data=[],
                total=0,
            )

        return self.get_year(latest_record.year, limit=len(MONTH_ORDER), offset=0)

    def get_history_between(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
    ) -> dict[str, Any]:
        """
        Return macro records with report_date within [start_date, end_date].

        Args:
            start_date: Inclusive lower bound.
            end_date:   Inclusive upper bound.
            limit:      Maximum records to return. Defaults to 10.
            offset:     Number of records to skip. Defaults to 0.

        Returns:
            Success envelope with matching records, or a failure envelope
            if the date range is invalid (start_date after end_date).
        """
        is_valid, error = self._validate_date_range(start_date, end_date)
        if not is_valid:
            return self._failure_response(message=error)

        resolved_start = self._ensure_timezone(start_date)
        resolved_end = self._ensure_timezone(end_date)

        conditions = [
            MacroData.report_date >= resolved_start,
            MacroData.report_date <= resolved_end,
        ]

        return self._run_collection_query(
            message="Macro history between the given dates retrieved successfully.",
            filters=conditions,
            limit=limit,
            offset=offset,
        )

    # ==================================================================
    # INDICATOR QUERIES — thin wrappers over the registry
    # ==================================================================

    def get_inflation_history(
        self, limit: int = DEFAULT_LIMIT, offset: int = DEFAULT_OFFSET
    ) -> dict[str, Any]:
        """Return chronological inflation history (rows with non-null inflation)."""
        return self._get_indicator_history("inflation", limit=limit, offset=offset)

    def get_fuel_history(
        self, limit: int = DEFAULT_LIMIT, offset: int = DEFAULT_OFFSET
    ) -> dict[str, Any]:
        """Return chronological fuel price history (rows with non-null fuel_price)."""
        return self._get_indicator_history("fuel", limit=limit, offset=offset)

    def get_exchange_history(
        self, limit: int = DEFAULT_LIMIT, offset: int = DEFAULT_OFFSET
    ) -> dict[str, Any]:
        """Return chronological exchange-rate history (USD/EUR/GBP all non-null)."""
        return self._get_indicator_history("exchange", limit=limit, offset=offset)

    def get_cbr_history(
        self, limit: int = DEFAULT_LIMIT, offset: int = DEFAULT_OFFSET
    ) -> dict[str, Any]:
        """Return chronological Central Bank Rate history (rows with non-null cbk_rate)."""
        return self._get_indicator_history("cbr", limit=limit, offset=offset)

    def _get_indicator_history(
        self, indicator: str, limit: int = DEFAULT_LIMIT, offset: int = DEFAULT_OFFSET
    ) -> dict[str, Any]:
        """
        Generic indicator-history engine used by every get_*_history() wrapper.

        Looks up the indicator's column(s) via the registry and filters out
        rows where ANY of those columns is NULL, so — for example — an
        inflation query never returns a row where inflation hasn't been
        ingested yet, even if other columns on that row are populated.

        Args:
            indicator: Registry key, e.g. "inflation", "fuel", "exchange", "cbr".
            limit:     Maximum records to return. Defaults to 10.
            offset:    Number of records to skip. Defaults to 0.

        Returns:
            Success envelope with matching records in chronological order,
            or a failure envelope if the indicator is not registered.
        """
        is_valid, columns, error = self._validate_indicator(indicator)
        if not is_valid:
            return self._failure_response(message=error)

        conditions = [column.isnot(None) for column in columns]

        return self._run_collection_query(
            message=f"{indicator.title()} history retrieved successfully.",
            filters=conditions,
            limit=limit,
            offset=offset,
        )

    # ==================================================================
    # STATISTICS
    # ==================================================================

    def get_summary(self) -> dict[str, Any]:
        """
        Return a high-level summary of the entire MacroData table.

        Includes total observation count, the earliest and latest records,
        and per-indicator observation counts (how many non-null rows exist
        for each registered indicator).

        Returns:
            Success envelope whose `data` is a single summary dict.
        """
        try:
            all_records = self._build_query().all()
        except SQLAlchemyError as exc:
            return self._handle_db_error(exc, context="get_summary")

        summary = self._build_summary(all_records)

        return self._success_response(
            message="Macro data summary generated successfully.",
            data=[summary],
            total=1,
        )

    def get_statistics(self) -> dict[str, Any]:
        """
        Return min/max/average/latest/earliest statistics for every
        registered indicator in a single call.

        Returns:
            Success envelope whose `data` is a single dict keyed by
            indicator name, each value the output of _build_statistics().
        """
        try:
            all_records = self._build_query().all()
        except SQLAlchemyError as exc:
            return self._handle_db_error(exc, context="get_statistics")

        statistics = {
            indicator: self._build_statistics(all_records, indicator)
            for indicator in self._registry
        }

        return self._success_response(
            message="Macro statistics generated successfully.",
            data=[statistics],
            total=1,
        )

    def get_indicator_statistics(self, indicator: str) -> dict[str, Any]:
        """
        Return min/max/average/latest/earliest statistics for one indicator.

        Args:
            indicator: Registry key, e.g. "inflation", "fuel", "exchange", "cbr".

        Returns:
            Success envelope whose `data` is a single statistics dict, or a
            failure envelope if the indicator is not registered.
        """
        is_valid, _columns, error = self._validate_indicator(indicator)
        if not is_valid:
            return self._failure_response(message=error)

        try:
            all_records = self._build_query().all()
        except SQLAlchemyError as exc:
            return self._handle_db_error(exc, context="get_indicator_statistics")

        statistics = self._build_statistics(all_records, indicator)

        return self._success_response(
            message=f"{indicator.title()} statistics generated successfully.",
            data=[statistics],
            total=1,
        )

    # ==================================================================
    # COMPARISON QUERIES
    # ==================================================================

    def compare_months(
        self, month_a: str, year_a: str, month_b: str, year_b: str
    ) -> dict[str, Any]:
        """
        Compare two specific (month, year) records field-by-field.

        Args:
            month_a: First record's month name.
            year_a:  First record's year.
            month_b: Second record's month name.
            year_b:  Second record's year.

        Returns:
            Success envelope whose `data` is a single dict with the two
            serialized records plus a `deltas` dict (b - a) for every
            numeric indicator column. Returns a failure envelope if either
            period has no record.
        """
        record_a = self._fetch_month_record(month_a, year_a)
        record_b = self._fetch_month_record(month_b, year_b)

        if record_a is None or record_b is None:
            missing = []
            if record_a is None:
                missing.append(f"{month_a} {year_a}")
            if record_b is None:
                missing.append(f"{month_b} {year_b}")
            return self._failure_response(
                message=f"No macro record found for: {', '.join(missing)}."
            )

        comparison = {
            "period_a": self._serialize(record_a),
            "period_b": self._serialize(record_b),
            "deltas": self._compute_deltas(record_a, record_b),
        }

        return self._success_response(
            message="Month comparison generated successfully.",
            data=[comparison],
            total=1,
        )

    def compare_years(self, year_a: str, year_b: str) -> dict[str, Any]:
        """
        Compare aggregate statistics between two full years.

        Args:
            year_a: First year, e.g. "2025".
            year_b: Second year, e.g. "2026".

        Returns:
            Success envelope whose `data` is a single dict with per-year
            aggregate statistics (via _build_statistics) for every
            registered indicator, plus a `deltas` dict of average-value
            differences (year_b - year_a).
        """
        try:
            records_a = self._build_query().filter(MacroData.year == str(year_a)).all()
            records_b = self._build_query().filter(MacroData.year == str(year_b)).all()
        except SQLAlchemyError as exc:
            return self._handle_db_error(exc, context="compare_years")

        stats_a = {
            indicator: self._build_statistics(records_a, indicator)
            for indicator in self._registry
        }
        stats_b = {
            indicator: self._build_statistics(records_b, indicator)
            for indicator in self._registry
        }

        deltas = {
            indicator: self._safe_subtract(
                stats_b[indicator].get("average"), stats_a[indicator].get("average")
            )
            for indicator in self._registry
        }

        comparison = {
            "year_a": year_a,
            "year_b": year_b,
            "statistics_a": stats_a,
            "statistics_b": stats_b,
            "deltas": deltas,
        }

        return self._success_response(
            message="Year comparison generated successfully.",
            data=[comparison],
            total=1,
        )

    def compare_periods(
        self,
        start_a: datetime,
        end_a: datetime,
        start_b: datetime,
        end_b: datetime,
    ) -> dict[str, Any]:
        """
        Compare aggregate statistics between two arbitrary date ranges.

        Args:
            start_a: Inclusive start of the first period.
            end_a:   Inclusive end of the first period.
            start_b: Inclusive start of the second period.
            end_b:   Inclusive end of the second period.

        Returns:
            Success envelope whose `data` is a single dict with per-period
            aggregate statistics for every registered indicator, plus a
            `deltas` dict of average-value differences (period_b - period_a).
            Returns a failure envelope if either range is invalid.
        """
        for start, end, label in ((start_a, end_a, "period_a"), (start_b, end_b, "period_b")):
            is_valid, error = self._validate_date_range(start, end)
            if not is_valid:
                return self._failure_response(message=f"{label}: {error}")

        try:
            records_a = self._records_in_range(start_a, end_a)
            records_b = self._records_in_range(start_b, end_b)
        except SQLAlchemyError as exc:
            return self._handle_db_error(exc, context="compare_periods")

        stats_a = {
            indicator: self._build_statistics(records_a, indicator)
            for indicator in self._registry
        }
        stats_b = {
            indicator: self._build_statistics(records_b, indicator)
            for indicator in self._registry
        }

        deltas = {
            indicator: self._safe_subtract(
                stats_b[indicator].get("average"), stats_a[indicator].get("average")
            )
            for indicator in self._registry
        }

        comparison = {
            "period_a": {"start": start_a.isoformat(), "end": end_a.isoformat()},
            "period_b": {"start": start_b.isoformat(), "end": end_b.isoformat()},
            "statistics_a": stats_a,
            "statistics_b": stats_b,
            "deltas": deltas,
        }

        return self._success_response(
            message="Period comparison generated successfully.",
            data=[comparison],
            total=1,
        )

    # ==================================================================
    # SEARCH
    # ==================================================================

    def search(
        self,
        month: Optional[str] = None,
        year: Optional[str] = None,
        report_date: Optional[datetime] = None,
        inflation_min: Optional[float] = None,
        inflation_max: Optional[float] = None,
        fuel_min: Optional[float] = None,
        fuel_max: Optional[float] = None,
        cbr_min: Optional[float] = None,
        cbr_max: Optional[float] = None,
        usd_min: Optional[float] = None,
        usd_max: Optional[float] = None,
        euro_min: Optional[float] = None,
        euro_max: Optional[float] = None,
        pound_min: Optional[float] = None,
        pound_max: Optional[float] = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
    ) -> dict[str, Any]:
        """
        Flexible multi-filter search across MacroData.

        Only the filters that are supplied (non-None) are applied; all
        supplied filters are combined with AND logic. This intentionally
        replaces a large set of narrow single-purpose filter methods with
        one dynamically configured query.

        Args:
            month:          Exact month name match (case-insensitive).
            year:           Exact year match.
            report_date:    Exact report_date match.
            inflation_min:  Minimum inflation (inclusive).
            inflation_max:  Maximum inflation (inclusive).
            fuel_min:       Minimum fuel_price (inclusive).
            fuel_max:       Maximum fuel_price (inclusive).
            cbr_min:        Minimum cbk_rate (inclusive).
            cbr_max:        Maximum cbk_rate (inclusive).
            usd_min:        Minimum usd_kes_rate (inclusive).
            usd_max:        Maximum usd_kes_rate (inclusive).
            euro_min:       Minimum euro_kes_rate (inclusive).
            euro_max:       Maximum euro_kes_rate (inclusive).
            pound_min:      Minimum pounds_kes_rate (inclusive).
            pound_max:      Maximum pounds_kes_rate (inclusive).
            limit:          Maximum records to return. Defaults to 10.
            offset:         Number of records to skip. Defaults to 0.

        Returns:
            Success envelope with matching records in chronological order.
        """
        conditions: list[Any] = []

        if month is not None:
            normalized_month = self._normalize_month(month)
            if normalized_month not in MONTH_ORDER:
                return self._failure_response(
                    message=f"'{month}' is not a recognised month name."
                )
            conditions.append(MacroData.month == normalized_month)

        if year is not None:
            conditions.append(MacroData.year == str(year).strip())

        if report_date is not None:
            conditions.append(MacroData.report_date == self._ensure_timezone(report_date))

        range_filters: list[tuple[Column, Optional[float], Optional[float]]] = [
            (MacroData.inflation, inflation_min, inflation_max),
            (MacroData.fuel_price, fuel_min, fuel_max),
            (MacroData.cbk_rate, cbr_min, cbr_max),
            (MacroData.usd_kes_rate, usd_min, usd_max),
            (MacroData.euro_kes_rate, euro_min, euro_max),
            (MacroData.pounds_kes_rate, pound_min, pound_max),
        ]

        for column, min_value, max_value in range_filters:
            if min_value is not None:
                conditions.append(column >= min_value)
            if max_value is not None:
                conditions.append(column <= max_value)

        return self._run_collection_query(
            message="Search completed successfully.",
            filters=conditions,
            limit=limit,
            offset=offset,
        )

    # ==================================================================
    # PRIVATE — BASE QUERY ENGINE
    # ==================================================================

    def _build_query(self) -> Query:
        """
        Start a fresh, unfiltered SQLAlchemy query against MacroData.

        Every public method that needs custom filtering starts here rather
        than calling `self._db.query(...)` directly, keeping the query
        engine's entry point in exactly one place.

        Returns:
            A base SQLAlchemy Query over MacroData.
        """
        return self._db.query(MacroData)

    @staticmethod
    def _apply_filters(query: Query, conditions: list[Any]) -> Query:
        """
        Apply a list of SQLAlchemy filter conditions to a query.

        Args:
            query:      Query to filter.
            conditions: List of SQLAlchemy boolean expressions
                        (e.g. MacroData.year == "2026"). Empty list is a
                        no-op.

        Returns:
            Query with all conditions applied via AND logic.
        """
        if not conditions:
            return query
        return query.filter(*conditions)

    @staticmethod
    def _apply_sorting(query: Query, order: str = "asc") -> Query:
        """
        Apply chronological sorting to a query.

        Sorts by report_date primarily (the authoritative chronological
        column populated at ingestion time), with `id` as a stable
        tiebreaker for rows sharing a report_date.

        Args:
            query: Query to sort.
            order: "asc" (oldest -> newest, the default for every public
                   method) or "desc" (newest -> oldest).

        Returns:
            Sorted query.
        """
        if order == "desc":
            return query.order_by(MacroData.report_date.desc(), MacroData.id.desc())
        return query.order_by(MacroData.report_date.asc(), MacroData.id.asc())

    @staticmethod
    def _apply_pagination(query: Query, limit: int, offset: int) -> Query:
        """
        Apply OFFSET/LIMIT pagination to a query.

        Args:
            query:  Query to paginate.
            limit:  Maximum records to return.
            offset: Number of records to skip.

        Returns:
            Paginated query.
        """
        return query.offset(offset).limit(limit)

    @staticmethod
    def _execute_query(query: Query) -> list[MacroData]:
        """
        Execute a fully configured query and return the raw ORM results.

        This is the single point where SQLAlchemy exceptions can surface
        from a `.all()` call, so callers that want structured failure
        responses should wrap calls to this method in try/except
        SQLAlchemyError (see _run_collection_query).

        Args:
            query: Fully configured Query.

        Returns:
            List of MacroData ORM objects (never None).
        """
        return query.all()

    def _run_collection_query(
        self,
        message: str,
        filters: list[Any],
        limit: int,
        offset: int,
    ) -> dict[str, Any]:
        """
        Shared execution path for every collection-returning public method.

        Wires the base query engine together end-to-end:
        _build_query -> _apply_filters -> _apply_sorting -> count total ->
        _apply_pagination -> _execute_query -> _serialize -> response.

        Args:
            message: Success message to embed in the response envelope.
            filters: List of SQLAlchemy filter conditions (may be empty).
            limit:   Maximum records to return.
            offset:  Number of records to skip.

        Returns:
            Success envelope with paginated, serialized data, or a failure
            envelope on invalid pagination parameters or a database error.
        """
        is_valid, error = self._validate_pagination(limit, offset)
        if not is_valid:
            return self._failure_response(message=error, limit=limit, offset=offset)

        try:
            base_query = self._apply_filters(self._build_query(), filters)
            sorted_query = self._apply_sorting(base_query, order="asc")
            total = base_query.count()
            paginated_query = self._apply_pagination(sorted_query, limit, offset)
            records = self._execute_query(paginated_query)
        except SQLAlchemyError as exc:
            return self._handle_db_error(exc, context="_run_collection_query")

        return self._success_response(
            message=message,
            data=[self._serialize(record) for record in records],
            total=total,
            limit=limit,
            offset=offset,
        )

    # ------------------------------------------------------------------
    # PRIVATE — validation helpers
    # ------------------------------------------------------------------

    def _validate_indicator(
        self, indicator: str
    ) -> tuple[bool, list[Column], Optional[str]]:
        """
        Validate an indicator key against the registry and resolve its
        SQLAlchemy column objects.

        Args:
            indicator: Registry key, e.g. "inflation".

        Returns:
            Tuple of (is_valid, columns, error_message | None). `columns`
            is an empty list when invalid.
        """
        entry = self._registry.get(indicator)

        if entry is None:
            supported = ", ".join(sorted(self._registry))
            return (
                False,
                [],
                f"Unsupported indicator '{indicator}'. Supported: {supported}.",
            )

        columns = [getattr(MacroData, name) for name in entry["columns"]]
        return True, columns, None

    @staticmethod
    def _validate_pagination(limit: int, offset: int) -> tuple[bool, Optional[str]]:
        """
        Validate limit/offset pagination parameters.

        Args:
            limit:  Requested page size. Must be >= 1.
            offset: Requested skip count. Must be >= 0.

        Returns:
            Tuple of (is_valid, error_message | None).
        """
        if limit < 1:
            return False, f"limit must be >= 1, got {limit}."
        if offset < 0:
            return False, f"offset must be >= 0, got {offset}."
        return True, None

    @staticmethod
    def _validate_date_range(
        start_date: Optional[datetime], end_date: Optional[datetime]
    ) -> tuple[bool, Optional[str]]:
        """
        Validate that a date range is well-formed.

        Args:
            start_date: Inclusive lower bound.
            end_date:   Inclusive upper bound.

        Returns:
            Tuple of (is_valid, error_message | None).
        """
        if start_date is None or end_date is None:
            return False, "Both start_date and end_date are required."
        if start_date > end_date:
            return False, "start_date must not be after end_date."
        return True, None

    # ------------------------------------------------------------------
    # PRIVATE — record lookups
    # ------------------------------------------------------------------

    def _get_latest_record(self) -> Optional[MacroData]:
        """
        Fetch the single most recent MacroData row, chronologically.

        Returns:
            The latest MacroData ORM object, or None if the table is empty.
        """
        try:
            return self._apply_sorting(self._build_query(), order="desc").first()
        except SQLAlchemyError as exc:
            logger.error("_get_latest_record failed: %s", exc)
            return None

    def _fetch_month_record(self, month: str, year: str) -> Optional[MacroData]:
        """
        Fetch a single MacroData row for (month, year) without wrapping the
        result in a response envelope — used internally by comparison
        methods that need the raw ORM object for delta computation.

        Args:
            month: Month name (normalized internally).
            year:  Year as a string.

        Returns:
            MacroData ORM object, or None if not found or on DB error.
        """
        normalized_month = self._normalize_month(month)

        if normalized_month not in MONTH_ORDER:
            return None

        try:
            return (
                self._build_query()
                .filter(
                    MacroData.month == normalized_month,
                    MacroData.year == str(year).strip(),
                )
                .first()
            )
        except SQLAlchemyError as exc:
            logger.error("_fetch_month_record failed: %s", exc)
            return None

    def _records_in_range(self, start_date: datetime, end_date: datetime) -> list[MacroData]:
        """
        Fetch all MacroData rows whose report_date falls within an
        inclusive range, in chronological order.

        Args:
            start_date: Inclusive lower bound.
            end_date:   Inclusive upper bound.

        Returns:
            List of matching MacroData ORM objects.
        """
        resolved_start = self._ensure_timezone(start_date)
        resolved_end = self._ensure_timezone(end_date)

        query = self._apply_filters(
            self._build_query(),
            [
                MacroData.report_date >= resolved_start,
                MacroData.report_date <= resolved_end,
            ],
        )
        return self._execute_query(self._apply_sorting(query, order="asc"))

    # ------------------------------------------------------------------
    # PRIVATE — statistics / summary builders
    # ------------------------------------------------------------------

    def _build_summary(self, records: list[MacroData]) -> dict[str, Any]:
        """
        Build a high-level summary dict from a list of MacroData records.

        Args:
            records: All records to summarise (typically the full table).

        Returns:
            Dict with total_observations, earliest/latest serialized
            records, and a per-indicator non-null observation count.
        """
        total_observations = len(records)

        if total_observations == 0:
            return {
                "total_observations": 0,
                "earliest": None,
                "latest": None,
                "indicator_counts": {indicator: 0 for indicator in self._registry},
            }

        sorted_records = sorted(
            records, key=lambda r: r.report_date or datetime.min.replace(tzinfo=NAIROBI_TZ)
        )
        earliest = sorted_records[0]
        latest = sorted_records[-1]

        indicator_counts = {
            indicator: self._count_non_null(records, indicator)
            for indicator in self._registry
        }

        return {
            "total_observations": total_observations,
            "earliest": self._serialize(earliest),
            "latest": self._serialize(latest),
            "indicator_counts": indicator_counts,
        }

    def _build_statistics(self, records: list[MacroData], indicator: str) -> dict[str, Any]:
        """
        Compute min/max/average/latest/earliest statistics for one
        indicator across a set of records, using only non-null values.

        For multi-column indicators (e.g. "exchange"), statistics are
        computed per-column and nested under the column name.

        Args:
            records:   Records to compute statistics over.
            indicator: Registry key, e.g. "inflation", "exchange".

        Returns:
            Dict of statistics. For single-column indicators, keys are
            {minimum, maximum, average, latest, earliest,
            total_observations}. For multi-column indicators, keys are the
            column names, each mapping to that same statistics shape.
        """
        is_valid, _columns, error = self._validate_indicator(indicator)
        if not is_valid:
            return {"error": error}

        column_names = self._registry[indicator]["columns"]

        if len(column_names) == 1:
            return self._compute_column_statistics(records, column_names[0])

        return {
            column_name: self._compute_column_statistics(records, column_name)
            for column_name in column_names
        }

    def _compute_column_statistics(
        self, records: list[MacroData], column_name: str
    ) -> dict[str, Any]:
        """
        Compute min/max/average/latest/earliest for a single MacroData
        column, using only non-null values in chronological order.

        Args:
            records:     Records to compute statistics over.
            column_name: MacroData attribute name, e.g. "inflation".

        Returns:
            Dict with minimum, maximum, average, latest, earliest, and
            total_observations. All value fields are None when there are
            no non-null observations.
        """
        chronological = sorted(
            records, key=lambda r: r.report_date or datetime.min.replace(tzinfo=NAIROBI_TZ)
        )

        values_in_order = [
            getattr(record, column_name)
            for record in chronological
            if getattr(record, column_name) is not None
        ]

        if not values_in_order:
            return {
                "minimum": None,
                "maximum": None,
                "average": None,
                "latest": None,
                "earliest": None,
                "total_observations": 0,
            }

        return {
            "minimum": round(min(values_in_order), 4),
            "maximum": round(max(values_in_order), 4),
            "average": round(sum(values_in_order) / len(values_in_order), 4),
            "latest": round(values_in_order[-1], 4),
            "earliest": round(values_in_order[0], 4),
            "total_observations": len(values_in_order),
        }

    def _count_non_null(self, records: list[MacroData], indicator: str) -> int:
        """
        Count how many records have a non-null value for an indicator.

        For multi-column indicators, a record counts only if ALL of the
        indicator's columns are non-null — matching the same "complete
        row" rule used by _get_indicator_history's NULL filtering.

        Args:
            records:   Records to inspect.
            indicator: Registry key, e.g. "inflation".

        Returns:
            Count of qualifying records. Returns 0 if `indicator` is not
            a registered indicator.
        """
        entry = self._registry.get(indicator)
        if entry is None:
            return 0

        column_names = entry["columns"]
        return sum(
            1
            for record in records
            if all(getattr(record, name) is not None for name in column_names)
        )

    def _compute_deltas(self, record_a: MacroData, record_b: MacroData) -> dict[str, Any]:
        """
        Compute (b - a) deltas for every numeric indicator column shared
        between two MacroData records.

        Args:
            record_a: First (baseline) record.
            record_b: Second (comparison) record.

        Returns:
            Dict of column_name -> delta (None if either side is missing).
        """
        deltas: dict[str, Any] = {}

        for entry in self._registry.values():
            for column_name in entry["columns"]:
                value_a = getattr(record_a, column_name)
                value_b = getattr(record_b, column_name)
                deltas[column_name] = self._safe_subtract(value_b, value_a)

        return deltas

    @staticmethod
    def _safe_subtract(
        value_b: Optional[float], value_a: Optional[float]
    ) -> Optional[float]:
        """
        Subtract two optional numeric values, returning None if either is
        missing instead of raising.

        Args:
            value_b: Minuend (the "later"/"second" value).
            value_a: Subtrahend (the "earlier"/"first" value).

        Returns:
            Rounded float difference, or None if either input is None.
        """
        if value_a is None or value_b is None:
            return None
        return round(value_b - value_a, 4)

    # ------------------------------------------------------------------
    # PRIVATE — normalisation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_month(value: Any) -> Optional[str]:
        """Trim whitespace and normalise casing, e.g. 'MARCH' -> 'March'."""
        if not value:
            return None
        return str(value).strip().capitalize()

    @staticmethod
    def _ensure_timezone(value: datetime) -> datetime:
        """
        Ensure a datetime is timezone-aware, assuming Africa/Nairobi for
        naive input. Mirrors the timezone convention used throughout the
        project (MacroService, models).

        Args:
            value: Datetime to normalise.

        Returns:
            Timezone-aware datetime.
        """
        if value.tzinfo is None:
            return value.replace(tzinfo=NAIROBI_TZ)
        return value

    # ------------------------------------------------------------------
    # PRIVATE — serialization
    # ------------------------------------------------------------------

    @staticmethod
    def _serialize(record: MacroData) -> dict[str, Any]:
        """
        Convert a MacroData ORM object into a JSON-serialisable dict.

        This is the ONLY place ORM attribute access happens for output
        purposes — every public method returns the result of this method
        (or a structure built from it), never a raw ORM object.

        Args:
            record: MacroData ORM instance.

        Returns:
            Dict with all MacroData fields, datetimes as ISO-8601 strings.
        """
        return {
            "id": record.id,
            "month": record.month,
            "year": record.year,
            "report_date": record.report_date.isoformat() if record.report_date else None,
            "inflation": record.inflation,
            "inflation_trend": record.inflation_trend,
            "fuel_price": record.fuel_price,
            "fuel_trend": record.fuel_trend,
            "cbk_rate": record.cbk_rate,
            "usd_kes_rate": record.usd_kes_rate,
            "euro_kes_rate": record.euro_kes_rate,
            "pounds_kes_rate": record.pounds_kes_rate,
            "timestamp": record.timestamp.isoformat() if record.timestamp else None,
        }

    # ------------------------------------------------------------------
    # PRIVATE — response builders
    # ------------------------------------------------------------------

    @staticmethod
    def _success_response(
        message: str,
        data: list[dict[str, Any]],
        total: Optional[int] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> dict[str, Any]:
        """
        Build a consistent success response envelope.

        Args:
            message: Human-readable success message.
            data:    List of serialized record dicts (may be empty).
            total:   Total matching records before pagination. Defaults to
                     len(data) when not supplied (single-record queries).
            limit:   Page size used. Defaults to len(data) when omitted.
            offset:  Skip count used. Defaults to 0 when omitted.

        Returns:
            {"status": "success", "message", "count", "total", "limit",
             "offset", "data"}.
        """
        resolved_total = total if total is not None else len(data)
        resolved_limit = limit if limit is not None else len(data)
        resolved_offset = offset if offset is not None else 0

        return {
            "status": "success",
            "message": message,
            "count": len(data),
            "total": resolved_total,
            "limit": resolved_limit,
            "offset": resolved_offset,
            "data": data,
        }

    @staticmethod
    def _failure_response(
        message: str,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
    ) -> dict[str, Any]:
        """
        Build a consistent failure response envelope.

        Never raises — every error path in this service (invalid
        indicator, invalid date range, invalid pagination, DB errors)
        resolves to this structured response instead of an exception.

        Args:
            message: Human-readable failure reason.
            limit:   Echoes back the requested limit, if any.
            offset:  Echoes back the requested offset, if any.

        Returns:
            {"status": "failed", "message", "count": 0, "total": 0,
             "limit", "offset", "data": []}.
        """
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
        """
        Log a SQLAlchemy exception and convert it into a structured
        failure response. Ensures no database exception ever escapes this
        service's public API.

        Args:
            exc:     The caught SQLAlchemyError.
            context: Name of the calling method, for log correlation.

        Returns:
            Failure envelope with a generic, user-safe message.
        """
        logger.error("%s: database error: %s", context, exc)
        return self._failure_response(
            message="A database error occurred while processing the query."
        )