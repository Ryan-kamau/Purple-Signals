"""
services/macro_service.py

Service Layer — owns ALL business logic for macroeconomic data ingestion.

Pipeline position:

    FastAPI Route
        │
        ▼
    MacroService                 <- this file
        │
        ▼
    KNBSExtractor
        │
        ▼
    Individual table extractors (CBR / Inflation / Exchange / Fuel)
        │
        ▼
    Structured dictionaries  {"status", "report_date", "data": [...]}
        │
        ▼
    MacroService  (normalize -> merge -> analytics -> upsert)
        │
        ▼
    MacroData ORM  ->  MySQL

Responsibilities:
  - Call the KNBS extraction layer (never parse PDFs itself)
  - Validate extractor responses
  - Normalize extracted records (types, whitespace, casing)
  - Merge per-extractor records into (month, year) keyed rows
  - Upsert into MacroData with a "never overwrite real data with None" rule
  - Compute derived analytics (inflation_trend, fuel_trend)
  - Persist using an injected SQLAlchemy Session
  - Return structured, JSON-serialisable dicts — never raw ORM objects

No FastAPI routes, no HTTP concerns, no PDF/table parsing live here.
"""

import logging
from datetime import datetime
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from models.macro_data import MacroData
from scrapers.macro_scraper import KNBSExtractor

logger = logging.getLogger(__name__)

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

# Canonical month ordering — used to walk backwards to the "previous month"
# for trend calculations without depending on report_date.
MONTH_ORDER: list[str] = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


class MacroService:
    """
    Orchestrates the full KNBS macroeconomic ingestion pipeline.

    Encapsulation notes:
      - `_db` and `_extractor` are injected, never created internally —
        keeps this class trivially testable (pass a fake Session/extractor).
      - `_registry` is built once per instance and is the single source of
        truth for "which extractor feeds which MacroData columns". Adding
        a new macro indicator (money supply, reserves, etc.) means adding
        one registry entry — no orchestration code changes.
      - All helper methods are private (leading underscore) and each does
        exactly one job (validate / normalize / merge / persist / trend),
        so the public refresh_* methods read like a checklist.

    Example:
        service = MacroService(db)
        result  = service.refresh_macro_data(pdf_url)
    """

    def __init__(self, db: Session, extractor: Optional[KNBSExtractor] = None) -> None:
        """
        Args:
            db:        Active SQLAlchemy session (injected via FastAPI Depends).
                       This service never opens or closes its own session.
            extractor: Optional KNBSExtractor override (useful for testing / DI).
        """
        self._db = db
        self._extractor = extractor or KNBSExtractor()
        self._registry = self._build_registry()

    # ------------------------------------------------------------------
    # Registry — the only place that knows "extractor -> MacroData fields"
    # ------------------------------------------------------------------

    def _build_registry(self) -> dict[str, dict[str, Any]]:
        """
        Build the extractor registry.

        Each entry maps a short key to:
          - "method":     the bound KNBSExtractor.get_*() callable
          - "field_map":  raw extractor field name -> MacroData column name

        Extending the pipeline (e.g. adding Foreign Reserves) means adding
        one more entry here — refresh_macro_data() and _run_single() need
        no changes.
        """
        return {
            "inflation": {
                "method": self._extractor.get_inflation,
                "field_map": {"kenya_inflation": "inflation"},
            },
            "exchange": {
                "method": self._extractor.get_exchange_rates,
                "field_map": {
                    "usd": "usd_kes_rate",
                    "pound_sterling": "pounds_kes_rate",
                    "euro": "euro_kes_rate",
                },
            },
            "cbr": {
                "method": self._extractor.get_cbr,
                "field_map": {"cbr": "cbk_rate"},
            },
            "fuel": {
                "method": self._extractor.get_fuels,
                "field_map": {"diesel_price": "fuel_price"},
            },
        }

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def refresh_inflation(self, pdf_url: str) -> dict[str, Any]:
        """Refresh inflation data only. See `_run_single` for the pipeline."""
        return self._run_single("inflation", pdf_url)

    def refresh_exchange_rates(self, pdf_url: str) -> dict[str, Any]:
        """Refresh USD/GBP/EUR exchange rate data only."""
        return self._run_single("exchange", pdf_url)

    def refresh_cbr(self, pdf_url: str) -> dict[str, Any]:
        """Refresh Central Bank Rate data only."""
        return self._run_single("cbr", pdf_url)

    def refresh_fuel_prices(self, pdf_url: str) -> dict[str, Any]:
        """Refresh diesel fuel price data only."""
        return self._run_single("fuel", pdf_url)

    def refresh_macro_data(self, pdf_url: str) -> dict[str, Any]:
        """
        Run the full macro ingestion pipeline: every registered extractor,
        against the same KNBS "Leading Economic Indicators" PDF.

        Each sub-extractor upserts its own fields into MacroData rows keyed
        by (month, year). Because updates never overwrite a real value with
        None, running all four in sequence converges to the same merged
        state a single combined pass would produce — without needing to
        hold everything in memory at once.

        One failing table (e.g. fuel prices) does not abort the others.

        Returns:
            Aggregate response with overall status ("success" |
            "partial_success" | "failed"), summed rows_saved, and a
            per-extractor breakdown under "results".
        """
        logger.info("Starting full macro data refresh")

        results: dict[str, dict[str, Any]] = {}
        for key in self._registry:
            results[key] = self._run_single(key, pdf_url)

        return self._aggregate(results)

    # ------------------------------------------------------------------
    # Core single-extractor pipeline
    # ------------------------------------------------------------------

    def _run_single(self, registry_key: str, pdf_url: str) -> dict[str, Any]:
        """
        Full pipeline for one extractor: extract -> validate -> normalize
        -> merge -> persist -> compute trends -> serialise.

        Args:
            registry_key: Key into self._registry (e.g. "inflation").
            pdf_url:      KNBS PDF URL to extract from.

        Returns:
            Structured service response dict (see module docstring).
        """
        entry = self._registry[registry_key]
        logger.info("Loading %s extractor...", registry_key)

        try:
            raw_response = entry["method"](pdf_url)
        except Exception as exc:  # noqa: BLE001
            logger.error("%s extractor raised an exception: %s", registry_key, exc)
            return self._failure_response(
                message=f"{registry_key} extraction failed: {exc}",
                report_date=None,
            )

        is_valid, error = self._validate_response(raw_response)
        if not is_valid:
            logger.error("%s extraction validation failed: %s", registry_key, error)
            return self._failure_response(message=error, report_date=None)

        normalized_records = self._normalize_records(
            raw_response["data"], entry["field_map"]
        )
        logger.info("Normalized %d records for %s", len(normalized_records), registry_key)

        merged = self._merge_records(normalized_records)
        logger.info("Merged %d monthly records", len(merged))

        report_date = raw_response.get("report_date")

        try:
            saved_records = self._persist_records(merged, report_date)
        except Exception as exc:  # noqa: BLE001
            self._db.rollback()
            logger.error("Database transaction rolled back for %s: %s", registry_key, exc)
            return self._failure_response(
                message="Database transaction rolled back.",
                report_date=report_date,
            )

        self._compute_and_persist_trends(saved_records)

        return self._success_response(
            message=f"{registry_key.title()} data updated successfully",
            rows_saved=len(saved_records),
            report_date=report_date,
            records=saved_records,
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_response(response: Any) -> tuple[bool, Optional[str]]:
        """
        Validate a raw KNBSExtractor response before any processing.

        Checks: response exists, is a dict, status == "success",
        report_date exists, data exists and is a non-empty list.

        Returns:
            (is_valid, error_message | None)
        """
        if not response:
            return False, "Extractor returned no response."

        if not isinstance(response, dict):
            return False, "Extractor response is not a dictionary."

        if response.get("status") != "success":
            return False, f"Extractor status is '{response.get('status')}', expected 'success'."

        if not response.get("report_date"):
            return False, "Extractor response is missing report_date."

        data = response.get("data")

        if data is None:
            return False, "Extractor response is missing 'data'."

        if not isinstance(data, list):
            return False, "Extractor response 'data' is not a list."

        if not data:
            return False, "Extractor response 'data' is empty."

        return True, None

    # ------------------------------------------------------------------
    # Normalization
    # ------------------------------------------------------------------

    def _normalize_records(
        self, raw_data: list[dict[str, Any]], field_map: dict[str, str]
    ) -> list[dict[str, Any]]:
        """
        Normalize raw extractor rows into MacroData-column-named dicts.

        Args:
            raw_data:  List of raw record dicts from one extractor
                       (each has "month", "year", plus extractor-specific keys).
            field_map: Maps extractor field name -> MacroData column name,
                       e.g. {"kenya_inflation": "inflation"}.

        Returns:
            List of normalized dicts: {"month": ..., "year": ..., <column>: value, ...}
            Records with no usable month are skipped.
        """
        normalized: list[dict[str, Any]] = []

        for raw_record in raw_data:
            month = self._normalize_month(raw_record.get("month"))
            year = self._normalize_year(raw_record.get("year"))

            if not month or month not in MONTH_ORDER:
                logger.debug("Skipping record with invalid month: %s", raw_record)
                continue

            normalized_record: dict[str, Any] = {"month": month, "year": year}

            for raw_field, column_name in field_map.items():
                normalized_record[column_name] = self._normalize_numeric(
                    raw_record.get(raw_field)
                )

            normalized.append(normalized_record)

        return normalized

    @staticmethod
    def _normalize_month(value: Any) -> Optional[str]:
        """Trim whitespace and normalise casing, e.g. 'MARCH' / 'march' -> 'March'."""
        if not value:
            return None
        return str(value).strip().capitalize()

    @staticmethod
    def _normalize_year(value: Any) -> Optional[str]:
        """Coerce year to a clean string (MacroData.year is a String column)."""
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _normalize_numeric(value: Any) -> Optional[float]:
        """
        Coerce a raw numeric-ish value into a float.

        Handles: None, already-numeric, comma-formatted strings, and
        empty/whitespace-only strings (-> None).
        """
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return float(value)

        text = str(value).replace(",", "").strip()

        if not text:
            return None

        try:
            return float(text)
        except ValueError:
            logger.debug("Could not normalize numeric value: %r", value)
            return None

    # ------------------------------------------------------------------
    # Merging
    # ------------------------------------------------------------------

    @staticmethod
    def _merge_records(normalized_records: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
        """
        Merge normalized records into an in-memory lookup keyed by (month, year).

        A single extractor's records never collide on the same month twice,
        but this still guards against duplicate rows within one PDF table.

        Returns:
            {(month, year): {"month": ..., "year": ..., <column>: value, ...}}
        """
        merged: dict[tuple[str, str], dict[str, Any]] = {}

        for record in normalized_records:
            key = (record["month"], record["year"])
            entry = merged.setdefault(key, {"month": record["month"], "year": record["year"]})

            for field, value in record.items():
                if field in ("month", "year"):
                    continue
                entry[field] = value

        return merged

    # ------------------------------------------------------------------
    # Persistence (upsert)
    # ------------------------------------------------------------------

    def _persist_records(
        self,
        merged: dict[tuple[str, str], dict[str, Any]],
        report_date: Optional[str],
    ) -> list[MacroData]:
        """
        Upsert merged (month, year) records into MacroData.

        Update rule: only overwrite a column when the new value is not
        None. This is what lets refresh_inflation(), refresh_fuel_prices(),
        etc. be called independently over time and still converge to one
        fully populated row per (month, year), instead of one call erasing
        another's fields.

        Args:
            merged:      {(month, year): {column: value, ...}}
            report_date: Report-level date string (e.g. "March 2026") used
                         to populate/refresh the report_date column.

        Returns:
            List of persisted (added or updated) MacroData ORM objects.

        Raises:
            Exception: Propagated to the caller, which rolls back and
                       reports failure. Callers must not assume partial
                       writes are safe to keep.
        """
        parsed_report_date = self._parse_report_date(report_date)
        saved: list[MacroData] = []

        for (month, year), fields in merged.items():
            existing: Optional[MacroData] = (
                self._db.query(MacroData)
                .filter(MacroData.month == month, MacroData.year == year)
                .first()
            )

            if existing:
                logger.info("Updating existing record %s %s", month, year)
                for column, value in fields.items():
                    if value is not None:
                        setattr(existing, column, value)
                if parsed_report_date is not None:
                    existing.report_date = parsed_report_date
                saved.append(existing)
            else:
                logger.info("Inserted %s %s", month, year)
                new_record = MacroData(
                    month=month,
                    year=year,
                    report_date=parsed_report_date,
                    **{k: v for k, v in fields.items() if k not in ("month", "year")},
                )
                self._db.add(new_record)
                saved.append(new_record)

        self._db.commit()

        for record in saved:
            self._db.refresh(record)

        logger.info("Committed %d records", len(saved))
        return saved

    @staticmethod
    def _parse_report_date(report_date: Optional[str]) -> Optional[datetime]:
        """
        Parse a KNBS report-level date string (e.g. "March 2026") into a
        Nairobi-aware datetime anchored to the first of that month.

        Returns None on missing/unparseable input rather than raising —
        report_date is metadata, not a value worth failing ingestion over.
        """
        if not report_date:
            return None

        try:
            parsed = datetime.strptime(report_date.strip(), "%B %Y")
            return parsed.replace(tzinfo=NAIROBI_TZ)
        except ValueError:
            logger.debug("Could not parse report_date: %r", report_date)
            return None

    # ------------------------------------------------------------------
    # Derived analytics
    # ------------------------------------------------------------------

    def _compute_and_persist_trends(self, records: list[MacroData]) -> None:
        """
        Compute inflation_trend and fuel_trend for each record by comparing
        against the chronologically previous month in the database.

        trend = current_value - previous_value
        If no previous month record exists (or the relevant value is
        missing on either side), the trend is left as None.
        """
        for record in records:
            previous = self._find_previous_month_record(record.month, record.year)

            if previous is None:
                record.inflation_trend = None
                record.fuel_trend = None
                continue

            if record.inflation is not None and previous.inflation is not None:
                record.inflation_trend = round(record.inflation - previous.inflation, 4)
            else:
                record.inflation_trend = None

            if record.fuel_price is not None and previous.fuel_price is not None:
                record.fuel_trend = round(record.fuel_price - previous.fuel_price, 4)
            else:
                record.fuel_trend = None

        self._db.commit()

        for record in records:
            self._db.refresh(record)

        logger.info("Computed inflation trends")
        logger.info("Computed fuel trends")

    def _find_previous_month_record(self, month: str, year: str) -> Optional[MacroData]:
        """
        Look up the MacroData row for the month immediately preceding
        (month, year), walking December -> January across a year boundary.

        Returns:
            The previous month's MacroData row, or None if it doesn't
            exist yet or `month` is not a recognised month name.
        """
        if month not in MONTH_ORDER:
            logger.debug("Unrecognised month '%s' — cannot compute trend.", month)
            return None

        try:
            year_int = int(year)
        except (TypeError, ValueError):
            logger.debug("Unrecognised year '%s' — cannot compute trend.", year)
            return None

        index = MONTH_ORDER.index(month)

        if index == 0:
            previous_month, previous_year = "December", str(year_int - 1)
        else:
            previous_month, previous_year = MONTH_ORDER[index - 1], str(year_int)

        return (
            self._db.query(MacroData)
            .filter(MacroData.month == previous_month, MacroData.year == previous_year)
            .first()
        )

    # ------------------------------------------------------------------
    # Response builders
    # ------------------------------------------------------------------

    def _success_response(
        self,
        *,
        message: str,
        rows_saved: int,
        report_date: Optional[str],
        records: list[MacroData],
    ) -> dict[str, Any]:
        return {
            "status": "success",
            "message": message,
            "rows_saved": rows_saved,
            "report_date": report_date,
            "data": [self._serialize(record) for record in records],
        }

    @staticmethod
    def _failure_response(message: str, report_date: Optional[str]) -> dict[str, Any]:
        return {
            "status": "failed",
            "message": message,
            "rows_saved": 0,
            "report_date": report_date,
            "data": [],
        }

    def _aggregate(self, results: dict[str, dict[str, Any]]) -> dict[str, Any]:
        """
        Roll up per-extractor responses from refresh_macro_data() into one
        overall response.

        Overall status:
          - "success"         if every extractor succeeded
          - "partial_success" if at least one succeeded
          - "failed"          if none succeeded
        """
        statuses = [result["status"] for result in results.values()]
        succeeded = [key for key, result in results.items() if result["status"] == "success"]
        failed = [key for key, result in results.items() if result["status"] != "success"]

        rows_saved = sum(result.get("rows_saved", 0) for result in results.values())
        report_date = next(
            (result.get("report_date") for result in results.values() if result.get("report_date")),
            None,
        )

        if not failed:
            status = "success"
            message = "Macro data updated successfully"
        elif succeeded:
            status = "partial_success"
            message = f"{', '.join(failed)} extraction failed. Remaining records saved."
        else:
            status = "failed"
            message = "All macro extractors failed."

        return {
            "status": status,
            "message": message,
            "rows_saved": rows_saved,
            "report_date": report_date,
            "results": results,
        }

    @staticmethod
    def _serialize(record: MacroData) -> dict[str, Any]:
        """Convert a MacroData ORM object into a JSON-serialisable dict."""
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