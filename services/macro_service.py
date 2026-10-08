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
    Structured dictionaries
        {"table_name", "report_date", "status", "source_url", "data": [...]}
        │
        ▼
    MacroService
        extract
        → validate structure (shape only, not extraction success)
        → propagate extractor-level failures (status != "success")
        → normalize
        → persist (upsert, "never overwrite real data with None")
        → classify rows as inserted / updated / unchanged
        → compute trends (only for inserted/updated rows, batched)
        → single commit
        → build service response
        │
        ▼
    MacroData ORM  ->  MySQL

Responsibilities:
  - Call the KNBS extraction layer (never parse PDFs itself)
  - Validate extractor response *structure*, independent of extraction outcome
  - Propagate extractor-level failures with their original metadata intact
  - Normalize extracted records (types, whitespace, casing)
  - Upsert into MacroData with a "never overwrite real data with None" rule
  - Track which rows were inserted, updated, or left unchanged
  - Compute derived analytics (inflation_trend, fuel_trend) only for rows
    that actually changed, using batched lookups
  - Persist using an injected SQLAlchemy Session, one transaction per run
  - Return structured, JSON-serialisable dicts — never raw ORM objects

No FastAPI routes, no HTTP concerns, no PDF/table parsing live here.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from models.macro_data import MacroData
from scrapers.macro_scraper import KNBSExtractor

logger = logging.getLogger(__name__)

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

# Canonical month ordering — used to walk backwards to the "previous month"
# for trend calculations without depending on report_date. Isolated here
# so month/year-specific logic doesn't leak into persistence or response
# building; a future non-monthly dataset would only need its own key
# resolver, not a rewrite of the pipeline.
MONTH_ORDER: list[str] = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

# Single source of truth for "extractor field -> MacroData column".
# The registry below only references this; nothing else in the service
# should hardcode a field mapping.
FIELD_MAPS: dict[str, dict[str, str]] = {
    "inflation": {"kenya_inflation": "inflation"},
    "exchange": {
        "usd": "usd_kes_rate",
        "pound_sterling": "pounds_kes_rate",
        "euro": "euro_kes_rate",
    },
    "cbr": {"cbr": "cbk_rate"},
    "fuel": {"diesel_price": "fuel_price"},
}

# Structural keys every extractor response must carry, regardless of
# whether extraction itself succeeded.
_REQUIRED_RESPONSE_KEYS = ("table_name", "report_date", "status", "source_url", "data")


# ---------------------------------------------------------------------------
# Persistence result container
# ---------------------------------------------------------------------------

@dataclass
class PersistStats:
    """
    Classifies every MacroData row processed by `_persist_records()` into
    exactly one of three buckets, so callers can report accurate save
    counts (`rows_saved = inserted + updated`) and skip trend
    recomputation for rows that didn't actually change.

    A row is:
      - inserted:  a brand-new MacroData object was created.
      - updated:   the row already existed and at least one column changed.
      - unchanged: the row already existed and nothing changed.
    """

    inserted: list[MacroData] = field(default_factory=list)
    updated: list[MacroData] = field(default_factory=list)
    unchanged: list[MacroData] = field(default_factory=list)

    @property
    def records(self) -> list[MacroData]:
        """Every row processed, regardless of whether it changed."""
        return self.inserted + self.updated + self.unchanged

    @property
    def changed_records(self) -> list[MacroData]:
        """Only rows that were actually inserted or updated."""
        return self.inserted + self.updated


class MacroService:
    """
    Orchestrates the full KNBS macroeconomic ingestion pipeline.

    Encapsulation notes:
      - `_db` and `_extractor` are injected, never created internally —
        keeps this class trivially testable (pass a fake Session/extractor).
      - `_registry` is built once per instance and is the single source of
        truth for "which extractor feeds which MacroData columns". Adding
        a new macro indicator means adding one FIELD_MAPS entry and one
        registry entry — no orchestration code changes.
      - All helper methods are private (leading underscore) and each does
        exactly one job, so the public refresh_* methods read like a
        checklist.

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
    # Registry — configuration only. Field mappings live in FIELD_MAPS;
    # this just wires each key to its bound extractor method.
    # ------------------------------------------------------------------

    def _build_registry(self) -> dict[str, dict[str, Any]]:
        return {
            "inflation": {
                "method": self._extractor.get_inflation,
                "field_map": FIELD_MAPS["inflation"],
            },
            "exchange": {
                "method": self._extractor.get_exchange_rates,
                "field_map": FIELD_MAPS["exchange"],
            },
            "cbr": {
                "method": self._extractor.get_cbr,
                "field_map": FIELD_MAPS["cbr"],
            },
            "fuel": {
                "method": self._extractor.get_fuels,
                "field_map": FIELD_MAPS["fuel"],
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
        state a single combined pass would produce.

        One failing extractor does not abort the others.

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
        Full pipeline for one extractor:

            extract -> validate structure -> propagate extractor failure
            -> normalize -> persist (insert/update/unchanged) -> compute
            trends (changed rows only) -> single commit -> serialise

        Args:
            registry_key: Key into self._registry (e.g. "inflation").
            pdf_url:      KNBS PDF URL to extract from.

        Returns:
            Structured service response dict (see module docstring).
        """
        entry = self._registry[registry_key]
        logger.info("Loading %s extractor...", registry_key)

        # --- Call the extractor ------------------------------------------
        try:
            raw_response = entry["method"](pdf_url)
        except Exception as exc:  # noqa: BLE001
            logger.error("%s extractor raised an exception: %s", registry_key, exc)
            return self._failure_response(
                message=f"{registry_key} extraction failed: {exc}",
                report_date=None,
            )

        # --- Validate structure only — NOT extraction success ------------
        is_valid_structure, structure_error = self._validate_structure(raw_response)
        if not is_valid_structure:
            logger.error("%s response failed structural validation: %s", registry_key, structure_error)
            return self._failure_response(
                message=structure_error,
                report_date=raw_response.get("report_date") if isinstance(raw_response, dict) else None,
                table_name=raw_response.get("table_name") if isinstance(raw_response, dict) else None,
                source_url=raw_response.get("source_url") if isinstance(raw_response, dict) else None,
            )

        table_name = raw_response.get("table_name")
        report_date = raw_response.get("report_date")
        source_url = raw_response.get("source_url")
        extractor_status = raw_response.get("status")
        data = raw_response.get("data") or []

        # --- Propagate extractor-level failure, don't mask it -------------
        if extractor_status != "success":
            logger.warning(
                "%s extractor reported failure (status=%s); propagating.",
                registry_key, extractor_status,
            )
            return self._failure_response(
                message=f"{registry_key} extractor reported status='{extractor_status}'.",
                report_date=report_date,
                table_name=table_name,
                source_url=source_url,
            )

        # --- A successful extractor with no rows is not an error ----------
        if not data:
            logger.info("%s extractor succeeded but returned no rows.", registry_key)
            return self._success_response(
                message=f"{registry_key.title()} extractor returned no rows.",
                report_date=report_date,
                table_name=table_name,
                source_url=source_url,
                stats=PersistStats(),
            )

        # --- Normalize (no merge stage — extractor already yields one
        #     record per month/year) -------------------------------------
        normalized_records = self._normalize_records(data, entry["field_map"])
        logger.info("Normalized %d records for %s", len(normalized_records), registry_key)

        # --- Persist + trends in a single transaction ----------------------
        try:
            stats = self._persist_and_compute(normalized_records, report_date)
        except Exception as exc:  # noqa: BLE001
            self._db.rollback()
            logger.error("Database transaction rolled back for %s: %s", registry_key, exc)
            return self._failure_response(
                message="Database transaction rolled back.",
                report_date=report_date,
                table_name=table_name,
                source_url=source_url,
            )

        return self._success_response(
            message=f"{registry_key.title()} data updated successfully",
            report_date=report_date,
            table_name=table_name,
            source_url=source_url,
            stats=stats,
        )

    # ------------------------------------------------------------------
    # Structural validation (shape only — never opines on extraction
    # success/failure or on whether data is non-empty)
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_structure(response: Any) -> tuple[bool, Optional[str]]:
        """
        Validate that an extractor response has the expected shape.

        Deliberately does NOT check response["status"] == "success" and
        does NOT require response["data"] to be non-empty — those are
        extraction-outcome concerns, handled separately in _run_single so
        extractor failures/empty results are propagated, not discarded.

        Returns:
            (is_valid, error_message | None)
        """
        if not isinstance(response, dict):
            return False, "Extractor response is not a dictionary."

        missing = [key for key in _REQUIRED_RESPONSE_KEYS if key not in response]
        if missing:
            return False, f"Extractor response missing required keys: {missing}"

        if not isinstance(response.get("data"), list):
            return False, "Extractor response 'data' is not a list."

        return True, None

    # ------------------------------------------------------------------
    # Normalization — split into small, single-purpose helpers
    # ------------------------------------------------------------------

    def _normalize_records(
        self, raw_data: list[dict[str, Any]], field_map: dict[str, str]
    ) -> list[dict[str, Any]]:
        """
        Normalize raw extractor rows into MacroData-column-named dicts.

        Delegates to:
          - _validate_record(): is this row even usable?
          - _normalize_record(): clean month/year into a base dict
          - _map_record_fields(): extractor field -> MacroData column,
            with numeric coercion and missing-field warnings

        Records that fail validation or have no usable month are skipped —
        identical behavior to before, just decomposed.
        """
        normalized: list[dict[str, Any]] = []

        for raw_record in raw_data:
            if not self._validate_record(raw_record):
                logger.debug("Skipping invalid record: %s", raw_record)
                continue

            base = self._normalize_record(raw_record)
            if base is None:
                logger.debug("Skipping record with invalid month: %s", raw_record)
                continue

            mapped_fields = self._map_record_fields(raw_record, field_map)
            normalized.append({**base, **mapped_fields})

        return normalized

    @staticmethod
    def _validate_record(raw_record: Any) -> bool:
        """Minimal shape check: must be a dict carrying a month value."""
        return isinstance(raw_record, dict) and bool(raw_record.get("month"))

    def _normalize_record(self, raw_record: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Clean month/year into the base of a normalized record."""
        month = self._normalize_month(raw_record.get("month"))
        year = self._normalize_year(raw_record.get("year"))

        if not month or month not in MONTH_ORDER:
            return None

        return {"month": month, "year": year}

    def _map_record_fields(
        self, raw_record: dict[str, Any], field_map: dict[str, str]
    ) -> dict[str, Any]:
        """
        Apply extractor-field -> MacroData-column mapping with numeric
        coercion.

        If an expected extractor field is absent from the raw record, a
        WARNING is logged (e.g. "Expected extractor field 'diesel_price'
        missing for March 2026.") — this usually signals an
        extractor/service field-mapping mismatch worth investigating.
        The mapped value still falls back to None so behaviour stays
        backwards compatible; no exception is raised.
        """
        mapped: dict[str, Any] = {}

        for raw_field, column_name in field_map.items():
            if raw_field not in raw_record:
                logger.warning(
                    "Expected extractor field '%s' missing for %s %s.",
                    raw_field,
                    raw_record.get("month"),
                    raw_record.get("year"),
                )

            mapped[column_name] = self._normalize_numeric(raw_record.get(raw_field))

        return mapped

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
    # Persistence (upsert) + trends — one transaction
    # ------------------------------------------------------------------

    def _persist_and_compute(
        self,
        records: list[dict[str, Any]],
        report_date: Optional[str],
    ) -> PersistStats:
        """
        Persist normalized records and compute trends in a single
        transaction: persist -> classify (insert/update/unchanged) ->
        compute trends for changed rows only -> one commit.
        """
        parsed_report_date = self._parse_report_date(report_date)

        stats = self._persist_records(records, parsed_report_date)

        # Trends only matter for rows whose values actually changed —
        # unchanged rows already have whatever trend they had before.
        self._apply_trends(stats.changed_records)

        self._db.commit()

        for record in stats.records:
            self._db.refresh(record)

        logger.info(
            "Committed: Inserted=%d Updated=%d Unchanged=%d",
            len(stats.inserted), len(stats.updated), len(stats.unchanged),
        )
        return stats

    def _persist_records(
        self,
        records: list[dict[str, Any]],
        parsed_report_date: Optional[datetime],
    ) -> PersistStats:
        """
        Upsert normalized records into MacroData, one row per (month, year).

        Existing rows are fetched in a single batched query (keyed on the
        exact (month, year) pairs being processed) rather than one query
        per record, and every processed row is classified as inserted,
        updated, or unchanged so callers can report accurate save counts.

        Update rule unchanged: only overwrite a column when the new value
        is not None — this is what lets refresh_inflation(),
        refresh_fuel_prices(), etc. be called independently over time and
        still converge to one fully populated row per (month, year).

        No merge stage: the extractor already yields at most one record
        per (month, year), so records are persisted directly.

        Flushes (does not commit) so callers can layer additional
        in-transaction work — e.g. trend computation — before the single
        commit in `_persist_and_compute`.
        """
        stats = PersistStats()

        if not records:
            return stats

        keys = [(record["month"], record["year"]) for record in records]
        existing_lookup = self._fetch_existing_records(keys)

        for record in records:
            month = record["month"]
            year = record["year"]
            fields = {k: v for k, v in record.items() if k not in ("month", "year")}

            existing: Optional[MacroData] = existing_lookup.get((month, year))

            if existing:
                changed = False

                for column, value in fields.items():
                    current_value = getattr(existing, column)

                    # Never overwrite an existing non-null value.
                    if current_value is not None:
                        continue

                    if value is not None:
                        setattr(existing, column, value)
                        changed = True

                # Only set report_date if it hasn't been set already.
                if existing.report_date is None and parsed_report_date is not None:
                    existing.report_date = parsed_report_date
                    changed = True

                if changed:
                    logger.debug("Updated %s %s", month, year)
                    stats.updated.append(existing)
                else:
                    logger.debug("No missing values to fill for %s %s", month, year)
                    stats.unchanged.append(existing)
            else:
                logger.debug("Inserting new record for %s %s", month, year)
                new_record = MacroData(
                    month=month,
                    year=year,
                    report_date=parsed_report_date,
                    **fields,
                )
                self._db.add(new_record)
                stats.inserted.append(new_record)

        self._db.flush()

        logger.info(
            "Persistence summary: Inserted=%d Updated=%d Unchanged=%d",
            len(stats.inserted), len(stats.updated), len(stats.unchanged),
        )

        return stats

    def _fetch_existing_records(
        self, keys: list[tuple[str, str]]
    ) -> dict[tuple[str, str], MacroData]:
        """
        Batch-fetch every existing MacroData row for the given (month, year)
        keys in a single query, instead of one query per record.

        Mirrors the over-fetch-then-filter approach used in
        `_fetch_previous_month_records`: queries by month-set x year-set
        (portable across backends, since not every backend supports
        multi-column IN-tuple filters cleanly) and then filters down to
        the exact pairs requested.

        Args:
            keys: (month, year) pairs for every record about to be persisted.

        Returns:
            {(month, year): MacroData} for every key that already exists.
        """
        if not keys:
            return {}

        unique_keys = set(keys)
        months = {month for month, _year in unique_keys}
        years = {year for _month, year in unique_keys}

        candidates = (
            self._db.query(MacroData)
            .filter(MacroData.month.in_(months), MacroData.year.in_(years))
            .all()
        )

        return {
            (candidate.month, candidate.year): candidate
            for candidate in candidates
            if (candidate.month, candidate.year) in unique_keys
        }

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
    # Derived analytics — batched to avoid N+1 queries
    # ------------------------------------------------------------------

    def _apply_trends(self, records: list[MacroData]) -> None:
        """
        Compute inflation_trend and fuel_trend for each record in-memory,
        using a single batched query for all required previous-month rows
        instead of one query per record.

        Only called with inserted/updated records — unchanged rows are
        skipped by the caller since their values (and therefore their
        trends) haven't moved.

        trend = current_value - previous_value
        If no previous month record exists (or the relevant value is
        missing on either side), the trend is left as None.
        """
        if not records:
            return

        needed_keys: set[tuple[str, str]] = set()
        for record in records:
            key = self._previous_month_key(record.month, record.year)
            if key is not None:
                needed_keys.add(key)

        previous_lookup = self._fetch_previous_month_records(needed_keys)

        for record in records:
            key = self._previous_month_key(record.month, record.year)
            previous = previous_lookup.get(key) if key else None

            if previous is None:
                record.inflation_trend = None
                record.fuel_trend = None
                continue

            record.inflation_trend = (
                round(record.inflation - previous.inflation, 4)
                if record.inflation is not None and previous.inflation is not None
                else None
            )
            record.fuel_trend = (
                round(record.fuel_price - previous.fuel_price, 4)
                if record.fuel_price is not None and previous.fuel_price is not None
                else None
            )

        logger.info("Computed inflation and fuel trends for %d record(s)", len(records))

    @staticmethod
    def _previous_month_key(month: str, year: str) -> Optional[tuple[str, str]]:
        """
        Resolve the (month, year) key immediately preceding (month, year),
        walking December -> January across a year boundary.

        Isolated as its own helper so the "monthly" assumption stays in
        one place rather than spread across query logic.

        Returns:
            (previous_month, previous_year), or None if `month`/`year`
            aren't recognisable.
        """
        if month not in MONTH_ORDER:
            logger.debug("Unrecognised month '%s' — cannot resolve previous key.", month)
            return None

        try:
            year_int = int(year)
        except (TypeError, ValueError):
            logger.debug("Unrecognised year '%s' — cannot resolve previous key.", year)
            return None

        index = MONTH_ORDER.index(month)

        if index == 0:
            return "December", str(year_int - 1)
        return MONTH_ORDER[index - 1], str(year_int)

    def _fetch_previous_month_records(
        self, needed_keys: set[tuple[str, str]]
    ) -> dict[tuple[str, str], MacroData]:
        """
        Batch-fetch every MacroData row needed for trend computation in a
        single query, instead of one query per record.

        Over-fetches slightly (queries by month-set x year-set rather than
        exact pair-set, since exact tuple matching isn't portable across
        all backends) and then filters down to exact (month, year) keys —
        cheap in-memory work that avoids N+1 round trips to the DB.

        Args:
            needed_keys: Set of (month, year) pairs required for trends.

        Returns:
            {(month, year): MacroData} for every key that exists in the DB.
        """
        if not needed_keys:
            return {}

        months = {month for month, _year in needed_keys}
        years = {year for _month, year in needed_keys}

        candidates = (
            self._db.query(MacroData)
            .filter(MacroData.month.in_(months), MacroData.year.in_(years))
            .all()
        )

        return {
            (candidate.month, candidate.year): candidate
            for candidate in candidates
            if (candidate.month, candidate.year) in needed_keys
        }

    # ------------------------------------------------------------------
    # Response builders — now consistent with extractor response shape
    # ------------------------------------------------------------------

    def _success_response(
        self,
        *,
        message: str,
        report_date: Optional[str],
        table_name: Optional[str] = None,
        source_url: Optional[str] = None,
        stats: PersistStats,
    ) -> dict[str, Any]:
        """
        Build a success response.

        rows_saved reflects only rows that actually changed
        (inserted + updated) — unchanged rows are reported separately via
        rows_unchanged and are never counted as "saved".
        """
        rows_inserted = len(stats.inserted)
        rows_updated = len(stats.updated)
        rows_unchanged = len(stats.unchanged)

        return {
            "status": "success",
            "message": message,
            "rows_saved": rows_inserted + rows_updated,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_unchanged": rows_unchanged,
            "report_date": report_date,
            "table_name": table_name,
            "source_url": source_url,
            "data": [self._serialize(record) for record in stats.records],
        }

    @staticmethod
    def _failure_response(
        message: str,
        report_date: Optional[str],
        table_name: Optional[str] = None,
        source_url: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Build a failure response, whether the failure originated in the
        service (structural/DB issue) or was propagated from the
        extractor (status != "success"). Extractor metadata is always
        preserved when available rather than discarded.

        rows_inserted / rows_updated / rows_unchanged are intentionally
        omitted from failure responses.
        """
        return {
            "status": "failed",
            "message": message,
            "rows_saved": 0,
            "report_date": report_date,
            "table_name": table_name,
            "source_url": source_url,
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