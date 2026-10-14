"""
services/macro_csv_service.py

Service Layer — owns ALL business logic for CSV-based macroeconomic ingestion.

Pipeline position:

    caller (script / tester / future route)
        │
        ▼
    MacroCSVService                  <- this file
        │
        ▼
    MacroCSVParser                   (scrapers/macro_csv_parser.py)
        │
        ▼
    normalized rows + row errors
        │
        ▼
    MacroCSVService
        validate (plausibility ranges)
        → persist (upsert, "fill NULL columns only")
        → classify rows as inserted / updated / unchanged
        → compute trends (inserted/updated rows only, batched)
        → single commit
        → build response
        │
        ▼
    MacroData ORM  ->  MySQL

Behaves like MacroService (services/macro_service.py), with a CSV as the
source instead of the KNBS PDF extractor:
  - Existing (month, year) rows are never overwritten; only NULL columns
    are filled.
  - inflation_trend / fuel_trend = current - previous month, computed for
    inserted/updated rows only, using one batched lookup.
  - One transaction, one commit per run.

Bad rows (parse or validation failures) are skipped and reported with their
CSV line number; good rows are still saved.

This module does NOT:
  - Parse CSV text (see MacroCSVParser)
  - Contain FastAPI routes
  - Open or close its own database session
  - Modify MacroService

Known duplication (tracked in backlog): the upsert + trend logic below
mirrors MacroService. Extract a shared helper after the MVP.
"""

import logging
from typing import Any, Optional

from sqlalchemy.orm import Session
from dataclasses import dataclass, field
from models.macro_data import MacroData
from scrapers.macro_csv import INDICATOR_COLUMNS, MacroCSVParser

logger = logging.getLogger(__name__)

# Plausibility ranges (inclusive). None values always pass.
# Deliberately loose — they exist to catch typos and unit mistakes, not to
# police real economic movements. Tune as needed.
VALIDATION_RANGES: dict[str, tuple[float, float]] = {
    "inflation": (-5.0, 50.0),
    "fuel_price": (0.01, 1000.0),
    "cbk_rate": (0.0, 50.0),
    "usd_kes_rate": (0.01, 1000.0),
    "euro_kes_rate": (0.01, 1000.0),
    "pounds_kes_rate": (0.01, 1000.0),
}

MONTH_ORDER: list[str] = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

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



class MacroCSVService:
    """
    Orchestrates CSV -> MacroData ingestion.

    Example:
        service = MacroCSVService(db)
        result  = service.ingest_csv("data/macro_input.csv")
    """

    def __init__(self, db: Session, parser: Optional[MacroCSVParser] = None) -> None:
        """
        Args:
            db:     Active SQLAlchemy session (injected). This service never
                    opens or closes its own session.
            parser: Optional MacroCSVParser override (useful for testing / DI).
        """
        self._db = db
        self._parser = parser or MacroCSVParser()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def ingest_csv(self, csv_path: str) -> dict[str, Any]:
        """
        Run the full pipeline for one CSV file:

            parse -> validate -> persist (insert / fill-NULL update)
            -> trends (changed rows) -> single commit -> response

        Args:
            csv_path: Path to a CSV in the documented macro format.

        Returns:
            {
                "status": "success" | "partial_success" | "failed",
                "message": str,
                "source_file": str,
                "rows_read": int,
                "rows_saved": int,          # inserted + updated
                "rows_inserted": int,
                "rows_updated": int,
                "rows_unchanged": int,
                "rows_skipped": int,        # parse errors + validation errors
                "errors": [{"row": int, "message": str}, ...],
                "data": [serialized MacroData dicts],
            }
        """
        logger.info("Macro CSV ingestion started: %s", csv_path)

        parsed = self._parser.parse(csv_path)
        rows_read = parsed.get("total_rows", 0)
        parse_errors = list(parsed.get("errors", []))

        if parsed.get("status") == "failed":
            message = parse_errors[0]["message"] if parse_errors else "CSV parsing failed."
            return self._failure_response(csv_path, rows_read, parse_errors, message)

        records, validation_errors = self._validate_rows(parsed.get("rows", []))
        errors = sorted(parse_errors + validation_errors, key=lambda e: e["row"])

        if not records:
            return self._failure_response(
                csv_path, rows_read, errors, "No valid rows to ingest."
            )

        try:
            stats = self._persist_and_compute(records)
        except Exception as exc:  # noqa: BLE001
            self._db.rollback()
            logger.error("Database transaction rolled back for %s: %s", csv_path, exc)
            return self._failure_response(
                csv_path, rows_read, errors, "Database transaction rolled back."
            )

        return self._success_response(csv_path, rows_read, errors, stats)

    # ------------------------------------------------------------------
    # Validation (business rules — syntax was already handled by the parser)
    # ------------------------------------------------------------------

    def _validate_rows(
        self, rows: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """
        Apply plausibility rules to parsed rows.

        Returns:
            (clean_records, errors). Clean records have the parser's "_line"
            key removed, so they contain only MacroData-ready fields.
        """
        clean: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []

        for row in rows:
            line = row.get("_line", 0)
            problem = self._find_problem(row)

            if problem:
                logger.warning("Skipping CSV line %s: %s", line, problem)
                errors.append({"row": line, "message": problem})
                continue

            clean.append({key: value for key, value in row.items() if key != "_line"})

        return clean, errors

    @staticmethod
    def _find_problem(row: dict[str, Any]) -> Optional[str]:
        """Return a human-readable problem description, or None if the row is OK."""
        if row.get("month") not in MONTH_ORDER:
            return f"month {row.get('month')!r} is not a recognised month name."
        if not row.get("year"):
            return "year is missing."

        for column, (low, high) in VALIDATION_RANGES.items():
            value = row.get(column)
            if value is not None and not low <= value <= high:
                return f"{column}={value} is outside the plausible range [{low}, {high}]."

        return None

    # ------------------------------------------------------------------
    # Persistence + trends — one transaction
    # ------------------------------------------------------------------

    def _persist_and_compute(self, records: list[dict[str, Any]]) -> PersistStats:
        """persist -> classify -> flush -> trends for changed rows -> one commit."""
        stats = self._persist_records(records)

        # Flushed inside _persist_records, so previous-month rows that were
        # inserted by this same file are visible to the trend lookup below.
        self._apply_trends(stats.changed_records)

        self._db.commit()

        for record in stats.records:
            self._db.refresh(record)

        logger.info(
            "Committed: Inserted=%d Updated=%d Unchanged=%d",
            len(stats.inserted), len(stats.updated), len(stats.unchanged),
        )
        return stats

    def _persist_records(self, records: list[dict[str, Any]]) -> PersistStats:
        """
        Upsert records into MacroData, one row per (month, year).

        Existing rows are fetched in one batched query. For an existing row,
        a column is written ONLY if it is currently NULL and the CSV value is
        not None — real data is never overwritten. report_date is set only if
        currently NULL.
        """
        stats = PersistStats()

        keys = [(record["month"], record["year"]) for record in records]
        existing_lookup = self._fetch_existing_records(keys)

        for record in records:
            key = (record["month"], record["year"])
            existing: Optional[MacroData] = existing_lookup.get(key)

            if existing is not None:
                changed = False

                for column in INDICATOR_COLUMNS:
                    new_value = record.get(column)
                    if new_value is not None and getattr(existing, column) is None:
                        setattr(existing, column, new_value)
                        changed = True

                if existing.report_date is None and record.get("report_date") is not None:
                    existing.report_date = record["report_date"]
                    changed = True

                (stats.updated if changed else stats.unchanged).append(existing)
                continue

            new_record = MacroData(
                month=record["month"],
                year=record["year"],
                report_date=record.get("report_date"),
                **{column: record.get(column) for column in INDICATOR_COLUMNS},
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
        Batch-fetch existing MacroData rows for the given (month, year) keys
        in one query (month-set x year-set over-fetch, then exact-pair filter).
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
            (row.month, row.year): row
            for row in candidates
            if (row.month, row.year) in unique_keys
        }

    # ------------------------------------------------------------------
    # Derived analytics — batched, same semantics as MacroService
    # ------------------------------------------------------------------

    def _apply_trends(self, records: list[MacroData]) -> None:
        """
        trend = current_value - previous_month_value (rounded to 4 dp).
        Left as None when the previous month row, or either value, is missing.
        """
        if not records:
            return

        needed_keys: set[tuple[str, str]] = set()
        for record in records:
            key = self._previous_month_key(record.month, record.year)
            if key is not None:
                needed_keys.add(key)

        previous_lookup = self._fetch_existing_records(list(needed_keys))

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
        """(month, year) of the prior calendar month; December wraps the year."""
        if month not in MONTH_ORDER:
            return None

        try:
            year_int = int(year)
        except (TypeError, ValueError):
            return None

        index = MONTH_ORDER.index(month)
        if index == 0:
            return "December", str(year_int - 1)
        return MONTH_ORDER[index - 1], str(year_int)

    # ------------------------------------------------------------------
    # Response builders
    # ------------------------------------------------------------------

    def _success_response(
        self,
        csv_path: str,
        rows_read: int,
        errors: list[dict[str, Any]],
        stats: PersistStats,
    ) -> dict[str, Any]:
        inserted = len(stats.inserted)
        updated = len(stats.updated)
        unchanged = len(stats.unchanged)
        skipped = len(errors)

        status = "success" if skipped == 0 else "partial_success"
        message = (
            f"Macro CSV ingested: inserted={inserted} updated={updated} "
            f"unchanged={unchanged} skipped={skipped}."
        )

        return {
            "status": status,
            "message": message,
            "source_file": csv_path,
            "rows_read": rows_read,
            "rows_saved": inserted + updated,
            "rows_inserted": inserted,
            "rows_updated": updated,
            "rows_unchanged": unchanged,
            "rows_skipped": skipped,
            "errors": errors,
            "data": [self._serialize(record) for record in stats.records],
        }

    @staticmethod
    def _failure_response(
        csv_path: str,
        rows_read: int,
        errors: list[dict[str, Any]],
        message: str,
    ) -> dict[str, Any]:
        """Same keys as the success envelope so callers never branch on shape."""
        return {
            "status": "failed",
            "message": message,
            "source_file": csv_path,
            "rows_read": rows_read,
            "rows_saved": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_unchanged": 0,
            "rows_skipped": len(errors),
            "errors": errors,
            "data": [],
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


# ---------------------------------------------------------------------------
# CLI smoke-test:  python -m services.macro_csv_service path/to/file.csv
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    if len(sys.argv) < 2:
        print("Usage: python -m services.macro_csv_service <csv_path>")
        sys.exit(2)

    from database.session import SessionLocal

    session = SessionLocal()
    try:
        output = MacroCSVService(session).ingest_csv(sys.argv[1])
        print(json.dumps(output, indent=2, default=str))
    finally:
        session.close()