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

Persistence and trend calculations are delegated to the shared MacroRepository.
"""

import logging
from typing import Any, Optional

from sqlalchemy.orm import Session
from core.periods import MONTH_NAMES
from services.macro_repository import MacroRepository, PersistStats, serialize_macro
from scrapers.macro_csv import MacroCSVParser

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
        self._repo = MacroRepository(db)
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
        if row.get("month") not in MONTH_NAMES:
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
        stats = self._repo.upsert(records)

        # Flushed inside MacroRepository.upsert, so previous-month rows that were
        # inserted by this same file are visible to the trend lookup below.
        self._repo.apply_trends(stats.changed_records)

        self._db.commit()

        for record in stats.records:
            self._db.refresh(record)

        logger.info(
            "Committed: Inserted=%d Updated=%d Unchanged=%d",
            len(stats.inserted), len(stats.updated), len(stats.unchanged),
        )
        return stats

    # ------------------------------------------------------------------
    # Derived analytics — batched, same semantics as MacroService
    # ------------------------------------------------------------------

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
            "data": [serialize_macro(record) for record in stats.records],
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

