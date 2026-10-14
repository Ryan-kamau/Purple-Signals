"""
scrapers/macro_csv_parser.py

CSV Parser — reads a macro-data CSV file and returns normalized row dicts.

Pipeline position:

    CSV file  ->  MacroCSVParser  ->  normalized rows + row errors
                                          │
                                          ▼
                                   MacroCSVService  ->  MacroData  ->  MySQL

Expected CSV header (order-insensitive, case-insensitive; extra columns are
ignored with a warning):

    month,year,inflation,fuel_price,cbk_rate,usd_kes_rate,euro_kes_rate,
    pounds_kes_rate,report_date

    7,2026,6.5,217.08,8.75,129.72,174.60,145.37,2026-07

`month`, `year` and the six indicator columns are REQUIRED in the header.
`report_date` is optional; when the column or the cell is blank it is derived
as the 1st of (month, year).

Responsibilities:
  - Open and read the file (utf-8-sig, so Excel BOMs are handled)
  - Validate the header
  - Clean / coerce each row into MacroData-column-named values
  - Report every bad row with its CSV line number and keep going
  - Drop in-file duplicates of (month, year): first occurrence wins

This module does NOT:
  - Touch the database
  - Apply business / plausibility rules (see MacroCSVService)
  - Compute trends
  - Contain FastAPI routes

Output keys are exactly the MacroData column names, so the service layer
needs no field mapping.
"""

import csv
import logging
import math
import os
import re
from datetime import date
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Defined locally (not imported from services.macro_service) so the scrapers
# layer never depends on the services layer.
MONTH_NAMES: list[str] = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

INDICATOR_COLUMNS: tuple[str, ...] = (
    "inflation",
    "fuel_price",
    "cbk_rate",
    "usd_kes_rate",
    "euro_kes_rate",
    "pounds_kes_rate",
)

REQUIRED_COLUMNS: tuple[str, ...] = ("month", "year") + INDICATOR_COLUMNS
OPTIONAL_COLUMNS: tuple[str, ...] = ("report_date",)

MIN_YEAR: int = 2000
MAX_YEAR: int = 2100

# File-level problems (missing file, bad header, ...) are reported with row 0.
FILE_LEVEL_ROW: int = 0

_YEAR_MONTH_PATTERN = re.compile(r"^(\d{4})-(\d{1,2})$")
_MONTH_LOOKUP: dict[str, int] = {name.lower(): i + 1 for i, name in enumerate(MONTH_NAMES)}


class MacroCSVParser:
    """
    Stateless CSV parser for macro-indicator files.

    Example:
        result = MacroCSVParser().parse("data/macro_input.csv")
        for row in result["rows"]:
            ...
    """

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def parse(self, csv_path: str) -> dict[str, Any]:
        """
        Parse a macro CSV file.

        Args:
            csv_path: Path to the CSV file.

        Returns:
            {
                "status": "success" | "partial_success" | "failed",
                "source_file": csv_path,
                "total_rows": int,   # non-blank data rows read
                "rows": [normalized dict, ...],
                "errors": [{"row": <CSV line number, header = 1>, "message": str}],
            }

            "failed"          : file missing/unreadable/empty, header columns
                                missing, or no valid row at all.
            "partial_success" : at least one valid row and at least one error.
            "success"         : no errors.

            Each normalized row holds: month (name), year (str), the six
            indicator floats (or None), report_date (date), plus "_line"
            (the CSV line number, for downstream error reporting).
        """
        logger.info("Macro CSV parse started: %s", csv_path)

        if not csv_path or not os.path.isfile(csv_path):
            return self._failure(csv_path, f"CSV file not found: {csv_path}")

        try:
            with open(csv_path, "r", encoding="utf-8-sig", newline="") as handle:
                return self._parse_stream(handle, csv_path)
        except (OSError, UnicodeDecodeError, csv.Error) as exc:
            logger.error("Could not read CSV %s: %s", csv_path, exc)
            return self._failure(csv_path, f"Could not read CSV file: {exc}")

    # ------------------------------------------------------------------
    # File-level parsing
    # ------------------------------------------------------------------

    def _parse_stream(self, handle: Any, csv_path: str) -> dict[str, Any]:
        reader = csv.reader(handle)

        header_cells = next(reader, None)
        if header_cells is None:
            return self._failure(csv_path, "CSV file is empty.")

        header = [cell.strip().lower() for cell in header_cells]
        column_index: dict[str, int] = {}
        for position, name in enumerate(header):
            if name and name not in column_index:
                column_index[name] = position

        missing = [col for col in REQUIRED_COLUMNS if col not in column_index]
        if missing:
            return self._failure(
                csv_path, f"CSV header is missing required column(s): {missing}"
            )

        known = set(REQUIRED_COLUMNS) | set(OPTIONAL_COLUMNS)
        extras = [name for name in column_index if name not in known]
        if extras:
            logger.warning("Ignoring unrecognised CSV column(s): %s", extras)

        rows: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        first_seen: dict[tuple[str, str], int] = {}
        total_rows = 0

        for cells in reader:
            line = reader.line_num

            if not cells or all(not cell.strip() for cell in cells):
                continue  # blank line

            total_rows += 1
            raw = {
                name: (cells[pos].strip() if pos < len(cells) else "")
                for name, pos in column_index.items()
            }

            try:
                record = self._parse_row(raw)
            except ValueError as exc:
                logger.debug("Row %d rejected: %s", line, exc)
                errors.append({"row": line, "message": str(exc)})
                continue

            key = (record["month"], record["year"])
            if key in first_seen:
                errors.append(
                    {
                        "row": line,
                        "message": (
                            f"Duplicate (month, year) = {key} in file; "
                            f"first occurrence on line {first_seen[key]} kept."
                        ),
                    }
                )
                continue

            first_seen[key] = line
            record["_line"] = line
            rows.append(record)

        if total_rows == 0:
            return self._failure(csv_path, "CSV file contains no data rows.")

        if not rows:
            status = "failed"
        elif errors:
            status = "partial_success"
        else:
            status = "success"

        logger.info(
            "Macro CSV parse finished: status=%s total=%d valid=%d errors=%d",
            status, total_rows, len(rows), len(errors),
        )

        return {
            "status": status,
            "source_file": csv_path,
            "total_rows": total_rows,
            "rows": rows,
            "errors": errors,
        }

    @staticmethod
    def _failure(csv_path: Optional[str], message: str) -> dict[str, Any]:
        logger.error("Macro CSV parse failed: %s", message)
        return {
            "status": "failed",
            "source_file": csv_path,
            "total_rows": 0,
            "rows": [],
            "errors": [{"row": FILE_LEVEL_ROW, "message": message}],
        }

    # ------------------------------------------------------------------
    # Row-level parsing — every helper raises ValueError(<message>) on bad input
    # ------------------------------------------------------------------

    def _parse_row(self, raw: dict[str, str]) -> dict[str, Any]:
        month_number = self._parse_month(raw.get("month"))
        year_number = self._parse_year(raw.get("year"))

        values = {
            column: self._parse_number(column, raw.get(column))
            for column in INDICATOR_COLUMNS
        }
        if all(value is None for value in values.values()):
            raise ValueError("No indicator values present in row.")

        report_date = self._parse_report_date(
            raw.get("report_date"), month_number, year_number
        )

        return {
            "month": MONTH_NAMES[month_number - 1],
            "year": str(year_number),
            **values,
            "report_date": report_date,
        }

    @staticmethod
    def _parse_month(text: Optional[str]) -> int:
        """Accept 1-12 (e.g. '7', '07') or a full month name (any case)."""
        value = (text or "").strip()
        if not value:
            raise ValueError("month is empty.")

        if value.isdigit():
            number = int(value)
            if 1 <= number <= 12:
                return number
            raise ValueError(f"month {value!r} is not in 1-12.")

        number = _MONTH_LOOKUP.get(value.lower())
        if number is None:
            raise ValueError(f"month {value!r} is not a recognised month.")
        return number

    @staticmethod
    def _parse_year(text: Optional[str]) -> int:
        value = (text or "").strip()
        if not value.isdigit():
            raise ValueError(f"year {value!r} is not a valid integer.")
        number = int(value)
        if not MIN_YEAR <= number <= MAX_YEAR:
            raise ValueError(f"year {number} is outside {MIN_YEAR}-{MAX_YEAR}.")
        return number

    @staticmethod
    def _parse_number(column: str, text: Optional[str]) -> Optional[float]:
        """Blank -> None; thousands commas stripped; non-numeric -> error."""
        value = (text or "").replace(",", "").strip()
        if not value:
            return None
        try:
            number = float(value)
        except ValueError:
            raise ValueError(f"{column} {text!r} is not numeric.") from None
        if not math.isfinite(number):
            raise ValueError(f"{column} {text!r} is not a finite number.")
        return number

    @staticmethod
    def _parse_report_date(
        text: Optional[str], month_number: int, year_number: int
    ) -> date:
        """
        'YYYY-MM' or 'YYYY-MM-DD' -> date anchored to the 1st of that month.
        Blank -> the 1st of (month, year) from the same row.
        """
        value = (text or "").strip()
        if not value:
            return date(year_number, month_number, 1)

        match = _YEAR_MONTH_PATTERN.match(value)
        try:
            if match:
                return date(int(match.group(1)), int(match.group(2)), 1)
            return date.fromisoformat(value).replace(day=1)
        except ValueError:
            raise ValueError(
                f"report_date {value!r} is not a valid YYYY-MM or YYYY-MM-DD."
            ) from None


# ---------------------------------------------------------------------------
# CLI smoke-test:  python -m scrapers.macro_csv_parser path/to/file.csv
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    if len(sys.argv) < 2:
        print("Usage: python -m scrapers.macro_csv_parser <csv_path>")
        sys.exit(2)

    print(json.dumps(MacroCSVParser().parse(sys.argv[1]), indent=2, default=str))