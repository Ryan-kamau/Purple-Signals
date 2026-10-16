"""
scrapers/fundamentals_csv.py

Parse layer for fundamentals CSV files.

Responsibilities:
  - Open the file, validate the header
  - Yield raw rows (all values kept as stripped strings)
  - Report row-level structural problems

Does NOT: validate business rules, convert numbers/dates, touch the DB.
That all lives in services/fundamentals_service.py.

Expected header (case-insensitive, order irrelevant):
  Required: ticker, report_date, eps, revenue, net_profit,
            shareholders_equity, total_liabilities, dividend_per_share
  Optional: pe_ratio, dividend_yield, debt_ratio, net_profit_margin
"""

import csv
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS: tuple[str, ...] = (
    "ticker",
    "report_date",
    "eps",
    "revenue",
    "net_profit",
    "shareholders_equity",
    "total_liabilities",
    "dividend_per_share",
)

OPTIONAL_COLUMNS: tuple[str, ...] = (
    "pe_ratio",
    "dividend_yield",
    "debt_ratio",
    "net_profit_margin",
)


class CSVParseError(Exception):
    """File-level problem (missing file, empty file, bad header, bad encoding)."""


def parse_fundamentals_csv(path: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Read a fundamentals CSV.

    Args:
        path: Local filesystem path to the CSV.

    Returns:
        (rows, errors)
          rows:   [{"row": <1-based file line number>, "values": {column: str}}]
          errors: [{"row": int, "error": str}] for structurally broken rows.

    Raises:
        CSVParseError: for file-level problems.
    """
    if not os.path.isfile(path):
        raise CSVParseError(f"CSV file not found: {path}")

    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)

            if not reader.fieldnames:
                raise CSVParseError("CSV file is empty or has no header row.")

            reader.fieldnames = [name.strip().lower() for name in reader.fieldnames]

            missing = [c for c in REQUIRED_COLUMNS if c not in reader.fieldnames]
            if missing:
                raise CSVParseError(f"CSV header is missing required column(s): {missing}")

            for record in reader:
                line_no = reader.line_num

                if None in record:
                    errors.append({"row": line_no, "error": "Row has more values than header columns."})
                    continue

                values = {key: (value or "").strip() for key, value in record.items()}

                if not any(values.values()):
                    continue  # blank line

                rows.append({"row": line_no, "values": values})

    except UnicodeDecodeError as exc:
        raise CSVParseError(f"CSV is not valid UTF-8: {exc}") from exc
    except csv.Error as exc:
        raise CSVParseError(f"Malformed CSV: {exc}") from exc

    logger.info("Parsed fundamentals CSV: rows=%d row_errors=%d", len(rows), len(errors))
    return rows, errors