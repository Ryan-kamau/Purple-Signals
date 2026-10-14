"""
testers/macro_csv_tester.py

Standalone, manual, real-database end-to-end tester for the CSV macro
ingestion pipeline (MacroCSVParser + MacroCSVService).

    python -m testers.macro_csv_tester                      # uses DEFAULT_CSV_PATH
    python -m testers.macro_csv_tester path/to/macro.csv
    python -m testers.macro_csv_tester path/to/macro.csv --no-write

WHAT THIS IS
    A plain runnable script — NOT pytest/unittest, no mocks, no fixtures.
    Section A exercises the parser alone with throw-away temp files (no DB).
    Sections B-F run the real service against the real MySQL database via
    SessionLocal, then independently re-query the database (fresh sessions)
    to confirm what the service reported is what actually got persisted.

WRITES TO THE REAL DATABASE
    Sections B, C, E and F insert rows into macro_data and leave them there
    (no cleanup), consistent with the other testers. Section F inserts a row
    for January 2099 — delete it manually if you don't want it.
    --no-write skips B, C, E and F. Section D never changes data by design
    (it proves existing values are NOT overwritten).

SECTIONS
    A  Parser only (temp CSVs, no DB)
    B  Service on the real CSV: counts, statuses, persisted values
    C  Idempotency: second run inserts/updates nothing
    D  Never-overwrite rule on an existing row
    E  Trend values on rows inserted in B
    F  Error path: bad row + out-of-range row, line numbers reported

The CSV itself is yours to supply: CSV_PATH defaults to DEFAULT_CSV_PATH
below, or pass a path as the first non-flag argument.
"""

import os
import sys
import tempfile
from contextlib import contextmanager
from datetime import date
from typing import Any, Iterator, Optional

from sqlalchemy import func, select

from database.session import SessionLocal
from models.macro_data import MacroData
from scrapers.macro_csv import INDICATOR_COLUMNS, MONTH_NAMES, MacroCSVParser
from services.macro_csv_service import MacroCSVService

# ===========================================================================
# CONFIGURATION
# ===========================================================================

DEFAULT_CSV_PATH = "data/macro_input.csv"

HEADER = (
    "month,year,inflation,fuel_price,cbk_rate,usd_kes_rate,"
    "euro_kes_rate,pounds_kes_rate,report_date"
)

# Synthetic rows used ONLY by the parser-only section (no DB involved).
ROW_JUL = "7,2026,6.5,217.08,8.75,129.72,174.60,145.37,2026-07"
ROW_AUG = "8,2026,6.6,217.07,8.75,129.72,174.60,145.37,2026-08"

ENVELOPE_KEYS = (
    "status", "message", "source_file", "rows_read", "rows_saved",
    "rows_inserted", "rows_updated", "rows_unchanged", "rows_skipped",
    "errors", "data",
)

# ===========================================================================
# Result tracking — simple counters, no test framework
# ===========================================================================

_PASS = 0
_FAIL = 0
_SKIP = 0
_TEMP_FILES: list[str] = []


def check(condition: bool, message: str) -> bool:
    """Record and print a PASS/FAIL for one assertion. Never raises."""
    global _PASS, _FAIL
    if condition:
        _PASS += 1
        print(f"[PASS] {message}")
    else:
        _FAIL += 1
        print(f"[FAIL] {message}")
    return condition


def skip(message: str) -> None:
    global _SKIP
    _SKIP += 1
    print(f"[SKIP] {message}")


def section(title: str) -> None:
    print("\n" + "-" * 70)
    print(title)
    print("-" * 70)


def print_result(label: str, result: dict) -> None:
    print(f"\n{label} result:")
    for key, value in result.items():
        if key == "data":
            print(f"  data: {len(value)} record(s)")
        elif key == "errors":
            print(f"  errors: {len(value)} entrie(s)")
            for err in value[:5]:
                print(f"    - line {err.get('row')}: {err.get('message')}")
        else:
            print(f"  {key}: {value}")


# ===========================================================================
# Helpers
# ===========================================================================

def write_temp_csv(content: str, encoding: str = "utf-8") -> str:
    """Write a throw-away CSV and remember it for cleanup."""
    fd, path = tempfile.mkstemp(suffix=".csv", prefix="macro_csv_test_")
    os.close(fd)
    with open(path, "w", encoding=encoding, newline="") as handle:
        handle.write(content)
    _TEMP_FILES.append(path)
    return path


def csv_text(*rows: str, header: str = HEADER) -> str:
    """Header on line 1, then one line per row (so row N is on line N + 1)."""
    return header + "\n" + "\n".join(rows) + "\n"


def approx(left: Optional[float], right: Optional[float], tol: float = 1e-6) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return abs(float(left) - float(right)) <= tol


@contextmanager
def fresh_session() -> Iterator[Any]:
    """
    Short-lived session for verification queries. A NEW session per query
    avoids REPEATABLE READ snapshots hiding what the service just committed.
    """
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def snapshot_keys() -> set[tuple[str, str]]:
    with fresh_session() as session:
        rows = session.execute(select(MacroData.month, MacroData.year)).all()
    return {(month, year) for month, year in rows}


def count_rows() -> int:
    with fresh_session() as session:
        return session.execute(select(func.count(MacroData.id))).scalar_one()


def fetch_row(month: str, year: str) -> Optional[MacroData]:
    with fresh_session() as session:
        return session.execute(
            select(MacroData).where(MacroData.month == month, MacroData.year == year)
        ).scalars().first()


def previous_key(month: str, year: str) -> Optional[tuple[str, str]]:
    """Independent re-implementation of 'previous calendar month'."""
    if month not in MONTH_NAMES or not str(year).isdigit():
        return None
    index = MONTH_NAMES.index(month)
    if index == 0:
        return "December", str(int(year) - 1)
    return MONTH_NAMES[index - 1], str(year)


def chronological(key: tuple[str, str]) -> tuple[int, int]:
    month, year = key
    return int(year), MONTH_NAMES.index(month)


def trend(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous is None:
        return None
    return round(current - previous, 4)


# ===========================================================================
# Section A — parser only (no DB)
# ===========================================================================

def test_parser() -> None:
    section("A. MacroCSVParser (temp files, no database)")
    parser = MacroCSVParser()

    # --- valid file ----------------------------------------------------
    result = parser.parse(write_temp_csv(csv_text(ROW_JUL, ROW_AUG)))
    check(result["status"] == "success", "valid file -> status 'success'")
    check(result["total_rows"] == 2 and len(result["rows"]) == 2, "valid file -> 2 rows parsed")
    first = result["rows"][0] if result["rows"] else {}
    check(first.get("month") == "July", "month 7 -> 'July'")
    check(first.get("year") == "2026", "year -> str '2026'")
    check(first.get("inflation") == 6.5 and first.get("pounds_kes_rate") == 145.37,
          "indicator values parsed as floats")
    check(first.get("report_date") == date(2026, 7, 1), "report_date '2026-07' -> date(2026, 7, 1)")
    check(first.get("_line") == 2, "first data row carries line number 2")

    # --- header problems -----------------------------------------------
    no_inflation_header = HEADER.replace("inflation,", "")
    result = parser.parse(write_temp_csv(csv_text("7,2026,217.08,8.75,129.72,174.60,145.37,2026-07",
                                                  header=no_inflation_header)))
    check(result["status"] == "failed" and result["rows"] == [] and bool(result["errors"]),
          "missing required header column -> 'failed'")

    result = parser.parse(write_temp_csv(
        csv_text("2026,7,x,6.5,217.08,8.75,129.72,174.60,145.37",
                 header="year,month,extra,inflation,fuel_price,cbk_rate,usd_kes_rate,euro_kes_rate,pounds_kes_rate")
    ))
    check(result["status"] == "success" and result["rows"] and result["rows"][0]["month"] == "July",
          "header order-insensitive; extra column ignored")

    result = parser.parse(write_temp_csv(
        csv_text("7,2026,6.5,217.08,8.75,129.72,174.60,145.37",
                 header=HEADER.replace(",report_date", ""))
    ))
    check(result["status"] == "success" and result["rows"]
          and result["rows"][0]["report_date"] == date(2026, 7, 1),
          "no report_date column -> derived as 1st of (month, year)")

    # --- row problems --------------------------------------------------
    result = parser.parse(write_temp_csv(csv_text(ROW_JUL, "13,2026,6.5,217.08,8.75,129.72,174.60,145.37,2026-07")))
    check(result["status"] == "partial_success" and len(result["rows"]) == 1,
          "month 13 -> partial_success, good row kept")
    check(bool(result["errors"]) and result["errors"][0]["row"] == 3,
          "month 13 error reported on CSV line 3")

    result = parser.parse(write_temp_csv(csv_text(ROW_AUG, "7,2026,abc,217.08,8.75,129.72,174.60,145.37,2026-07")))
    check(result["status"] == "partial_success" and result["errors"]
          and result["errors"][0]["row"] == 3 and "inflation" in result["errors"][0]["message"],
          "non-numeric inflation -> row error naming the column")

    result = parser.parse(write_temp_csv(csv_text("7,2026,,217.08,8.75,129.72,174.60,145.37,2026-07")))
    check(result["status"] == "success" and result["rows"] and result["rows"][0]["inflation"] is None
          and result["rows"][0]["fuel_price"] == 217.08,
          "blank cell -> None, row still valid")

    result = parser.parse(write_temp_csv(csv_text(ROW_AUG, "7,2026,6.5,217.08,8.75,129.72,174.60,145.37,2026-13")))
    check(result["status"] == "partial_success" and result["errors"] and result["errors"][0]["row"] == 3,
          "bad report_date '2026-13' -> row error on line 3")

    result = parser.parse(write_temp_csv(csv_text("7,2026,6.5,217.08,8.75,129.72,174.60,145.37,")))
    check(result["status"] == "success" and result["rows"]
          and result["rows"][0]["report_date"] == date(2026, 7, 1),
          "blank report_date cell -> derived date(2026, 7, 1)")

    result = parser.parse(write_temp_csv(csv_text(ROW_AUG, "7,2026,,,,,,,2026-07")))
    check(result["status"] == "partial_success" and result["errors"] and result["errors"][0]["row"] == 3,
          "row with all indicators blank -> row error")

    result = parser.parse(write_temp_csv(csv_text("July,2026,6.5,217.08,8.75,129.72,174.60,145.37,2026-07")))
    check(result["status"] == "success" and result["rows"] and result["rows"][0]["month"] == "July",
          "month name 'July' accepted")

    result = parser.parse(write_temp_csv(csv_text(ROW_JUL, "7,2026,9.9,217.08,8.75,129.72,174.60,145.37,2026-07")))
    check(len(result["rows"]) == 1 and result["rows"][0]["inflation"] == 6.5,
          "in-file duplicate -> first occurrence kept")
    check(bool(result["errors"]) and result["errors"][0]["row"] == 3,
          "in-file duplicate reported on line 3")

    result = parser.parse(write_temp_csv(csv_text(ROW_JUL, "", ROW_AUG)))
    check(result["total_rows"] == 2 and len(result["rows"]) == 2, "blank lines skipped silently")

    # --- file problems -------------------------------------------------
    result = parser.parse(write_temp_csv(csv_text(ROW_JUL), encoding="utf-8-sig"))
    check(result["status"] == "success", "UTF-8 BOM file parsed correctly")

    result = parser.parse(write_temp_csv(""))
    check(result["status"] == "failed", "empty file -> 'failed'")

    result = parser.parse(write_temp_csv(HEADER + "\n"))
    check(result["status"] == "failed", "header-only file -> 'failed'")

    result = parser.parse(os.path.join(tempfile.gettempdir(), "definitely_missing_macro.csv"))
    check(result["status"] == "failed" and bool(result["errors"]), "nonexistent path -> 'failed'")


# ===========================================================================
# Section B — service on the real CSV
# ===========================================================================

def test_real_csv_ingest(service: MacroCSVService, csv_path: str) -> Optional[dict[str, Any]]:
    section(f"B. Service end-to-end on real CSV: {csv_path}")

    if not os.path.isfile(csv_path):
        skip(f"CSV not found at '{csv_path}' — pass a path or create the file")
        return None

    parsed = MacroCSVParser().parse(csv_path)
    if not check(parsed["status"] != "failed", f"parser accepts the CSV (status={parsed['status']})"):
        print_result("parser", parsed)
        return None

    parsed_by_key = {(row["month"], row["year"]): row for row in parsed["rows"]}

    before_keys = snapshot_keys()
    before_count = count_rows()

    result = service.ingest_csv(csv_path)
    print_result("ingest_csv", result)

    check(isinstance(result, dict), "ingest_csv() returns a dict")
    for key in ENVELOPE_KEYS:
        check(key in result, f"envelope contains '{key}'")

    check(result.get("status") in ("success", "partial_success"),
          f"status is success/partial_success (got {result.get('status')})")

    after_keys = snapshot_keys()
    after_count = count_rows()
    inserted_keys = after_keys - before_keys

    check(after_count - before_count == result.get("rows_inserted", -1),
          f"DB row delta ({after_count - before_count}) == rows_inserted ({result.get('rows_inserted')})")
    check(len(inserted_keys) == result.get("rows_inserted", -1),
          f"new (month, year) keys ({len(inserted_keys)}) == rows_inserted")
    check(result.get("rows_saved") == result.get("rows_inserted", 0) + result.get("rows_updated", 0),
          "rows_saved == rows_inserted + rows_updated")
    check(result.get("rows_read") == parsed["total_rows"],
          f"rows_read ({result.get('rows_read')}) == rows in CSV ({parsed['total_rows']})")
    check(result.get("rows_skipped", 0) >= len(parsed["errors"]),
          "rows_skipped covers every parser error")

    mismatches: list[str] = []
    for key in sorted(inserted_keys, key=chronological):
        csv_row = parsed_by_key.get(key)
        db_row = fetch_row(*key)
        if csv_row is None:
            mismatches.append(f"{key}: inserted but not present in CSV")
            continue
        if db_row is None:
            mismatches.append(f"{key}: not retrievable from DB")
            continue
        for column in INDICATOR_COLUMNS:
            if not approx(getattr(db_row, column), csv_row[column]):
                mismatches.append(f"{key} {column}: db={getattr(db_row, column)} csv={csv_row[column]}")
        if db_row.report_date != csv_row["report_date"]:
            mismatches.append(f"{key} report_date: db={db_row.report_date} csv={csv_row['report_date']}")

    check(not mismatches,
          f"all {len(inserted_keys)} inserted row(s) match the CSV values"
          + (f" — mismatches: {mismatches[:5]}" if mismatches else ""))

    return {"result": result, "inserted_keys": inserted_keys}


# ===========================================================================
# Section C — idempotency
# ===========================================================================

def test_idempotency(service: MacroCSVService, csv_path: str, first: Optional[dict[str, Any]]) -> None:
    section("C. Idempotency (second run of the same CSV)")

    if first is None:
        skip("idempotency: section B did not run")
        return

    first_result = first["result"]
    before_count = count_rows()

    second = service.ingest_csv(csv_path)
    print_result("ingest_csv (second run)", second)

    expected_unchanged = (
        first_result["rows_inserted"] + first_result["rows_updated"] + first_result["rows_unchanged"]
    )

    check(second["rows_inserted"] == 0, "second run inserts 0 rows")
    check(second["rows_updated"] == 0, "second run updates 0 rows")
    check(second["rows_unchanged"] == expected_unchanged,
          f"second run unchanged ({second['rows_unchanged']}) == valid rows from first run ({expected_unchanged})")
    check(count_rows() == before_count, "DB row count unchanged by second run")


# ===========================================================================
# Section D — never overwrite existing values
# ===========================================================================

def test_never_overwrite(service: MacroCSVService) -> None:
    section("D. Existing non-null values are never overwritten")

    with fresh_session() as session:
        candidate = session.execute(
            select(MacroData)
            .where(MacroData.inflation.is_not(None), MacroData.report_date.is_not(None))
            .order_by(MacroData.id.desc())
            .limit(1)
        ).scalars().first()

    if candidate is None:
        skip("never-overwrite: no existing row with non-null inflation and report_date")
        return

    month, year, original = candidate.month, candidate.year, candidate.inflation

    if month not in MONTH_NAMES or not str(year).isdigit() or not -5.0 <= original <= 50.0:
        skip(f"never-overwrite: candidate row ({month}, {year}, inflation={original}) not usable")
        return

    different = original + 1.0 if original < 40.0 else original - 1.0
    month_number = MONTH_NAMES.index(month) + 1
    path = write_temp_csv(csv_text(f"{month_number},{year},{different},,,,,,"))

    result = service.ingest_csv(path)
    print_result(f"ingest_csv (conflicting value for {month} {year})", result)

    check(result["rows_inserted"] == 0, "no new row inserted for an existing (month, year)")
    check(result["rows_updated"] == 0 and result["rows_unchanged"] == 1,
          "existing row reported as unchanged (nothing was NULL to fill)")

    after = fetch_row(month, year)
    check(after is not None and approx(after.inflation, original),
          f"inflation for {month} {year} still {original} (CSV tried {different})")


# ===========================================================================
# Section E — trends
# ===========================================================================

def test_trends(first: Optional[dict[str, Any]]) -> None:
    section("E. Trends on rows inserted in section B")

    if first is None:
        skip("trends: section B did not run")
        return

    inserted_keys = first["inserted_keys"]
    if not inserted_keys:
        skip("trends: section B inserted no rows (CSV already ingested earlier)")
        return

    mismatches: list[str] = []
    for key in sorted(inserted_keys, key=chronological):
        current = fetch_row(*key)
        if current is None:
            mismatches.append(f"{key}: row missing")
            continue

        prev_key = previous_key(*key)
        previous = fetch_row(*prev_key) if prev_key else None

        expected_inflation = trend(current.inflation, previous.inflation) if previous else None
        expected_fuel = trend(current.fuel_price, previous.fuel_price) if previous else None

        if not approx(current.inflation_trend, expected_inflation):
            mismatches.append(f"{key} inflation_trend: db={current.inflation_trend} expected={expected_inflation}")
        if not approx(current.fuel_trend, expected_fuel):
            mismatches.append(f"{key} fuel_trend: db={current.fuel_trend} expected={expected_fuel}")

    check(not mismatches,
          f"inflation_trend / fuel_trend correct for {len(inserted_keys)} inserted row(s)"
          + (f" — mismatches: {mismatches[:5]}" if mismatches else ""))


# ===========================================================================
# Section F — error path
# ===========================================================================

def test_error_path(service: MacroCSVService) -> None:
    section("F. Error path: bad month + out-of-range value (writes a Jan 2099 row)")

    good = "1,2099,5.0,200.0,9.0,130.0,175.0,150.0,2099-01"
    bad_month = "13,2099,5.0,200.0,9.0,130.0,175.0,150.0,2099-01"
    out_of_range = "2,2099,500,200.0,9.0,130.0,175.0,150.0,2099-02"

    path = write_temp_csv(csv_text(good, bad_month, out_of_range))
    result = service.ingest_csv(path)
    print_result("ingest_csv (mixed good/bad rows)", result)

    check(result["status"] == "partial_success", f"status 'partial_success' (got {result['status']})")
    check(result["rows_skipped"] == 2, f"rows_skipped == 2 (got {result['rows_skipped']})")
    check(sorted(err["row"] for err in result["errors"]) == [3, 4],
          "errors reported on CSV lines 3 (parser) and 4 (service validation)")
    check(result["rows_inserted"] + result["rows_unchanged"] == 1,
          "the one good row was saved (inserted, or unchanged if a previous run saved it)")
    check(fetch_row("January", "2099") is not None, "Jan 2099 row exists in the database")
    check(fetch_row("February", "2099") is None, "out-of-range Feb 2099 row was NOT saved")

    # Whole-file failure paths return the same envelope shape.
    failed = service.ingest_csv(os.path.join(tempfile.gettempdir(), "definitely_missing_macro.csv"))
    check(failed["status"] == "failed" and all(key in failed for key in ENVELOPE_KEYS),
          "missing file -> 'failed' with the full envelope")


# ===========================================================================
# Orchestration
# ===========================================================================

def main() -> int:
    flags = [arg for arg in sys.argv[1:] if arg.startswith("--")]
    args = [arg for arg in sys.argv[1:] if not arg.startswith("--")]
    no_write = "--no-write" in flags
    csv_path = args[0] if args else DEFAULT_CSV_PATH

    print("=" * 70)
    print("MacroCSVService End-to-End Test (real MySQL database)")
    print("=" * 70)
    print(f"CSV under test : {csv_path}")
    if no_write:
        print("Mode           : --no-write (sections B, C, E, F skipped)")
    else:
        print("NOTE: sections B, C, E and F WRITE to the real macro_data table.")

    test_parser()

    session = SessionLocal()
    try:
        service = MacroCSVService(session)

        first: Optional[dict[str, Any]] = None
        if no_write:
            skip("B/C/E: skipped by --no-write")
        else:
            first = test_real_csv_ingest(service, csv_path)
            test_idempotency(service, csv_path, first)

        test_never_overwrite(service)

        if no_write:
            skip("F: skipped by --no-write")
        else:
            test_trends(first)
            test_error_path(service)
    finally:
        session.close()
        for path in _TEMP_FILES:
            try:
                os.remove(path)
            except OSError:
                pass

    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)
    print(f"\nPassed:  {_PASS}")
    print(f"Failed:  {_FAIL}")
    print(f"Skipped: {_SKIP}")
    print(f"Total:   {_PASS + _FAIL}")
    print()

    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())