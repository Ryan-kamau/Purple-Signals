"""
testers/test_MacroFeatures.py

Standalone, real-database tester for `MacroFeaturesService`.

    python -m testers.test_MacroFeatures

No mocks, no fixtures, no fabricated rows. Uses the real SessionLocal and
the real daily_features / macro_data tables. NOTE: successful runs DO write
macro values into daily_features (that is the feature under test) and are
not rolled back.

Only the public method `fill()` is called; everything else here is
read-only verification SQL.
"""

import sys
from typing import Any

from sqlalchemy import func, select

from database.session import SessionLocal
from models.daily_market_features import DailyMarketFeatures
from models.macro_data import MacroData
from services.feature_service.MacroFeatureService import (
    COLUMN_MAP,
    MacroFeaturesService,
)

MACRO_COLS = list(COLUMN_MAP.values())

_PASS = 0
_FAIL = 0
_SKIP = 0


def check(condition: bool, message: str) -> bool:
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


def run_test(name: str, func_) -> None:
    """Run one test; an unexpected exception is a FAIL, never an abort."""
    global _FAIL
    try:
        func_()
    except Exception as exc:  # noqa: BLE001
        _FAIL += 1
        print(f"[FAIL] {name} raised unexpectedly: {type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# Read-only verification helpers
# ---------------------------------------------------------------------------

def snapshot(session) -> dict[int, dict[str, Any]]:
    """{row_id: {ticker, trading_date, <macro cols>}} for all daily_features."""
    session.expire_all()
    rows = session.execute(select(DailyMarketFeatures)).scalars().all()
    return {
        r.id: {
            "ticker": r.ticker,
            "trading_date": r.trading_date,
            **{c: getattr(r, c) for c in MACRO_COLS},
        }
        for r in rows
    }


def latest_date_per_ticker(snap: dict[int, dict[str, Any]]) -> dict[str, Any]:
    latest: dict[str, Any] = {}
    for row in snap.values():
        if row["ticker"] not in latest or row["trading_date"] > latest[row["ticker"]]:
            latest[row["ticker"]] = row["trading_date"]
    return latest


def no_value_overwritten(before, after) -> bool:
    """Every previously non-null macro value must be unchanged."""
    for row_id, old in before.items():
        new = after.get(row_id)
        if new is None:
            return False
        for col in MACRO_COLS:
            if old[col] is not None and new[col] != old[col]:
                return False
    return True


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_default_run(session, service) -> None:
    section("fill() — default (latest trading date per ticker)")

    before = snapshot(session)
    latest = latest_date_per_ticker(before)

    result = service.fill()
    print(f"result: {result}")

    after = snapshot(session)

    check(isinstance(result, dict), "fill() returns a dict")
    for key in (
        "status", "message", "backfill", "macro_sources", "rows_examined",
        "rows_updated", "rows_unchanged", "columns_filled", "errors",
    ):
        check(key in result, f"result contains '{key}'")

    check(
        result["status"] in ("success", "partial_success"),
        f"status is success/partial_success (got {result['status']})",
    )
    check(len(after) == len(before), "no daily_features rows were created or deleted")
    check(no_value_overwritten(before, after), "no existing non-null macro value was overwritten")
    check(
        result["rows_examined"] == len(latest),
        f"rows_examined ({result['rows_examined']}) == distinct tickers ({len(latest)})",
    )
    check(
        result["rows_examined"] == result["rows_updated"] + result["rows_unchanged"],
        "rows_examined == rows_updated + rows_unchanged",
    )

    changed_ids = [
        rid for rid in before
        if any(before[rid][c] != after[rid][c] for c in MACRO_COLS)
    ]
    check(
        all(before[rid]["trading_date"] == latest[before[rid]["ticker"]] for rid in changed_ids),
        "only each ticker's latest-date row was modified",
    )
    check(
        len(changed_ids) == result["rows_updated"],
        f"rows actually changed ({len(changed_ids)}) == reported rows_updated ({result['rows_updated']})",
    )


def test_idempotent(session, service) -> None:
    section("fill() — idempotency")

    result = service.fill()
    print(f"result: {result}")
    check(result["rows_updated"] == 0, f"second run updates 0 rows (got {result['rows_updated']})")


def test_backfill(session, service) -> None:
    section("fill(backfill=True)")

    before = snapshot(session)
    result = service.fill(backfill=True)
    print(f"result: {result}")
    after = snapshot(session)

    check(result["backfill"] is True, "result reports backfill=True")
    check(
        result["status"] in ("success", "partial_success"),
        f"status is success/partial_success (got {result['status']})",
    )
    check(len(after) == len(before), "no daily_features rows were created or deleted")
    check(no_value_overwritten(before, after), "no existing non-null macro value was overwritten")

    sources = result.get("macro_sources", {})
    still_null = [
        (rid, col)
        for rid, row in after.items()
        for col in MACRO_COLS
        if col in sources and row[col] is None
    ]
    check(
        not still_null,
        "every row has a non-null value for each indicator that has a macro source "
        f"({len(still_null)} remaining NULLs)",
    )

    again = service.fill(backfill=True)
    check(again["rows_updated"] == 0, f"second backfill run updates 0 rows (got {again['rows_updated']})")


def test_empty_macro(session, service) -> None:
    section("Error path: empty macro_data")

    count = session.execute(select(func.count(MacroData.id))).scalar_one()
    if count > 0:
        skip(f"macro_data has {count} row(s) — empty-table path cannot be exercised on real data")
        return

    result = service.fill()
    check(result["status"] == "failed", f"empty macro_data returns 'failed' (got {result['status']})")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def main() -> int:
    print("=" * 70)
    print("MacroFeaturesService End-to-End Test (real MySQL database)")
    print("=" * 70)

    session = SessionLocal()
    try:
        service = MacroFeaturesService(session)

        macro_count = session.execute(select(func.count(MacroData.id))).scalar_one()
        feature_count = session.execute(select(func.count(DailyMarketFeatures.id))).scalar_one()
        print(f"macro_data rows: {macro_count} | daily_features rows: {feature_count}")

        if macro_count == 0:
            test_empty_macro(session, service)
        elif feature_count == 0:
            skip("daily_features is empty — run MarketCalculatorService first (this service never creates rows)")
        else:
            run_test("default run", lambda: test_default_run(session, service))
            run_test("idempotency", lambda: test_idempotent(session, service))
            run_test("backfill", lambda: test_backfill(session, service))
            run_test("empty macro", lambda: test_empty_macro(session, service))
    finally:
        session.close()

    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)
    print(f"\nPassed:  {_PASS}")
    print(f"Failed:  {_FAIL}")
    print(f"Skipped: {_SKIP}")
    print(f"Total:   {_PASS + _FAIL}\n")

    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())