"""
test_market_calculator_service.py

Standalone, manual, real-database end-to-end tester for
`MarketCalculatorService` (services/feature_service/MarketcalculatorService.py).

WHAT THIS IS
    A plain, runnable script — NOT pytest/unittest, no mocks, no fixtures,
    no fake data. It exercises MarketCalculatorService's public API only
    (process_all, process_ticker, process_date, process_date_range) against
    the real MySQL database via the project's real SessionLocal, then
    independently re-queries the database to confirm the service's
    reported result actually matches what got persisted.

RUN:
    python -m test_market_calculator_service

WHAT IT DOES NOT DO
    - No pytest / unittest / fixtures / mocks / monkeypatching.
    - No fake or inserted market_data rows — it discovers real rows.
    - No calls into private service methods (_persist, _compute_features,
      _process_ticker, _load_daily_series, etc). Only the four public
      methods are called; everything else is read-only verification SQL.
    - No cleanup / rollback of successful writes. Rows this script causes
      the service to insert or update into daily_market_features are left
      in place — that's the point: prove the real persistence workflow.

ARCHITECTURAL NOTE (read before running):
    DailyMarketFeatures currently imports its declarative Base from
    `app.database` while the rest of this project (MarketData, session,
    init_db) uses `database.base.Base`. Two different Base registries
    means `daily_market_features` will NOT be created by the project's
    normal `init_db()` call. If that table doesn't already exist in your
    real database, every query below will fail with a "table doesn't
    exist" error — that's a real gap in the model wiring, not a bug in
    this tester. Fixing it (pointing DailyMarketFeatures at the same
    `database.base.Base` as everything else) is a follow-up task, out of
    scope here since it touches the model, not the tester.
"""

import random
import string
import sys
from datetime import date, datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from database.session import SessionLocal
from services.feature_service.MarketcalculatorService import MarketCalculatorService
from models.daily_market_features import DailyMarketFeatures
from models.market_data import MarketData

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

# ---------------------------------------------------------------------------
# Result tracking — deliberately simple counters, no test framework.
# ---------------------------------------------------------------------------

_PASS = 0
_FAIL = 0
_SKIP = 0


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
    """Record a test that could not run because real data was unavailable."""
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
        if key == "errors" and value:
            print(f"  errors: {len(value)} entrie(s)")
            for err in value[:5]:
                print(f"    - {err}")
        else:
            print(f"  {key}: {value}")


# ---------------------------------------------------------------------------
# Timezone helpers — mirror MarketCalculatorService's own Nairobi-date
# resolution so discovery/verification queries agree with what the service
# itself considers a "trading date". This is independent verification
# logic, not a call into the service's private methods.
# ---------------------------------------------------------------------------

def to_nairobi_date(timestamp: datetime) -> date:
    if timestamp.tzinfo is None:
        return timestamp.date()
    return timestamp.astimezone(NAIROBI_TZ).date()


def nairobi_day_bounds(target_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(target_date, time.min, tzinfo=NAIROBI_TZ)
    end = datetime.combine(target_date, time.max, tzinfo=NAIROBI_TZ)
    return start, end


# ---------------------------------------------------------------------------
# Discovery / verification helpers — real, read-only SQL against the real
# database. Allowed per the task: direct queries for finding test data and
# checking results, never for bypassing the service's own logic.
# ---------------------------------------------------------------------------

def discover_ticker_with_most_history(session) -> Optional[str]:
    """Pick the ticker with the most market_data rows — gives rolling
    features (5d/20d windows) the best chance of actually computing."""
    rows = session.execute(
        select(MarketData.ticker, func.count(MarketData.id).label("cnt"))
        .group_by(MarketData.ticker)
        .order_by(func.count(MarketData.id).desc())
    ).all()
    return rows[0][0] if rows else None


def kplc_has_real_data(session) -> bool:
    count = session.execute(
        select(func.count(MarketData.id)).where(MarketData.ticker == "KPLC")
    ).scalar_one()
    return count > 0


def discover_trading_dates(session, ticker: str) -> list[date]:
    timestamps = (
        session.execute(select(MarketData.timestamp).where(MarketData.ticker == ticker))
        .scalars()
        .all()
    )
    return sorted({to_nairobi_date(ts) for ts in timestamps if ts is not None})


def count_features_for_ticker(session, ticker: str) -> int:
    return session.execute(
        select(func.count(DailyMarketFeatures.id)).where(DailyMarketFeatures.ticker == ticker)
    ).scalar_one()


def fetch_feature_row(session, ticker: str, trading_date: date):
    return session.execute(
        select(DailyMarketFeatures).where(
            DailyMarketFeatures.ticker == ticker,
            DailyMarketFeatures.trading_date == trading_date,
        )
    ).scalar_one_or_none()


def tickers_with_data_on_date(session, target_date: date) -> list[str]:
    start, end = nairobi_day_bounds(target_date)
    rows = session.execute(
        select(MarketData.ticker)
        .distinct()
        .where(MarketData.timestamp >= start, MarketData.timestamp < end)
    ).all()
    return sorted({row[0] for row in rows})


def random_nonexistent_ticker() -> str:
    suffix = "".join(random.choices(string.ascii_uppercase, k=6))
    return f"ZZZNOPE{suffix}"


# ---------------------------------------------------------------------------
# Individual tests — each calls exactly one public service method, then
# independently re-queries the database to confirm the result is real.
# ---------------------------------------------------------------------------

def test_process_ticker(session, service, ticker: str, label: str) -> None:
    section(f"process_ticker('{ticker}')  [{label}]")

    before_count = count_features_for_ticker(session, ticker)

    try:
        result = service.process_ticker(ticker)
    except Exception as exc:  # noqa: BLE001
        check(False, f"process_ticker('{ticker}') raised unexpectedly: {exc}")
        return

    print_result(f"process_ticker('{ticker}')", result)

    check(isinstance(result, dict), "process_ticker() returns a dict")
    for key in (
        "status", "tickers_processed", "rows_processed",
        "rows_inserted", "rows_updated", "rows_skipped", "errors",
    ):
        check(key in result, f"result contains '{key}'")

    after_count = count_features_for_ticker(session, ticker)
    actual_new_rows = after_count - before_count

    check(
        actual_new_rows == result.get("rows_inserted", -1),
        f"DB row-count delta ({actual_new_rows}) matches reported "
        f"rows_inserted ({result.get('rows_inserted')})",
    )
    check(
        result.get("rows_processed", 0)
        == result.get("rows_inserted", 0) + result.get("rows_updated", 0) + result.get("rows_skipped", 0),
        "rows_processed == rows_inserted + rows_updated + rows_skipped",
    )
    check(after_count >= before_count, "daily_market_features row count did not decrease")


def test_process_date(session, service, ticker_for_discovery: str) -> None:
    section("process_date()")

    dates = discover_trading_dates(session, ticker_for_discovery)
    if not dates:
        skip("process_date(): no real trading dates found for discovery ticker")
        return

    target_date = dates[-1]
    expected_tickers = tickers_with_data_on_date(session, target_date)

    if not expected_tickers:
        skip(f"process_date({target_date}): no tickers found with data on that date")
        return

    before_counts = {t: count_features_for_ticker(session, t) for t in expected_tickers}

    try:
        result = service.process_date(target_date)
    except Exception as exc:  # noqa: BLE001
        check(False, f"process_date({target_date}) raised unexpectedly: {exc}")
        return

    print_result(f"process_date({target_date})", result)

    check(isinstance(result, dict), "process_date() returns a dict")
    check(
        result.get("tickers_processed") == len(expected_tickers),
        f"tickers_processed ({result.get('tickers_processed')}) matches real "
        f"tickers with data on {target_date} ({len(expected_tickers)})",
    )

    for ticker in expected_tickers:
        row = fetch_feature_row(session, ticker, target_date)
        check(row is not None, f"daily_market_features row exists for ({ticker}, {target_date})")

    after_counts = {t: count_features_for_ticker(session, t) for t in expected_tickers}
    total_new = sum(after_counts[t] - before_counts[t] for t in expected_tickers)
    check(
        total_new == result.get("rows_inserted", -1),
        f"total new rows across affected tickers ({total_new}) matches "
        f"reported rows_inserted ({result.get('rows_inserted')})",
    )


def test_process_date_range(session, service, ticker: str) -> None:
    section("process_date_range()")

    dates = discover_trading_dates(session, ticker)
    if len(dates) < 2:
        skip(f"process_date_range(): fewer than 2 real trading dates available for '{ticker}'")
        return

    start_date, end_date = dates[0], dates[-1]
    before_count = count_features_for_ticker(session, ticker)

    try:
        result = service.process_date_range(start_date, end_date)
    except Exception as exc:  # noqa: BLE001
        check(False, f"process_date_range() raised unexpectedly: {exc}")
        return

    print_result(f"process_date_range({start_date} -> {end_date})", result)

    check(isinstance(result, dict), "process_date_range() returns a dict")
    check(
        result.get("status") in ("success", "partial_success"),
        f"status is success/partial_success (got {result.get('status')})",
    )

    after_count = count_features_for_ticker(session, ticker)
    check(after_count >= before_count, "row count did not decrease after range run")

    rows_in_range = session.execute(
        select(func.count(DailyMarketFeatures.id)).where(
            DailyMarketFeatures.ticker == ticker,
            DailyMarketFeatures.trading_date >= start_date,
            DailyMarketFeatures.trading_date <= end_date,
        )
    ).scalar_one()
    check(
        rows_in_range == len(dates),
        f"daily_market_features has a row for every real trading date in "
        f"range ({rows_in_range}/{len(dates)})",
    )


def test_process_all(session, service) -> None:
    section("process_all()")

    all_tickers = sorted({row[0] for row in session.execute(select(MarketData.ticker).distinct()).all()})

    if not all_tickers:
        skip("process_all(): no tickers present in market_data at all")
        return

    before_total = session.execute(select(func.count(DailyMarketFeatures.id))).scalar_one()

    try:
        result = service.process_all()
    except Exception as exc:  # noqa: BLE001
        check(False, f"process_all() raised unexpectedly: {exc}")
        return

    print_result("process_all()", result)

    check(isinstance(result, dict), "process_all() returns a dict")
    check(
        result.get("tickers_processed") == len(all_tickers),
        f"tickers_processed ({result.get('tickers_processed')}) matches distinct "
        f"tickers in market_data ({len(all_tickers)})",
    )

    after_total = session.execute(select(func.count(DailyMarketFeatures.id))).scalar_one()
    actual_new = after_total - before_total
    check(
        actual_new == result.get("rows_inserted", -1),
        f"table-wide row delta ({actual_new}) matches reported "
        f"rows_inserted ({result.get('rows_inserted')})",
    )


def test_repeated_updates(session, service, ticker: str) -> None:
    """
    Confirms the removed 3-update cap really is gone: the same
    (ticker, trading_date) row must keep getting reprocessed on every
    call, not silently skipped after a fixed number of updates.
    """
    section("Repeated-update behaviour (3-update cap must NOT apply)")

    dates = discover_trading_dates(session, ticker)
    if not dates:
        skip("repeated-update test: no real trading dates for ticker")
        return

    target_date = dates[-1]

    # Ensure a row exists for this (ticker, date) before measuring updates.
    service.process_ticker(ticker)
    row = fetch_feature_row(session, ticker, target_date)

    if row is None:
        skip(f"repeated-update test: no daily_market_features row materialised for ({ticker}, {target_date})")
        return

    previous_updated_at = row.updated_at

    for attempt in range(1, 5):
        try:
            result = service.process_ticker(ticker)
        except Exception as exc:  # noqa: BLE001
            check(False, f"repeated update #{attempt} raised unexpectedly: {exc}")
            return

        session.expire_all()
        row = fetch_feature_row(session, ticker, target_date)
        row_exists = row is not None
        was_updated = row_exists and row.updated_at != previous_updated_at

        check(row_exists, f"repeated update #{attempt}: row for ({ticker}, {target_date}) still exists")
        check(
            result.get("status") in ("success", "partial_success"),
            f"repeated update #{attempt}: service reports success/partial_success",
        )
        check(
            was_updated or result.get("rows_updated", 0) >= 1,
            f"repeated update #{attempt}: row was actually reprocessed "
            f"(updated_at changed or rows_updated >= 1) — not silently skipped by a cap",
        )

        if row_exists:
            previous_updated_at = row.updated_at


def test_feature_values(session, ticker: str) -> None:
    section(f"Feature value sanity check for '{ticker}'")

    dates = discover_trading_dates(session, ticker)
    if not dates:
        skip("feature value check: no trading dates for ticker")
        return

    latest_date = dates[-1]
    row = fetch_feature_row(session, ticker, latest_date)

    if row is None:
        skip(f"feature value check: no persisted row for ({ticker}, {latest_date})")
        return

    check(row.close_price is not None, "close_price is populated")
    check(row.volume is not None, "volume is populated")

    history_length = len(dates)

    if history_length >= 5:
        check(
            row.price_ma_5 is not None,
            f"price_ma_5 populated with >= 5 real trading days of history ({history_length} available)",
        )
    else:
        skip(f"price_ma_5: insufficient history ({history_length} < 5) — None is expected, not a failure")

    if history_length >= 20:
        check(
            row.price_ma_20 is not None,
            f"price_ma_20 populated with >= 20 real trading days of history ({history_length} available)",
        )
    else:
        skip(f"price_ma_20: insufficient history ({history_length} < 20) — None is expected, not a failure")

    # The most recent trading day has no future observation yet, so
    # next_day_return/direction must be None by design — that is the ONE
    # deliberately future-looking field, and it can never be filled for
    # the newest row until a later day's data arrives.
    check(
        row.next_day_return is None and row.next_day_direction is None,
        "next_day_return/direction correctly None for the most recent "
        "trading day (no future observation exists yet)",
    )


def test_invalid_date_range(service) -> None:
    section("Error path: start_date after end_date")

    start_date = date.today() + timedelta(days=1)
    end_date = date.today()

    try:
        result = service.process_date_range(start_date, end_date)
    except Exception as exc:  # noqa: BLE001
        check(False, f"invalid date range raised unexpectedly instead of returning a failure result: {exc}")
        return

    print_result("process_date_range(invalid range)", result)

    check(isinstance(result, dict), "invalid range returns a dict, not an exception")
    check(result.get("status") == "failed", f"status is 'failed' (got {result.get('status')})")
    check(bool(result.get("errors")), "errors list is populated with a validation message")


def test_nonexistent_ticker(service) -> None:
    section("Error path: nonexistent ticker")

    fake_ticker = random_nonexistent_ticker()

    try:
        result = service.process_ticker(fake_ticker)
    except Exception as exc:  # noqa: BLE001
        check(False, f"nonexistent ticker raised unexpectedly: {exc}")
        return

    print_result(f"process_ticker('{fake_ticker}')", result)

    check(isinstance(result, dict), "nonexistent ticker returns a dict, not an exception")
    check(
        result.get("rows_processed", -1) == 0,
        f"rows_processed is 0 for a ticker with no market_data (got {result.get('rows_processed')})",
    )
    check(
        result.get("status") in ("success", "partial_success"),
        f"service does not treat an unknown ticker as a hard failure (status={result.get('status')})",
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def main() -> int:
    print("=" * 70)
    print("MarketCalculatorService End-to-End Test (real MySQL database)")
    print("=" * 70)

    session = SessionLocal()

    try:
        check(session is not None, "Database session established")

        service = MarketCalculatorService(session)

        primary_ticker = discover_ticker_with_most_history(session)
        if not check(primary_ticker is not None, "Real market_data rows discovered"):
            print("\nNo market_data rows exist at all — cannot continue.")
            return 1

        print(f"\nDiscovered primary ticker for testing: {primary_ticker}")

        if kplc_has_real_data(session):
            print("KPLC has real market_data — running KPLC-specific test.")
            test_process_ticker(session, service, "KPLC", label="KPLC-specific")
        else:
            skip("KPLC-specific test: no real market_data rows exist for KPLC in this database")

        test_process_ticker(session, service, primary_ticker, label="auto-discovered")
        test_process_date(session, service, primary_ticker)
        test_process_date_range(session, service, primary_ticker)
        test_process_all(session, service)
        test_repeated_updates(session, service, primary_ticker)
        test_feature_values(session, primary_ticker)
        test_invalid_date_range(service)
        test_nonexistent_ticker(service)

    finally:
        session.close()

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