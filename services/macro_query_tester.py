"""
test_macro_query_service.py

Standalone, read-only manual test runner for `MacroQueryService`.

    python -m test_macro_query_service

This is NOT pytest / unittest. It is a plain executable module that hits
the REAL MySQL database through the project's existing SessionLocal and
the REAL MacroQueryService — no mocks, no fixtures, no fake data, no
writes. It exists so you can eyeball every public method's actual output
shape against production-like data before wiring it into API routes.

--------------------------------------------------------------------------
ASSUMPTIONS ABOUT MacroQueryService — ADJUST IF YOUR SIGNATURES DIFFER
--------------------------------------------------------------------------
This file was written against the method list you specified. Since the
exact positional/keyword signatures weren't available at generation time,
the calls below follow the conventions already established elsewhere in
this codebase (MacroService, NewsQueryService):

    * MacroQueryService(db)                       -- db injected at construction
    * month/year are strings, e.g. "March", "2026" (matches MacroData columns)
    * report_date strings use "%B %Y", e.g. "March 2026"
    * pagination uses limit / offset (matches the response envelope keys)
    * every public method returns the same envelope:
          {status, message, count, total, limit, offset, data}

If a method's real signature differs, only the single `execute_case(...)`
line inside that method's test function needs to change — everything else
(timing, validation, pass/fail bookkeeping, summary) keeps working.

Every service call is wrapped so a wrong-signature TypeError, a missing
method, or any other unexpected exception is caught, logged, and counted
as a FAIL for that case — it will never crash the whole run.
--------------------------------------------------------------------------
"""

import json
import sys
import time
import traceback
from datetime import datetime
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from database.session import SessionLocal
from services.macro_query_service import MacroQueryService


# ===========================================================================
# CONFIGURATION — EDIT THESE TO MATCH DATA THAT ACTUALLY EXISTS IN YOUR DB
# ===========================================================================

KNOWN_MONTH = "March"
KNOWN_YEAR = "2026"

SECOND_KNOWN_MONTH = "February"
SECOND_KNOWN_YEAR = "2026"

KNOWN_REPORT_DATE = "March 2026"

NONEXISTENT_MONTH = "January"
NONEXISTENT_YEAR = "1900"

# --- datetime constants (Africa/Nairobi, matches project convention) ---
NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

JAN_2025 = datetime(2025, 1, 1, tzinfo=NAIROBI_TZ)
MAR_2025 = datetime(2025, 3, 1, tzinfo=NAIROBI_TZ)

JAN_2026 = datetime(2026, 1, 1, tzinfo=NAIROBI_TZ)
FEB_2026 = datetime(2026, 2, 1, tzinfo=NAIROBI_TZ)
MAR_2026 = datetime(2026, 3, 1, tzinfo=NAIROBI_TZ)

KNOWN_REPORT_DATETIME = MAR_2026
INVALID_DATETIME_TYPE = "Not A Date"  # deliberately wrong type, not a malformed string date
EMPTY_DATETIME_TYPE = None  # deliberately wrong type for an "empty" case

VALID_INDICATORS = [
    "inflation",
    "fuel_price",
    "cbk_rate",
    "usd_kes_rate",
    "euro_kes_rate",
    "pounds_kes_rate",
]

REQUIRED_ENVELOPE_KEYS = ("status", "message", "count", "total", "limit", "offset", "data")

SEPARATOR = "=" * 70
SUB_SEPARATOR = "-" * 70


# ===========================================================================
# Helpers
# ===========================================================================

def print_header(title: str) -> None:
    print(f"\n{SEPARATOR}")
    print(f"Testing: {title}")
    print(SEPARATOR)


def print_case(label: str) -> None:
    print(f"\n{SUB_SEPARATOR}")
    print(f"Case: {label}")
    print(SUB_SEPARATOR)


def format_response(response: Any) -> str:
    """Pretty-print a response, tolerating non-JSON-native types (datetime, Decimal)."""
    try:
        return json.dumps(response, indent=4, default=str)
    except (TypeError, ValueError):
        return repr(response)


def validate_response(response: Any) -> tuple[bool, list[str]]:
    """
    Verify a service response is a dict containing the standard envelope keys.

    Returns:
        (is_valid, missing_or_wrong_type_keys)
    """
    if not isinstance(response, dict):
        return False, [f"response is not a dict (got {type(response).__name__})"]

    problems = [key for key in REQUIRED_ENVELOPE_KEYS if key not in response]
    return (len(problems) == 0), problems


def get_method(service: Any, name: str) -> Optional[Callable]:
    """Resolve a method off the service, warning (not crashing) if it's missing."""
    method = getattr(service, name, None)
    if method is None or not callable(method):
        print(f"  [SKIP] Service has no callable method named '{name}'.")
        return None
    return method


def execute_case(label: str, call: Callable[[], Any]) -> bool:
    """
    Run one service call: time it, print the response, validate the envelope,
    and never let an exception escape.

    Returns:
        True  -> call completed and returned a well-formed envelope dict.
        False -> call raised, or the envelope was malformed.
    """
    print_case(label)

    start = time.perf_counter()
    try:
        response = call()
    except Exception as exc:  # noqa: BLE001 - intentional: nothing may crash the run
        elapsed = time.perf_counter() - start
        print(f"Exception raised: {type(exc).__name__}: {exc}")
        traceback.print_exc(limit=3)
        print(f"\nExecution time:\n{elapsed:.4f} sec")
        print("\nCase result: FAIL\n")
        return False

    elapsed = time.perf_counter() - start

    print("Returned response:\n")
    print(format_response(response))

    is_valid, problems = validate_response(response)

    print(f"\nExecution time:\n{elapsed:.4f} sec")

    if not is_valid:
        print(f"\nValidation FAILED — issues: {problems}")
        print("\nCase result: FAIL\n")
        return False

    print("\nCase result: PASS\n")
    return True


def run_test(name: str, test_func: Callable[[], bool]) -> bool:
    """
    Execute one test_* function, guaranteeing it can never abort the run.

    Returns:
        True if the test function reported overall success, else False.
    """
    print_header(name)
    try:
        passed = bool(test_func())
    except Exception as exc:  # noqa: BLE001
        print(f"UNEXPECTED ERROR while running test '{name}': {exc}")
        traceback.print_exc(limit=3)
        passed = False

    print(f"\nOverall result for {name}: {'PASS' if passed else 'FAIL'}")
    return passed


# ===========================================================================
# Individual method tests
# One function per public MacroQueryService method.
# Each returns True only if every case it runs completed without an
# unhandled exception and returned a well-formed envelope.
# ===========================================================================

def test_get_latest(service) -> bool:
    method = get_method(service, "get_latest")
    if method is None:
        return False
    return execute_case("get_latest()", lambda: method())


def test_get_macro_snapshot(service) -> bool:
    method = get_method(service, "get_macro_snapshot")
    if method is None:
        return False
    return execute_case("get_macro_snapshot()", lambda: method())


def test_get_by_month(service) -> bool:
    method = get_method(service, "get_by_month")
    if method is None:
        return False

    results = [
        execute_case(
            f"get_by_month('{KNOWN_MONTH}', '{KNOWN_YEAR}')  [valid]",
            lambda: method(KNOWN_MONTH, KNOWN_YEAR),
        ),
        execute_case(
            "get_by_month('ABC', '2026')  [invalid month string]",
            lambda: method("ABC", "2026"),
        ),
        execute_case(
            f"get_by_month('{NONEXISTENT_MONTH}', '{NONEXISTENT_YEAR}')  [nonexistent period]",
            lambda: method(NONEXISTENT_MONTH, NONEXISTENT_YEAR),
        ),
    ]
    return all(results)


def test_get_by_report_date(service) -> bool:
    method = get_method(service, "get_by_report_date")
    if method is None:
        return False

    results = [
        execute_case(
            f"get_by_report_date({KNOWN_REPORT_DATETIME!r})  [valid]",
            lambda: method(KNOWN_REPORT_DATETIME),
        ),
        execute_case(
            f"get_by_report_date({INVALID_DATETIME_TYPE!r})  [invalid type, not a datetime]",
            lambda: method(INVALID_DATETIME_TYPE),
        ),
        execute_case(
            f"get_by_report_date({EMPTY_DATETIME_TYPE!r})  [invalid type, None instead of datetime]",
            lambda: method(EMPTY_DATETIME_TYPE),
        ),
    ]
    return all(results)


def test_get_history(service) -> bool:
    method = get_method(service, "get_history")
    if method is None:
        return False

    results = [
        execute_case("get_history() [defaults]", lambda: method()),
        execute_case("get_history(limit=5, offset=0)", lambda: method(limit=5, offset=0)),
        execute_case("get_history(limit=0) [invalid]", lambda: method(limit=0)),
        execute_case("get_history(limit=-1) [invalid]", lambda: method(limit=-1)),
        execute_case("get_history(offset=-5) [invalid]", lambda: method(offset=-5)),
    ]
    return all(results)


def test_get_last_n_months(service) -> bool:
    method = get_method(service, "get_last_n_months")
    if method is None:
        return False

    results = [
        execute_case("get_last_n_months(3) [valid]", lambda: method(3)),
        execute_case("get_last_n_months(0) [edge]", lambda: method(0)),
        execute_case("get_last_n_months(-5) [invalid]", lambda: method(-5)),
    ]
    return all(results)


def test_get_year(service) -> bool:
    method = get_method(service, "get_year")
    if method is None:
        return False

    results = [
        execute_case(f"get_year('{KNOWN_YEAR}') [valid]", lambda: method(KNOWN_YEAR)),
        execute_case(
            f"get_year('{NONEXISTENT_YEAR}') [nonexistent]",
            lambda: method(NONEXISTENT_YEAR),
        ),
    ]
    return all(results)


def test_get_latest_year(service) -> bool:
    method = get_method(service, "get_latest_year")
    if method is None:
        return False
    return execute_case("get_latest_year()", lambda: method())


def test_get_history_between(service) -> bool:
    method = get_method(service, "get_history_between")
    if method is None:
        return False

    valid_start, valid_end = JAN_2026, MAR_2026
    reversed_start, reversed_end = MAR_2026, JAN_2026

    results = [
        execute_case(
            f"get_history_between({valid_start!r}, {valid_end!r}) [valid range]",
            lambda: method(valid_start, valid_end),
        ),
        execute_case(
            f"get_history_between({reversed_start!r}, {reversed_end!r}) [start > end]",
            lambda: method(reversed_start, reversed_end),
        ),
    ]
    return all(results)


def test_get_inflation_history(service) -> bool:
    method = get_method(service, "get_inflation_history")
    if method is None:
        return False

    results = [
        execute_case("get_inflation_history() [defaults]", lambda: method()),
        execute_case("get_inflation_history(limit=6)", lambda: method(limit=6)),
    ]
    return all(results)


def test_get_fuel_history(service) -> bool:
    method = get_method(service, "get_fuel_history")
    if method is None:
        return False

    results = [
        execute_case("get_fuel_history() [defaults]", lambda: method()),
        execute_case("get_fuel_history(limit=6)", lambda: method(limit=6)),
    ]
    return all(results)


def test_get_exchange_history(service) -> bool:
    method = get_method(service, "get_exchange_history")
    if method is None:
        return False

    results = [
        execute_case("get_exchange_history() [defaults]", lambda: method()),
        execute_case("get_exchange_history(limit=6)", lambda: method(limit=6)),
    ]
    return all(results)


def test_get_cbr_history(service) -> bool:
    method = get_method(service, "get_cbr_history")
    if method is None:
        return False

    results = [
        execute_case("get_cbr_history() [defaults]", lambda: method()),
        execute_case("get_cbr_history(limit=6)", lambda: method(limit=6)),
    ]
    return all(results)


def test_get_summary(service) -> bool:
    method = get_method(service, "get_summary")
    if method is None:
        return False
    return execute_case("get_summary()", lambda: method())


def test_get_statistics(service) -> bool:
    method = get_method(service, "get_statistics")
    if method is None:
        return False
    return execute_case("get_statistics()", lambda: method())


def test_get_indicator_statistics(service) -> bool:
    method = get_method(service, "get_indicator_statistics")
    if method is None:
        return False

    results = [
        execute_case(
            f"get_indicator_statistics('{VALID_INDICATORS[0]}') [valid]",
            lambda: method(VALID_INDICATORS[0]),
        ),
        execute_case(
            "get_indicator_statistics('banana') [invalid indicator]",
            lambda: method("banana"),
        ),
    ]
    return all(results)


def test_compare_months(service) -> bool:
    method = get_method(service, "compare_months")
    if method is None:
        return False

    results = [
        execute_case(
            f"compare_months('{KNOWN_MONTH}', '{KNOWN_YEAR}', "
            f"'{SECOND_KNOWN_MONTH}', '{SECOND_KNOWN_YEAR}') [valid]",
            lambda: method(KNOWN_MONTH, KNOWN_YEAR, SECOND_KNOWN_MONTH, SECOND_KNOWN_YEAR),
        ),
        execute_case(
            "compare_months('December', '1899', 'January', '1900') [missing data]",
            lambda: method("December", "1899", "January", "1900"),
        ),
    ]
    return all(results)


def test_compare_years(service) -> bool:
    method = get_method(service, "compare_years")
    if method is None:
        return False

    results = [
        execute_case(
            f"compare_years('{KNOWN_YEAR}', '2025') [valid]",
            lambda: method(KNOWN_YEAR, "2025"),
        ),
        execute_case(
            f"compare_years('{NONEXISTENT_YEAR}', '{KNOWN_YEAR}') [one year missing]",
            lambda: method(NONEXISTENT_YEAR, KNOWN_YEAR),
        ),
    ]
    return all(results)


def test_compare_periods(service) -> bool:
    method = get_method(service, "compare_periods")
    if method is None:
        return False

    results = [
        execute_case(
            f"compare_periods({JAN_2026!r}, {MAR_2026!r}, "
            f"{JAN_2025!r}, {MAR_2025!r}) [valid]",
            lambda: method(JAN_2026, MAR_2026, JAN_2025, MAR_2025),
        ),
        execute_case(
            f"compare_periods({MAR_2026!r}, {JAN_2026!r}, "
            f"{JAN_2025!r}, {MAR_2025!r}) [invalid range in period 1]",
            lambda: method(MAR_2026, JAN_2026, JAN_2025, MAR_2025),
        ),
    ]
    return all(results)


def test_search(service) -> bool:
    method = get_method(service, "search")
    if method is None:
        return False

    results = [
        execute_case("search() [empty search]", lambda: method()),
        execute_case(
            f"search(year='{NONEXISTENT_YEAR}') [impossible search]",
            lambda: method(year=NONEXISTENT_YEAR),
        ),
        execute_case(
            f"search(month='{KNOWN_MONTH}', year='{KNOWN_YEAR}') [filtered search]",
            lambda: method(month=KNOWN_MONTH, year=KNOWN_YEAR),
        ),
    ]
    return all(results)


# ===========================================================================
# Orchestration
# ===========================================================================

# Ordered (name, function) pairs. Add a new tuple here whenever
# MacroQueryService grows a new public method — nothing else needs to change.
TEST_REGISTRY: list[tuple[str, Callable[[Any], bool]]] = [
    ("get_latest", test_get_latest),
    ("get_macro_snapshot", test_get_macro_snapshot),
    ("get_by_month", test_get_by_month),
    ("get_by_report_date", test_get_by_report_date),
    ("get_history", test_get_history),
    ("get_last_n_months", test_get_last_n_months),
    ("get_year", test_get_year),
    ("get_latest_year", test_get_latest_year),
    ("get_history_between", test_get_history_between),
    ("get_inflation_history", test_get_inflation_history),
    ("get_fuel_history", test_get_fuel_history),
    ("get_exchange_history", test_get_exchange_history),
    ("get_cbr_history", test_get_cbr_history),
    ("get_summary", test_get_summary),
    ("get_statistics", test_get_statistics),
    ("get_indicator_statistics", test_get_indicator_statistics),
    ("compare_months", test_compare_months),
    ("compare_years", test_compare_years),
    ("compare_periods", test_compare_periods),
    ("search", test_search),
]


def discover_untested_public_methods(service: Any) -> list[str]:
    """
    Surface any public method on the live service instance that isn't in
    TEST_REGISTRY yet, so new methods don't silently go unverified.
    """
    tested = {name for name, _ in TEST_REGISTRY}
    public_methods = {
        name
        for name in dir(service)
        if not name.startswith("_") and callable(getattr(service, name, None))
    }
    return sorted(public_methods - tested)


def main() -> int:
    print(SEPARATOR)
    print("MacroQueryService — Standalone Read-Only Test Runner")
    print(SEPARATOR)
    print(
        "\nConnecting to the real database via SessionLocal(). "
        "No data will be created, updated, or deleted.\n"
    )

    db = SessionLocal()
    passed_count = 0
    failed_count = 0
    failed_names: list[str] = []

    try:
        service = MacroQueryService(db)
    except TypeError:
        # Fallback in case MacroQueryService takes no constructor args and
        # expects `db` passed per-method instead (NewsQueryService-style).
        print(
            "MacroQueryService(db) raised TypeError — retrying with a "
            "no-arg constructor. If your service is per-call `db`-scoped, "
            "you'll need to adjust the test functions above to pass `db` "
            "into each call."
        )
        service = MacroQueryService()

    try:
        untested = discover_untested_public_methods(service)
        if untested:
            print(
                "NOTE: the following public methods exist on the service "
                f"but have no dedicated test yet: {untested}\n"
                "Add a test_<method_name>() function and register it in "
                "TEST_REGISTRY.\n"
            )

        for name, test_func in TEST_REGISTRY:
            result = run_test(name, lambda tf=test_func: tf(service))
            if result:
                passed_count += 1
            else:
                failed_count += 1
                failed_names.append(name)

    finally:
        db.close()

    total = passed_count + failed_count

    print(f"\n{SEPARATOR}")
    print("MacroQueryService Test Summary")
    print(SEPARATOR)
    print(f"\nPassed: {passed_count}")
    print(f"Failed: {failed_count}")
    print(f"Total:  {total}")

    if failed_names:
        print(f"\nFailed tests: {failed_names}")

    print()
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())