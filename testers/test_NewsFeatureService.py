"""
testers/test_NewsFeatureService.py

Standalone, manual, real-database end-to-end tester for the news feature
service (the service that fills the NEWS-owned columns of
`daily_features` / DailyMarketFeatures).

RUN (from project root):
    python -m testers.test_NewsFeatureService

STYLE — identical to testers/test_MarketCalculatorService.py:
    - Plain script, NOT pytest/unittest. No mocks, no fixtures, no fake data.
    - Real MySQL via SessionLocal, real headlines, real daily_features rows.
    - Only PUBLIC service methods are called. Everything else is read-only
      verification SQL.
    - Every result is independently re-verified against the database.
    - [PASS] / [FAIL] / [SKIP] counters, section headers, never aborts.
    - Runtime guard diffs the live service's public methods against the
      tests in this file so new methods can't silently go untested.

--------------------------------------------------------------------------
ASSUMPTIONS ABOUT THE SERVICE  (adjust the CONFIG block if any differ)
--------------------------------------------------------------------------
The service source was not available when this was written, so the contract
is inferred from the PRD (section 10, "Row ownership") and from
MarketCalculatorService, which it is meant to mirror:

  A1. Constructor:  Service(db)   — session injected, never created inside.
  A2. Public API mirrors the market service:
        process_all()
        process_ticker(ticker)
        process_date(target_date)
        process_date_range(start_date, end_date)
      (called with keyword args first, positional as fallback)
  A3. Returns the structured dict:
        {status, tickers_processed, rows_processed, rows_updated,
         rows_skipped, errors}     (+ optional rows_inserted, must be 0)
  A4. ROW OWNERSHIP (PRD): the news service UPDATES existing
      (ticker, trading_date) rows created by the market service. It must
      NEVER insert rows, and must NEVER touch market / macro / target
      columns. This is the most important contract and the core of this file.
  A5. Missing data is NULL, not zero: a day with no relevant news leaves
      sentiment fields NULL (same rule as volatility in the market service).
  A6. Headlines are bucketed by their Africa/Nairobi calendar date.
--------------------------------------------------------------------------
"""

import importlib
import random
import string
import sys
from datetime import date, datetime, timedelta
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import func, inspect as sa_inspect, select

from database.session import SessionLocal
from models.daily_market_features import DailyMarketFeatures
from models.headline_data import Headline

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

# ===========================================================================
# CONFIG — the only block that should need editing
# ===========================================================================

SERVICE_MODULE_CANDIDATES = (
    "services.feature_service.NewsCalculatorService",
    "services.feature_service.NewsFeatureService",
    "services.feature_service.newsCalculatorService",
    "services.feature_service.news_feature_service",
    "services.feature_service.news_service",
)
SERVICE_CLASS_CANDIDATES = (
    "NewsCalculatorService",
    "NewsFeatureService",
    "NewsFeaturesService",
    "NewsService",
)

# Columns the news service is allowed to write (see DailyMarketFeatures).
NEWS_OWNED_COLUMNS = (
    "average_sentiment",
    "weighted_sentiment",
    "sentiment_volatility",
    "average_kplc_relevance",   # legacy name — drop/rename if you generalised it
    "average_impact_score",
    "relevant_headline_count",
    "energy_sentiment",
    "electricity_sentiment",
    "oil_sentiment",
    "macro_sentiment",
    "government_sentiment",
)

# Columns bounded to [-1, 1] / >= 0 for the sanity checks.
SENTIMENT_COLUMNS = (
    "average_sentiment", "weighted_sentiment",
    "energy_sentiment", "electricity_sentiment", "oil_sentiment",
    "macro_sentiment", "government_sentiment",
)
NON_NEGATIVE_COLUMNS = ("sentiment_volatility", "average_impact_score")

# Optional headline<->security association model (PRD: article_securities).
# The headline cross-check is SKIPPED if these can't be imported.
ASSOCIATION_MODEL = ("models.article_security", "ArticleSecurity")  # headline_id, security_id
SECURITY_MODEL = ("models.security", "Security")                    # id, ticker

REQUIRED_RESULT_KEYS = (
    "status", "tickers_processed", "rows_processed",
    "rows_updated", "rows_skipped", "errors",
)

FLOAT_TOL = 1e-9
CROSS_CHECK_TOL = 1e-3
CROSS_CHECK_SAMPLE = 5

# ===========================================================================
# Column bookkeeping (derived from the real model — no hardcoding)
# ===========================================================================

ALL_COLUMNS = [a.key for a in sa_inspect(DailyMarketFeatures).mapper.column_attrs]
_BOOKKEEPING = {"id", "created_at", "updated_at"}
OWNED = [c for c in NEWS_OWNED_COLUMNS if c in ALL_COLUMNS]
MISSING_OWNED = [c for c in NEWS_OWNED_COLUMNS if c not in ALL_COLUMNS]
PROTECTED = [c for c in ALL_COLUMNS if c not in OWNED and c not in _BOOKKEEPING]

# ===========================================================================
# Result tracking
# ===========================================================================

_PASS = _FAIL = _SKIP = 0


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


def print_result(label: str, result: Any) -> None:
    print(f"\n{label} result:")
    if not isinstance(result, dict):
        print(f"  {result!r}")
        return
    for key, value in result.items():
        if key == "errors" and value:
            print(f"  errors: {len(value)} entrie(s)")
            for err in value[:5]:
                print(f"    - {err}")
        else:
            print(f"  {key}: {value}")


# ===========================================================================
# Service resolution / calling
# ===========================================================================

def resolve_service_class() -> Optional[type]:
    for module_path in SERVICE_MODULE_CANDIDATES:
        try:
            module = importlib.import_module(module_path)
        except ModuleNotFoundError:
            continue
        for class_name in SERVICE_CLASS_CANDIDATES:
            cls = getattr(module, class_name, None)
            if cls is not None:
                print(f"Resolved service: {module_path}.{class_name}")
                return cls
    return None


def call(fn: Callable, kwargs: dict, args: tuple) -> Any:
    """Keyword-first (project convention), positional fallback."""
    try:
        return fn(**kwargs)
    except TypeError as exc:
        text = str(exc)
        if "unexpected keyword" in text or "required positional" in text:
            return fn(*args)
        raise


def get_method(service: Any, name: str) -> Optional[Callable]:
    method = getattr(service, name, None)
    if method is None or not callable(method):
        skip(f"service has no callable '{name}' — tests for it not run")
        return None
    return method


# ===========================================================================
# Snapshot / diff helpers — independent verification, read-only SQL
# ===========================================================================

def snapshot(session, ticker: Optional[str] = None) -> dict[int, dict[str, Any]]:
    session.expire_all()
    stmt = select(DailyMarketFeatures)
    if ticker:
        stmt = stmt.where(DailyMarketFeatures.ticker == ticker)
    rows = session.execute(stmt).scalars().all()
    return {r.id: {c: getattr(r, c) for c in ALL_COLUMNS} for r in rows}


def feq(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        return abs(float(a) - float(b)) <= FLOAT_TOL
    return a == b


def columns_changed(before: dict, after: dict, columns: list[str]) -> list[str]:
    return [c for c in columns if not feq(before.get(c), after.get(c))]


def n(result: Any, key: str, default: int = 0) -> int:
    return result.get(key, default) if isinstance(result, dict) else default


def to_nairobi_date(ts: datetime) -> date:
    if ts.tzinfo is None:
        return ts.date()
    return ts.astimezone(NAIROBI_TZ).date()


def distinct_feature_tickers(session) -> list[str]:
    rows = session.execute(select(DailyMarketFeatures.ticker).distinct()).all()
    return sorted({r[0] for r in rows})


def ticker_with_most_feature_rows(session) -> Optional[str]:
    rows = session.execute(
        select(DailyMarketFeatures.ticker, func.count(DailyMarketFeatures.id))
        .group_by(DailyMarketFeatures.ticker)
        .order_by(func.count(DailyMarketFeatures.id).desc())
    ).all()
    return rows[0][0] if rows else None


def feature_dates(session, ticker: str) -> list[date]:
    rows = session.execute(
        select(DailyMarketFeatures.trading_date)
        .where(DailyMarketFeatures.ticker == ticker)
        .order_by(DailyMarketFeatures.trading_date.asc())
    ).scalars().all()
    return list(rows)


def random_nonexistent_ticker() -> str:
    return "ZZZNOPE" + "".join(random.choices(string.ascii_uppercase, k=6))


# ===========================================================================
# Shared verification harness
# ===========================================================================

def verify_run(
    session,
    label: str,
    runner: Callable[[], Any],
    *,
    allowed_ids: Optional[set[int]] = None,
    scope_ticker: Optional[str] = None,
) -> Optional[tuple[Any, dict, dict]]:
    """
    Run one service call, then re-query the DB and verify the ownership
    contract (A4):
      - result is a well-formed dict, status success/partial_success
      - NO rows inserted, NO rows deleted
      - PROTECTED columns (market/macro/targets) are byte-identical
      - if allowed_ids is given, OWNED columns of every OTHER row are
        also unchanged (scope discipline: a date/range run must not
        leak into rows outside the requested scope)
      - actual changed rows <= reported rows_updated

    Returns (result, before, after) or None if the call raised.
    """
    before = snapshot(session, scope_ticker)

    try:
        result = runner()
    except Exception as exc:  # noqa: BLE001
        check(False, f"{label}: raised unexpectedly: {exc}")
        return None

    print_result(label, result)
    after = snapshot(session, scope_ticker)

    check(isinstance(result, dict), f"{label}: returns a dict")
    if not isinstance(result, dict):
        return None

    for key in REQUIRED_RESULT_KEYS:
        check(key in result, f"{label}: result contains '{key}'")
    check(
        result.get("status") in ("success", "partial_success"),
        f"{label}: status is success/partial_success (got {result.get('status')})",
    )

    inserted_ids = set(after) - set(before)
    deleted_ids = set(before) - set(after)
    check(not inserted_ids, f"{label}: created NO daily_features rows (found {len(inserted_ids)} new)")
    check(not deleted_ids, f"{label}: deleted NO daily_features rows (found {len(deleted_ids)} gone)")
    if "rows_inserted" in result:
        check(result["rows_inserted"] == 0, f"{label}: reported rows_inserted == 0")

    protected_violations = []
    for rid in set(before) & set(after):
        changed = columns_changed(before[rid], after[rid], PROTECTED)
        if changed:
            protected_violations.append((rid, changed))
    check(
        not protected_violations,
        f"{label}: market/macro/target columns untouched"
        + (f" — VIOLATIONS: {protected_violations[:3]}" if protected_violations else ""),
    )

    changed_rows = [
        rid for rid in set(before) & set(after)
        if columns_changed(before[rid], after[rid], OWNED)
    ]

    if allowed_ids is not None:
        leaked = [rid for rid in changed_rows if rid not in allowed_ids]
        check(not leaked, f"{label}: no news columns changed outside requested scope ({len(leaked)} leaked)")

    check(
        len(changed_rows) <= n(result, "rows_updated"),
        f"{label}: rows actually changed ({len(changed_rows)}) <= reported rows_updated ({n(result, 'rows_updated')})",
    )

    return result, before, after


# ===========================================================================
# Tests
# ===========================================================================

def test_preconditions(session) -> bool:
    section("Preconditions (market service must have run first)")

    if MISSING_OWNED:
        print(f"  NOTE: NEWS_OWNED_COLUMNS not on the model, ignored: {MISSING_OWNED}")

    feature_rows = session.execute(select(func.count(DailyMarketFeatures.id))).scalar_one()
    headline_rows = session.execute(select(func.count(Headline.id))).scalar_one()
    scored = session.execute(
        select(func.count(Headline.id)).where(Headline.sentiment_score.isnot(None))
    ).scalar_one()

    print(f"  daily_features rows : {feature_rows}")
    print(f"  headlines           : {headline_rows}")
    print(f"  scored headlines    : {scored}")

    ok = check(feature_rows > 0, "daily_features has market rows (dependency: market calc -> news calc)")
    check(headline_rows > 0, "headlines table has real rows")
    if scored == 0:
        print("  NOTE: no scored headlines — run PUT /sentiment/update-unsentimental first "
              "for meaningful sentiment assertions.")
    return ok


def test_process_ticker(session, service, ticker: str) -> None:
    section(f"process_ticker('{ticker}')")
    method = get_method(service, "process_ticker")
    if method is None:
        return

    ids = {rid for rid, row in snapshot(session, ticker).items()}
    verify_run(
        session,
        f"process_ticker('{ticker}')",
        lambda: call(method, {"ticker": ticker}, (ticker,)),
        allowed_ids=ids,
        scope_ticker=None,  # snapshot whole table -> catches leakage into OTHER tickers
    )


def test_process_date(session, service, ticker: str) -> None:
    section("process_date()")
    method = get_method(service, "process_date")
    if method is None:
        return

    dates = feature_dates(session, ticker)
    if not dates:
        skip("process_date(): no daily_features dates for discovery ticker")
        return

    target = dates[-1]
    stmt = select(DailyMarketFeatures.id).where(DailyMarketFeatures.trading_date == target)
    allowed = set(session.execute(stmt).scalars().all())

    verify_run(
        session,
        f"process_date({target})",
        lambda: call(method, {"target_date": target}, (target,)),
        allowed_ids=allowed,
    )


def test_date_without_market_rows(session, service) -> None:
    section("Ownership rule: date with NO market rows must not create rows")
    method = get_method(service, "process_date")
    if method is None:
        return

    far_future = date.today() + timedelta(days=365)
    verify_run(
        session,
        f"process_date({far_future}) [no market rows exist]",
        lambda: call(method, {"target_date": far_future}, (far_future,)),
        allowed_ids=set(),
    )


def test_process_date_range(session, service, ticker: str) -> None:
    section("process_date_range()")
    method = get_method(service, "process_date_range")
    if method is None:
        return

    dates = feature_dates(session, ticker)
    if len(dates) < 3:
        skip(f"process_date_range(): need >= 3 feature dates for '{ticker}', have {len(dates)}")
        return

    # Deliberately a STRICT sub-range so the scope-leak check has teeth.
    start, end = dates[1], dates[-2]
    stmt = select(DailyMarketFeatures.id).where(
        DailyMarketFeatures.trading_date >= start,
        DailyMarketFeatures.trading_date <= end,
    )
    allowed = set(session.execute(stmt).scalars().all())

    verify_run(
        session,
        f"process_date_range({start} -> {end})",
        lambda: call(method, {"start_date": start, "end_date": end}, (start, end)),
        allowed_ids=allowed,
    )


def test_process_all(session, service) -> None:
    section("process_all()")
    method = get_method(service, "process_all")
    if method is None:
        return

    tickers = distinct_feature_tickers(session)
    out = verify_run(session, "process_all()", lambda: method())
    if out is None:
        return
    result, _, _ = out
    check(
        n(result, "tickers_processed", -1) <= len(tickers),
        f"tickers_processed ({result.get('tickers_processed')}) <= tickers with feature rows ({len(tickers)})",
    )


def test_idempotency(session, service, ticker: str) -> None:
    section("Idempotency: same run twice -> identical news columns")
    method = get_method(service, "process_ticker")
    if method is None:
        return

    run = lambda: call(method, {"ticker": ticker}, (ticker,))  # noqa: E731

    try:
        run()
        first = snapshot(session, ticker)
        run()
        second = snapshot(session, ticker)
    except Exception as exc:  # noqa: BLE001
        check(False, f"idempotency run raised unexpectedly: {exc}")
        return

    check(set(first) == set(second), "row set identical after second run (no duplicates created)")
    drift = [
        (rid, columns_changed(first[rid], second[rid], OWNED))
        for rid in set(first) & set(second)
        if columns_changed(first[rid], second[rid], OWNED)
    ]
    check(not drift, f"news columns identical after second run — drift: {drift[:3]}" if drift
          else "news columns identical after second run")


def test_value_invariants(session, ticker: str) -> None:
    section(f"Value sanity for '{ticker}'")
    rows = snapshot(session, ticker)
    if not rows:
        skip("value sanity: no rows")
        return

    populated = [r for r in rows.values() if r.get("relevant_headline_count")]
    print(f"  rows with relevant_headline_count > 0: {len(populated)}/{len(rows)}")
    if not populated:
        skip("value sanity: no rows carry news — nothing to bound-check "
             "(no relevant headlines for this ticker, or the service found none)")

    bad_count = [r["trading_date"] for r in rows.values()
                 if r.get("relevant_headline_count") is None or r["relevant_headline_count"] < 0]
    check(not bad_count, "relevant_headline_count is a non-null, non-negative integer on every row")

    for col in SENTIMENT_COLUMNS:
        if col not in ALL_COLUMNS:
            continue
        bad = [r["trading_date"] for r in rows.values()
               if r[col] is not None and not (-1.0 - 1e-6 <= r[col] <= 1.0 + 1e-6)]
        check(not bad, f"{col} within [-1, 1] where populated")

    for col in NON_NEGATIVE_COLUMNS:
        if col not in ALL_COLUMNS:
            continue
        bad = [r["trading_date"] for r in rows.values() if r[col] is not None and r[col] < 0]
        check(not bad, f"{col} >= 0 where populated")

    # A5: no news => NULL, not a fabricated 0.0
    fabricated = [r["trading_date"] for r in rows.values()
                  if r.get("relevant_headline_count") == 0
                  and r.get("average_sentiment") is not None]
    check(not fabricated,
          "days with 0 relevant headlines keep sentiment NULL (assumption A5: missing != 0.0)")


def _load_model(spec: tuple[str, str]) -> Optional[type]:
    try:
        return getattr(importlib.import_module(spec[0]), spec[1], None)
    except ModuleNotFoundError:
        return None


def test_cross_check_headlines(session, ticker: str) -> None:
    section(f"Independent cross-check against raw headlines ('{ticker}')")

    Assoc = _load_model(ASSOCIATION_MODEL)
    Security = _load_model(SECURITY_MODEL)
    if Assoc is None or Security is None:
        skip("cross-check: article_securities / securities models not importable "
             "(set ASSOCIATION_MODEL / SECURITY_MODEL in CONFIG once they exist)")
        return

    rows = [r for r in snapshot(session, ticker).values()
            if r.get("relevant_headline_count")]
    if not rows:
        skip("cross-check: no populated rows to verify")
        return

    sample = random.sample(rows, min(CROSS_CHECK_SAMPLE, len(rows)))

    for row in sample:
        day = row["trading_date"]
        pairs = session.execute(
            select(Headline.published_at, Headline.sentiment_score)
            .join(Assoc, Assoc.headline_id == Headline.id)
            .join(Security, Security.id == Assoc.security_id)
            .where(Security.ticker == ticker)
        ).all()

        same_day = [(ts, s) for ts, s in pairs if ts is not None and to_nairobi_date(ts) == day]
        scores = [s for _, s in same_day if s is not None]

        check(
            row["relevant_headline_count"] <= len(same_day),
            f"{ticker} {day}: relevant_headline_count ({row['relevant_headline_count']}) "
            f"<= associated headlines that Nairobi day ({len(same_day)})",
        )

        # Only compare the mean when no filtering could have happened
        # (service may legitimately drop low-impact headlines).
        if row["relevant_headline_count"] == len(same_day) and scores and row["average_sentiment"] is not None:
            expected = sum(scores) / len(scores)
            check(
                abs(row["average_sentiment"] - expected) <= CROSS_CHECK_TOL,
                f"{ticker} {day}: average_sentiment {row['average_sentiment']:.4f} "
                f"matches independent mean {expected:.4f}",
            )
        else:
            skip(f"{ticker} {day}: mean comparison skipped (service filtered headlines or none scored)")


def test_invalid_date_range(service) -> None:
    section("Error path: start_date after end_date")
    method = get_method(service, "process_date_range")
    if method is None:
        return

    start, end = date.today() + timedelta(days=1), date.today()
    try:
        result = call(method, {"start_date": start, "end_date": end}, (start, end))
    except Exception as exc:  # noqa: BLE001
        check(False, f"invalid range raised instead of returning a failure result: {exc}")
        return

    print_result("process_date_range(invalid range)", result)
    check(isinstance(result, dict), "invalid range returns a dict, not an exception")
    check(result.get("status") == "failed", f"status is 'failed' (got {result.get('status')})")
    check(bool(result.get("errors")), "errors list carries a validation message")


def test_nonexistent_ticker(session, service) -> None:
    section("Error path: nonexistent ticker")
    method = get_method(service, "process_ticker")
    if method is None:
        return

    fake = random_nonexistent_ticker()
    out = verify_run(
        session,
        f"process_ticker('{fake}')",
        lambda: call(method, {"ticker": fake}, (fake,)),
        allowed_ids=set(),
    )
    if out is None:
        return
    result, _, _ = out
    check(n(result, "rows_processed", -1) == 0,
          f"rows_processed is 0 for unknown ticker (got {result.get('rows_processed')})")
    check(result.get("status") in ("success", "partial_success"),
          "unknown ticker is not treated as a hard failure")


# ===========================================================================
# Coverage guard — surfaces public methods this file doesn't test
# ===========================================================================

TESTED_METHODS = {"process_all", "process_ticker", "process_date", "process_date_range"}


def report_untested_methods(service: Any) -> None:
    public = {
        name for name in dir(service)
        if not name.startswith("_") and callable(getattr(service, name, None))
    }
    untested = sorted(public - TESTED_METHODS)
    if untested:
        print(f"\nNOTE: public methods with no test in this file: {untested}\n"
              f"Add a test and register it in TESTED_METHODS.")


# ===========================================================================
# Orchestration
# ===========================================================================

def main() -> int:
    print("=" * 70)
    print("News Feature Service End-to-End Test (real MySQL database)")
    print("=" * 70)

    service_cls = resolve_service_class()
    if service_cls is None:
        print("\nCould not import the news feature service.\n"
              "Set SERVICE_MODULE_CANDIDATES / SERVICE_CLASS_CANDIDATES in the CONFIG block.")
        return 1

    session = SessionLocal()
    try:
        check(session is not None, "Database session established")
        service = service_cls(session)
        report_untested_methods(service)

        if not test_preconditions(session):
            print("\nNo daily_features rows — run MarketCalculatorService first. Cannot continue.")
            return 1

        ticker = ticker_with_most_feature_rows(session)
        print(f"\nDiscovered primary ticker for testing: {ticker}")

        test_process_ticker(session, service, ticker)
        test_process_date(session, service, ticker)
        test_date_without_market_rows(session, service)
        test_process_date_range(session, service, ticker)
        test_process_all(session, service)
        test_idempotency(session, service, ticker)
        test_value_invariants(session, ticker)
        test_cross_check_headlines(session, ticker)
        test_invalid_date_range(service)
        test_nonexistent_ticker(session, service)

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