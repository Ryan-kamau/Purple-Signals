"""
testers/test_fundamentals_ingestion.py

Standalone, real-database end-to-end tester for FundamentalsService.
Plain script — NOT pytest, no mocks.

    python -m testers.test_fundamentals_ingestion          # run from project root
    python -m testers.test_fundamentals_ingestion --keep   # leave test rows in the DB

What it does:
  1. Builds a sample CSV with deliberately dated rows (1999-12-31, 1999-06-30)
     so they can never collide with a real report, plus one row dated on
     KPLC's latest market_data day (only if market data exists) to exercise
     the price-based P/E and yield calculation.
  2. Ingests it and verifies inserts, per-row errors, warnings, units
     multiplier, calculated ratios and "supplied ratio wins".
  3. Re-ingests: nothing must be inserted (idempotent).
  4. NULLs one column and re-ingests: it must be filled (fill-NULL rule).
  5. Re-ingests with a changed EPS: the stored EPS must NOT change.
  6. Calls get_latest / get_history.
  7. Removes ONLY the rows this run created (unless --keep).

Prerequisite: the fundamentals table must already have the new columns and
the unique constraint (see the ALTER TABLE script).
"""

import os
import sys
import tempfile
from datetime import date, datetime
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import delete, func, select, update

from database.session import SessionLocal
from models.fundamental_data import Fundamentals
from models.market_data import MarketData
from services.fundamentals_service import FundamentalsService

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")
TICKER = "KPLC"
D1 = date(1999, 12, 31)
D2 = date(1999, 6, 30)

HEADER = (
    "ticker,report_date,eps,revenue,net_profit,shareholders_equity,"
    "total_liabilities,dividend_per_share,pe_ratio"
)

_PASS = _FAIL = 0


def check(condition: bool, message: str) -> bool:
    global _PASS, _FAIL
    if condition:
        _PASS += 1
        print(f"[PASS] {message}")
    else:
        _FAIL += 1
        print(f"[FAIL] {message}")
    return condition


def section(title: str) -> None:
    print("\n" + "-" * 70 + f"\n{title}\n" + "-" * 70)


def approx(a: Optional[float], b: float, tol: float) -> bool:
    return a is not None and abs(a - b) <= tol


def fetch_row(session, d: date) -> Optional[Fundamentals]:
    session.expire_all()
    return session.execute(
        select(Fundamentals).where(
            Fundamentals.ticker == TICKER, func.date(Fundamentals.report_date) == d
        )
    ).scalars().first()


def latest_market_point(session) -> tuple[Optional[date], Optional[float]]:
    row = session.execute(
        select(MarketData.timestamp, MarketData.price)
        .where(MarketData.ticker == TICKER)
        .order_by(MarketData.timestamp.desc())
        .limit(1)
    ).first()
    if row is None:
        return None, None
    ts = row[0]
    ts_date = ts.astimezone(NAIROBI_TZ).date() if ts.tzinfo else ts.date()
    return ts_date, float(row[1])


def build_csv(eps_row1: str, price_date: Optional[date]) -> str:
    lines = [
        HEADER,
        # valid: ratios calculated, no market data in 1999 -> warning, pe/yield NULL
        f"{TICKER},{D1},{eps_row1},219285,13500,109335,250000,0.5,",
        # valid: supplied pe_ratio must win
        f"{TICKER},{D2},10.00,200000,9000,100000,240000,0.4,9.99",
        # invalid: ticker not in registry
        "ZZZZ,1999-12-31,1,1,1,1,1,1,",
        # invalid: non-numeric eps
        f"{TICKER},1999-03-31,abc,1,1,1,1,1,",
        # invalid: duplicate (ticker, date) within the file
        f"{TICKER},{D1},{eps_row1},219285,13500,109335,250000,0.5,",
    ]
    if price_date is not None:
        lines.append(f"{TICKER},{price_date},10.00,200000,9000,100000,240000,0.4,")
    return "\n".join(lines) + "\n"


def run_ingest(service: FundamentalsService, csv_text: str, label: str) -> dict:
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8") as tmp:
        tmp.write(csv_text)
        path = tmp.name
    try:
        result = service.ingest_csv(path)
    finally:
        os.remove(path)

    print(f"\n{label}:")
    for key in ("status", "message", "rows_saved", "rows_inserted",
                "rows_updated", "rows_unchanged", "rows_skipped"):
        print(f"  {key}: {result[key]}")
    for err in result["errors"]:
        print(f"  error:   {err}")
    for warn in result["warnings"]:
        print(f"  warning: {warn}")
    return result


def main() -> int:
    keep = "--keep" in sys.argv
    session = SessionLocal()
    service = FundamentalsService(session)

    price_date, price = latest_market_point(session)
    if price_date in (D1, D2):
        price_date = None

    test_dates = [D1, D2] + ([price_date] if price_date else [])
    pre_existing = {d for d in test_dates if fetch_row(session, d) is not None}

    if pre_existing & {D1, D2}:
        print(f"Aborting: sample dates already present in fundamentals: {pre_existing}")
        session.close()
        return 1
    if price_date and price_date in pre_existing:
        print(f"Note: a real {TICKER} row exists on {price_date}; price-based check skipped.")
        price_date = None
        test_dates = [D1, D2]

    expected_inserts = len(test_dates)

    try:
        section("Run 1 — first ingest")
        r1 = run_ingest(service, build_csv("12.54", price_date), "ingest #1")
        check(r1["status"] == "partial_success", "status is partial_success (3 bad rows)")
        check(r1["rows_inserted"] == expected_inserts, f"rows_inserted == {expected_inserts}")
        check(r1["rows_skipped"] == 3, "3 invalid rows reported (registry, number, duplicate)")
        check(len(r1["warnings"]) >= 1, "warnings reported for rows without a market price")

        row1, row2 = fetch_row(session, D1), fetch_row(session, D2)
        if check(row1 is not None, "row for 1999-12-31 persisted"):
            check(approx(row1.revenue, 219285e6, 1), "revenue scaled by units multiplier (KES millions)")
            check(approx(row1.net_profit, 13500e6, 1), "net_profit scaled by units multiplier")
            check(approx(row1.eps, 12.54, 1e-3), "eps NOT scaled")
            check(approx(row1.net_profit_margin, 13500 / 219285, 1e-5), "net_profit_margin calculated")
            check(approx(row1.debt_ratio, 250000 / 109335, 1e-3), "debt_ratio calculated")
            check(row1.pe_ratio is None, "pe_ratio NULL when no price exists on/before report_date")
        if check(row2 is not None, "row for 1999-06-30 persisted"):
            check(approx(row2.pe_ratio, 9.99, 1e-3), "supplied pe_ratio takes precedence")

        if price_date:
            prow = fetch_row(session, price_date)
            if check(prow is not None, f"row for market day {price_date} persisted"):
                check(approx(prow.pe_ratio, price / 10.0, 1e-2), "pe_ratio = price / eps from market_data")
                check(approx(prow.dividend_yield, 0.4 / price, 1e-4), "dividend_yield = dps / price")

        section("Run 2 — idempotency")
        r2 = run_ingest(service, build_csv("12.54", price_date), "ingest #2")
        check(r2["rows_inserted"] == 0, "no new rows inserted on re-ingest")
        check(r2["rows_unchanged"] == expected_inserts, "all valid rows reported unchanged")

        section("Run 3 — fill-NULL rule")
        session.execute(
            update(Fundamentals).where(Fundamentals.id == row1.id).values(net_profit_margin=None)
        )
        session.commit()
        r3 = run_ingest(service, build_csv("12.54", price_date), "ingest #3")
        check(r3["rows_updated"] == 1, "exactly one row updated (the NULLed one)")
        check(fetch_row(session, D1).net_profit_margin is not None, "NULL column was refilled")

        section("Run 4 — never overwrite non-NULL values")
        r4 = run_ingest(service, build_csv("99.99", price_date), "ingest #4 (eps changed in CSV)")
        check(approx(fetch_row(session, D1).eps, 12.54, 1e-3), "stored eps unchanged")
        check(r4["rows_inserted"] == 0 and r4["rows_updated"] == 0, "nothing inserted or updated")

        section("Run 5 — file-level failures")
        r5 = run_ingest(service, "ticker,report_date\nKPLC,1999-01-01\n", "ingest #5 (bad header)")
        check(r5["status"] == "failed" and r5["rows_saved"] == 0, "missing columns -> failed, nothing saved")

        section("Read methods")
        latest = service.get_latest(TICKER)
        check(latest["status"] == "success" and latest["count"] == 1, "get_latest returns one row")
        if latest["data"]:
            print(f"  latest report_date: {latest['data'][0]['report_date']}")
            check("current_pe_ratio" in latest["data"][0], "get_latest includes current_pe_ratio")
        history = service.get_history(ticker=TICKER, limit=5)
        check(history["status"] == "success" and history["count"] >= 1, "get_history returns rows")
        check(service.get_history(limit=0)["status"] == "failed", "get_history(limit=0) -> failed envelope")

    finally:
        if keep:
            print("\n--keep supplied: test rows left in the database.")
        else:
            removable = [d for d in test_dates if d not in pre_existing]
            for d in removable:
                row = fetch_row(session, d)
                if row is not None:
                    session.execute(delete(Fundamentals).where(Fundamentals.id == row.id))
            session.commit()
            print(f"\nCleanup: removed {len(removable)} test row(s) created by this run.")
        session.close()

    print("\n" + "=" * 70)
    print(f"Passed: {_PASS}   Failed: {_FAIL}")
    print("=" * 70)
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())