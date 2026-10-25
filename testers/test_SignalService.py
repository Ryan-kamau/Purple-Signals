"""
testers/test_SignalService.py

Standalone tester for the combined signal logic. Same style as the other
testers: plain script, no pytest, [PASS]/[FAIL]/[SKIP] counters.

    python -m testers.test_SignalService            # engine + real-DB service
    python -m testers.test_SignalService --no-db    # engine only (no database)

Section A  signal_engine rules (pure, no DB)
Section B  SignalService against the real MySQL database. NOTE: it WRITES
           rows into analysis_results (that is the feature under test) and
           does not roll them back. Run the SQL migration first.
"""

import sys
from datetime import date, timedelta

from config.signal_config import PRICE_MOVE_PCT, VOLUME_RATIO_MIN
from intelligence.signal_engine import evaluate

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
    print("\n" + "-" * 70 + f"\n{title}\n" + "-" * 70)


# ---------------------------------------------------------------------------
# A. Engine
# ---------------------------------------------------------------------------

HIGH = [10_000.0] * 20   # avg 10k, median 10k -> "high" tier, floor 10,000
LOW = [100.0] * 20       # avg 100,  median 100  -> "low" tier,  floor 1,000


def run(daily_return, volume, prior, sentiment=None, count=0):
    return evaluate(
        daily_return=daily_return,
        volume=volume,
        prior_volumes=prior,
        weighted_sentiment=sentiment,
        average_sentiment=sentiment,
        headline_count=count,
    )


def expect(label, result, signal, strength=None, confidence=None):
    ok = result is not None and result.signal == signal and result.strength == strength
    if ok and confidence is not None:
        ok = abs(result.confidence_score - confidence) < 1e-3
    got = None if result is None else (result.signal, result.strength, result.confidence_score)
    check(ok, f"{label} -> {signal}/{strength}" + ("" if ok else f"  (got {got})"))


def test_engine() -> None:
    section("A. signal_engine rules (no database)")

    expect("spike + up + positive sentiment", run(0.05, 31_000, HIGH, 0.4, 2), "bullish", "strong", 1.0)
    expect("spike + up + no news", run(0.05, 31_000, HIGH), "bullish", "moderate", 0.6667)
    expect("spike + up + neutral sentiment", run(0.05, 31_000, HIGH, 0.05, 1), "bullish", "moderate")
    expect("spike + down + negative sentiment", run(-0.05, 31_000, HIGH, -0.4, 2), "bearish", "strong")
    expect("spike + down + no news", run(-0.05, 31_000, HIGH), "bearish", "moderate")
    expect("spike + up + NEGATIVE sentiment", run(0.05, 31_000, HIGH, -0.5, 2), "divergence", None, 0.0)
    expect("no spike + up + negative sentiment", run(0.05, 20_000, HIGH, -0.5, 2), "divergence")
    expect("spike, flat price", run(0.0, 31_000, HIGH), "unusual_activity", None, 0.3333)
    expect("no spike, price up, no news", run(0.05, 20_000, HIGH), "neutral", None, 0.0)

    # Relative-volume boundary: exactly 3.0x counts, just under does not.
    check(run(0.05, 30_000, HIGH).components["volume_spike"] is True, f"ratio == {VOLUME_RATIO_MIN}x is a spike")
    check(run(0.05, 29_990, HIGH).components["volume_spike"] is False, "ratio just under 3x is not a spike")

    # Price boundary.
    check(run(PRICE_MOVE_PCT, 31_000, HIGH).components["price"] == 1, "price == +threshold counts as up")
    check(run(PRICE_MOVE_PCT - 1e-6, 31_000, HIGH).components["price"] == 0, "price just under threshold is flat")

    # Absolute floor (low tier -> 1,000 shares): 4x but only 400 shares is NOT a spike.
    low_small = run(0.05, 400, LOW)
    check(low_small.components["volume_ratio"] == 4.0 and low_small.components["volume_spike"] is False,
          "4x on 400 shares is filtered by the 1,000-share floor")
    expect("  ...and the signal is neutral", low_small, "neutral")
    expect("15x on 1,500 shares clears the floor", run(0.05, 1_500, LOW), "bullish", "moderate")
    check("floor" in low_small.explanation, "explanation says why it was filtered")

    # Sentiment gating.
    check(run(0.05, 31_000, HIGH, 0.9, 0).components["sentiment"] is None,
          "sentiment ignored with 0 scored headlines")

    # Cold start / unassessable volume -> None (service counts it as skipped).
    check(run(0.05, 31_000, HIGH[:19]) is None, "19 prior sessions -> None (cold start)")
    check(run(0.05, None, HIGH) is None, "missing volume -> None")
    check(run(0.05, 5_000, [0.0] * 20) is None, "zero average volume -> None (ratio undefined)")

    # No lookahead: today's volume must not influence its own baseline.
    with_today_in_window = run(0.05, 31_000, HIGH[1:] + [31_000.0])
    check(with_today_in_window.components["volume_ratio"] < run(0.05, 31_000, HIGH).components["volume_ratio"],
          "a high prior day lowers the ratio (baseline = previous sessions only)")

    r = run(0.0542, 31_000, HIGH, 0.41, 2)
    check(r.components["daily_return"] == 0.0542 and r.components["headline_count"] == 2,
          "components JSON carries the raw inputs")
    check(len(r.explanation) <= 1000, "explanation fits the 1000-char column")


# ---------------------------------------------------------------------------
# B. Service (real DB)
# ---------------------------------------------------------------------------

def test_service() -> None:
    from sqlalchemy import func, select

    from database.session import SessionLocal
    from models.analytics_data import AnalysisResult
    from models.daily_market_features import DailyMarketFeatures as F
    from services.signal_service.SignalService import SignalService

    section("B. SignalService (real MySQL database; writes analysis_results)")

    session = SessionLocal()
    try:
        feature_rows = session.execute(select(func.count(F.id))).scalar_one()
        if not check(feature_rows > 0, f"daily_features has rows ({feature_rows})"):
            return

        top = session.execute(
            select(F.ticker, func.count(F.id)).group_by(F.ticker).order_by(func.count(F.id).desc())
        ).first()
        ticker = top[0]
        print(f"primary ticker: {ticker} ({top[1]} feature rows)")

        service = SignalService(session)

        def count_for(t: str) -> int:
            return session.execute(
                select(func.count(AnalysisResult.id)).where(AnalysisResult.ticker == t)
            ).scalar_one()

        # --- process_ticker -------------------------------------------------
        first = service.process_ticker(ticker)
        print(f"first run: {first}")
        for key in ("status", "tickers_processed", "rows_processed", "rows_inserted",
                    "rows_updated", "rows_skipped", "errors"):
            check(key in first, f"result contains '{key}'")
        check(first["status"] in ("success", "partial_success"), f"status ok ({first['status']})")
        check(first["rows_processed"] == first["rows_inserted"] + first["rows_updated"] + first["rows_skipped"],
              "rows_processed == inserted + updated + skipped")

        session.expire_all()
        stored = count_for(ticker)
        check(stored == first["rows_inserted"] + first["rows_updated"],
              f"DB rows for {ticker} ({stored}) == inserted + updated")
        check(session.execute(select(func.count(F.id))).scalar_one() == feature_rows,
              "daily_features untouched")

        # --- idempotency / uniqueness --------------------------------------
        second = service.process_ticker(ticker)
        session.expire_all()
        check(second["rows_inserted"] == 0, "second run inserts 0 rows")
        check(second["rows_updated"] == stored, f"second run updates the same {stored} rows")
        distinct_dates = session.execute(
            select(func.count(func.distinct(AnalysisResult.trading_date)))
            .where(AnalysisResult.ticker == ticker)
        ).scalar_one()
        check(distinct_dates == count_for(ticker), "one row per (ticker, trading_date)")

        # --- stored values obey the rules ---------------------------------
        rows = session.execute(select(AnalysisResult).where(AnalysisResult.ticker == ticker)).scalars().all()
        bad = []
        for row in rows:
            c = row.components or {}
            if not 0.0 <= row.confidence_score <= 1.0:
                bad.append((row.trading_date, "confidence out of range"))
            if row.signal in ("bullish", "bearish"):
                if not (c.get("volume_spike") and c.get("volume_ratio", 0) >= VOLUME_RATIO_MIN
                        and c.get("volume", 0) >= c.get("volume_floor", 10**12)):
                    bad.append((row.trading_date, "directional signal without a valid spike"))
                move = c.get("daily_return") or 0
                if (row.signal == "bullish" and move < PRICE_MOVE_PCT) or \
                   (row.signal == "bearish" and move > -PRICE_MOVE_PCT):
                    bad.append((row.trading_date, "price does not match direction"))
        check(not bad, f"stored rows obey the rules ({len(rows)} checked)" + (f" — {bad[:3]}" if bad else ""))

        # --- date scoping ---------------------------------------------------
        dates = [r.trading_date for r in rows]
        if dates:
            target = max(dates)
            by_date = service.process_date(target)
            check(by_date["status"] in ("success", "partial_success"), f"process_date({target}) ok")
            rng = service.process_date_range(target - timedelta(days=7), target)
            check(rng["status"] in ("success", "partial_success"), "process_date_range ok")
        else:
            skip("date scoping: no stored rows (need > 20 sessions of history)")

        # --- read API -------------------------------------------------------
        latest = service.get_signals(ticker=ticker, limit=5)
        check(len(latest) <= 5, "get_signals respects limit")
        if len(latest) >= 2:
            check(latest[0].trading_date >= latest[1].trading_date, "get_signals is newest-first")

        # --- error paths ----------------------------------------------------
        bad_range = service.process_date_range(date.today() + timedelta(days=1), date.today())
        check(bad_range["status"] == "failed" and bool(bad_range["errors"]), "start after end -> failed")
        unknown = service.process_ticker("ZZZNOPE")
        check(unknown["rows_processed"] == 0 and unknown["status"] == "success",
              "unknown ticker -> 0 rows, not a hard failure")

        # --- registry coverage ---------------------------------------------
        full = service.process_all()
        coverage = full.get("registry_coverage", {})
        check({"registry_total", "with_signals", "registry_without_features",
               "features_not_in_registry"} <= set(coverage), "process_all reports registry coverage")
        print(f"coverage: {coverage.get('with_signals')}/{coverage.get('registry_total')} registry companies; "
              f"not in registry: {coverage.get('features_not_in_registry')}")
    finally:
        session.close()


def main() -> int:
    print("=" * 70)
    print("Combined signal logic test")
    print("=" * 70)

    test_engine()

    if "--no-db" in sys.argv:
        skip("Section B skipped by --no-db")
    else:
        test_service()

    print("\n" + "=" * 70)
    print(f"Passed: {_PASS}   Failed: {_FAIL}   Skipped: {_SKIP}")
    print("=" * 70)
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())