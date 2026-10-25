"""
scripts/replay_volume_floor.py

Replays past trading days to validate candidate absolute share-volume floors
(config.signal_config.VOLUME_FLOOR_CANDIDATES) per liquidity tier.

    python -m scripts.replay_volume_floor [--split 0.6]

Read-only. Writes nothing; prints suggested config lines for you to paste.

HOW IT AVOIDS LOOKAHEAD
  - A day's volume ratio, tier and price direction use only that day and the
    20 sessions BEFORE it (same intelligence.signal_engine functions the live
    service uses, so replay and production cannot drift apart).
  - Forward returns (t+1, t+5) are used ONLY to score events afterwards.
  - Chronological split by date: first `--split` of days = selection period,
    the rest = holdout. The last 5 selection days are dropped (embargo) so a
    selection event's t+5 return never reaches into the holdout.
  - Floors are chosen on the selection period only; the holdout is shown for
    the chosen floor, never used to choose.

WHAT IS SCORED
  Events = volume ratio >= VOLUME_RATIO_MIN, shares >= floor, and a price move
  of at least PRICE_MOVE_PCT (direction d = +1 / -1). Sentiment is excluded
  here: it is sparse and has too little history to validate separately.
  Per event: directional forward return = d * (close[t+h] / close[t] - 1).
  Hit = directional return > 0. Flat = exactly 0 (common on illiquid NSE names).

CHOICE RULE (per tier, selection period)
  Among floors keeping >= MIN_EVENTS events AND >= MIN_RETENTION of the
  no-floor events, pick the highest mean next-day directional return.
  If no floor qualifies the tier is reported as "insufficient data".
"""

from __future__ import annotations

import argparse
import statistics
from collections import defaultdict
from typing import Any, Iterable, Optional

from config.signal_config import (
    VOLUME_FLOOR_CANDIDATES,
    VOLUME_FLOORS,
    VOLUME_LOOKBACK,
    VOLUME_RATIO_MIN,
)
from intelligence.signal_engine import classify_price, read_volume

HORIZONS = (1, 5)
EMBARGO_SESSIONS = max(HORIZONS)
MIN_EVENTS = 30
MIN_RETENTION = 0.5
MIN_SESSIONS_FOR_DECISION = 60
TIERS = ("low", "mid", "high")


# ---------------------------------------------------------------------------
# Pure functions (no DB) — importable and testable on their own
# ---------------------------------------------------------------------------

def build_observations(series: dict[str, list[Any]]) -> list[dict[str, Any]]:
    """
    One observation per (ticker, session) that has VOLUME_LOOKBACK prior
    sessions. Rows need: trading_date, close_price, daily_return, volume.
    """
    observations: list[dict[str, Any]] = []

    for ticker, rows in series.items():
        closes = [r.close_price for r in rows]

        for i in range(VOLUME_LOOKBACK, len(rows)):
            row = rows[i]
            reading = read_volume(
                row.volume, [r.volume for r in rows[i - VOLUME_LOOKBACK:i]], floor=0
            )
            if reading is None:
                continue

            obs: dict[str, Any] = {
                "ticker": ticker,
                "date": row.trading_date,
                "tier": reading.tier,
                "ratio": reading.ratio,
                "volume": row.volume,
                "dir": classify_price(row.daily_return),
            }
            for h in HORIZONS:
                j = i + h
                base = closes[i]
                obs[f"fwd{h}"] = (
                    closes[j] / base - 1
                    if j < len(rows) and base and closes[j] is not None
                    else None
                )
            observations.append(obs)

    return observations


def split_dates(
    observations: list[dict[str, Any]], train_fraction: float
) -> tuple[set, set, list]:
    """Chronological selection / holdout date sets (with embargo) and all dates."""
    dates = sorted({o["date"] for o in observations})
    split_idx = int(len(dates) * train_fraction)
    selection = set(dates[: max(0, split_idx - EMBARGO_SESSIONS)])
    holdout = set(dates[split_idx:])
    return selection, holdout, dates


def events_for(observations: Iterable[dict[str, Any]], floor: int) -> list[dict[str, Any]]:
    return [
        o for o in observations
        if o["ratio"] >= VOLUME_RATIO_MIN and o["volume"] >= floor and o["dir"] in (1, -1)
    ]


def summarize(events: list[dict[str, Any]], eligible: int) -> dict[str, Any]:
    out: dict[str, Any] = {"n": len(events), "freq": len(events) / eligible if eligible else 0.0}
    for h in HORIZONS:
        directional = [e["dir"] * e[f"fwd{h}"] for e in events if e[f"fwd{h}"] is not None]
        out[f"n{h}"] = len(directional)
        out[f"hit{h}"] = sum(1 for x in directional if x > 0) / len(directional) if directional else None
        out[f"flat{h}"] = sum(1 for x in directional if x == 0) / len(directional) if directional else None
        out[f"mean{h}"] = statistics.fmean(directional) if directional else None
        out[f"median{h}"] = statistics.median(directional) if directional else None
    return out


def choose_floor(selection_obs: list[dict[str, Any]]) -> Optional[tuple[int, dict[str, Any]]]:
    baseline = len(events_for(selection_obs, 0))
    best: Optional[tuple[int, dict[str, Any]]] = None

    for floor in VOLUME_FLOOR_CANDIDATES:
        stats = summarize(events_for(selection_obs, floor), len(selection_obs))
        if stats["n"] < MIN_EVENTS or stats["n"] < MIN_RETENTION * baseline:
            continue
        if stats["mean1"] is None:
            continue
        if best is None or stats["mean1"] > best[1]["mean1"]:
            best = (floor, stats)

    return best


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _pct(value: Optional[float], digits: int = 1) -> str:
    return "  n/a" if value is None else f"{value * 100:.{digits}f}%"


def _row(label: str, s: dict[str, Any]) -> str:
    return (
        f"  {label:<10} events={s['n']:>5} freq={s['freq'] * 100:5.2f}%  "
        f"hit1={_pct(s['hit1']):>6} flat1={_pct(s['flat1']):>6} mean1={_pct(s['mean1'], 2):>7} "
        f"med1={_pct(s['median1'], 2):>7} | hit5={_pct(s['hit5']):>6} mean5={_pct(s['mean5'], 2):>7} "
        f"(n5={s['n5']})"
    )


def report(observations: list[dict[str, Any]], train_fraction: float) -> None:
    selection_dates, holdout_dates, dates = split_dates(observations, train_fraction)
    selection = [o for o in observations if o["date"] in selection_dates]
    holdout = [o for o in observations if o["date"] in holdout_dates]

    print("=" * 78)
    print("Volume floor replay")
    print("=" * 78)
    print(f"sessions with 20 prior days : {len(dates)}  ({dates[0]} -> {dates[-1]})" if dates else "no data")
    print(f"tickers                     : {len({o['ticker'] for o in observations})}")
    print(f"selection / holdout days    : {len(selection_dates)} / {len(holdout_dates)} "
          f"(embargo {EMBARGO_SESSIONS} sessions)")
    print(f"ratio >= {VOLUME_RATIO_MIN}x of previous-{VOLUME_LOOKBACK}-session mean; "
          f"price move >= configured threshold")

    if len(dates) < MIN_SESSIONS_FOR_DECISION:
        print(
            f"\n!! Only {len(dates)} usable sessions (< {MIN_SESSIONS_FOR_DECISION}). Per-tier cells will be thin; "
            "treat this as a harness check, not a decision. Re-run as history accumulates."
        )

    # Descriptive only: helps you sanity-check LIQUIDITY_TIER_CUTOFFS.
    by_ticker: dict[str, list[float]] = defaultdict(list)
    for o in observations:
        by_ticker[o["ticker"]].append(o["volume"])
    medians = sorted(statistics.median(v) for v in by_ticker.values() if v)
    if len(medians) >= 4:
        q = statistics.quantiles(medians, n=10)
        print(f"\nper-ticker median shares/day: p10={q[0]:,.0f}  p50={q[4]:,.0f}  p90={q[8]:,.0f}")

    suggestions: dict[str, Any] = {}

    for tier in TIERS:
        sel_t = [o for o in selection if o["tier"] == tier]
        hold_t = [o for o in holdout if o["tier"] == tier]
        print(f"\n--- tier: {tier}  (selection obs={len(sel_t)}, holdout obs={len(hold_t)}) ---")

        print(" selection period")
        print(_row("no floor", summarize(events_for(sel_t, 0), len(sel_t))))
        for floor in VOLUME_FLOOR_CANDIDATES:
            print(_row(f"floor {floor:,}", summarize(events_for(sel_t, floor), len(sel_t))))

        chosen = choose_floor(sel_t)
        if chosen is None:
            print(f" -> insufficient data (need >= {MIN_EVENTS} events and >= "
                  f"{int(MIN_RETENTION * 100)}% retained); keep provisional {VOLUME_FLOORS[tier]:,}")
            suggestions[tier] = None
            continue

        floor, _ = chosen
        print(f" -> chosen on selection: {floor:,} shares")
        print(" holdout period (not used for the choice)")
        print(_row("no floor", summarize(events_for(hold_t, 0), len(hold_t))))
        print(_row(f"floor {floor:,}", summarize(events_for(hold_t, floor), len(hold_t))))
        suggestions[tier] = floor

    print("\nSuggested config/signal_config.py (review the holdout rows first):")
    merged = {t: (suggestions[t] if suggestions[t] is not None else VOLUME_FLOORS[t]) for t in TIERS}
    print(f"VOLUME_FLOORS = {merged}")
    kept = [t for t in TIERS if suggestions[t] is None]
    if kept:
        print(f"# unchanged (insufficient data): {kept}")


# ---------------------------------------------------------------------------
# DB loading + entry point
# ---------------------------------------------------------------------------

def load_series(db) -> dict[str, list[Any]]:
    from sqlalchemy import select

    from models.daily_market_features import DailyMarketFeatures as F

    rows = db.execute(
        select(F.ticker, F.trading_date, F.close_price, F.daily_return, F.volume)
        .order_by(F.ticker.asc(), F.trading_date.asc())
    ).all()

    series: dict[str, list[Any]] = defaultdict(list)
    for row in rows:
        series[row.ticker].append(row)
    return series


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay volume floors on historical NSE data.")
    parser.add_argument("--split", type=float, default=0.6, help="Selection fraction (default 0.6).")
    args = parser.parse_args()

    from database.session import SessionLocal

    db = SessionLocal()
    try:
        observations = build_observations(load_series(db))
    finally:
        db.close()

    if not observations:
        print(f"No observations: need tickers with > {VOLUME_LOOKBACK} sessions in daily_features.")
        return 1

    report(observations, args.split)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())