"""
config/analysis_config.py

Settings for the read-only Master Intelligence Service (GET /analysis).

Nothing here changes how any other part of the pipeline behaves.

!! ASSUMPTION TO VERIFY !!
    The alert engine's signal-freshness rule and the Alert "active" status
    value were NOT visible when this was written. Two places below are the
    ONLY swap points:

      * ALERT_ACTIVE_STATUS          -> the status value your Alert rows use
      * is_signal_stale()            -> delegate to your existing alert
                                        freshness check, so /analysis and the
                                        alert engine can never disagree.

    The defaults below are placeholders, not a competing rule: replace the
    body of is_signal_stale() with a call to the existing implementation.
"""

from __future__ import annotations

from datetime import date, timedelta

# --- Pagination ------------------------------------------------------------
DEFAULT_LIMIT: int = 50
MAX_LIMIT: int = 200

# --- Alerts ------------------------------------------------------------------
ALERT_ACTIVE_STATUS: str = "active"

# --- Freshness (per source, matched to reporting frequency) -----------------
# Market-date convention: Africa/Nairobi calendar date, weekends skipped,
# no holiday calendar (same operational definition MarketCalculatorService uses).
SIGNAL_MAX_AGE_BUSINESS_DAYS: int = 3            # PLACEHOLDER - see module docstring
MARKET_FEATURES_MAX_AGE_BUSINESS_DAYS: int = 3   # daily data
FUNDAMENTALS_MAX_AGE_DAYS: int = 365             # reports are half-yearly/annual + publication lag
MACRO_MAX_LAG_MONTHS: int = 3                    # KNBS publishes ~2 months behind


def business_days_between(start: date, end: date) -> int:
    """
    Number of Mon-Fri days in the half-open interval (start, end].

    Friday -> Monday is 1. Returns 0 when end <= start (a future-dated
    record is never "old").
    """
    if end <= start:
        return 0

    total_days = (end - start).days
    full_weeks, remainder = divmod(total_days, 7)
    count = full_weeks * 5

    cursor = start + timedelta(days=full_weeks * 7)
    for _ in range(remainder):
        cursor += timedelta(days=1)
        if cursor.weekday() < 5:
            count += 1
    return count


def is_signal_stale(trading_date: date, reference_date: date) -> bool:
    """Signal freshness. SWAP POINT: delegate to the existing alert rule."""
    return business_days_between(trading_date, reference_date) > SIGNAL_MAX_AGE_BUSINESS_DAYS


def is_market_features_stale(trading_date: date, reference_date: date) -> bool:
    return business_days_between(trading_date, reference_date) > MARKET_FEATURES_MAX_AGE_BUSINESS_DAYS


def is_fundamentals_stale(report_date: date, reference_date: date) -> bool:
    return (reference_date - report_date).days > FUNDAMENTALS_MAX_AGE_DAYS


def is_macro_stale(year: int, month: int, reference_date: date) -> bool:
    """month is 1-12. A period in the future is never stale."""
    months_behind = (reference_date.year - year) * 12 + (reference_date.month - month)
    return months_behind > MACRO_MAX_LAG_MONTHS


def freshness_rules() -> dict[str, int]:
    """Echoed in the response metadata so consumers see the thresholds applied."""
    return {
        "signal_max_age_business_days": SIGNAL_MAX_AGE_BUSINESS_DAYS,
        "market_features_max_age_business_days": MARKET_FEATURES_MAX_AGE_BUSINESS_DAYS,
        "fundamentals_max_age_days": FUNDAMENTALS_MAX_AGE_DAYS,
        "macro_max_lag_months": MACRO_MAX_LAG_MONTHS,
    }