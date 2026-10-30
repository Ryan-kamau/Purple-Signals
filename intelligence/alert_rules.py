"""
intelligence/alert_rules.py

Pure, stateless alert rules. No DB, no HTTP, no FastAPI.

Pipeline position:
    AnalysisResult (persisted)  ->  evaluate_alert_rule()  ->  AlertDecision
                                                          ->  AlertEvaluationService

Nothing here recomputes price / volume / sentiment / the combined signal; the
persisted AnalysisResult is authoritative (intelligence/signal_engine.py).
Invalid input raises InvalidSignalError so callers SKIP it with a reason
rather than inventing a classification (never default to "neutral").

Severity (single source of truth, `severity_for`):
    neutral            -> info
    strength "strong"   -> high
    strength "moderate" -> medium
    strength None       -> low
Messages DESCRIBE the stored signal; they are not buy/sell advice (PRD 5.9).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

from config.alert_config import MAX_MARKET_DATA_AGE_DAYS, MAX_SIGNAL_LAG_DAYS
from intelligence.signal_engine import (
    SIGNAL_BEARISH,
    SIGNAL_BULLISH,
    SIGNAL_DIVERGENCE,
    SIGNAL_NEUTRAL,
    SIGNAL_UNUSUAL,
    STRENGTH_MODERATE,
    STRENGTH_STRONG,
)

ALERT_TYPES: tuple[str, ...] = (
    SIGNAL_BULLISH,
    SIGNAL_BEARISH,
    SIGNAL_DIVERGENCE,
    SIGNAL_UNUSUAL,
    SIGNAL_NEUTRAL,
)

SEVERITY_HIGH = "high"
SEVERITY_MEDIUM = "medium"
SEVERITY_LOW = "low"
SEVERITY_INFO = "info"

VALID_STRENGTHS = (STRENGTH_STRONG, STRENGTH_MODERATE, None)

# Freshness reason codes
STALE_SIGNAL = "stale_signal"
STALE_MARKET = "market_data_stalled"

_HEADLINES: dict[str, str] = {
    SIGNAL_BULLISH: "Bullish combined signal: rising price on a volume spike",
    SIGNAL_BEARISH: "Bearish combined signal: falling price on a volume spike",
    SIGNAL_DIVERGENCE: "Divergence: price and news sentiment point in opposite directions",
    SIGNAL_UNUSUAL: "Unusual activity: volume spike without a clear price direction",
    SIGNAL_NEUTRAL: "No qualifying directional signal",
}

_MESSAGE_MAX = 1000


class InvalidSignalError(ValueError):
    """A stored signal cannot safely be turned into an alert."""


@dataclass(frozen=True)
class AlertDecision:
    ticker: str
    alert_type: str
    severity: str
    trading_date: date
    signal_strength: Optional[str]
    confidence_score: float
    message: str
    evidence: dict[str, Any]


def severity_for(alert_type: str, strength: Optional[str]) -> str:
    """Centralised severity mapping."""
    if alert_type == SIGNAL_NEUTRAL:
        return SEVERITY_INFO
    if strength == STRENGTH_STRONG:
        return SEVERITY_HIGH
    if strength == STRENGTH_MODERATE:
        return SEVERITY_MEDIUM
    if strength is None:
        return SEVERITY_LOW
    raise InvalidSignalError(f"unknown signal strength {strength!r}")


def evaluate_alert_rule(analysis_result: Any) -> AlertDecision:
    """
    Turn one stored AnalysisResult-like object into an AlertDecision.

    Raises:
        InvalidSignalError: missing/invalid ticker, trading_date, signal,
            strength, confidence_score, or malformed components.
    """
    ticker = str(getattr(analysis_result, "ticker", "") or "").strip().upper()
    if not ticker:
        raise InvalidSignalError("missing ticker")

    trading_date = getattr(analysis_result, "trading_date", None)
    if not isinstance(trading_date, date):
        raise InvalidSignalError("missing or invalid trading_date")

    signal = getattr(analysis_result, "signal", None)
    if signal not in ALERT_TYPES:
        raise InvalidSignalError(f"invalid signal classification {signal!r}")

    strength = getattr(analysis_result, "strength", None)
    if strength not in VALID_STRENGTHS:
        raise InvalidSignalError(f"invalid signal strength {strength!r}")

    confidence = getattr(analysis_result, "confidence_score", None)
    if (
        isinstance(confidence, bool)
        or not isinstance(confidence, (int, float))
        or not math.isfinite(confidence)
        or not 0.0 <= confidence <= 1.0
    ):
        raise InvalidSignalError(f"missing or invalid confidence_score {confidence!r}")

    components = getattr(analysis_result, "components", None)
    if components is not None and not isinstance(components, dict):
        raise InvalidSignalError("components is not a JSON object")

    explanation = str(getattr(analysis_result, "explanation", "") or "").strip()
    severity = severity_for(signal, strength)

    message = f"{ticker} {trading_date.isoformat()}: {_HEADLINES[signal]}."
    if explanation:
        message = f"{message} {explanation}"

    evidence = {
        "analysis_result_id": getattr(analysis_result, "id", None),
        "signal": signal,
        "strength": strength,
        "confidence_score": float(confidence),
        "trading_date": trading_date.isoformat(),
        "explanation": explanation or None,
        "components": components,
    }

    return AlertDecision(
        ticker=ticker,
        alert_type=signal,
        severity=severity,
        trading_date=trading_date,
        signal_strength=strength,
        confidence_score=float(confidence),
        message=message[:_MESSAGE_MAX],
        evidence=evidence,
    )


def check_freshness(
    signal_date: date,
    reference_session: Optional[date],
    today: date,
) -> Optional[str]:
    """
    Freshness policy (no NSE calendar exists in the project, so none is assumed).

    Returns None if fresh, else a reason code:
      STALE_MARKET  the market's latest observed session is more than
                    MAX_MARKET_DATA_AGE_DAYS before `today` (pipeline stalled).
      STALE_SIGNAL  the signal lags the market's latest observed session by
                    more than MAX_SIGNAL_LAG_DAYS calendar days.

    `reference_session` is the latest trading date the system has actually
    seen (derived from data, like MarketCalculatorService). Limitation: if the
    whole pipeline stopped less than MAX_MARKET_DATA_AGE_DAYS ago, signals are
    still considered fresh.
    """
    if reference_session is None:
        reference_session = signal_date
    if (today - reference_session).days > MAX_MARKET_DATA_AGE_DAYS:
        return STALE_MARKET
    if (reference_session - signal_date).days > MAX_SIGNAL_LAG_DAYS:
        return STALE_SIGNAL
    return None