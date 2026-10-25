"""
intelligence/signal_engine.py

Pure, stateless combined-signal logic. No DB, no HTTP, no FastAPI.

Pipeline position:
    daily_features  ->  SignalService  ->  signal_engine.evaluate()  ->  analysis_results

Three components are classified independently, then combined:

    price      +1 / -1 / 0 / None   daily_return vs +/-PRICE_MOVE_PCT
    spike      True / False / None  volume ratio AND absolute share floor
    sentiment  +1 / -1 / 0 / None   weighted sentiment, needs >= 1 scored headline

`None` always means "not available", never "false". It is what lets a ticker
with no news still produce a signal.

Combination table (first match wins):

    spike + price up   + sentiment positive          -> bullish    strong
    spike + price up   + sentiment neutral/None      -> bullish    moderate
    spike + price down + sentiment negative          -> bearish    strong
    spike + price down + sentiment neutral/None      -> bearish    moderate
    price and sentiment point opposite ways          -> divergence
    spike only (price flat / unknown)                -> unusual_activity
    anything else                                    -> neutral

confidence_score = (components supporting the signal) / 3
    strong = 1.0, moderate = 0.6667, unusual_activity = 0.3333,
    divergence / neutral = 0.0

The labels are DESCRIPTIONS of what is happening, not buy/sell advice (PRD 5.9).

No lookahead: every input is today's row or earlier. `prior_volumes` must hold
sessions strictly BEFORE the one being evaluated.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import Any, Optional, Sequence

from config.signal_config import (
    LIQUIDITY_TIER_CUTOFFS,
    MIN_SENTIMENT_HEADLINES,
    PRICE_MOVE_PCT,
    SENTIMENT_NEGATIVE,
    SENTIMENT_POSITIVE,
    VOLUME_FLOORS,
    VOLUME_LOOKBACK,
    VOLUME_RATIO_MIN,
)

SIGNAL_BULLISH = "bullish"
SIGNAL_BEARISH = "bearish"
SIGNAL_DIVERGENCE = "divergence"
SIGNAL_UNUSUAL = "unusual_activity"
SIGNAL_NEUTRAL = "neutral"

STRENGTH_STRONG = "strong"
STRENGTH_MODERATE = "moderate"

COMPONENT_COUNT = 3


@dataclass(frozen=True)
class VolumeReading:
    ratio: float      # today's shares / mean of previous sessions
    average: float    # mean shares over the previous VOLUME_LOOKBACK sessions
    median: float     # median shares over the same window (drives the tier)
    tier: str         # "low" | "mid" | "high"
    floor: int        # absolute share floor applied
    spike: bool       # ratio >= VOLUME_RATIO_MIN and volume >= floor


@dataclass(frozen=True)
class SignalResult:
    signal: str
    strength: Optional[str]
    confidence_score: float
    explanation: str
    components: dict[str, Any]


# ---------------------------------------------------------------------------
# Component classifiers
# ---------------------------------------------------------------------------

def liquidity_tier(median_volume: float) -> str:
    low_cut, mid_cut = LIQUIDITY_TIER_CUTOFFS
    if median_volume < low_cut:
        return "low"
    if median_volume < mid_cut:
        return "mid"
    return "high"


def read_volume(
    volume: Optional[float],
    prior_volumes: Sequence[Optional[float]],
    floor: Optional[int] = None,
) -> Optional[VolumeReading]:
    """
    Relative-volume reading for one session.

    Args:
        volume:        Shares traded today.
        prior_volumes: Shares traded in the sessions BEFORE today, oldest
                       first. Only the last VOLUME_LOOKBACK are used.
        floor:         Override the tier floor (replay harness). None -> the
                       tier's floor from VOLUME_FLOORS; 0 -> no floor.

    Returns:
        VolumeReading, or None when it cannot be assessed: today's volume
        missing, fewer than VOLUME_LOOKBACK prior sessions, a gap in them, or
        a zero average (ratio undefined).
    """
    if volume is None:
        return None

    window = list(prior_volumes)[-VOLUME_LOOKBACK:]
    if len(window) < VOLUME_LOOKBACK or any(v is None for v in window):
        return None

    average = statistics.fmean(window)
    if average <= 0:
        return None

    median = statistics.median(window)
    tier = liquidity_tier(median)
    resolved_floor = VOLUME_FLOORS[tier] if floor is None else floor
    ratio = volume / average
    spike = ratio >= VOLUME_RATIO_MIN and volume >= resolved_floor

    return VolumeReading(ratio, average, median, tier, resolved_floor, spike)


def classify_price(daily_return: Optional[float]) -> Optional[int]:
    if daily_return is None:
        return None
    if daily_return >= PRICE_MOVE_PCT:
        return 1
    if daily_return <= -PRICE_MOVE_PCT:
        return -1
    return 0


def classify_sentiment(
    weighted: Optional[float],
    average: Optional[float],
    headline_count: Optional[int],
) -> tuple[Optional[int], Optional[float]]:
    """Returns (component, score). Prefers impact-weighted over plain average."""
    if not headline_count or headline_count < MIN_SENTIMENT_HEADLINES:
        return None, None

    score = weighted if weighted is not None else average
    if score is None:
        return None, None

    if score >= SENTIMENT_POSITIVE:
        return 1, score
    if score <= SENTIMENT_NEGATIVE:
        return -1, score
    return 0, score


# ---------------------------------------------------------------------------
# Combiner
# ---------------------------------------------------------------------------

def combine(
    price: Optional[int],
    spike: Optional[bool],
    sentiment: Optional[int],
) -> tuple[str, Optional[str], int]:
    """Returns (signal, strength, supporting_component_count)."""
    if spike and price in (1, -1):
        name = SIGNAL_BULLISH if price == 1 else SIGNAL_BEARISH
        if sentiment == price:
            return name, STRENGTH_STRONG, 3
        if sentiment in (0, None):
            return name, STRENGTH_MODERATE, 2
        return SIGNAL_DIVERGENCE, None, 0  # sentiment points the other way

    if price in (1, -1) and sentiment == -price:
        return SIGNAL_DIVERGENCE, None, 0

    if spike:
        return SIGNAL_UNUSUAL, None, 1

    return SIGNAL_NEUTRAL, None, 0


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def evaluate(
    *,
    daily_return: Optional[float],
    volume: Optional[float],
    prior_volumes: Sequence[Optional[float]],
    weighted_sentiment: Optional[float] = None,
    average_sentiment: Optional[float] = None,
    headline_count: Optional[int] = 0,
) -> Optional[SignalResult]:
    """
    Evaluate one (ticker, trading_date).

    Returns None when volume cannot be assessed (cold start: fewer than
    VOLUME_LOOKBACK prior sessions). The caller counts that as skipped rather
    than storing a misleading "neutral".
    """
    reading = read_volume(volume, prior_volumes)
    if reading is None:
        return None

    price = classify_price(daily_return)
    sentiment, sentiment_score = classify_sentiment(
        weighted_sentiment, average_sentiment, headline_count
    )

    signal, strength, supporting = combine(price, reading.spike, sentiment)

    components: dict[str, Any] = {
        "price": price,
        "volume_spike": reading.spike,
        "sentiment": sentiment,
        "daily_return": None if daily_return is None else round(daily_return, 6),
        "volume": volume,
        "volume_avg_20": round(reading.average, 2),
        "volume_ratio": round(reading.ratio, 4),
        "volume_floor": reading.floor,
        "liquidity_tier": reading.tier,
        "sentiment_score": None if sentiment_score is None else round(sentiment_score, 4),
        "headline_count": int(headline_count or 0),
    }

    return SignalResult(
        signal=signal,
        strength=strength,
        confidence_score=round(supporting / COMPONENT_COUNT, 4),
        explanation=_explain(daily_return, volume, reading, sentiment_score, headline_count),
        components=components,
    )


def _explain(
    daily_return: Optional[float],
    volume: Optional[float],
    reading: VolumeReading,
    sentiment_score: Optional[float],
    headline_count: Optional[int],
) -> str:
    price_part = "price n/a" if daily_return is None else f"price {daily_return * 100:+.2f}%"

    if reading.spike:
        verdict = "spike"
    elif reading.ratio >= VOLUME_RATIO_MIN:
        verdict = f"below {reading.floor:,}-share floor"
    else:
        verdict = "normal"
    volume_part = (
        f"volume {volume:,.0f} shares = {reading.ratio:.1f}x "
        f"{VOLUME_LOOKBACK}-session avg ({reading.average:,.0f}), "
        f"{reading.tier} liquidity: {verdict}"
    )

    if sentiment_score is None:
        sentiment_part = "no scored headlines"
    else:
        sentiment_part = f"sentiment {sentiment_score:+.2f} ({int(headline_count or 0)} headline(s))"

    return "; ".join((price_part, volume_part, sentiment_part))[:1000]