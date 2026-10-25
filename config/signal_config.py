
"""
config/signal_config.py

Single place for every threshold used by the combined signal logic
(intelligence/signal_engine.py) and by the replay harness
(scripts/replay_volume_floor.py). Nothing else should hardcode these.

PROVISIONAL VALUES
    LIQUIDITY_TIER_CUTOFFS and VOLUME_FLOORS are provisional until
    `python -m scripts.replay_volume_floor` has >= 60 sessions of history to
    run on. Edit them here after reviewing the replay output; no other file
    needs to change.

All volumes are SHARES TRADED (never traded value).
"""

# --- Price component -------------------------------------------------------
# daily_return is a fraction (0.02 == +2%), as written by MarketCalculatorService.
PRICE_MOVE_PCT: float = 0.02

# --- Volume component ------------------------------------------------------
# spike = today's shares / mean shares of the PREVIOUS VOLUME_LOOKBACK sessions
#         (today excluded) >= VOLUME_RATIO_MIN, AND today's shares >= tier floor.
VOLUME_LOOKBACK: int = 20
VOLUME_RATIO_MIN: float = 3.0

# Liquidity tier = median shares/day over the same previous-20-session window.
#   median <  cutoffs[0]  -> "low"
#   median <  cutoffs[1]  -> "mid"
#   otherwise             -> "high"
LIQUIDITY_TIER_CUTOFFS: tuple[int, int] = (10_000, 100_000)

# Minimum absolute shares traded for a spike to count, per tier.
# Note the floor only binds when 3x the stock's own average is below it, so it
# mainly matters for thinly traded names.
VOLUME_FLOORS: dict[str, int] = {"low": 1_000, "mid": 5_000, "high": 10_000}

# Candidate floors evaluated by the replay harness.
VOLUME_FLOOR_CANDIDATES: tuple[int, ...] = (500, 1_000, 5_000, 10_000)

# --- Sentiment component ---------------------------------------------------
# Same bands as SentimentAnalyzer._classify_score (moderate positive/negative).
SENTIMENT_POSITIVE: float = 0.20
SENTIMENT_NEGATIVE: float = -0.20
MIN_SENTIMENT_HEADLINES: int = 1
