"""
config/alert_config.py

Thresholds for alert freshness. Nothing else should hardcode these.

PROVISIONAL: the project has no NSE trading calendar, so freshness is measured
against the market's own latest observed session (see
intelligence.alert_rules.check_freshness), never against "today" alone.
Tune after observing real scheduler behaviour.
"""

# A ticker's latest signal is stale if it lags the market's latest observed
# session by more than this many CALENDAR days. 5 covers a weekend plus a
# midweek holiday, or the Easter Friday-Monday gap, without flagging a
# normal ticker. Illiquid names that skip sessions simply keep their alert.
MAX_SIGNAL_LAG_DAYS: int = 5

# If the market's latest observed session is older than this relative to the
# real current date, the upstream pipeline has stalled: nothing is treated as
# fresh, and no alert is created, updated or resolved.
MAX_MARKET_DATA_AGE_DAYS: int = 7