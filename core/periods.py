"""Single home for month/period logic. Pure functions, no project imports."""
from __future__ import annotations

from typing import Any, Optional

MONTH_NAMES: tuple[str, ...] = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)
_MONTH_LOOKUP = {name.lower(): i + 1 for i, name in enumerate(MONTH_NAMES)}


def normalize_month(value: Any) -> Optional[str]:
    """'MARCH ' -> 'March'. Does not validate; check against MONTH_NAMES."""
    if not value:
        return None
    return str(value).strip().capitalize()


def month_number(name: Any) -> Optional[int]:
    """Month name (any case) -> 1-12, else None."""
    return _MONTH_LOOKUP.get(str(name or "").strip().lower())


def period_key(month: Any, year: Any) -> Optional[tuple[int, int]]:
    """(year, month_index 0-based) sort key, or None if unusable.
    0-based on purpose: matches the existing MacroFeatureService /
    MasterIntelligenceService contract, so no caller changes meaning."""
    name = normalize_month(month)
    if name not in MONTH_NAMES or not year:
        return None
    try:
        return int(str(year).strip()), MONTH_NAMES.index(name)
    except ValueError:
        return None


def previous_period(year: int, month_index: int) -> tuple[int, int]:
    """Prior calendar month; January wraps to December of the prior year."""
    return (year - 1, 11) if month_index == 0 else (year, month_index - 1)


def previous_month_key(month: Any, year: Any) -> Optional[tuple[str, str]]:
    """('March','2026') -> ('February','2026'); string form used by MacroData rows."""
    key = period_key(month, year)
    if key is None:
        return None
    prev_year, prev_index = previous_period(*key)
    return MONTH_NAMES[prev_index], str(prev_year)