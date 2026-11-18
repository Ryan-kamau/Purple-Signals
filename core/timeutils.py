"""Nairobi-time helpers. Convention: naive datetimes are already Nairobi-local."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Optional, Union
from zoneinfo import ZoneInfo

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")


def now_nairobi() -> datetime:
    return datetime.now(NAIROBI_TZ)


def as_nairobi(ts: datetime) -> datetime:
    return ts.replace(tzinfo=NAIROBI_TZ) if ts.tzinfo is None else ts.astimezone(NAIROBI_TZ)


def to_nairobi_date(value: Union[date, datetime, None]) -> Optional[date]:
    """datetime -> Nairobi calendar date; date passes through; None -> None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return as_nairobi(value).date()
    return value


def nairobi_day_bounds(day: date) -> tuple[datetime, datetime]:
    """True half-open [start, next-midnight) bounds for one Nairobi day."""
    start = datetime.combine(day, time.min, tzinfo=NAIROBI_TZ)
    return start, start + timedelta(days=1)