"""Shared MacroData persistence. Flushes, NEVER commits: the calling service
owns the single transaction (project convention)."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Optional

from sqlalchemy.orm import Session

from core.periods import previous_month_key
from core.timeutils import to_nairobi_date
from models.macro_data import MacroData

logger = logging.getLogger(__name__)
_KEY_FIELDS = ("month", "year", "report_date")


@dataclass
class PersistStats:
    inserted: list[MacroData] = field(default_factory=list)
    updated: list[MacroData] = field(default_factory=list)
    unchanged: list[MacroData] = field(default_factory=list)

    @property
    def records(self) -> list[MacroData]:
        return self.inserted + self.updated + self.unchanged

    @property
    def changed_records(self) -> list[MacroData]:
        return self.inserted + self.updated


class MacroRepository:
    def __init__(self, db: Session) -> None:
        self._db = db

    def fetch_existing(self, keys) -> dict[tuple[str, str], MacroData]:
        keys = set(keys)
        if not keys:
            return {}
        rows = (
            self._db.query(MacroData)
            .filter(
                MacroData.month.in_({m for m, _ in keys}),
                MacroData.year.in_({y for _, y in keys}),
            )
            .all()
        )
        return {(r.month, r.year): r for r in rows if (r.month, r.year) in keys}

    def upsert(self, records: list[dict[str, Any]], report_date: Optional[date] = None) -> PersistStats:
        """Insert new (month, year); for existing rows fill ONLY NULL columns.
        Indicator columns = every record key except month/year/report_date.
        A per-record 'report_date' wins over the run-level default."""
        stats = PersistStats()
        if not records:
            return stats

        existing = self.fetch_existing((r["month"], r["year"]) for r in records)

        for record in records:
            key = (record["month"], record["year"])
            fields = {k: v for k, v in record.items() if k not in _KEY_FIELDS}
            rdate = to_nairobi_date(record.get("report_date") or report_date)
            row = existing.get(key)

            if row is None:
                row = MacroData(month=key[0], year=key[1], report_date=rdate, **fields)
                self._db.add(row)
                stats.inserted.append(row)
                continue

            changed = False
            for column, value in fields.items():
                if value is not None and getattr(row, column) is None:
                    setattr(row, column, value)
                    changed = True
            if row.report_date is None and rdate is not None:
                row.report_date = rdate
                changed = True
            (stats.updated if changed else stats.unchanged).append(row)

        self._db.flush()  # makes new rows visible to the trend lookup (autoflush=False)
        return stats

    def apply_trends(self, records: list[MacroData]) -> None:
        """trend = current - previous month (4 dp); None if either side missing."""
        if not records:
            return
        prev_keys = {previous_month_key(r.month, r.year) for r in records} - {None}
        lookup = self.fetch_existing(prev_keys)

        for r in records:
            key = previous_month_key(r.month, r.year)
            prev = lookup.get(key) if key else None
            r.inflation_trend = (
                round(r.inflation - prev.inflation, 4)
                if prev and r.inflation is not None and prev.inflation is not None else None
            )
            r.fuel_trend = (
                round(r.fuel_price - prev.fuel_price, 4)
                if prev and r.fuel_price is not None and prev.fuel_price is not None else None
            )


def serialize_macro(record: MacroData) -> dict[str, Any]:
    return {
        "id": record.id, "month": record.month, "year": record.year,
        "report_date": record.report_date.isoformat() if record.report_date else None,
        "inflation": record.inflation, "inflation_trend": record.inflation_trend,
        "fuel_price": record.fuel_price, "fuel_trend": record.fuel_trend,
        "cbk_rate": record.cbk_rate, "usd_kes_rate": record.usd_kes_rate,
        "euro_kes_rate": record.euro_kes_rate, "pounds_kes_rate": record.pounds_kes_rate,
        "timestamp": record.timestamp.isoformat() if record.timestamp else None,
    }