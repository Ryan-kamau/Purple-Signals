# models/macro.py

from sqlalchemy import Column, Integer, Float, String, DateTime
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from database.base import Base


class MacroData(Base):
    __tablename__ = "macro_data"

    id = Column(Integer, primary_key=True, index=True)

    report_date = Column(DateTime(timezone=True), default=
                         datetime.now(ZoneInfo("Africa/Nairobi")))

    inflation = Column(Float)

    fuel_price = Column(Float)

    cbk_rate = Column(Float)

    usd_kes_rate = Column(Float)

    policy_signal = Column(String(255))

    timestamp = Column(DateTime(timezone=True), 
                       default=datetime.now(ZoneInfo("Africa/Nairobi")))