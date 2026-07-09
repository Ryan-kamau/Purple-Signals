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

    euro_kes_rate = Column(Float)

    pounds_kes_rate = Column(Float)
     
    month = Column(String, index=True)

    year = Column(String, index=True)

    fuel_trend = Column(Float)

    inflation_trend = Column(Float)

    timestamp = Column(DateTime(timezone=True), 
                       default=datetime.now(ZoneInfo("Africa/Nairobi")))