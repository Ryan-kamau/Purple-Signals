# models/alert.py

from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from database.base import Base


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, index=True)

    alert_type = Column(String(255), nullable=False)

    message = Column(String(1000), nullable=False)

    severity = Column(String(100))

    timestamp = Column(DateTime(timezone=True), 
                       default=datetime.now(ZoneInfo("Africa/Nairobi")))