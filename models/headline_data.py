# models/headline.py

from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from database.base import Base


class Headline(Base):
    __tablename__ = "headlines"

    id = Column(Integer, primary_key=True, index=True)

    source = Column(String(255), nullable=False)

    headline = Column(String(1000), nullable=False)

    sentiment_score = Column(Float)

    keyword_detected = Column(String(255))

    timestamp = Column(DateTime(timezone=True), 
                       default=datetime.now(ZoneInfo("Africa/Nairobi")))