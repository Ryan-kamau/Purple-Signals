# models/headline.py

from sqlalchemy import Column, Integer, String, Float, DateTime, Text
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from database.base import Base


class Headline(Base):
    __tablename__ = "headlines"

    id = Column(Integer, primary_key=True, index=True)

    source = Column(String(255), nullable=False)

    headline = Column(String(1000), nullable=False)

    description = Column(Text, nullable=True)

    content = Column(Text, nullable=True)

    sentiment_score = Column(Float)

    published_at = Column(DateTime(timezone=True), default=
                          lambda: datetime.now(ZoneInfo("Africa/Nairobi")))

    keyword_detected = Column(String(255))

    url = Column(String(1000), nullable=False)

    timestamp = Column(DateTime(timezone=True), 
                       default=lambda: datetime.now(ZoneInfo("Africa/Nairobi")))