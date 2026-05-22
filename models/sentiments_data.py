# models/sentiment.py

from sqlalchemy import Column, Integer, Float, String, DateTime
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from database.base import Base


class SentimentScore(Base):
    __tablename__ = "sentiment_scores"

    id = Column(Integer, primary_key=True, index=True)

    source = Column(String(255))

    sentiment_score = Column(Float)

    sentiment_label = Column(String(100))

    confidence = Column(Float)

    timestamp = Column(DateTime(timezone=True), 
                       default=datetime.now(ZoneInfo("Africa/Nairobi")))