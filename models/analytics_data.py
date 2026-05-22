# models/analysis.py

from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from database.base import Base


class AnalysisResult(Base):
    __tablename__ = "analysis_results"

    id = Column(Integer, primary_key=True, index=True)

    signal = Column(String(255))

    confidence_score = Column(Float)

    explanation = Column(String(1000))

    timestamp = Column(DateTime(timezone=True), 
                       default=datetime.now(ZoneInfo("Africa/Nairobi")))