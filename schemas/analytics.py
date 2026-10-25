# schemas/analytics.py

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel


class AnalysisResultBase(BaseModel):
    ticker: str
    trading_date: date
    signal: str
    strength: str | None = None
    confidence_score: float
    explanation: str
    components: dict[str, Any] | None = None


class AnalysisResultCreate(AnalysisResultBase):
    pass


class AnalysisResultResponse(AnalysisResultBase):
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True