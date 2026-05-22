# schemas/analysis.py

from pydantic import BaseModel
from datetime import datetime


class AnalysisResultBase(BaseModel):
    signal: str
    confidence_score: float
    explanation: str


class AnalysisResultCreate(AnalysisResultBase):
    pass


class AnalysisResultResponse(AnalysisResultBase):
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True