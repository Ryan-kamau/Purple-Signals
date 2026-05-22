# schemas/sentiment.py

from pydantic import BaseModel
from datetime import datetime


class SentimentBase(BaseModel):
    source: str
    sentiment_score: float
    sentiment_label: str
    confidence: float


class SentimentCreate(SentimentBase):
    pass


class SentimentResponse(SentimentBase):
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True