# schemas/headline.py

from pydantic import BaseModel
from datetime import datetime


class HeadlineBase(BaseModel):
    source: str
    headline: str
    sentiment_score: float | None = None
    keyword_detected: str | None = None


class HeadlineCreate(HeadlineBase):
    pass


class HeadlineResponse(HeadlineBase):
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True