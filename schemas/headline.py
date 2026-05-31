# schemas/headline.py

from configparser import NoOptionError

from pydantic import BaseModel
from datetime import datetime


class HeadlineBase(BaseModel):
    source: str
    headline: str
    description: str | None = None
    content: str | None = None
    published_at: datetime | None = None
    sentiment_score: float | None = None
    keyword_detected: str | None = None
    url: str


class HeadlineCreate(HeadlineBase):
    pass


class HeadlineResponse(HeadlineBase):
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True