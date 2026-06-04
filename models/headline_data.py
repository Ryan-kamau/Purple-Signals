# models/headline.py

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Text,
)
from datetime import datetime
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

    published_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(
            ZoneInfo("Africa/Nairobi")
        )
    )


    # New intelligence fields
    keywords_detected = Column(Text, nullable=True)

    categories = Column(Text, nullable=True)

    matched_keywords_count = Column(
        Integer,
        default=0,
        nullable=False
    )

    impact_score = Column(
        Integer,
        default=0,
        nullable=False
    )

    url = Column(String(1000), nullable=False, index=True)

    timestamp = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(
            ZoneInfo("Africa/Nairobi")
        )
    )