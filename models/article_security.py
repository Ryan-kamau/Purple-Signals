"""Persisted, explicit article-to-NSE-ticker associations."""
from sqlalchemy import Column, ForeignKey, Integer, String, Index
from database.base import Base


class ArticleSecurity(Base):
    __tablename__ = "article_securities"
    article_id = Column(Integer, ForeignKey("headlines.id", ondelete="CASCADE"), primary_key=True)
    ticker = Column(String(20), primary_key=True)
    __table_args__ = (Index("ix_article_securities_ticker", "ticker"),)
