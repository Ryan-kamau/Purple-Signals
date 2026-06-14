# schemas/fundamentals.py

from pydantic import BaseModel
from datetime import datetime


class FundamentalsBase(BaseModel):
    ticker: str
    company: str
    report_date: datetime
    eps: float
    pe_ratio: float
    dividend_yield: float
    revenue: float
    debt_ratio: float
    net_profit_margin: float


class FundamentalsCreate(FundamentalsBase):
    pass


class FundamentalsResponse(FundamentalsBase):
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True