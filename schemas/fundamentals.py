from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class FundamentalsBase(BaseModel):
    ticker: str
    company: str
    report_date: datetime

    eps: Decimal | None = None
    pe_ratio: Decimal | None = None
    dividend_yield: Decimal | None = None
    dividend_per_share: float | None = None

    revenue: float | None = None
    net_profit: float | None = None

    debt_ratio: Decimal | None = None
    net_profit_margin: Decimal | None = None


class FundamentalsCreate(FundamentalsBase):
    pass


class FundamentalsResponse(FundamentalsBase):
    id: int
    timestamp: datetime | None = None

    model_config = ConfigDict(from_attributes=True)
