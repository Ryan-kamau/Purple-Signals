from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict


class FundamentalsResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    ticker: str
    company: str
    report_date: datetime

    eps: float | None = None
    pe_ratio: float | None = None
    dividend_yield: float | None = None
    dividend_per_share: float | None = None
    revenue: float | None = None
    net_profit: float | None = None
    debt_ratio: float | None = None
    net_profit_margin: float | None = None
    timestamp: datetime | None = None

    # Present on latest-fundamentals responses
    current_market_price: float | None = None
    current_pe_ratio: float | None = None


class FundamentalsIssue(BaseModel):
    row: int | None = None
    ticker: str | None = None
    report_date: str | None = None
    error: str | None = None
    warning: str | None = None


class FundamentalsEnvelope(BaseModel):
    status: Literal["success", "failed"]
    message: str

    count: int
    total: int
    limit: int
    offset: int

    data: list[FundamentalsResponse]


class FundamentalsIngestEnvelope(BaseModel):
    status: Literal["success", "partial_success", "failed"]
    message: str

    rows_saved: int
    rows_inserted: int
    rows_updated: int
    rows_unchanged: int
    rows_skipped: int

    errors: list[FundamentalsIssue]
    warnings: list[FundamentalsIssue]

    data: list[FundamentalsResponse]
