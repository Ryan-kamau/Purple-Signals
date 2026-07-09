# schemas/macro.py

from pydantic import BaseModel
from datetime import datetime


class MacroDataBase(BaseModel):
    inflation: float
    report_date: datetime
    fuel_price: float
    cbk_rate: float
    usd_kes_rate: float
    euro_kes_rate: float
    pounds_kes_rate: float
    month: str
    year: str
    fuel_trend: float
    inflation_trend: float


class MacroDataCreate(MacroDataBase):
    pass


class MacroDataResponse(MacroDataBase):
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True