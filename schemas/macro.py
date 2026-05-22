# schemas/macro.py

from pydantic import BaseModel
from datetime import datetime


class MacroDataBase(BaseModel):
    inflation: float
    fuel_price: float
    policy_signal: str


class MacroDataCreate(MacroDataBase):
    pass


class MacroDataResponse(MacroDataBase):
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True