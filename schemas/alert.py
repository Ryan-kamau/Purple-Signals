# schemas/alert.py

from pydantic import BaseModel
from datetime import datetime


class AlertBase(BaseModel):
    alert_type: str
    message: str
    severity: str


class AlertCreate(AlertBase):
    pass


class AlertResponse(AlertBase):
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True