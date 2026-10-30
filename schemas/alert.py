# schemas/alert.py

from datetime import date, datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict


class AlertBase(BaseModel):
    alert_type: str
    message: str
    severity: str


class AlertCreate(AlertBase):
    pass


class AlertResponse(AlertBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    ticker: str
    trading_date: date
    signal_strength: Optional[str] = None
    confidence_score: float
    status: str
    first_detected_at: datetime
    last_detected_at: datetime
    resolved_at: Optional[datetime] = None
    evidence: Optional[dict[str, Any]] = None
    timestamp: datetime


class AlertEvaluationIssue(BaseModel):
    ticker: Optional[str] = None
    trading_date: Optional[str] = None
    reason: str
    detail: str


class AlertEvaluationResponse(BaseModel):
    """
    Per-ticker outcomes sum to `evaluated`:
    created + updated + transitioned + unchanged + skipped + failed.
    `resolved` counts resolved alerts (== transitioned).
    """

    status: str
    evaluated: int
    created: int
    updated: int
    transitioned: int
    resolved: int
    unchanged: int
    skipped: int
    failed: int
    errors: list[AlertEvaluationIssue]