"""
api/alerts.py

FastAPI router — alert evaluation (POST) and retrieval (GET).

Thin layer: validate params, call AlertEvaluationService, return schemas.
Manual evaluation here and scripts/alert_scheduler.py call the SAME
AlertEvaluationService.evaluate(). GET endpoints are read-only.

Mount in main.py:
    from api import alerts
    app.include_router(alerts.router)
"""

import logging
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from database.session import get_db
from schemas.alert import AlertEvaluationResponse, AlertResponse
from services.alert_service.AlertEvaluationService import REASON_NO_SIGNAL, AlertEvaluationService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/alerts", tags=["Alerts"])

_TICKER_PATTERN = r"^[A-Za-z0-9]{1,20}$"

AlertTypeParam = Literal["bullish", "bearish", "divergence", "unusual_activity", "neutral"]
StatusParam = Literal["active", "resolved"]


@router.post(
    "/evaluate",
    response_model=AlertEvaluationResponse,
    summary="Evaluate combined signals and update alerts",
    description=(
        "Evaluates the latest stored combined signal per ticker (or one ticker) "
        "and creates/updates/resolves alerts. Idempotent. Never recalculates signals."
    ),
)
def evaluate_alerts(
    ticker: Optional[str] = Query(default=None, pattern=_TICKER_PATTERN, description="NSE ticker, e.g. KPLC."),
    db: Session = Depends(get_db),
) -> AlertEvaluationResponse:
    try:
        result = AlertEvaluationService(db).evaluate(ticker=ticker)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error in POST /alerts/evaluate")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred in POST /alerts/evaluate.",
        ) from exc

    if ticker and result["errors"] and result["errors"][0]["reason"] == REASON_NO_SIGNAL:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No combined signal found for ticker '{ticker.upper()}'.",
        )

    if result["status"] == "failed":
        logger.error("POST /alerts/evaluate failed: %s", result["errors"][:3])
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Alert evaluation failed. See server logs.",
        )

    return result


@router.get(
    "",
    response_model=list[AlertResponse],
    summary="Query alerts",
    description="Newest first. Filters combine with AND. Read-only; never triggers evaluation.",
)
def get_alerts(
    ticker: Optional[str] = Query(default=None, pattern=_TICKER_PATTERN),
    status_filter: Optional[StatusParam] = Query(default=None, alias="status"),
    alert_type: Optional[AlertTypeParam] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> list[AlertResponse]:
    try:
        return AlertEvaluationService(db).get_alerts(
            ticker=ticker, status=status_filter, alert_type=alert_type, limit=limit, offset=offset
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error in GET /alerts")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred in GET /alerts.",
        ) from exc