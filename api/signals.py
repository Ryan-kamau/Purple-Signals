"""
api/signals.py

FastAPI router — read-only combined-signal endpoints.

Thin layer: validate query params, call SignalService.get_signals(), return
AnalysisResultResponse rows. No rules, no SQL here.

Mount in main.py:
    from api import signals
    app.include_router(signals.router)
"""

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from database.session import get_db
from schemas.analytics import AnalysisResultResponse
from services.signal_service.SignalService import SignalService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/signals", tags=["Signals"])


@router.get(
    "/",
    response_model=list[AnalysisResultResponse],
    summary="Query combined signals",
    description=(
        "Newest first. Filters are combined with AND. signal is one of: "
        "bullish, bearish, divergence, unusual_activity, neutral. Signals "
        "describe what is happening; they are not buy/sell advice."
    ),
)
def get_signals(
    ticker: Optional[str] = Query(default=None, description="NSE ticker, e.g. KCB."),
    signal: Optional[str] = Query(default=None, description="Signal label filter."),
    trading_date: Optional[date] = Query(default=None, description="YYYY-MM-DD."),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> list[AnalysisResultResponse]:
    try:
        return SignalService(db).get_signals(
            ticker=ticker, signal=signal, trading_date=trading_date, limit=limit, offset=offset
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error in GET /signals")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred in GET /signals.",
        ) from exc