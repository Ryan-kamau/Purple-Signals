"""
api/analysis.py

FastAPI router - Master Intelligence endpoint (read-only).

Thin layer: validate params, call MasterIntelligenceService, map errors.
No SQL, no calculations, no freshness logic here.

Mount in main.py:
    from api import analysis
    app.include_router(analysis.router)
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from config.analysis_config import DEFAULT_LIMIT, MAX_LIMIT
from database.session import get_db
from schemas.analysis import AnalysisResponse
from services.analysis_service.MasterIntelligenceService import (
    MasterIntelligenceService,
    TickerNotFoundError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analysis", tags=["Analysis"])


@router.get(
    "",
    response_model=AnalysisResponse,
    summary="NSE-wide market intelligence, or one security's full intelligence",
    description=(
        "Read-only aggregation of stored signals, daily features, sentiment aggregates, "
        "active alerts, fundamentals and macro data. Nothing is recalculated.\n\n"
        "Without `ticker`: NSE-wide summary + a paginated list of securities "
        "(`limit`/`offset`).\n\n"
        "With `ticker` (any NSE ticker, case-insensitive): the same NSE-wide summary + "
        "that ticker's summary and detail. `limit`/`offset` are ignored. Unknown "
        "ticker -> 404; known ticker with missing data -> 200 with null sections."
    ),
    responses={
        404: {"description": "Ticker is not in the NSE security universe."},
        500: {"description": "Unexpected server error."},
    },
)
def get_analysis(
    ticker: Optional[str] = Query(
        default=None, min_length=1, max_length=20, description="NSE ticker, e.g. KPLC, SCOM, EQTY."
    ),
    limit: int = Query(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT, description="Page size (1-200)."),
    offset: int = Query(default=0, ge=0, description="Securities to skip."),
    db: Session = Depends(get_db),
) -> AnalysisResponse:
    try:
        return MasterIntelligenceService(db).get_analysis(
            ticker=ticker, limit=limit, offset=offset
        )
    except TickerNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        # Full detail goes to the log only; clients get a generic message.
        logger.exception("Unexpected error in GET /analysis")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred in GET /analysis.",
        ) from exc