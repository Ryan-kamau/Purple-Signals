"""
api/macro_features.py

FastAPI router — feature-engineering endpoints (WRITE side).

Thin layer: inject a session, call the service, return its result.
No business logic lives here.

Mount in main.py:
    from api import macro_features
    app.include_router(macro_features.router)
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from database.session import get_db
from services.feature_service.MacroFeatureService import MacroFeaturesService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/features", tags=["Macro Feature Engineering"])


@router.post(
    "/macro-features",
    summary="Fill macro columns in daily_features from macro_data",
    description=(
        "Fills NULL inflation / interest_rate / usd_kes / oil_price columns "
        "on daily_features from the latest macro_data values. Never "
        "overwrites existing values and never creates rows. Default: only "
        "each ticker's latest trading date. backfill=true fills every row "
        "with a NULL macro column (note: applies current macro values to "
        "historical rows)."
    ),
)
def fill_macro_features(
    backfill: bool = Query(
        default=False,
        description="Fill all historical rows with NULL macro columns.",
    ),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    result = MacroFeaturesService(db).fill(backfill=backfill)

    if result["status"] == "failed":
        logger.error("POST /features/macro failed: %s", result["message"])
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=result["message"],
        )

    return result