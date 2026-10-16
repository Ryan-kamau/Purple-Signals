"""
api/fundamentals.py

FastAPI router — Fundamentals endpoints (CSV ingestion + reads).

Pipeline position:
    HTTP Request  ->  Router  ->  FundamentalsService  ->  fundamentals table

THIN layer only. It:
  - Receives/validates the HTTP request (file upload, query params)
  - Saves the upload to a temp file and always deletes it
  - Calls exactly one FundamentalsService method
  - Returns the service's envelope via a Pydantic response model

It does NOT parse CSVs, calculate ratios, query models, or touch market data.

NOTE: file uploads require the `python-multipart` package.

Mount in main.py (already mounted):
    from api import fundamentals
    app.include_router(fundamentals.router)
"""

import logging
import os
import tempfile
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from sqlalchemy.orm import Session

from database.session import get_db
from schemas.fundamentals import FundamentalsIngestResponse, FundamentalsQueryResponse
from services.fundamentals_service import FundamentalsService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/fundamentals", tags=["Fundamental Analysis"])

MAX_UPLOAD_BYTES: int = 5 * 1024 * 1024  # 5 MB is far beyond any realistic fundamentals CSV


def _get_service(db: Session = Depends(get_db)) -> FundamentalsService:
    """FastAPI dependency — FundamentalsService bound to the request session."""
    return FundamentalsService(db)


@router.post(
    "/ingest",
    response_model=FundamentalsIngestResponse,
    summary="Ingest fundamentals from a CSV upload",
    description=(
        "Upload a CSV with columns: ticker, report_date (YYYY-MM-DD), eps, revenue, "
        "net_profit, shareholders_equity, total_liabilities, dividend_per_share "
        "(optional: pe_ratio, dividend_yield, debt_ratio, net_profit_margin). "
        "New (ticker, report_date) rows are inserted; existing rows only have NULL "
        "columns filled. Invalid rows are skipped and reported per row."
    ),
)
def ingest_fundamentals(
    file: UploadFile = File(..., description="Fundamentals CSV file."),
    service: FundamentalsService = Depends(_get_service),
) -> FundamentalsIngestResponse:
    filename = file.filename or ""
    if not filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only .csv files are accepted.",
        )

    content = file.file.read(MAX_UPLOAD_BYTES + 1)
    if not content:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded file is empty.",
        )
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File exceeds the {MAX_UPLOAD_BYTES // (1024 * 1024)} MB limit.",
        )

    tmp_path: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        logger.info("POST /fundamentals/ingest: file=%s bytes=%d", filename, len(content))
        return service.ingest_csv(tmp_path)  # business failures come back as status="failed"

    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error in POST /fundamentals/ingest")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while ingesting fundamentals.",
        ) from exc
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


@router.get(
    "/latest",
    response_model=FundamentalsQueryResponse,
    summary="Latest fundamentals for a ticker with live P/E",
    description=(
        "Most recent fundamentals row (by report_date) for the ticker, plus "
        "current_market_price and current_pe_ratio recomputed from the latest price."
    ),
)
def get_latest_fundamentals(
    ticker: str = Query(..., min_length=1, description="NSE ticker, e.g. KPLC."),
    service: FundamentalsService = Depends(_get_service),
) -> FundamentalsQueryResponse:
    return service.get_latest(ticker)


@router.get(
    "/history",
    response_model=FundamentalsQueryResponse,
    summary="Paginated fundamentals history",
    description="Newest report first. Omit `ticker` to page across all companies.",
)
def get_fundamentals_history(
    ticker: Optional[str] = Query(default=None, description="Optional NSE ticker filter."),
    limit: int = Query(default=10, ge=1, le=100, description="Records per page (1-100)."),
    offset: int = Query(default=0, ge=0, description="Records to skip."),
    service: FundamentalsService = Depends(_get_service),
) -> FundamentalsQueryResponse:
    return service.get_history(ticker=ticker, limit=limit, offset=offset)