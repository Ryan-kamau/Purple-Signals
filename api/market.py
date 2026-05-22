"""
app/api/routes/market.py

FastAPI router — market data endpoints.

This layer is intentionally thin: validate path/query params, call the
service, and serialise the result.  Zero business logic lives here.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from database.session import get_db
from services.market_service import MarketDataResult, MarketService

router = APIRouter(prefix="/market-data", tags=["Market Data"])


# ---------------------------------------------------------------------------
# Helper — build a consistent JSON envelope from a MarketDataResult
# ---------------------------------------------------------------------------

def _to_response(result: MarketDataResult) -> dict:
    return {
        "status": "ok",
        "source": result.source,
        "summary": {
            "total": result.total,
            "saved": result.saved,
            "skipped": result.skipped,
        },
        "data": [_serialise_record(r) for r in result.records],
    }


def _serialise_record(record) -> dict:
    return {
        "id": record.id,
        "ticker": record.ticker,
        "company": record.company,
        "price": record.price,
        "volume": record.volume,
        "daily_change": record.daily_change,
        "daily_change_pct": record.daily_change_pct,
        "volatility": record.volatility,
        "timestamp": record.timestamp.isoformat() if record.timestamp else None,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/",
    summary="Fetch and persist all NSE market data",
    response_description="Snapshot of all available NSE stocks",
)
def get_all_market_data(db: Session = Depends(get_db)):
    """
    Fetch every available stock from the NSE API, persist each record,
    and return the full snapshot.
    """
    service = MarketService(db)
    result = service.get_all_market_data()
    return _to_response(result)


@router.get(
    "/search",
    summary="Search NSE stocks by ticker or name",
)
def search_market_data(
    q: str = Query(..., min_length=1, description="Ticker or company name fragment"),
    db: Session = Depends(get_db),
):
    """
    Example:  GET /market-data/search?q=KPLC
    """
    service = MarketService(db)
    result = service.search_market_data(query=q)

    if result.saved == 0 and result.total == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No stocks found matching '{q}'.",
        )

    return _to_response(result)


@router.get(
    "/top",
    summary="Retrieve top-ranked NSE stocks",
)
def get_top_market_data(
    limit: int = Query(5, ge=1, le=50, description="Number of results"),
    sort: str = Query("price", description="Field to sort by: price | volume"),
    order: str = Query("desc", pattern="^(asc|desc)$", description="asc or desc"),
    db: Session = Depends(get_db),
):
    """
    Example:  GET /market-data/top?limit=10&sort=volume&order=desc
    """
    service = MarketService(db)
    result = service.get_top_market_data(limit=limit, sort=sort, order=order)
    return _to_response(result)


@router.get(
    "/kplc",
    summary="Fetch and persist the latest KPLC record",
)
def get_kplc(db: Session = Depends(get_db)):
    """
    Convenience endpoint — always returns the single most-recent KPLC snapshot.
    """
    service = MarketService(db)
    record = service.get_kplc()

    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="KPLC data is currently unavailable.",
        )

    return {"status": "ok", "data": _serialise_record(record)}