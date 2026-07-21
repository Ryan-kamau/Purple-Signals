"""
api/macroeconomics.py

FastAPI router — read-only Macroeconomic Intelligence endpoints.

Pipeline position:

    HTTP Request  ->  Router  ->  MacroQueryService  ->  SQLAlchemy Session  ->  MySQL

This router is intentionally a thin orchestration layer. It:
  - Validates path/query parameters
  - Injects a SQLAlchemy session
  - Instantiates MacroQueryService
  - Calls exactly one service method
  - Returns the service's response dict unmodified

It does NOT:
  - Contain business logic, filtering, or comparison logic
  - Run SQLAlchemy queries directly
  - Perform serialization/aggregation
  - Wrap, reshape, or post-process service responses
  - Expose any write operations (POST/PUT/PATCH/DELETE)

Mount in main.py:
    from api import macroeconomics
    app.include_router(macroeconomics.router)
"""

import logging
from datetime import date
from enum import Enum
from typing import Optional

from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy.orm import Session

from database.session import get_db
from schemas.macro import MacroQueryResponse
from services.macro_query_service import MacroQueryService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/macroeconomics", tags=["Macro_economics"])


# ---------------------------------------------------------------------------
# Indicator enum — used to validate {indicator} path segments
# ---------------------------------------------------------------------------

class MacroIndicator(str, Enum):
    """Supported macro indicators for per-indicator history/statistics."""

    inflation = "inflation"
    fuel = "fuel"
    exchange = "exchange"
    cbr = "cbr"


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------

@router.get(
    "/latest",
    response_model=MacroQueryResponse,
    summary="Return the latest macroeconomic snapshot",
    description="Returns the most recently recorded macro data row.",
)
def get_latest(db: Session = Depends(get_db)) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/latest received")
    service = MacroQueryService(db)
    result = service.get_latest()
    logger.info("GET /macroeconomics/latest completed")
    return result


# ---------------------------------------------------------------------------
# Month lookup
# ---------------------------------------------------------------------------

@router.get(
    "/month/{year}/{month}",
    response_model=MacroQueryResponse,
    summary="Return macro data for a specific month",
    description="Looks up a single macro data record by year and month name.",
)
def get_by_month(
    year: int = Path(..., ge=1900, le=2100, description="Four-digit year, e.g. 2026."),
    month: str = Path(..., description="Month name, e.g. 'March'."),
    db: Session = Depends(get_db),
) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/month/%s/%s received", year, month)
    service = MacroQueryService(db)
    result = service.get_by_month(year=year, month=month)
    logger.info("GET /macroeconomics/month/%s/%s completed", year, month)
    return result


# ---------------------------------------------------------------------------
# Report date lookup
# ---------------------------------------------------------------------------

@router.get(
    "/report-date",
    response_model=MacroQueryResponse,
    summary="Return macro data for a specific report date",
    description="Looks up a macro data record by its KNBS report date.",
)
def get_by_report_date(
    date: date = Query(..., description="Report date in YYYY-MM-DD format, e.g. 2026-03-01."),
    db: Session = Depends(get_db),
) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/report-date received")
    service = MacroQueryService(db)
    result = service.get_by_report_date(report_date=date)
    logger.info("GET /macroeconomics/report-date completed")
    return result


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------

@router.get(
    "/history",
    response_model=MacroQueryResponse,
    summary="Return paginated macro data history",
    description="Returns historical macro data records, most recent first.",
)
def get_history(
    limit: int = Query(50, ge=1, description="Maximum number of records to return."),
    offset: int = Query(0, ge=0, description="Number of records to skip."),
    db: Session = Depends(get_db),
) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/history received")
    service = MacroQueryService(db)
    result = service.get_history(limit=limit, offset=offset)
    logger.info("GET /macroeconomics/history completed")
    return result


# ---------------------------------------------------------------------------
# History by indicator
# ---------------------------------------------------------------------------

@router.get(
    "/history/{indicator}",
    response_model=MacroQueryResponse,
    summary="Return paginated history for a single indicator",
    description=(
        "Returns historical values for one macro indicator: "
        "inflation, fuel, exchange, or cbr."
    ),
)
def get_indicator_history(
    indicator: MacroIndicator = Path(..., description="Indicator to fetch history for."),
    limit: int = Query(50, ge=1, description="Maximum number of records to return."),
    offset: int = Query(0, ge=0, description="Number of records to skip."),
    db: Session = Depends(get_db),
) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/history/%s received", indicator.value)
    service = MacroQueryService(db)

    dispatch = {
        MacroIndicator.inflation: service.get_inflation_history,
        MacroIndicator.fuel: service.get_fuel_history,
        MacroIndicator.exchange: service.get_exchange_history,
        MacroIndicator.cbr: service.get_cbr_history,
    }

    result = dispatch[indicator](limit=limit, offset=offset)
    logger.info("GET /macroeconomics/history/%s completed", indicator.value)
    return result


# ---------------------------------------------------------------------------
# Latest year
# ---------------------------------------------------------------------------

@router.get(
    "/latest-year",
    response_model=MacroQueryResponse,
    summary="Return the most recent year with recorded data",
    description="Returns the latest year present in the macro data table.",
)
def get_latest_year(db: Session = Depends(get_db)) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/latest-year received")
    service = MacroQueryService(db)
    result = service.get_latest_year()
    logger.info("GET /macroeconomics/latest-year completed")
    return result


# ---------------------------------------------------------------------------
# Year
# ---------------------------------------------------------------------------

@router.get(
    "/year/{year}",
    response_model=MacroQueryResponse,
    summary="Return paginated macro data for a specific year",
    description="Returns all macro data records for the given year.",
)
def get_by_year(
    year: int = Path(..., ge=1900, description="Four-digit year, e.g. 2026."),
    limit: int = Query(50, ge=1, description="Maximum number of records to return."),
    offset: int = Query(0, ge=0, description="Number of records to skip."),
    db: Session = Depends(get_db),
) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/year/%s received", year)
    service = MacroQueryService(db)
    result = service.get_by_year(year=year, limit=limit, offset=offset)
    logger.info("GET /macroeconomics/year/%s completed", year)
    return result


# ---------------------------------------------------------------------------
# History between dates
# ---------------------------------------------------------------------------

@router.get(
    "/history-between",
    response_model=MacroQueryResponse,
    summary="Return macro data between two dates",
    description="Returns paginated macro data records within a date range.",
)
def get_history_between(
    start_date: date = Query(..., description="Range start date, YYYY-MM-DD."),
    end_date: date = Query(..., description="Range end date, YYYY-MM-DD."),
    limit: int = Query(50, ge=1, description="Maximum number of records to return."),
    offset: int = Query(0, ge=0, description="Number of records to skip."),
    db: Session = Depends(get_db),
) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/history-between received")
    service = MacroQueryService(db)
    result = service.get_history_between(
        start_date=start_date, end_date=end_date, limit=limit, offset=offset
    )
    logger.info("GET /macroeconomics/history-between completed")
    return result


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

@router.get(
    "/summary",
    response_model=MacroQueryResponse,
    summary="Return a macroeconomic summary",
    description="Returns a high-level summary of current macro conditions.",
)
def get_summary(db: Session = Depends(get_db)) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/summary received")
    service = MacroQueryService(db)
    result = service.get_summary()
    logger.info("GET /macroeconomics/summary completed")
    return result


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

@router.get(
    "/statistics",
    response_model=MacroQueryResponse,
    summary="Return macro data statistics",
    description="Returns aggregate statistics across all recorded macro data.",
)
def get_statistics(db: Session = Depends(get_db)) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/statistics received")
    service = MacroQueryService(db)
    result = service.get_statistics()
    logger.info("GET /macroeconomics/statistics completed")
    return result


# ---------------------------------------------------------------------------
# Indicator statistics
# ---------------------------------------------------------------------------

@router.get(
    "/statistics/{indicator}",
    response_model=MacroQueryResponse,
    summary="Return statistics for a single indicator",
    description=(
        "Returns aggregate statistics for one macro indicator: "
        "inflation, fuel, exchange, or cbr."
    ),
)
def get_indicator_statistics(
    indicator: MacroIndicator = Path(..., description="Indicator to compute statistics for."),
    db: Session = Depends(get_db),
) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/statistics/%s received", indicator.value)
    service = MacroQueryService(db)
    result = service.get_indicator_statistics(indicator=indicator.value)
    logger.info("GET /macroeconomics/statistics/%s completed", indicator.value)
    return result


# ---------------------------------------------------------------------------
# Compare months
# ---------------------------------------------------------------------------

@router.get(
    "/compare/months",
    response_model=MacroQueryResponse,
    summary="Compare macro data between two months",
    description="Compares macro indicators between two (month, year) records.",
)
def compare_months(
    month_a: str = Query(..., description="First month name, e.g. 'March'."),
    year_a: int = Query(..., ge=1900, description="First year."),
    month_b: str = Query(..., description="Second month name, e.g. 'April'."),
    year_b: int = Query(..., ge=1900, description="Second year."),
    db: Session = Depends(get_db),
) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/compare/months received")
    service = MacroQueryService(db)
    result = service.compare_months(
        month_a=month_a, year_a=year_a, month_b=month_b, year_b=year_b
    )
    logger.info("GET /macroeconomics/compare/months completed")
    return result


# ---------------------------------------------------------------------------
# Compare years
# ---------------------------------------------------------------------------

@router.get(
    "/compare/years",
    response_model=MacroQueryResponse,
    summary="Compare macro data between two years",
    description="Compares aggregate macro indicators between two years.",
)
def compare_years(
    year_a: int = Query(..., ge=1900, description="First year."),
    year_b: int = Query(..., ge=1900, description="Second year."),
    db: Session = Depends(get_db),
) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/compare/years received")
    service = MacroQueryService(db)
    result = service.compare_years(year_a=year_a, year_b=year_b)
    logger.info("GET /macroeconomics/compare/years completed")
    return result


# ---------------------------------------------------------------------------
# Compare periods
# ---------------------------------------------------------------------------

@router.get(
    "/compare/periods",
    response_model=MacroQueryResponse,
    summary="Compare macro data between two date ranges",
    description="Compares aggregate macro indicators between two arbitrary date ranges.",
)
def compare_periods(
    start_a: date = Query(..., description="Start date of the first period, YYYY-MM-DD."),
    end_a: date = Query(..., description="End date of the first period, YYYY-MM-DD."),
    start_b: date = Query(..., description="Start date of the second period, YYYY-MM-DD."),
    end_b: date = Query(..., description="End date of the second period, YYYY-MM-DD."),
    db: Session = Depends(get_db),
) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/compare/periods received")
    service = MacroQueryService(db)
    result = service.compare_periods(
        start_a=start_a, end_a=end_a, start_b=start_b, end_b=end_b
    )
    logger.info("GET /macroeconomics/compare/periods completed")
    return result


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

@router.get(
    "/search",
    response_model=MacroQueryResponse,
    summary="Search macro data records",
    description="Searches macro data using common filters, with pagination.",
)
def search(
    month: Optional[str] = Query(default=None, description="Filter by month name."),
    year: Optional[int] = Query(default=None, ge=1900, description="Filter by year."),
    inflation_min: Optional[float] = Query(default=None, description="Minimum inflation value."),
    inflation_max: Optional[float] = Query(default=None, description="Maximum inflation value."),
    fuel_min: Optional[float] = Query(default=None, description="Minimum fuel price."),
    fuel_max: Optional[float] = Query(default=None, description="Maximum fuel price."),
    cbr_min: Optional[float] = Query(default=None, description="Minimum Central Bank Rate."),
    cbr_max: Optional[float] = Query(default=None, description="Maximum Central Bank Rate."),
    limit: int = Query(default=50, ge=1, description="Maximum number of records to return."),
    offset: int = Query(default=0, ge=0, description="Number of records to skip."),
    db: Session = Depends(get_db),
) -> MacroQueryResponse:
    logger.info("GET /macroeconomics/search received")
    service = MacroQueryService(db)
    result = service.search(
        month=month,
        year=year,
        inflation_min=inflation_min,
        inflation_max=inflation_max,
        fuel_min=fuel_min,
        fuel_max=fuel_max,
        cbr_min=cbr_min,
        cbr_max=cbr_max,
        limit=limit,
        offset=offset,
    )
    logger.info("GET /macroeconomics/search completed")
    return result