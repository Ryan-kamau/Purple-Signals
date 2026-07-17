"""
api/macro_ingestor.py

FastAPI router — Macroeconomic Data Ingestion endpoints.

This router is strictly an HTTP layer. It contains NO business logic.
All ingestion, validation, normalization, persistence, and trend
computation already live inside MacroService (services/macro_service.py).

This router's only responsibilities are:
  - Receive and validate HTTP requests (via Pydantic)
  - Inject a database session and construct MacroService
  - Call the appropriate MacroService method
  - Translate the service's response dict into an IngestionResponse
  - Raise the appropriate HTTPException on failure

Mount in main.py:
    from api import macro_ingestor
    app.include_router(macro_ingestor.router)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, HttpUrl
from sqlalchemy.orm import Session

from database.session import get_db
from services.macro_service import MacroService

router = APIRouter(prefix="/macro", tags=["Macro Ingestion"])

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class IngestionRequest(BaseModel):
    """Request body shared by every macro ingestion endpoint."""

    pdf_url: HttpUrl


class IngestionResponse(BaseModel):
    """
    Response returned by MacroData ingestion operations.
    """

    success: bool
    report_date: datetime
    saved: int
    results: Dict[str, Dict[str, Any]]
    errors: list[str]
    fallback_used: bool


# ---------------------------------------------------------------------------
# Private helpers — translation only, no business logic
# ---------------------------------------------------------------------------

def _parse_report_date(value: Optional[str]) -> Optional[datetime]:
    """
    Convert a KNBS-style report date string (e.g. "March 2026") into a
    Nairobi-aware datetime anchored to the first of that month.

    Args:
        value: Report date string from the service response, or None.

    Returns:
        Parsed datetime, or None if `value` is missing/unparseable.
    """
    if not value:
        return None

    try:
        parsed = datetime.strptime(value.strip(), "%B %Y")
    except ValueError:
        return None

    return parsed.replace(tzinfo=NAIROBI_TZ)


def _collect_errors(results: Dict[str, Dict[str, Any]]) -> list[str]:
    """
    Build the flat error list from any non-successful extractor results.

    Args:
        results: Mapping of extractor key -> service response dict.

    Returns:
        List of human-readable messages for every entry whose status is
        not "success". Empty list if everything succeeded.
    """
    errors: list[str] = []

    for entry in results.values():
        entry_status = entry.get("status")
        if entry_status != "success":
            message = entry.get("message") or f"Extractor failed with status '{entry_status}'."
            errors.append(message)

    return errors


def _map_failure_to_exception(message: str) -> HTTPException:
    """
    Translate a service failure message into the appropriate HTTPException.

    Heuristics (message content, since MacroService does not currently
    return a structured error code):
      - Database-related failures  -> 500 Internal Server Error
      - Everything else            -> 400 Bad Request

    Args:
        message: Human-readable failure message from the service layer.

    Returns:
        A ready-to-raise HTTPException.
    """
    lowered = message.lower()

    if "database" in lowered or "rolled back" in lowered:
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="A database error occurred while saving macro data.",
        )

    return HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=message,
    )


def _build_response(
    raw: Dict[str, Any],
    extractor_key: Optional[str] = None,
) -> IngestionResponse:
    """
    Translate a MacroService response dict into an IngestionResponse.

    Handles two shapes:
      - Aggregate (refresh_macro_data): already has a "results" dict keyed
        by extractor name.
      - Single-extractor (refresh_inflation / _cbr / _exchange_rates /
        _fuel_prices): wrapped into {extractor_key: raw} so every endpoint
        returns the same response shape.

    Args:
        raw:           Raw dict returned by a MacroService method.
        extractor_key: Key to wrap `raw` under for single-extractor calls.
                       None for the aggregate /macro endpoint.

    Returns:
        Fully populated IngestionResponse.

    Raises:
        HTTPException: 400/500 if the service reports a hard failure, or
                       404/500 if the report date cannot be resolved.
    """
    results: Dict[str, Dict[str, Any]] = (
        raw.get("results", {}) if extractor_key is None else {extractor_key: raw}
    )

    overall_status = raw.get("status")

    if overall_status == "failed":
        raise _map_failure_to_exception(raw.get("message") or "Macro extraction failed.")

    report_date = _parse_report_date(raw.get("report_date"))
    if report_date is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Could not determine a report date from the supplied PDF.",
        )

    return IngestionResponse(
        success=overall_status in ("success", "partial_success"),
        report_date=report_date,
        saved=raw.get("rows_saved", 0),
        results=results,
        errors=_collect_errors(results),
        fallback_used=False,
    )


def _run_ingestion(
    service_call: Callable[[str], Dict[str, Any]],
    pdf_url: str,
    extractor_key: Optional[str] = None,
) -> IngestionResponse:
    """
    Call a MacroService method and translate its result, converting any
    unexpected exception into a safe 500 response.

    Args:
        service_call:  Bound MacroService method, e.g. service.refresh_cbr.
        pdf_url:       KNBS PDF URL to ingest.
        extractor_key: See `_build_response`.

    Returns:
        IngestionResponse ready to return from the endpoint.

    Raises:
        HTTPException: Propagated from `_build_response`, or a generic 500
                       for unexpected errors.
    """
    try:
        raw = service_call(pdf_url)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while ingesting macro data.",
        ) from exc

    return _build_response(raw, extractor_key=extractor_key)


def _get_service(db: Session = Depends(get_db)) -> MacroService:
    """FastAPI dependency — construct a MacroService bound to the request session."""
    return MacroService(db)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=IngestionResponse,
    summary="Refresh all macro indicators from a KNBS PDF",
    description=(
        "Runs the full macro ingestion pipeline (inflation, exchange rates, "
        "CBR, fuel prices) against a single KNBS Leading Economic Indicators PDF."
    ),
)
def ingest_macro_data(
    body: IngestionRequest,
    service: MacroService = Depends(_get_service),
) -> IngestionResponse:
    """Trigger the full macro ingestion pipeline for all indicators."""
    return _run_ingestion(service.refresh_macro_data, str(body.pdf_url))


@router.post(
    "/inflation",
    response_model=IngestionResponse,
    summary="Refresh inflation data only",
)
def ingest_inflation(
    body: IngestionRequest,
    service: MacroService = Depends(_get_service),
) -> IngestionResponse:
    """Trigger ingestion for the inflation indicator only."""
    return _run_ingestion(service.refresh_inflation, str(body.pdf_url), extractor_key="inflation")


@router.post(
    "/exchange",
    response_model=IngestionResponse,
    summary="Refresh exchange rate data only",
)
def ingest_exchange_rates(
    body: IngestionRequest,
    service: MacroService = Depends(_get_service),
) -> IngestionResponse:
    """Trigger ingestion for the USD/GBP/EUR exchange rate indicator only."""
    return _run_ingestion(service.refresh_exchange_rates, str(body.pdf_url), extractor_key="exchange")


@router.post(
    "/cbr",
    response_model=IngestionResponse,
    summary="Refresh Central Bank Rate data only",
)
def ingest_cbr(
    body: IngestionRequest,
    service: MacroService = Depends(_get_service),
) -> IngestionResponse:
    """Trigger ingestion for the Central Bank Rate indicator only."""
    return _run_ingestion(service.refresh_cbr, str(body.pdf_url), extractor_key="cbr")


@router.post(
    "/fuel",
    response_model=IngestionResponse,
    summary="Refresh fuel price data only",
)
def ingest_fuel_prices(
    body: IngestionRequest,
    service: MacroService = Depends(_get_service),
) -> IngestionResponse:
    """Trigger ingestion for the diesel fuel price indicator only."""
    return _run_ingestion(service.refresh_fuel_prices, str(body.pdf_url), extractor_key="fuel")