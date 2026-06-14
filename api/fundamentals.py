"""
api/fundamentals.py

FastAPI router — KPLC Fundamental Analysis endpoints.

Pipeline position:
    HTTP Request  →  Router  →  KPLCFundamentalExtractor  →  Fundamentals DB

This router is a THIN ORCHESTRATION LAYER only.  It:
  - Validates incoming request payloads
  - Delegates all extraction and ratio logic to KPLCFundamentalExtractor
  - Enforces the duplicate-detection rule (ticker + report_date)
  - Serialises ORM results into response dicts

It does NOT:
  - Parse PDFs
  - Compute financial ratios
  - Access models directly beyond duplicate checks and reads
  - Duplicate any logic from KPLCFundamentalExtractor

Mount in main.py:
    from api import fundamentals
    app.include_router(fundamentals.router)
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field, HttpUrl
from sqlalchemy.orm import Session

from database.session import get_db
from models.fundamental_data import Fundamentals
from models.market_data import MarketData
from scrapers.kplc_pdf import KPLCFundamentalExtractor

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/fundamentals",
    tags=["Fundamental Analysis"],
)

KPLC_TICKER: str = "KPLC"


# ---------------------------------------------------------------------------
# Pydantic request models
# ---------------------------------------------------------------------------


class FundamentalsIngestRequest(BaseModel):
    """Request body for ingesting KPLC fundamentals from a PDF URL."""

    pdf_url: HttpUrl = Field(
        ...,
        description="Direct URL to a KPLC audited financial results PDF.",
        examples=[
            "https://www.nse.co.ke/wp-content/uploads/The-Kenya-Power-Lighting-Company-Plc-Audited-Financial-Results-for-the-Year-Ended-30-Jun-2025.pdf"
        ],
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _serialise_record(record: Fundamentals) -> dict[str, Any]:
    """
    Convert a Fundamentals ORM object into a serialisable dict.

    Args:
        record: Committed Fundamentals ORM instance.

    Returns:
        Dict with all fundamental fields.
    """
    return {
        "id": record.id,
        "ticker": record.ticker,
        "company": record.company,
        "report_date": record.report_date.isoformat() if record.report_date else None,
        "eps": record.eps,
        "pe_ratio": record.pe_ratio,
        "dividend_yield": record.dividend_yield,
        "revenue": record.revenue,
        "debt_ratio": record.debt_ratio,
        "net_profit_margin": record.net_profit_margin,
        "timestamp": record.timestamp.isoformat() if record.timestamp else None,
    }


def _fetch_latest_market_price(db: Session) -> float | None:
    """
    Fetch the most recent KPLC market price from the market_data table.

    Args:
        db: Active SQLAlchemy session.

    Returns:
        Latest KPLC price as a float, or None if no record exists.
    """
    record: MarketData | None = (
        db.query(MarketData)
        .filter(MarketData.ticker == KPLC_TICKER)
        .order_by(MarketData.timestamp.desc())
        .first()
    )
    return float(record.price) if record else None


def _compute_current_pe(eps: float, market_price: float) -> float | None:
    """
    Calculate a live P/E ratio from the current market price and stored EPS.

    We recompute this at read-time (not from the stored pe_ratio column) so
    the value always reflects today's price rather than the historical price
    at the time of ingestion.

    Args:
        eps:          Earnings per share from the stored record.
        market_price: Current KPLC market price.

    Returns:
        Rounded float P/E, or None if EPS is zero.
    """
    if not eps:
        return None
    return round(market_price / eps, 4)


def _handle_unexpected(exc: Exception, context: str) -> None:
    """Escalate an unexpected exception to a safe HTTP 500."""
    logger.exception("Unexpected error in %s: %s", context, exc)
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"An unexpected error occurred in {context}.",
    ) from exc


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/ingest",
    status_code=status.HTTP_201_CREATED,
    summary="Ingest KPLC fundamentals from a PDF report",
    description=(
        "Downloads a KPLC audited results PDF, extracts financial metrics, "
        "calculates ratios using the current market price, and persists the "
        "record.  Returns HTTP 409 if the same report_date already exists for KPLC."
    ),
)
def ingest_fundamentals(
    body: FundamentalsIngestRequest,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """
    Full ingestion pipeline for one KPLC annual/interim results PDF.

    Steps:
      1. Instantiate extractor.
      2. Download + extract raw metrics from PDF.
      3. Read report_date from extracted data.
      4. Duplicate check against (ticker, report_date).
      5. Fetch current KPLC market price from DB.
      6. Calculate ratios.
      7. Persist to Fundamentals table.
      8. Return the created record.

    Raises:
        HTTP 409 — record for this report_date already exists.
        HTTP 422 — PDF download or extraction failed.
        HTTP 500 — unexpected error.
    """
    pdf_url_str = str(body.pdf_url)
    logger.info("Fundamentals ingest started: pdf_url=%s", pdf_url_str)

    extractor = KPLCFundamentalExtractor()

    # ── Step 1: Extract raw metrics from PDF ────────────────────────────────
    try:
        raw = extractor.extract(pdf_url_str)
    except Exception as exc:
        logger.error(
            "PDF extraction failed: pdf_url=%s error=%s", pdf_url_str, exc
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Failed to extract data from PDF: {exc}",
        ) from exc

    report_date = raw.get("report_date")
    ticker = raw.get("ticker", KPLC_TICKER)

    logger.info(
        "Extraction succeeded: ticker=%s report_date=%s", ticker, report_date
    )

    # ── Step 2: Duplicate check ──────────────────────────────────────────────
    existing: Fundamentals | None = (
        db.query(Fundamentals)
        .filter(
            Fundamentals.ticker == ticker,
            Fundamentals.report_date == report_date,
        )
        .first()
    )

    if existing:
        logger.warning(
            "Duplicate fundamentals record detected: ticker=%s report_date=%s id=%d",
            ticker,
            report_date,
            existing.id,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Fundamentals for {ticker} with report_date={report_date} "
                f"already exist (id={existing.id}). "
                "Ingest aborted to prevent duplication."
            ),
        )

    # ── Step 3: Fetch current market price ──────────────────────────────────
    try:
        stock_price = extractor._fetch_latest_price(db)
    except RuntimeError as exc:
        logger.error("Market price unavailable: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "No KPLC market price found in the database. "
                "Run GET /market-data/kplc first to populate price data."
            ),
        ) from exc

    # ── Step 4: Calculate ratios ─────────────────────────────────────────────
    try:
        metrics = extractor.calculate_ratios(raw, stock_price=stock_price)
    except (ValueError, Exception) as exc:
        logger.error("Ratio calculation failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Failed to calculate financial ratios: {exc}",
        ) from exc

    # ── Step 5: Persist ──────────────────────────────────────────────────────
    try:
        record = extractor.save_to_db(db, metrics)
    except RuntimeError as exc:
        # save_to_db raises RuntimeError on duplicate (belt-and-suspenders)
        # and on DB write failure.
        logger.error("DB save failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database save failed: {exc}",
        ) from exc
    except Exception as exc:
        _handle_unexpected(exc, "POST /fundamentals/ingest")

    logger.info(
        "Fundamentals saved: id=%d ticker=%s report_date=%s eps=%.4f pe_ratio=%.4f",
        record.id,
        record.ticker,
        record.report_date,
        record.eps,
        record.pe_ratio,
    )

    return {
        "status": "created",
        "message": f"Fundamentals ingested successfully for report_date={record.report_date}.",
        "data": _serialise_record(record),
    }


@router.get(
    "/latest",
    summary="Return the latest KPLC fundamentals with live P/E",
    description=(
        "Returns the most recently ingested KPLC fundamental record "
        "(ordered by report_date DESC), enriched with the current market price "
        "and a live-calculated P/E ratio.  The live P/E uses today's price "
        "against the stored EPS — not the historical pe_ratio column."
    ),
)
def get_latest_fundamentals(
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """
    Return the latest Fundamentals record with a live P/E ratio.

    The ``current_pe_ratio`` is computed at read-time from the most recent
    KPLC market price and the stored EPS.  This keeps the value accurate
    even when the market price has moved since the report was ingested.

    Raises:
        HTTP 404 — no fundamentals record exists yet.
        HTTP 500 — unexpected error.
    """
    logger.info("Latest fundamentals retrieval started")

    try:
        record: Fundamentals | None = (
            db.query(Fundamentals)
            .filter(Fundamentals.ticker == KPLC_TICKER)
            .order_by(Fundamentals.report_date.desc())
            .first()
        )
    except Exception as exc:
        _handle_unexpected(exc, "GET /fundamentals/latest")

    if record is None:
        logger.warning("No fundamentals record found for ticker=%s", KPLC_TICKER)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No fundamentals data found for {KPLC_TICKER}. "
                "Run POST /fundamentals/ingest to populate."
            ),
        )

    current_price = _fetch_latest_market_price(db)
    current_pe = (
        _compute_current_pe(record.eps, current_price)
        if current_price is not None
        else None
    )

    logger.info(
        "Latest fundamentals retrieved: id=%d report_date=%s current_price=%s current_pe=%s",
        record.id,
        record.report_date,
        current_price,
        current_pe,
    )

    return {
        "fundamentals": _serialise_record(record),
        "current_market_price": current_price,
        "current_pe_ratio": current_pe,
    }


@router.get(
    "/history",
    summary="Return historical KPLC fundamentals",
    description=(
        "Returns the most recent N fundamentals records ordered by report_date DESC. "
        "Limit defaults to 3 and is capped at 20.  Returns an empty list "
        "when no records exist — never raises 404."
    ),
)
def get_fundamentals_history(
    limit: int = Query(
        default=3,
        ge=1,
        le=20,
        description="Number of records to return (1–20). Default: 3.",
    ),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """
    Return a historical list of KPLC fundamental snapshots.

    Ordered by ``report_date DESC`` so the most recent annual/interim report
    appears first.  Useful for trend analysis and EPS growth comparisons
    across reporting periods.

    Returns ``{"count": 0, "records": []}`` when the table is empty.

    Raises:
        HTTP 500 — unexpected error.
    """
    logger.info("Fundamentals history retrieval started: limit=%d", limit)

    try:
        records: list[Fundamentals] = (
            db.query(Fundamentals)
            .filter(Fundamentals.ticker == KPLC_TICKER)
            .order_by(Fundamentals.report_date.desc())
            .limit(limit)
            .all()
        )
    except Exception as exc:
        _handle_unexpected(exc, "GET /fundamentals/history")

    serialised = [_serialise_record(r) for r in records]

    logger.info(
        "Fundamentals history retrieved: ticker=%s count=%d limit=%d",
        KPLC_TICKER,
        len(serialised),
        limit,
    )

    return {
        "count": len(serialised),
        "records": serialised,
    }