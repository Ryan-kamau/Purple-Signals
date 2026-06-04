"""
api/news.py

FastAPI router — News Intelligence endpoints.

This layer is intentionally thin:
  - Validate path/query parameters
  - Delegate all DB queries to NewsQueryService
  - Serialise results via HeadlineResponse

No SQL, no keyword matching, no scraping, no NewsAPI calls live here.

Mount in main.py:
    from api import news
    app.include_router(news.router)
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from database.session import get_db
from schemas.headline import HeadlineResponse
from services.news_query_service import NewsQueryService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/news", tags=["News Intelligence"])

# Single shared service instance — stateless so this is safe.
_service = NewsQueryService()


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _handle_value_error(exc: ValueError, context: str) -> None:
    """Convert a service-layer ValueError into a 400 HTTPException."""
    logger.warning("Bad request in %s: %s", context, exc)
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=str(exc),
    )


def _handle_unexpected(exc: Exception, context: str) -> None:
    """Convert an unexpected exception into a 500 HTTPException."""
    logger.exception("Unexpected error in %s", context)
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"An unexpected error occurred in {context}.",
    ) from exc


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/",
    response_model=list[HeadlineResponse],
    summary="Query stored news headlines",
    description=(
        "Return stored headlines with optional filtering by keyword, category, "
        "source, and minimum impact score. All filters are additive (AND logic). "
        "Returns all headlines paginated by recency when no filters are supplied."
    ),
)
def get_news(
    keyword: Optional[str] = Query(
        default=None,
        description=(
            "Case-insensitive match against detected keywords. "
            "Examples: 'fuel', 'inflation', 'interest rates', 'cbk', 'epra'."
        ),
    ),
    category: Optional[str] = Query(
        default=None,
        description=(
            "Case-insensitive match against intelligence categories. "
            "Examples: 'energy_sector', 'macro_economy', 'geopolitics', "
            "'market_stress', 'kenya_policy'."
        ),
    ),
    source: Optional[str] = Query(
        default=None,
        description=(
            "Case-insensitive source filter. "
            "Examples: 'Reuters', 'Business Daily', 'CNBC'."
        ),
    ),
    min_impact_score: Optional[int] = Query(
        default=None,
        ge=0,
        description=(
            "Return only articles with impact_score >= this value. "
            "High-weight keywords (war=5, oil=4, inflation=4) push scores higher."
        ),
    ),
    page: int = Query(default=1, ge=1, description="Page number (1-indexed)."),
    page_size: int = Query(
        default=20, ge=1, le=100, description="Records per page (1–100)."
    ),
    db: Session = Depends(get_db),
) -> list[HeadlineResponse]:
    """
    General-purpose news search endpoint.

    Supports filtering by keyword, category, source, and minimum impact score.
    All supplied filters are combined with AND logic.

    Returns an empty list when no records match — never raises 404.
    """
    try:
        results = _service.get_news(
            db,
            keyword=keyword,
            category=category,
            source=source,
            min_impact_score=min_impact_score,
            page=page,
            page_size=page_size,
        )
        return results
    except ValueError as exc:
        _handle_value_error(exc, "GET /news")
    except Exception as exc:
        _handle_unexpected(exc, "GET /news")


@router.get(
    "/latest",
    response_model=list[HeadlineResponse],
    summary="Return the most recently published headlines",
    description=(
        "Returns the newest headlines sorted by published_at DESC. "
        "Useful for the dashboard news feed and terminal output."
    ),
)
def get_latest_news(
    limit: int = Query(
        default=10,
        ge=1,
        le=100,
        description="Maximum number of headlines to return (1–100).",
    ),
    db: Session = Depends(get_db),
) -> list[HeadlineResponse]:
    """
    Return the most recently published headlines.

    Sorted by ``published_at DESC``.  Returns an empty list when the
    headlines table is empty.
    """
    try:
        return _service.get_latest_news(db, limit=limit)
    except ValueError as exc:
        _handle_value_error(exc, "GET /news/latest")
    except Exception as exc:
        _handle_unexpected(exc, "GET /news/latest")


@router.get(
    "/high-impact",
    response_model=list[HeadlineResponse],
    summary="Return highest-impact intelligence headlines",
    description=(
        "Returns headlines ranked by weighted impact score (DESC). "
        "High-weight keywords include: war (5), interest rates (5), oil (4), "
        "inflation (4), cbk (4), fuel (3), crisis (3), profit warning (3). "
        "Secondary sort is published_at DESC."
    ),
)
def get_high_impact_news(
    limit: int = Query(
        default=20,
        ge=1,
        le=100,
        description="Maximum number of headlines to return (1–100).",
    ),
    db: Session = Depends(get_db),
) -> list[HeadlineResponse]:
    """
    Return the highest-impact news intelligence.

    Sorted by ``impact_score DESC``, then ``published_at DESC``.
    Useful for the KPLC alert engine and macro risk dashboards.
    """
    try:
        return _service.get_high_impact_news(db, limit=limit)
    except ValueError as exc:
        _handle_value_error(exc, "GET /news/high-impact")
    except Exception as exc:
        _handle_unexpected(exc, "GET /news/high-impact")


@router.get(
    "/nse-relevant",
    response_model=list[HeadlineResponse],
    summary="Return NSE-relevant intelligence",
    description=(
        "Returns headlines most likely to affect Nairobi Securities Exchange stocks. "
        "Includes articles with impact_score >= 5 OR articles tagged with "
        "macro_economy, energy_sector, geopolitics, market_stress, or kenya_policy. "
        "Sorted by impact_score DESC, published_at DESC."
    ),
)
def get_nse_relevant_news(
    page: int = Query(default=1, ge=1, description="Page number (1-indexed)."),
    page_size: int = Query(
        default=20, ge=1, le=100, description="Records per page (1–100)."
    ),
    db: Session = Depends(get_db),
) -> list[HeadlineResponse]:
    """
    Return news most relevant to NSE-listed stocks and the Kenyan economy.

    This endpoint is the primary feed for the KPLC Intelligence Engine's
    macro and sentiment analysis pipeline.
    """
    try:
        return _service.get_nse_relevant_news(db, page=page, page_size=page_size)
    except ValueError as exc:
        _handle_value_error(exc, "GET /news/nse-relevant")
    except Exception as exc:
        _handle_unexpected(exc, "GET /news/nse-relevant")


@router.get(
    "/by-keyword/{keyword}",
    response_model=list[HeadlineResponse],
    summary="Return headlines matching a specific keyword",
    description=(
        "Filters against the keywords_detected column. "
        "Examples: 'fuel', 'inflation', 'cbk', 'debt', 'epra', "
        "'power outage', 'interest rates', 'finance bill'."
    ),
)
def get_news_by_keyword(
    keyword: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
) -> list[HeadlineResponse]:
    """
    Return headlines that matched a specific financial keyword during ingestion.

    Sorted by ``impact_score DESC``, then ``published_at DESC``.
    """
    try:
        return _service.get_news_by_keyword(db, keyword, page=page, page_size=page_size)
    except ValueError as exc:
        _handle_value_error(exc, f"GET /news/by-keyword/{keyword}")
    except Exception as exc:
        _handle_unexpected(exc, f"GET /news/by-keyword/{keyword}")


@router.get(
    "/by-category/{category}",
    response_model=list[HeadlineResponse],
    summary="Return headlines tagged with a specific intelligence category",
    description=(
        "Filters against the categories column. "
        "Valid categories: energy_sector, macro_economy, geopolitics, "
        "market_stress, kenya_policy."
    ),
)
def get_news_by_category(
    category: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
) -> list[HeadlineResponse]:
    """
    Return headlines belonging to a specific intelligence category.

    Sorted by ``impact_score DESC``, then ``published_at DESC``.
    """
    try:
        return _service.get_news_by_category(db, category, page=page, page_size=page_size)
    except ValueError as exc:
        _handle_value_error(exc, f"GET /news/by-category/{category}")
    except Exception as exc:
        _handle_unexpected(exc, f"GET /news/by-category/{category}")


@router.get(
    "/by-source/{source}",
    response_model=list[HeadlineResponse],
    summary="Return all headlines from a specific source",
    description=(
        "Filters by news source using a case-insensitive LIKE match. "
        "Examples: 'Reuters', 'Bloomberg', 'Business Daily', 'CNBC'."
    ),
)
def get_news_by_source(
    source: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
) -> list[HeadlineResponse]:
    """
    Return all headlines published by a specific source.

    Sorted by ``published_at DESC``.
    """
    try:
        return _service.get_news_by_source(db, source, page=page, page_size=page_size)
    except ValueError as exc:
        _handle_value_error(exc, f"GET /news/by-source/{source}")
    except Exception as exc:
        _handle_unexpected(exc, f"GET /news/by-source/{source}")


@router.get(
    "/sources",
    response_model=list[str],
    summary="Return all distinct news sources",
    description=(
        "Returns a sorted list of all unique sources present in the database. "
        "Useful for filter dropdowns and pipeline auditing."
    ),
)
def get_sources(db: Session = Depends(get_db)) -> list[str]:
    """
    Return all distinct news sources stored in the database.

    Example response: ["BBC News", "Business Daily", "CNBC", "Reuters"]
    """
    try:
        return _service.get_sources(db)
    except Exception as exc:
        _handle_unexpected(exc, "GET /news/sources")


@router.get(
    "/categories",
    response_model=list[str],
    summary="Return all distinct intelligence categories",
    description=(
        "Unpacks and deduplicates the comma-separated categories column "
        "across all stored records. "
        "Example: ['energy_sector', 'geopolitics', 'kenya_policy', 'macro_economy']"
    ),
)
def get_categories(db: Session = Depends(get_db)) -> list[str]:
    """
    Return all unique intelligence categories present in stored records.

    Categories are unpacked from comma-separated Text columns, deduplicated,
    and returned sorted alphabetically.
    """
    try:
        return _service.get_categories(db)
    except Exception as exc:
        _handle_unexpected(exc, "GET /news/categories")


@router.get(
    "/keywords",
    response_model=list[str],
    summary="Return all distinct detected keywords",
    description=(
        "Unpacks and deduplicates the comma-separated keywords_detected column "
        "across all stored records. "
        "Useful for building filter autocomplete and keyword frequency analysis."
    ),
)
def get_keywords(db: Session = Depends(get_db)) -> list[str]:
    """
    Return all unique financial keywords present in stored records.

    Keywords are unpacked from comma-separated Text columns, deduplicated,
    and returned sorted alphabetically.
    """
    try:
        return _service.get_keywords(db)
    except Exception as exc:
        _handle_unexpected(exc, "GET /news/keywords")