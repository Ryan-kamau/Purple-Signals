"""
api/news_ingestion.py

FastAPI router — RSS News Ingestion endpoints (WRITE side).

Pipeline position:
    HTTP Request  →  Router  →  RSSNewsIngestor  →  KeywordEngine  →  Headline DB

This router is a THIN ORCHESTRATION LAYER only.  It:
  - Validates incoming request payloads
  - Instantiates RSSNewsIngestor with an injected DB session
  - Delegates all ingestion logic to RSSNewsIngestor's public methods
  - Serialises results and computes aggregate metrics

It does NOT:
  - Parse RSS feeds
  - Run keyword enrichment
  - Access the Headline model directly
  - Duplicate any logic from RSSNewsIngestor

Mount in main.py:
    from api import news_ingestion
    app.include_router(news_ingestion.router)
"""

import logging
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, HttpUrl
from sqlalchemy.orm import Session

from database.session import get_db
from scrapers.rss_news import DEFAULT_FEEDS, RSSNewsIngestor
from schemas.headline import IngestionResponse

logger = logging.getLogger(__name__)

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

router = APIRouter(
    prefix="/news",
    tags=["news Intelligence Ingestion"],
)


# ---------------------------------------------------------------------------
# Feed sub-registries
# Defined here so they stay in sync with DEFAULT_FEEDS in rss_news.py without
# duplicating URLs.  Labels are used to filter the shared registry.
# ---------------------------------------------------------------------------

_KENYA_LABELS: frozenset[str] = frozenset(
    {
        "Business Daily Africa",
        "Nation Business",
        "The Standard Business",
        "Capital FM Business",
    }
)

_GLOBAL_LABELS: frozenset[str] = frozenset(
    {
        "Reuters Business",
        "CNBC Markets",
        "Financial Times World",
        "IMF News",
        "World Bank News",
    }
)


def _kenya_feeds() -> list[dict[str, str]]:
    """Return only Kenyan-source feeds from the default registry."""
    return [f for f in DEFAULT_FEEDS if f.get("label") in _KENYA_LABELS]


def _global_feeds() -> list[dict[str, str]]:
    """Return only global macro feeds from the default registry."""
    return [f for f in DEFAULT_FEEDS if f.get("label") in _GLOBAL_LABELS]


# ---------------------------------------------------------------------------
# Pydantic request models
# ---------------------------------------------------------------------------

class FeedIngestRequest(BaseModel):
    """Request body for ingesting a single named feed."""

    url: HttpUrl = Field(
        ...,
        description="Full RSS/Atom feed URL.",
        examples=["https://www.businessdailyafrica.com/rss/266"],
    )
    label: str | None = Field(
        default=None,
        description=(
            "Human-readable source name stored on each ingested headline. "
            "Falls back to the feed's own title or URL when omitted."
        ),
        examples=["Business Daily Africa"],
    )


# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------

class AggregateIngestionResponse(BaseModel):
    """
    Aggregate metrics returned by multi-feed ingestion endpoints.

    Wraps individual IngestionResponse objects and provides rolled-up totals
    so callers can assess a full ingestion run without iterating results.
    """

    feeds_run: int = Field(..., description="Total number of feeds attempted.")
    feeds_succeeded: int = Field(..., description="Feeds that returned success=True.")
    feeds_failed: int = Field(..., description="Feeds that returned success=False.")
    total_fetched: int = Field(..., description="Sum of articles fetched across all feeds.")
    total_saved: int = Field(..., description="Sum of new articles saved to the database.")
    total_duplicates: int = Field(..., description="Sum of duplicate articles skipped.")
    total_invalid: int = Field(
        ..., description="Sum of articles that failed validation or impact-score threshold."
    )
    fallback_feeds: int = Field(
        ..., description="Number of feeds that fell back to synthetic articles."
    )
    processed_at: datetime = Field(..., description="Timestamp when the run completed.")
    results: list[IngestionResponse] = Field(
        ..., description="Per-feed IngestionResponse objects in order of execution."
    )


class FeedRegistryEntry(BaseModel):
    """Single entry in the feed registry."""

    label: str
    url: str
    region: str = Field(..., description="'kenya' | 'global'")


class FeedRegistryResponse(BaseModel):
    """Response for GET /rss/feeds."""

    total: int
    kenya_feeds: int
    global_feeds: int
    feeds: list[FeedRegistryEntry]


class RSSHealthResponse(BaseModel):
    """Response for GET /rss/health."""

    service: str
    status: str
    configured_feeds: int
    kenya_feeds: int
    global_feeds: int
    timestamp: datetime


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _aggregate(results: list[IngestionResponse]) -> AggregateIngestionResponse:
    """
    Roll up a list of per-feed IngestionResponse objects into aggregate metrics.

    Args:
        results: List of IngestionResponse objects from RSSNewsIngestor.

    Returns:
        AggregateIngestionResponse with summed totals.
    """
    return AggregateIngestionResponse(
        feeds_run=len(results),
        feeds_succeeded=sum(1 for r in results if r.success),
        feeds_failed=sum(1 for r in results if not r.success),
        total_fetched=sum(r.fetched for r in results),
        total_saved=sum(r.saved for r in results),
        total_duplicates=sum(r.duplicates for r in results),
        total_invalid=sum(r.invalid for r in results),
        fallback_feeds=sum(1 for r in results if r.fallback_used),
        processed_at=datetime.now(NAIROBI_TZ),
        results=results,
    )


def _run_feeds(
    feeds: list[dict[str, str]],
    db: Session,
) -> list[IngestionResponse]:
    """
    Run RSSNewsIngestor.ingest_all_feeds() for a specific feed list.

    Args:
        feeds: Feed dicts with 'url' and 'label' keys.
        db:    Active SQLAlchemy session.

    Returns:
        List of IngestionResponse, one per feed.
    """
    return RSSNewsIngestor.ingest_all_feeds(session=db, feeds=feeds)


def _label_region(label: str) -> str:
    """Resolve a feed's region tag from its label."""
    if label in _KENYA_LABELS:
        return "kenya"
    if label in _GLOBAL_LABELS:
        return "global"
    return "other"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post(
    "/ingest",
    response_model=IngestionResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest a single RSS feed by URL",
    description=(
        "Fetches, enriches, deduplicates, and persists articles from the supplied "
        "RSS/Atom feed URL.  All processing is delegated to RSSNewsIngestor — "
        "the router only orchestrates the call."
    ),
)
def ingest_feed(
    body: FeedIngestRequest,
    db: Session = Depends(get_db),
) -> IngestionResponse:
    """
    Trigger ingestion for a single RSS feed.

    The label is optional — when omitted, RSSNewsIngestor resolves the source
    name from the feed's own title element or falls back to the URL.

    Returns the raw IngestionResponse from RSSNewsIngestor including counts of
    fetched / saved / duplicates / invalid articles and any error details.
    """
    url_str = str(body.url)
    logger.info(
        "RSS ingest request: url=%s label=%s",
        url_str,
        body.label,
    )

    ingestor = RSSNewsIngestor(db)
    result = ingestor.ingest_feed(feed_url=url_str, source_label=body.label)

    logger.info(
        "RSS ingest complete: url=%s saved=%d duplicates=%d invalid=%d fallback=%s",
        url_str,
        result.saved,
        result.duplicates,
        result.invalid,
        result.fallback_used,
    )

    return result


@router.post(
    "/ingest/all",
    response_model=AggregateIngestionResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest all configured RSS feeds",
    description=(
        "Runs every feed in the default registry (Kenyan + global sources). "
        "Returns per-feed results and rolled-up aggregate metrics."
    ),
)
def ingest_all_feeds(db: Session = Depends(get_db)) -> AggregateIngestionResponse:
    """
    Trigger ingestion for every feed in the default registry.

    Equivalent to running /rss/ingest/kenya and /rss/ingest/global in sequence,
    but in a single transaction context.

    The aggregate response includes ``feeds_succeeded``, ``feeds_failed``,
    ``total_saved``, and the full per-feed ``results`` list so callers can
    inspect individual failures without a follow-up request.
    """
    logger.info(
        "RSS ingest/all started: feeds=%d",
        len(DEFAULT_FEEDS),
    )

    results = _run_feeds(DEFAULT_FEEDS, db)
    aggregate = _aggregate(results)

    logger.info(
        "RSS ingest/all complete: feeds_run=%d saved=%d failed=%d",
        aggregate.feeds_run,
        aggregate.total_saved,
        aggregate.feeds_failed,
    )

    return aggregate


@router.post(
    "/ingest/kenya",
    response_model=AggregateIngestionResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest Kenyan news sources only",
    description=(
        "Runs only the Kenya-specific feeds: "
        "Business Daily Africa, Nation Business, The Standard Business, Capital FM Business. "
        "Use this for a fast local-market intelligence refresh."
    ),
)
def ingest_kenya_feeds(db: Session = Depends(get_db)) -> AggregateIngestionResponse:
    """
    Trigger ingestion for Kenyan-only news sources.

    This is the primary feed for KPLC-specific intelligence — local business
    news, NSE coverage, energy sector stories, and government policy signals.

    Useful for scheduled jobs that need a quick local refresh without
    waiting for slower international feeds (FT, IMF, Reuters).
    """
    feeds = _kenya_feeds()

    if not feeds:
        logger.error(
            "No Kenyan feeds found in DEFAULT_FEEDS — check label names in rss_news.py"
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kenyan feed registry is empty. Check DEFAULT_FEEDS configuration.",
        )

    logger.info("RSS ingest/kenya started: feeds=%d", len(feeds))

    results = _run_feeds(feeds, db)
    aggregate = _aggregate(results)

    logger.info(
        "RSS ingest/kenya complete: saved=%d failed=%d",
        aggregate.total_saved,
        aggregate.feeds_failed,
    )

    return aggregate


@router.post(
    "/ingest/global",
    response_model=AggregateIngestionResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest global macro news sources only",
    description=(
        "Runs only the global macro feeds: "
        "Reuters Business, CNBC Markets, Financial Times World, IMF News, World Bank News. "
        "Use this to refresh the macro risk and geopolitics intelligence layer."
    ),
)
def ingest_global_feeds(db: Session = Depends(get_db)) -> AggregateIngestionResponse:
    """
    Trigger ingestion for global macro news sources.

    These feeds provide macroeconomic context for KPLC analysis:
    oil price movements, Fed/IMF policy signals, geopolitical risk,
    and global market stress indicators that flow through to NSE.

    Designed to complement /rss/ingest/kenya — run both together via
    /rss/ingest/all for a full intelligence refresh.
    """
    feeds = _global_feeds()

    if not feeds:
        logger.error(
            "No global feeds found in DEFAULT_FEEDS — check label names in rss_news.py"
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Global feed registry is empty. Check DEFAULT_FEEDS configuration.",
        )

    logger.info("RSS ingest/global started: feeds=%d", len(feeds))

    results = _run_feeds(feeds, db)
    aggregate = _aggregate(results)

    logger.info(
        "RSS ingest/global complete: saved=%d failed=%d",
        aggregate.total_saved,
        aggregate.feeds_failed,
    )

    return aggregate


@router.post(
    "/ingest/custom",
    response_model=IngestionResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest an arbitrary RSS feed not in the default registry",
    description=(
        "Accepts any valid RSS/Atom URL.  Useful for one-off ingestion of "
        "custom sources — CBK press releases, company investor relations pages, "
        "EPRA announcements, or any feed relevant to KPLC / NSE analysis."
    ),
)
def ingest_custom_feed(
    body: FeedIngestRequest,
    db: Session = Depends(get_db),
) -> IngestionResponse:
    """
    Ingest an arbitrary RSS/Atom feed not in the default registry.

    Identical to POST /rss/ingest in behaviour — provided as a semantically
    distinct endpoint so API consumers can distinguish one-off custom ingestion
    from standard scheduled ingestion in their logs and monitoring.

    The label is optional.  If omitted, the feed's own ``<title>`` element
    is used as the source name, falling back to the URL itself.
    """
    url_str = str(body.url)
    logger.info(
        "RSS ingest/custom request: url=%s label=%s",
        url_str,
        body.label,
    )

    ingestor = RSSNewsIngestor(db)
    result = ingestor.ingest_feed(feed_url=url_str, source_label=body.label)

    logger.info(
        "RSS ingest/custom complete: url=%s saved=%d fallback=%s",
        url_str,
        result.saved,
        result.fallback_used,
    )

    return result


# ---------------------------------------------------------------------------
# Read endpoints (no DB writes)
# ---------------------------------------------------------------------------

@router.get(
    "/health",
    response_model=RSSHealthResponse,
    status_code=status.HTTP_200_OK,
    summary="RSS ingestion service health check",
    description=(
        "Returns the operational status of the RSS ingestion service and "
        "a count of configured feeds.  Does not touch the database or make "
        "any external HTTP calls — safe to use as a liveness probe."
    ),
)
def health() -> RSSHealthResponse:
    """
    Liveness probe for the RSS ingestion subsystem.

    Reports the number of configured feeds broken down by region.
    Since this endpoint is side-effect-free it can be polled frequently
    by uptime monitors or Docker/k8s health checks.
    """
    return RSSHealthResponse(
        service="rss_news_ingestor",
        status="healthy",
        configured_feeds=len(DEFAULT_FEEDS),
        kenya_feeds=len(_kenya_feeds()),
        global_feeds=len(_global_feeds()),
        timestamp=datetime.now(NAIROBI_TZ),
    )


@router.get(
    "/feeds",
    response_model=FeedRegistryResponse,
    status_code=status.HTTP_200_OK,
    summary="Return the configured RSS feed registry",
    description=(
        "Lists every feed configured in the default registry with its label, "
        "URL, and region tag ('kenya' | 'global').  Useful for debugging, "
        "documentation, and admin dashboards."
    ),
)
def get_feed_registry() -> FeedRegistryResponse:
    """
    Return the full RSS feed registry with region annotations.

    Each entry is tagged as 'kenya' or 'global' so consumers can understand
    which feeds contribute to which intelligence layer without reading the
    source code.
    """
    entries = [
        FeedRegistryEntry(
            label=feed["label"],
            url=feed["url"],
            region=_label_region(feed["label"]),
        )
        for feed in DEFAULT_FEEDS
    ]

    return FeedRegistryResponse(
        total=len(entries),
        kenya_feeds=len(_kenya_feeds()),
        global_feeds=len(_global_feeds()),
        feeds=entries,
    )