# api/sentiment.py

"""
api/sentiment.py

FastAPI router — Sentiment Analysis endpoints.

Pipeline position:
    HTTP Request  ->  Router  ->  SentimentAnalyzer  ->  Headline DB  ->  MySQL

This router is a THIN ORCHESTRATION LAYER only. It:
  - Validates incoming request payloads
  - Injects a SQLAlchemy session and constructs SentimentAnalyzer
  - Fetches ORM objects strictly needed to call analyze_record()/analyze_batch()
  - Delegates every scoring, batching, and persistence decision to
    SentimentAnalyzer
  - Serialises results into explicit Pydantic response models
  - Converts failure conditions into HTTPException

It does NOT:
  - Score any text itself
  - Duplicate SentimentAnalyzer's validation, cleaning, or weighting logic
  - Create database sessions or commit transactions
  - Return raw dicts

Mount in main.py:
    from api import sentiment
    app.include_router(sentiment.router)
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy.orm import Session

from database.session import get_db
from intelligence.sentiment_analyzer import DEFAULT_BATCH_SIZE, SentimentAnalyzer
from models.headline_data import Headline
from schemas.sentiment import (
    AnalyzeBatchItemResponse,
    AnalyzeBatchRequest,
    AnalyzeBatchResponse,
    AnalyzeHeadlineData,
    AnalyzeHeadlineRequest,
    AnalyzeHeadlineResponse,
    AnalyzeRecordData,
    AnalyzeRecordResponse,
    StatisticsResponse,
    UpdateSentimentResponse,
    UpdateSentimentStatsData,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sentiment", tags=["Sentiment_analysis"])


# ---------------------------------------------------------------------------
# Dependency injection
# ---------------------------------------------------------------------------

def _get_sentiment_service(db: Session = Depends(get_db)) -> SentimentAnalyzer:
    """
    FastAPI dependency — construct a SentimentAnalyzer bound to the request
    session. Mirrors _get_service() in api/macro_ingestor.py.
    """
    return SentimentAnalyzer(db)


# ---------------------------------------------------------------------------
# Private helpers — translation only, no business logic
# ---------------------------------------------------------------------------

def _fetch_headline_or_404(db: Session, headline_id: int) -> Headline:
    """
    Fetch a single Headline by primary key, raising 404 if absent.

    This is plain ORM retrieval — no business logic. Scoring the record is
    always delegated to SentimentAnalyzer.analyze_record().
    """
    record = db.get(Headline, headline_id)
    if record is None:
        logger.warning("Headline id=%d not found.", headline_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Headline with id={headline_id} was not found.",
        )
    return record


def _fetch_headlines_by_ids(db: Session, headline_ids: list[int]) -> list[Headline]:
    """
    Fetch multiple Headline records by primary key.

    Records that don't exist are silently omitted (not a request error —
    the batch preview simply reflects what was actually found). Raises 404
    only if NONE of the requested IDs resolve to a record.
    """
    unique_ids = list(dict.fromkeys(headline_ids))
    records = (
        db.query(Headline)
        .filter(Headline.id.in_(unique_ids))
        .all()
    )

    if not records:
        logger.warning("Batch analyze: none of the requested IDs exist: %s", unique_ids)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="None of the requested headline_ids were found.",
        )

    return records


def _build_stats_data(stats: dict) -> UpdateSentimentStatsData:
    """
    Convert SentimentAnalyzer's statistics dict
    (status, message, processed, updated, skipped, failed,
    average_sentiment, execution_time) into UpdateSentimentStatsData.

    Only pure re-shaping — the numbers themselves come entirely from the
    service.
    """
    return UpdateSentimentStatsData(
        status=stats["status"],
        processed=stats["processed"],
        updated=stats["updated"],
        skipped=stats["skipped"],
        failed=stats["failed"],
        average_sentiment=stats["average_sentiment"],
        execution_time=stats["execution_time"],
    )


def _handle_unexpected(exc: Exception, context: str) -> None:
    """Convert an unexpected exception into a safe 500 HTTPException."""
    logger.exception("Unexpected error in %s: %s", context, exc)
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"An unexpected error occurred in {context}.",
    ) from exc


# ---------------------------------------------------------------------------
# 1. Analyze arbitrary text
# ---------------------------------------------------------------------------

@router.put(
    "/analyze",
    response_model=AnalyzeHeadlineResponse,
    status_code=status.HTTP_200_OK,
    summary="Analyze sentiment for arbitrary text",
    description=(
        "Scores an arbitrary piece of text end-to-end (validate -> clean -> "
        "VADER -> custom weighted score) via SentimentAnalyzer.analyze_headline(). "
        "Performs no database I/O. Returns 422 if the text contains no "
        "meaningful, scorable content (e.g. empty, a bare link, an RSS "
        "placeholder, or HTML-only)."
    ),
    responses={
        422: {"description": "Text contained no meaningful, scorable content."},
        500: {"description": "Unexpected server error."},
    },
)
def analyze_text(
    body: AnalyzeHeadlineRequest,
    service: SentimentAnalyzer = Depends(_get_sentiment_service),
) -> AnalyzeHeadlineResponse:
    """
    Score arbitrary text without touching the database.

    Delegates entirely to `service.analyze_headline(text)`. If the service
    returns None (no usable natural language content), responds with
    422 Unprocessable Entity rather than fabricating a score.
    """
    try:
        score = service.analyze_headline(body.text)
    except Exception as exc:  # noqa: BLE001
        _handle_unexpected(exc, "PUT /sentiment/analyze")

    if score is None:
        logger.info("analyze_text: text yielded no usable sentiment signal.")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="The supplied text contains no meaningful, scorable content.",
        )

    label = service._classify_score(score)

    return AnalyzeHeadlineResponse(
        status="success",
        message="Sentiment analysis completed successfully.",
        data=AnalyzeHeadlineData(
            text=body.text,
            sentiment_score=score,
            sentiment_label=label,
        ),
    )


# ---------------------------------------------------------------------------
# 2. Analyze one database record
# ---------------------------------------------------------------------------

@router.put(
    "/analyze-record/{headline_id}",
    response_model=AnalyzeRecordResponse,
    status_code=status.HTTP_200_OK,
    summary="Analyze sentiment for a stored headline",
    description=(
        "Retrieves a Headline by ID and scores it via "
        "SentimentAnalyzer.analyze_record(), using the headline / description "
        "/ content priority rules. This is a PREVIEW ONLY — the score is "
        "NOT persisted to the database by this endpoint. Use "
        "PUT /sentiment/update-unsentimental or PUT /sentiment/update-all "
        "to persist scores."
    ),
    responses={
        404: {"description": "Headline with the given id does not exist."},
        500: {"description": "Unexpected server error."},
    },
)
def analyze_record(
    headline_id: int = Path(
        ..., gt=0, description="Primary key of the Headline record to analyze."
    ),
    db: Session = Depends(get_db),
    service: SentimentAnalyzer = Depends(_get_sentiment_service),
) -> AnalyzeRecordResponse:
    """
    Score a single stored Headline record by ID.

    The router only retrieves the ORM object; all text assembly and
    scoring logic lives in `service.analyze_record()`. A record with no
    usable text is not an error — it is returned with a null score.
    """
    record = _fetch_headline_or_404(db, headline_id)

    try:
        score = service.analyze_record(record)
    except Exception as exc:  # noqa: BLE001
        _handle_unexpected(exc, f"PUT /sentiment/analyze-record/{headline_id}")

    label = service._classify_score(score) if score is not None else None
    message = (
        "Headline sentiment analyzed successfully."
        if score is not None
        else "No meaningful text was found to analyze for this headline."
    )

    return AnalyzeRecordResponse(
        status="success",
        message=message,
        data=AnalyzeRecordData(
            id=record.id,
            headline=record.headline,
            sentiment_score=score,
            sentiment_label=label,
        ),
    )


# ---------------------------------------------------------------------------
# 3. Analyze batch (preview only)
# ---------------------------------------------------------------------------

@router.put(
    "/analyze-batch",
    response_model=AnalyzeBatchResponse,
    status_code=status.HTTP_200_OK,
    summary="Preview sentiment for a batch of stored headlines",
    description=(
        "Retrieves the requested Headline records and scores them via "
        "SentimentAnalyzer.analyze_batch(). This is a PREVIEW ONLY — no "
        "database writes occur beyond what the service itself performs "
        "(none, for this method). IDs that don't exist in the database are "
        "silently omitted from the result."
    ),
    responses={
        404: {"description": "None of the requested headline_ids were found."},
        500: {"description": "Unexpected server error."},
    },
)
def analyze_batch(
    body: AnalyzeBatchRequest,
    db: Session = Depends(get_db),
    service: SentimentAnalyzer = Depends(_get_sentiment_service),
) -> AnalyzeBatchResponse:
    """
    Preview sentiment for a batch of stored headlines by ID.

    The router only retrieves the ORM objects and formats the response;
    scoring is entirely owned by `service.analyze_batch()`.
    """
    records = _fetch_headlines_by_ids(db, body.headline_ids)

    try:
        results = service.analyze_batch(records)
    except Exception as exc:  # noqa: BLE001
        _handle_unexpected(exc, "PUT /sentiment/analyze-batch")

    items = [
        AnalyzeBatchItemResponse(
            id=item["id"],
            score=item["score"],
            sentiment_label=(
                service._classify_score(item["score"])
                if item["score"] is not None
                else None
            ),
            valid=item["valid"],
        )
        for item in results
    ]

    return AnalyzeBatchResponse(
        status="success",
        message=f"Batch sentiment preview completed for {len(items)} record(s).",
        data=items,
    )


# ---------------------------------------------------------------------------
# 4. Update only unsentimental headlines
# ---------------------------------------------------------------------------

@router.put(
    "/update-unsentimental",
    response_model=UpdateSentimentResponse,
    status_code=status.HTTP_200_OK,
    summary="Score and persist sentiment for unscored headlines",
    description=(
        "Runs SentimentAnalyzer.update_unsentimental_headlines(), scoring "
        "and persisting sentiment_score for every Headline where "
        "sentiment_score IS NULL. Processes in batches so memory stays flat "
        "regardless of table size."
    ),
    responses={500: {"description": "Unexpected server error."}},
)
def update_unsentimental(
    batch_size: int = Query(
        default=DEFAULT_BATCH_SIZE,
        ge=1,
        le=5000,
        description="Number of rows to fetch/process/commit per batch.",
    ),
    service: SentimentAnalyzer = Depends(_get_sentiment_service),
) -> UpdateSentimentResponse:
    """Persist sentiment scores for every currently unscored Headline row."""
    try:
        stats = service.update_unsentimental_headlines(batch_size=batch_size)
    except Exception as exc:  # noqa: BLE001
        _handle_unexpected(exc, "PUT /sentiment/update-unsentimental")

    return UpdateSentimentResponse(
        status="success",
        message=stats.get("message", "Sentiment update run completed."),
        data=_build_stats_data(stats),
    )


# ---------------------------------------------------------------------------
# 5. Update every headline
# ---------------------------------------------------------------------------

@router.put(
    "/update-all",
    response_model=UpdateSentimentResponse,
    status_code=status.HTTP_200_OK,
    summary="Recompute and persist sentiment for every headline",
    description=(
        "Runs SentimentAnalyzer.update_all_headlines(), recomputing and "
        "persisting sentiment_score for EVERY Headline row, regardless of "
        "whether it already has a score. Processes in batches via a keyset "
        "cursor."
    ),
    responses={500: {"description": "Unexpected server error."}},
)
def update_all(
    batch_size: int = Query(
        default=DEFAULT_BATCH_SIZE,
        ge=1,
        le=5000,
        description="Number of rows to fetch/process/commit per batch.",
    ),
    service: SentimentAnalyzer = Depends(_get_sentiment_service),
) -> UpdateSentimentResponse:
    """Recompute and persist sentiment scores for every Headline row."""
    try:
        stats = service.update_all_headlines(batch_size=batch_size)
    except Exception as exc:  # noqa: BLE001
        _handle_unexpected(exc, "PUT /sentiment/update-all")

    return UpdateSentimentResponse(
        status="success",
        message=stats.get("message", "Sentiment update run completed."),
        data=_build_stats_data(stats),
    )


# ---------------------------------------------------------------------------
# 6. Statistics
# ---------------------------------------------------------------------------

@router.get(
    "/statistics",
    response_model=StatisticsResponse,
    status_code=status.HTTP_200_OK,
    summary="Return statistics from the most recent sentiment update run",
    description=(
        "Returns SentimentAnalyzer.get_statistics() — the statistics from "
        "the most recently completed update_unsentimental_headlines() or "
        "update_all_headlines() run on this service instance. All counters "
        "are zero with status='no_data' if no run has completed yet."
    ),
    responses={500: {"description": "Unexpected server error."}},
)
def get_statistics(
    service: SentimentAnalyzer = Depends(_get_sentiment_service),
) -> StatisticsResponse:
    """Return the most recently computed sentiment run statistics."""
    try:
        stats = service.get_statistics()
    except Exception as exc:  # noqa: BLE001
        _handle_unexpected(exc, "GET /sentiment/statistics")

    return StatisticsResponse(
        status="success",
        message="Statistics retrieved successfully.",
        data=_build_stats_data(stats),
    )