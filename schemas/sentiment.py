# schemas/sentiment.py

"""
Pydantic schemas for the Sentiment Analysis API layer.

These models exist ONLY to shape HTTP request/response payloads for
api/sentiment.py. No scoring, validation, or business logic lives here —
that is entirely owned by intelligence.sentiment_analyzer.SentimentAnalyzer.

Every endpoint returns a typed Pydantic model, never a raw dict, and every
successful response follows the same envelope shape:

    {"status": "success", "message": "...", "data": {...} | [...]}
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

# Mirrors intelligence.sentiment_analyzer's LABEL_* constants. Duplicated
# here only as a typing/documentation aid for OpenAPI — the actual
# classification logic remains in SentimentAnalyzer._classify_score().
SentimentLabel = str  # e.g. "Strong Positive", "Moderate Negative", "Strict Neutral"


# ---------------------------------------------------------------------------
# 1. Analyze arbitrary text — PUT /sentiment/analyze
# ---------------------------------------------------------------------------

class AnalyzeHeadlineRequest(BaseModel):
    """Request body for scoring an arbitrary piece of text."""

    text: str = Field(
        ...,
        min_length=1,
        description="Raw text to analyze for sentiment (e.g. a headline or snippet).",
        examples=["KPLC reports record profits."],
    )


class AnalyzeHeadlineData(BaseModel):
    """Payload returned for a successfully scored piece of text."""

    text: str = Field(..., description="The original text that was analyzed.")
    sentiment_score: float = Field(
        ..., description="Weighted sentiment score in the range [-1.0, 1.0]."
    )
    sentiment_label: SentimentLabel = Field(
        ..., description="Human-readable classification of the sentiment score."
    )


class AnalyzeHeadlineResponse(BaseModel):
    """Response envelope for PUT /sentiment/analyze."""

    status: str = Field(default="success", examples=["success"])
    message: str = Field(..., examples=["Sentiment analysis completed successfully."])
    data: AnalyzeHeadlineData


# ---------------------------------------------------------------------------
# 2. Analyze one database record — PUT /sentiment/analyze-record/{headline_id}
# ---------------------------------------------------------------------------

class AnalyzeRecordData(BaseModel):
    """Payload returned for a single Headline record's sentiment analysis."""

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(..., description="Primary key of the Headline record.")
    headline: str = Field(..., description="The headline text that was analyzed.")
    sentiment_score: Optional[float] = Field(
        default=None,
        description="Weighted sentiment score in [-1.0, 1.0], or null if no usable text was found.",
    )
    sentiment_label: Optional[SentimentLabel] = Field(
        default=None,
        description="Human-readable sentiment classification, or null if unscored.",
    )


class AnalyzeRecordResponse(BaseModel):
    """Response envelope for PUT /sentiment/analyze-record/{headline_id}."""

    status: str = Field(default="success", examples=["success"])
    message: str = Field(..., examples=["Headline sentiment analyzed successfully."])
    data: AnalyzeRecordData


# ---------------------------------------------------------------------------
# 3. Analyze batch (preview only) — PUT /sentiment/analyze-batch
# ---------------------------------------------------------------------------

class AnalyzeBatchRequest(BaseModel):
    """Request body for previewing sentiment across a batch of headline IDs."""

    headline_ids: list[int] = Field(
        ...,
        min_length=1,
        description="List of Headline primary keys to preview sentiment for.",
        examples=[[101, 102, 103]],
    )


class AnalyzeBatchItemResponse(BaseModel):
    """Sentiment preview result for a single headline within a batch."""

    id: Optional[int] = Field(
        default=None, description="Primary key of the Headline record."
    )
    score: Optional[float] = Field(
        default=None,
        description="Weighted sentiment score in [-1.0, 1.0], or null if unscorable.",
    )
    sentiment_label: Optional[SentimentLabel] = Field(
        default=None,
        description="Human-readable sentiment classification, or null if unscored.",
    )
    valid: bool = Field(
        ..., description="False when no usable text could be found for this record."
    )


class AnalyzeBatchResponse(BaseModel):
    """Response envelope for PUT /sentiment/analyze-batch."""

    status: str = Field(default="success", examples=["success"])
    message: str = Field(..., examples=["Batch sentiment preview completed."])
    data: list[AnalyzeBatchItemResponse]


# ---------------------------------------------------------------------------
# 4 & 5. Update unsentimental / update all — PUT /sentiment/update-*
# ---------------------------------------------------------------------------

class UpdateSentimentStatsData(BaseModel):
    """
    Execution statistics returned by a database-backed sentiment update run.

    Mirrors the shape produced by
    SentimentAnalyzer._build_result() / get_statistics().
    """

    run_status: str = Field(
        ...,
        alias="status",
        description=(
            "Outcome of the run: 'success', 'completed_with_errors', or 'no_data'."
        ),
        examples=["success"],
    )
    processed: int = Field(..., description="Total headlines examined.")
    updated: int = Field(..., description="Headlines whose sentiment_score was persisted.")
    skipped: int = Field(
        ..., description="Headlines skipped due to no usable/meaningful text."
    )
    failed: int = Field(..., description="Headlines that raised an unexpected error.")
    average_sentiment: float = Field(
        ..., description="Average sentiment score across all updated headlines."
    )
    execution_time: float = Field(..., description="Wall-clock run duration, in seconds.")

    model_config = ConfigDict(populate_by_name=True)


class UpdateSentimentResponse(BaseModel):
    """Response envelope for PUT /sentiment/update-unsentimental and PUT /sentiment/update-all."""

    status: str = Field(default="success", examples=["success"])
    message: str = Field(..., examples=["Sentiment update run completed."])
    data: UpdateSentimentStatsData


# ---------------------------------------------------------------------------
# 6. Statistics — GET /sentiment/statistics
# ---------------------------------------------------------------------------

class StatisticsResponse(BaseModel):
    """Response envelope for GET /sentiment/statistics."""

    status: str = Field(default="success", examples=["success"])
    message: str = Field(..., examples=["Statistics retrieved successfully."])
    data: UpdateSentimentStatsData


# ---------------------------------------------------------------------------
# Generic error envelope
# ---------------------------------------------------------------------------

class ErrorResponse(BaseModel):
    """Standard error envelope returned alongside non-2xx HTTP status codes."""

    status: str = Field(default="error", examples=["error"])
    message: str = Field(..., examples=["Headline with id=999 was not found."])
    detail: Optional[str] = Field(
        default=None, description="Additional context about the failure, if available."
    )