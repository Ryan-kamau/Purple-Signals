"""
intelligence/sentiment_analyzer.py

Production-grade sentiment analysis service for stored news headlines.

Pipeline position:

    Headline DB (MySQL)
        │
        ▼
    SentimentAnalyzer            <- this file
        │
        ├── text assembly (headline + description + content)
        ├── text validation / cleaning
        ├── VADER polarity scoring
        ├── custom weighted scoring
        │
        ▼
    Headline.sentiment_score  ->  MySQL (batched commits)

Responsibilities:
  - Read headlines from MySQL via an injected SQLAlchemy Session
  - Assemble analyzable text from headline / description / content
  - Validate and clean text (strip HTML, decode entities, normalise unicode)
  - Score text using VADER, converted into a custom weighted score
  - Persist sentiment_score in batches (commit per batch, not per row)
  - Isolate per-record failures so one bad row never aborts a batch
  - Return structured, JSON-serialisable execution statistics

This module intentionally contains:
  - NO FastAPI routes
  - NO session creation (the Session is always injected)
  - NO knowledge of HTTP, scraping, or the RSS/NewsAPI ingestion pipeline

Reusability:
    from intelligence.sentiment_analyzer import SentimentAnalyzer

    analyzer = SentimentAnalyzer(db)
    score = analyzer.analyze_headline("KPLC reports strong profit growth")

`analyze_headline()` performs zero database I/O, so other modules (alerts,
feature engineering, ad-hoc scripts) can reuse the scoring logic directly.

Future-proofing:
    The VADER-specific logic is isolated behind `_run_vader()` and
    `_compute_weighted_score()`. Swapping VADER for FinBERT / a HuggingFace
    transformer / an LLM later only requires replacing those two methods —
    every batching, persistence, validation, and statistics helper is
    engine-agnostic.
"""

from __future__ import annotations

import html
import logging
import re
import time
import unicodedata
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from models.headline_data import Headline

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tunable constants — no magic numbers in the method bodies below.
# ---------------------------------------------------------------------------

# Batch processing
DEFAULT_BATCH_SIZE: int = 250

# Minimum length (post-HTML-strip) for a text fragment to be considered
# meaningful natural language rather than a stub / placeholder / link.
MIN_TEXT_LENGTH: int = 3

# Custom weighted-score blend.
# weighted_score = (compound * COMPOUND_WEIGHT) + ((pos - neg) * POLARITY_WEIGHT)
# The two weights sum to 1.0 so the output stays within VADER's [-1, 1] range.
# Compound carries most of the signal (VADER's own normalised polarity);
# (pos - neg) is added as a secondary signal so headlines with strong but
# offsetting positive/negative language don't get flattened purely by
# compound's internal normalisation. Swap these two weights (or the whole
# formula) here — nothing else in the class needs to change.
COMPOUND_WEIGHT: float = 0.7
POLARITY_WEIGHT: float = 0.3

# Score interpretation bands (documented in the class docstring / PRD).
STRONG_POSITIVE_MIN: float = 0.50
MODERATE_POSITIVE_MIN: float = 0.20
STRICT_NEUTRAL_MIN: float = -0.19
STRICT_NEUTRAL_MAX: float = 0.19
MODERATE_NEGATIVE_MAX: float = -0.20
STRONG_NEGATIVE_MAX: float = -0.50

# Score labels — not persisted (the DB only stores the float), but exposed
# via _classify_score() / classify_score() for downstream modules (alerts,
# dashboards) that want a human-readable label.
LABEL_STRONG_POSITIVE: str = "Strong Positive"
LABEL_MODERATE_POSITIVE: str = "Moderate Positive"
LABEL_STRICT_NEUTRAL: str = "Strict Neutral"
LABEL_MODERATE_NEGATIVE: str = "Moderate Negative"
LABEL_STRONG_NEGATIVE: str = "Strong Negative"


# ---------------------------------------------------------------------------
# Compiled regex patterns — module level so they are compiled once, not on
# every call.
# ---------------------------------------------------------------------------

_HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
_WHITESPACE_PATTERN = re.compile(r"\s+")
_GOOGLE_REDIRECT_PATTERN = re.compile(r"https?://(?:www\.)?google\.[a-z.]+/url\?", re.IGNORECASE)
_GENERIC_URL_PATTERN = re.compile(r"https?://\S+")
_ALPHA_PATTERN = re.compile(r"[A-Za-z]")

# Known RSS / feed placeholder strings that carry no real sentiment signal.
# Matched against the FULLY STRIPPED field (case-insensitive, exact match).
_RSS_PLACEHOLDER_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"^\[removed\]$",
        r"^\[no description\]$",
        r"^no description available$",
        r"^read more\.*$",
        r"^click here\.*$",
        r"^n/?a$",
        r"^untitled$",
        r"^continue reading\.*$",
    )
]


class SentimentAnalyzer:
    """
    Reusable, DB-aware sentiment analysis service for the Headline model.

    Dependency injection mirrors the rest of the service layer
    (MacroService, MarketService, NewsQueryService): the caller owns the
    Session lifecycle, this class never opens or closes one.

    Example:
        db = SessionLocal()
        analyzer = SentimentAnalyzer(db)
        stats = analyzer.update_unsentimental_headlines(batch_size=500)
        print(stats["updated"], "headlines scored")
    """

    def __init__(self, db: Session) -> None:
        """
        Args:
            db: Active SQLAlchemy session (injected via FastAPI Depends or
                a caller-managed SessionLocal()). Never created internally.
        """
        self.db = db

        # VADER is expensive-ish to instantiate (loads its lexicon) — build
        # exactly once per analyzer instance, never inside a loop.
        self._vader = SentimentIntensityAnalyzer()

        self._stats: dict[str, Any] = {}
        self._reset_statistics()

    # ==================================================================
    # PUBLIC API — pure scoring (no database I/O)
    # ==================================================================

    def analyze_headline(self, text: str) -> Optional[float]:
        """
        Score an arbitrary piece of text end-to-end: validate -> clean ->
        VADER -> custom weighted score.

        This method touches the database in no way whatsoever, so any
        other module can import SentimentAnalyzer and call this directly
        for one-off scoring (e.g. previewing sentiment before ingestion).

        Args:
            text: Raw text to analyze. May be messy (HTML, entities, etc).

        Returns:
            Weighted sentiment score in [-1.0, 1.0], or None if the text
            is not meaningful natural language (empty, a bare link, an
            RSS placeholder, HTML-only, etc).
        """
        if not self._is_valid_text(text):
            return None

        cleaned = self._clean_text(text)

        if not self._is_valid_text(cleaned):
            # Cleaning can occasionally collapse a field down to nothing
            # (e.g. a string that was 100% HTML markup).
            return None

        return self._score_cleaned_text(cleaned)

    def analyze_record(self, record: Headline) -> Optional[float]:
        """
        Score a single Headline ORM object using the headline / description
        / content priority rules (headline is mandatory; description and
        content are appended only if they are independently usable).

        Args:
            record: A Headline ORM instance (does not need to be attached
                    to `self.db` — this method never queries or writes).

        Returns:
            Weighted sentiment score in [-1.0, 1.0], or None if no
            meaningful text could be assembled from the record.
        """
        combined_text = self._build_analysis_text(
            headline=record.headline,
            description=record.description,
            content=record.content,
        )

        if combined_text is None:
            return None

        # combined_text is already validated + cleaned by
        # _build_analysis_text(), so we score it directly instead of
        # re-running _is_valid_text()/_clean_text() a second time.
        return self._score_cleaned_text(combined_text)

    def analyze_batch(self, records: list[Headline]) -> list[dict[str, Any]]:
        """
        Score a list of Headline ORM objects without persisting anything.

        Useful for previewing sentiment on an arbitrary in-memory batch
        (e.g. freshly scraped records not yet committed to the DB).

        Args:
            records: List of Headline ORM instances.

        Returns:
            List of dicts, one per input record, in the same order:
                {"id": int | None, "score": float | None, "valid": bool}
            `valid` is False when no usable text could be found — callers
            should treat that as "skip", not "error".
        """
        results: list[dict[str, Any]] = []

        for record in records:
            score = self.analyze_record(record)
            results.append(
                {
                    "id": getattr(record, "id", None),
                    "score": score,
                    "valid": score is not None,
                }
            )

        return results

    # ==================================================================
    # PUBLIC API — database-backed batch update pipelines
    # ==================================================================

    def update_unsentimental_headlines(
        self, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> dict[str, Any]:
        """
        Score and persist sentiment for every Headline where
        `sentiment_score IS NULL`.

        Processes records in batches so memory usage stays flat regardless
        of table size (safe for 5,000–50,000+ rows). Because scored rows
        drop out of the `IS NULL` filter as soon as they're committed, this
        simply re-queries "the next batch of unscored rows" until none
        remain — no offset bookkeeping required.

        Args:
            batch_size: Rows to fetch/process/commit per batch.
                        Defaults to DEFAULT_BATCH_SIZE (250).

        Returns:
            Structured statistics dict — see `get_statistics()` for shape.
        """
        self._reset_statistics()
        start_time = time.monotonic()

        logger.info(
            "Sentiment backfill started: mode=unsentimental batch_size=%d",
            batch_size,
        )

        while True:
            batch = self._fetch_batch(batch_size, only_unsentimental=True)
            logger.info("Fetched %d rows", len(batch))
            if not batch:
                break

            self._process_batch(batch)

        self._stats["execution_time"] = round(time.monotonic() - start_time, 2)
        self._log_summary()

        return self._build_result()

    def update_all_headlines(
        self, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> dict[str, Any]:
        """
        Recompute and persist sentiment for EVERY Headline row, regardless
        of whether it already has a sentiment_score.

        Unlike `update_unsentimental_headlines()`, already-scored rows
        stay in the result set after a commit, so pagination is done via a
        keyset cursor (`id > last_seen_id`) rather than re-querying the
        same filter.

        Args:
            batch_size: Rows to fetch/process/commit per batch.
                        Defaults to DEFAULT_BATCH_SIZE (250).

        Returns:
            Structured statistics dict — see `get_statistics()` for shape.
        """
        self._reset_statistics()
        start_time = time.monotonic()

        logger.info(
            "Sentiment recalculation started: mode=all batch_size=%d",
            batch_size,
        )

        last_id: Optional[int] = None

        while True:
            batch = self._fetch_batch(
                batch_size, only_unsentimental=True, after_id=last_id
            )
            if not batch:
                break

            last_id = batch[-1].id
            self._process_batch(batch)

        self._stats["execution_time"] = round(time.monotonic() - start_time, 2)
        self._log_summary()

        return self._build_result()

    def get_statistics(self) -> dict[str, Any]:
        """
        Return the statistics from the most recently completed run of
        `update_unsentimental_headlines()` or `update_all_headlines()`.

        Returns:
            Structured statistics dict:
                {
                    "status": "success" | "completed_with_errors" | "no_data",
                    "message": str,
                    "processed": int,
                    "updated": int,
                    "skipped": int,
                    "failed": int,
                    "average_sentiment": float,
                    "execution_time": float,
                }
            If no run has completed yet, all counters are zero and
            status is "no_data".
        """
        return self._build_result()

    # ==================================================================
    # PRIVATE — text assembly
    # ==================================================================

    def _build_analysis_text(
        self,
        headline: Optional[str],
        description: Optional[str],
        content: Optional[str],
    ) -> Optional[str]:
        """
        Assemble analyzable text from a record's fields, in priority order.

        Rules:
            - headline is mandatory: if it isn't usable text, the whole
              record is considered unscorable (returns None).
            - description is appended only if independently usable.
            - content is appended only if independently usable.

        Args:
            headline:    Headline.headline value.
            description: Headline.description value (nullable).
            content:     Headline.content value (nullable).

        Returns:
            A single cleaned, whitespace-normalised string ready for
            VADER, or None if no usable text exists.
        """
        if not self._is_valid_text(headline):
            return None

        parts: list[str] = [self._clean_text(headline)]

        if description and self._is_valid_text(description):
            parts.append(self._clean_text(description))

        if content and self._is_valid_text(content):
            parts.append(self._clean_text(content))

        combined = self._collapse_whitespace(" ".join(parts))

        return combined or None

    # ==================================================================
    # PRIVATE — text validation
    # ==================================================================

    def _is_valid_text(self, text: Optional[str]) -> bool:
        """
        Determine whether `text` is meaningful natural language worth
        sending to VADER.

        Rejects: None, empty/whitespace-only strings, bare HTML anchors,
        Google redirect links with no surrounding words, known RSS
        placeholder strings, and anything that has no alphabetic content
        left after stripping HTML tags.

        This intentionally does NOT reject financial terminology, numbers,
        percentages, or currency symbols — only structurally unreadable
        fragments.

        Args:
            text: Raw or partially-cleaned text.

        Returns:
            True if the text is worth analyzing, False otherwise.
        """
        if not text:
            return False

        stripped = text.strip()

        if not stripped:
            return False

        # A field that is essentially just a Google News redirect URL
        # (with no other words) carries no sentiment signal.
        if _GOOGLE_REDIRECT_PATTERN.search(stripped):
            without_url = _GENERIC_URL_PATTERN.sub(" ", stripped).strip()
            if len(without_url) < MIN_TEXT_LENGTH:
                return False

        for pattern in _RSS_PLACEHOLDER_PATTERNS:
            if pattern.fullmatch(stripped):
                return False

        # Strip tags to see what visible text (if any) remains — catches
        # bare "<a href="...">...</a>" anchors and other markup-only cells.
        visible = _HTML_TAG_PATTERN.sub(" ", stripped)
        visible = self._collapse_whitespace(visible)

        if len(visible) < MIN_TEXT_LENGTH:
            return False

        if not _ALPHA_PATTERN.search(visible):
            return False

        return True

    # ==================================================================
    # PRIVATE — text cleaning
    # ==================================================================

    def _clean_text(self, text: str) -> str:
        """
        Clean raw text for sentiment analysis without altering financial
        wording (numbers, percentages, currency symbols, tickers, etc. are
        left untouched).

        Pipeline: decode HTML entities -> strip HTML tags -> normalise
        unicode -> collapse repeated whitespace -> strip.

        Args:
            text: Raw text.

        Returns:
            Cleaned text, safe to hand to VADER.
        """
        cleaned = self._decode_html_entities(text)
        cleaned = self._remove_html(cleaned)
        cleaned = self._normalize_unicode(cleaned)
        cleaned = self._collapse_whitespace(cleaned)
        return cleaned

    @staticmethod
    def _remove_html(text: str) -> str:
        """Strip HTML tags, replacing them with a space to avoid word-joins."""
        return _HTML_TAG_PATTERN.sub(" ", text)

    @staticmethod
    def _decode_html_entities(text: str) -> str:
        """Decode HTML entities, e.g. '&amp;' -> '&', '&#39;' -> "'"."""
        return html.unescape(text)

    @staticmethod
    def _normalize_unicode(text: str) -> str:
        """Normalise unicode to NFKC form (consistent quotes/dashes/etc)."""
        return unicodedata.normalize("NFKC", text)

    @staticmethod
    def _collapse_whitespace(text: str) -> str:
        """Collapse repeated whitespace/newlines/tabs into single spaces."""
        return _WHITESPACE_PATTERN.sub(" ", text).strip()

    # ==================================================================
    # PRIVATE — scoring
    # ==================================================================

    def _score_cleaned_text(self, cleaned_text: str) -> float:
        """
        Run VADER on already-cleaned, already-validated text and convert
        the result into the custom weighted score.

        Kept separate from `analyze_headline()` so batch code paths that
        have already cleaned/validated text (via `_build_analysis_text`)
        never pay for a second cleaning pass.

        Args:
            cleaned_text: Text that has already passed `_is_valid_text()`
                          and `_clean_text()`.

        Returns:
            Weighted sentiment score in [-1.0, 1.0].
        """
        raw_scores = self._run_vader(cleaned_text)
        return self._compute_weighted_score(raw_scores)

    def _run_vader(self, text: str) -> dict[str, float]:
        """
        Isolated VADER call — the only method that talks to the sentiment
        engine directly. Swapping VADER for FinBERT / a transformer model
        later means changing this method (and `_compute_weighted_score`)
        only; every other method in this class is engine-agnostic.

        Args:
            text: Cleaned text.

        Returns:
            VADER's raw polarity_scores dict: {"neg", "neu", "pos", "compound"}.
        """
        return self._vader.polarity_scores(text)

    @staticmethod
    def _compute_weighted_score(scores: dict[str, float]) -> float:
        """
        Convert VADER's raw output into a single custom weighted score,
        instead of persisting VADER's compound value as-is.

        Formula:
            weighted = (compound * COMPOUND_WEIGHT) + ((pos - neg) * POLARITY_WEIGHT)

        Rationale:
            - `compound` is VADER's own normalised polarity in [-1, 1] and
              carries the primary signal.
            - `(pos - neg)` is added as a secondary signal so headlines
              where positive and negative language coexist (common in
              financial reporting — e.g. "profit rises despite debt
              concerns") aren't purely governed by VADER's compound
              normalisation.
            - Weights sum to 1.0 so the result stays within [-1.0, 1.0].

        This is intentionally a small, swappable pure function — replace
        the formula here to change the weighting algorithm platform-wide
        without touching any batching/persistence code.

        Args:
            scores: VADER's polarity_scores() output.

        Returns:
            Weighted score rounded to 4 decimal places, clamped to
            [-1.0, 1.0].
        """
        compound = scores.get("compound", 0.0)
        positive = scores.get("pos", 0.0)
        negative = scores.get("neg", 0.0)

        weighted = (compound * COMPOUND_WEIGHT) + ((positive - negative) * POLARITY_WEIGHT)
        weighted = max(-1.0, min(1.0, weighted))

        return round(weighted, 4)

    @staticmethod
    def _classify_score(score: float) -> str:
        """
        Convert a weighted score into a human-readable label.

        Bands (documented constants at module level):
            0.50  to  1.00   -> Strong Positive
            0.20  to  0.49   -> Moderate Positive
           -0.19  to  0.19   -> Strict Neutral
           -0.20  to -0.49   -> Moderate Negative
           -0.50  to -1.00   -> Strong Negative

        Labels are NOT persisted to the database — this exists purely as a
        reusable helper for downstream modules (alerts, dashboards).

        Args:
            score: Weighted sentiment score.

        Returns:
            One of the LABEL_* constants.
        """
        if STRICT_NEUTRAL_MIN <= score <= STRICT_NEUTRAL_MAX:
            return LABEL_STRICT_NEUTRAL
        if score >= STRONG_POSITIVE_MIN:
            return LABEL_STRONG_POSITIVE
        if score >= MODERATE_POSITIVE_MIN:
            return LABEL_MODERATE_POSITIVE
        if score <= STRONG_NEGATIVE_MAX:
            return LABEL_STRONG_NEGATIVE
        if score <= MODERATE_NEGATIVE_MAX:
            return LABEL_MODERATE_NEGATIVE

        # Narrow gap between band boundaries (e.g. -0.195) — treat as
        # neutral rather than leaving it unclassified.
        return LABEL_STRICT_NEUTRAL

    # ==================================================================
    # PRIVATE — database access
    # ==================================================================

    def _fetch_batch(
        self,
        batch_size: int,
        only_unsentimental: bool,
        after_id: Optional[int] = None,
    ) -> list[Headline]:
        """
        Fetch one batch of Headline ORM objects using SQLAlchemy 2.x style
        (`select()` + `scalars()`), never loading the full table into
        memory.

        Args:
            batch_size:          Max rows to fetch.
            only_unsentimental:  If True, filter to sentiment_score IS NULL.
            after_id:            Keyset cursor for full-table recalculation
                                 (only used when only_unsentimental=False).

        Returns:
            List of Headline ORM objects, ordered by id ascending.
        """
        stmt = select(Headline)

        if only_unsentimental:
            stmt = stmt.where(Headline.sentiment_score.is_(None))

        if after_id is not None:
            stmt = stmt.where(Headline.id > after_id)

        stmt = stmt.order_by(Headline.id).limit(batch_size)

        return list(self.db.scalars(stmt).all())

    def _process_batch(self, records: list[Headline]) -> None:
        """
        Score and stage every record in a batch, then commit once.

        Each record is handled independently: an invalid-text record is
        skipped, an unexpected exception on one record is logged and
        counted as failed WITHOUT aborting the rest of the batch. Only a
        commit-level failure rolls back the whole batch (a genuine DB
        error, not a per-row scoring issue).

        Args:
            records: Batch of Headline ORM objects, already attached to
                     `self.db` (fetched via `_fetch_batch`).
        """
        logger.info("Batch started: size=%d", len(records))

        for record in records:
            self._stats["processed"] += 1
            self._update_record(record)

        self._commit_batch()

        logger.info(
            "Batch finished: processed=%d updated=%d skipped=%d failed=%d",
            self._stats["processed"],
            self._stats["updated"],
            self._stats["skipped"],
            self._stats["failed"],
        )

    def _update_record(self, record: Headline) -> None:
        """
        Score a single record and stage the result on the ORM object
        (does not commit — that happens once per batch in
        `_commit_batch()`).

        - Invalid/unreadable text -> counted as "skipped" (intentional,
          not an error).
        - Any unexpected exception -> counted as "failed", logged, and the
          record is left untouched so a partial/garbage value never lands
          in the database.

        Args:
            record: Headline ORM object to score and stage for update.
        """
        try:
            score = self.analyze_record(record)

            if score is None:
                self._stats["skipped"] += 1
                logger.debug(
                    "Skipped headline id=%s — no usable text.", record.id
                )
                return

            record.sentiment_score = score
            self._stats["updated"] += 1
            self._stats["score_sum"] += score

        except Exception as exc:  # noqa: BLE001
            self._stats["failed"] += 1
            logger.error(
                "Failed to score headline id=%s: %s", getattr(record, "id", "?"), exc
            )

    def _commit_batch(self) -> None:
        """
        Commit the current batch's staged changes.

        On failure, rolls back the batch (a commit-level failure is a
        genuine database error — e.g. connection loss — not a per-row
        scoring issue, so per-row skip/fail semantics don't apply here).
        """
        try:
            self.db.commit()
        except Exception as exc:  # noqa: BLE001
            self.db.rollback()
            logger.error("Batch commit failed — rolled back: %s", exc)

    # ==================================================================
    # PRIVATE — statistics
    # ==================================================================

    def _reset_statistics(self) -> None:
        """Reset internal run counters before starting a new update run."""
        self._stats = {
            "processed": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "score_sum": 0.0,
            "execution_time": 0.0,
        }

    def _build_result(self) -> dict[str, Any]:
        """
        Convert internal counters into the public statistics response
        shape described in the module docstring.

        Returns:
            {
                "status": "success" | "completed_with_errors" | "no_data",
                "message": str,
                "processed": int,
                "updated": int,
                "skipped": int,
                "failed": int,
                "average_sentiment": float,
                "execution_time": float,
            }
        """
        processed = self._stats["processed"]
        updated = self._stats["updated"]
        failed = self._stats["failed"]

        if processed == 0:
            status = "no_data"
            message = "No headlines matched the requested criteria."
        elif failed > 0:
            status = "completed_with_errors"
            message = f"Sentiment analysis completed with {failed} failure(s)."
        else:
            status = "success"
            message = "Sentiment analysis completed"

        average_sentiment = (
            round(self._stats["score_sum"] / updated, 4) if updated else 0.0
        )

        return {
            "status": status,
            "message": message,
            "processed": processed,
            "updated": updated,
            "skipped": self._stats["skipped"],
            "failed": failed,
            "average_sentiment": average_sentiment,
            "execution_time": self._stats["execution_time"],
        }

    def _log_summary(self) -> None:
        """Log the final statistics for a completed run at INFO level."""
        result = self._build_result()
        logger.info(
            "Sentiment run complete | status=%s processed=%d updated=%d "
            "skipped=%d failed=%d avg_sentiment=%.4f duration=%.2fs",
            result["status"],
            result["processed"],
            result["updated"],
            result["skipped"],
            result["failed"],
            result["average_sentiment"],
            result["execution_time"],
        )


# ---------------------------------------------------------------------------
# CLI smoke-test:  python -m intelligence.sentiment_analyzer
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    from database.session import SessionLocal

    db_session = SessionLocal()
    try:
        service = SentimentAnalyzer(db_session)

        preview_score = service.analyze_headline(
            "KPLC reports strong profit growth despite rising fuel costs"
        )
        print(f"\nPreview score: {preview_score}")
        if preview_score is not None:
            print(f"Preview label: {service._classify_score(preview_score)}")

        stats = service.update_all_headlines(300)
        print("\nRun statistics:")
        print(stats)
    finally:
        db_session.close()