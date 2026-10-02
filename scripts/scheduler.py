# scripts/scheduler.py
"""
Standalone daily pipeline — intended to be triggered by an external
scheduler (Windows Task Scheduler / cron), NOT run inside the FastAPI process.

Pipeline (sequential, each step isolated):
    1. Market refresh      MarketService.get_all_market_data()
    2. RSS ingestion       RSSNewsIngestor.ingest_all_feeds()
    3. Sentiment scoring   SentimentAnalyzer.update_unsentimental_headlines()

Sentiment runs after RSS so it scores the freshly ingested headlines.

Architectural decisions (keep following these):
  - External scheduling was chosen over in-process APScheduler so refreshes
    survive app restarts/crashes and run even when the API server isn't up.
    Do not also enable an in-process scheduler in main.py.
  - Every step is independent: own DB session, own try/except. A failure in
    one step is logged and the pipeline continues to the next.
  - Adding a future step (features, alerts) = one run_*_step() function that
    returns a result dict, plus one entry in STEPS.

Run from the project root:
    python -m scripts.scheduler

Exit code: 0 if no step failed, 1 if any step failed.
A step reporting "warning" (partial problems) does not fail the run.
"""

import logging
import sys
import time
from typing import Any, Callable

from database.session import SessionLocal
from scrapers.rss_news import RSSNewsIngestor
from services.market_service import MarketService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger(__name__)

# Step status values
SUCCESS = "success"
WARNING = "warning"
FAILED = "failed"


# ---------------------------------------------------------------------------
# Steps — each opens/closes its own session and returns a result dict with
# at least a "status" key. Exceptions are handled by _run_step().
# ---------------------------------------------------------------------------

def run_market_step() -> dict[str, Any]:
    """Step 1: fetch and persist the latest NSE market snapshot."""
    db = SessionLocal()
    try:
        result = MarketService(db).get_all_market_data()
        logger.info(
            "Market refresh: total=%d saved=%d skipped=%d source=%s",
            result.total, result.saved, result.skipped, result.source,
        )
        # Nothing saved despite records being fetched means the step
        # effectively did no useful work.
        status = FAILED if (result.total > 0 and result.saved == 0) else SUCCESS
        return {
            "status": status,
            "total": result.total,
            "saved": result.saved,
            "skipped": result.skipped,
            "source": result.source,
        }
    finally:
        db.close()


def run_news_step() -> dict[str, Any]:
    """Step 2: ingest every default RSS feed, enrich, dedupe, and store."""
    db = SessionLocal()
    try:
        results = RSSNewsIngestor.ingest_all_feeds(session=db)

        feeds_run = len(results)
        feeds_succeeded = sum(1 for r in results if r.success)
        feeds_failed = feeds_run - feeds_succeeded
        total_saved = sum(r.saved for r in results)
        total_duplicates = sum(r.duplicates for r in results)
        fallback_feeds = sum(1 for r in results if r.fallback_used)

        logger.info(
            "RSS ingestion: feeds_run=%d succeeded=%d failed=%d "
            "saved=%d duplicates=%d fallback_feeds=%d",
            feeds_run, feeds_succeeded, feeds_failed,
            total_saved, total_duplicates, fallback_feeds,
        )

        if feeds_run == 0 or feeds_failed == feeds_run:
            status = FAILED
        elif feeds_failed or fallback_feeds:
            # A feed that fell back returns success=True but delivered no
            # real articles, so it is counted as a warning here.
            status = WARNING
        else:
            status = SUCCESS

        return {
            "status": status,
            "feeds_run": feeds_run,
            "feeds_succeeded": feeds_succeeded,
            "feeds_failed": feeds_failed,
            "total_saved": total_saved,
            "total_duplicates": total_duplicates,
            "fallback_feeds": fallback_feeds,
        }
    finally:
        db.close()


def run_sentiment_step() -> dict[str, Any]:
    """Step 3: score every headline that has no sentiment_score yet."""
    # Lazy import: SentimentAnalyzer pulls in transformers and loads FinBERT
    # in its constructor. Keep that cost out of module import time.
    from intelligence.sentiment_analyzer import SentimentAnalyzer

    db = SessionLocal()
    try:
        stats = SentimentAnalyzer(db).update_unsentimental_headlines()

        logger.info(
            "Sentiment scoring: processed=%d updated=%d skipped=%d failed=%d "
            "avg_sentiment=%.4f execution_time=%.2fs",
            stats["processed"], stats["updated"], stats["skipped"],
            stats["failed"], stats["average_sentiment"], stats["execution_time"],
        )

        # no_data = nothing to score, which is fine. completed_with_errors
        # means some rows failed but the run finished.
        status = WARNING if stats["status"] == "completed_with_errors" else SUCCESS
        return {
            "status": status,
            "processed": stats["processed"],
            "updated": stats["updated"],
            "skipped": stats["skipped"],
            "failed": stats["failed"],
            "average_sentiment": stats["average_sentiment"],
            "execution_time": stats["execution_time"],
        }
    finally:
        db.close()


# Ordered pipeline. Add future steps here.
STEPS: list[tuple[str, Callable[[], dict[str, Any]]]] = [
    ("market", run_market_step),
    ("news", run_news_step),
    ("sentiment", run_sentiment_step),
]


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _run_step(name: str, step: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    """Run one step in isolation: time it, and never let it raise."""
    logger.info("=== Step started: %s ===", name)
    start = time.monotonic()

    try:
        result = step()
    except Exception:
        logger.exception("Step '%s' raised an unhandled exception", name)
        result = {"status": FAILED}

    result["duration"] = round(time.monotonic() - start, 2)
    logger.info(
        "=== Step finished: %s | status=%s | %.2fs ===",
        name, result["status"], result["duration"],
    )
    return result


def main() -> int:
    run_start = time.monotonic()
    logger.info("Daily pipeline started")

    results = {name: _run_step(name, step) for name, step in STEPS}

    total_duration = round(time.monotonic() - run_start, 2)
    summary = " ".join(
        f"{name}={res['status']}({res['duration']}s)" for name, res in results.items()
    )
    logger.info("Daily pipeline finished in %.2fs | %s", total_duration, summary)

    return 1 if any(res["status"] == FAILED for res in results.values()) else 0


if __name__ == "__main__":
    sys.exit(main())