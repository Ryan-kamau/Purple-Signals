"""
Ingestion scheduler: market data refresh + RSS headline ingestion.
Triggered by Windows Task Scheduler via scripts/run_ingestion.bat.
Sentiment scoring lives in scripts/sentiment_scheduler.py.
"""

import logging
import sys

from database.session import SessionLocal
from scrapers.rss_news import RSSNewsIngestor
from services.market_service import MarketService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger(__name__)


def refresh_market(db) -> None:
    result = MarketService(db).get_all_market_data()
    logger.info(
        "Market refresh complete: total=%d saved=%d skipped=%d source=%s",
        result.total, result.saved, result.skipped, result.source,
    )


def ingest_rss(db) -> None:
    results = RSSNewsIngestor.ingest_all_feeds(session=db)
    logger.info(
        "RSS ingestion complete: feeds=%d fetched=%d saved=%d failed=%d",
        len(results),
        sum(r.fetched for r in results),
        sum(r.saved for r in results),
        sum(1 for r in results if not r.success),
    )


def run_step(name: str, fn) -> bool:
    """Run one step in its own session; a failure never blocks the next step."""
    logger.info("Step started: %s", name)
    db = SessionLocal()
    try:
        fn(db)
        return True
    except Exception:
        logger.exception("Step failed: %s", name)
        return False
    finally:
        db.close()


def main() -> int:
    ok = [
        run_step("market_refresh", refresh_market),
        run_step("rss_ingestion", ingest_rss),
    ]
    return 0 if all(ok) else 1


if __name__ == "__main__":
    sys.exit(main())