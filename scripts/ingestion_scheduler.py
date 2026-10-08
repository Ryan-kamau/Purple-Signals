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
from services.feature_service.MarketcalculatorService import MarketCalculatorService

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

def calculate_features(db) -> None:
    result = MarketCalculatorService(db).process_all()
    logger.info(
        "Feature calculation complete: status=%s tickers=%d inserted=%d "
        "updated=%d skipped=%d",
        result["status"], result["tickers_processed"],
        result["rows_inserted"], result["rows_updated"], result["rows_skipped"],
    )
    if result["status"] == "failed":
        # Surface persistence failures so Task Scheduler shows a non-zero exit.
        raise RuntimeError(f"Feature calculation failed: {result['errors'][-1]}")

import time

def run_step(name: str, fn) -> bool:
    """Run a step with up to 3 attempts; failures never block the next step."""
    for attempt in range(1, 4):
        logger.info(
            "Step started: %s | attempt %d/3",
            name,
            attempt,
        )

        db = SessionLocal()
        try:
            fn(db)

            logger.info(
                "Step succeeded: %s | attempt %d/3",
                name,
                attempt,
            )
            return True

        except Exception as e:
            logger.exception(
                "Step failed: %s | attempt %d/3 | Error: %s",
                name,
                attempt,
                e,
            )

            if attempt < 3:
                logger.info(
                    "Retrying %s in 5 minutes...",
                    name,
                )
                time.sleep(300)

        finally:
            db.close()

    logger.error(
        "Step permanently failed after 3 attempts: %s",
        name,
    )
    return False


def main() -> int:
    ok = [
        run_step("market_refresh", refresh_market),
        run_step("rss_ingestion", ingest_rss),
        run_step("market_features", calculate_features),
    ]
    return 0 if all(ok) else 1


if __name__ == "__main__":
    sys.exit(main())