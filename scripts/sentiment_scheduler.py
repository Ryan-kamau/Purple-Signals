"""
Sentiment scheduler: scores every headline where sentiment_score IS NULL.
Triggered by Windows Task Scheduler via scripts/run_sentiment.bat.
Schedule it AFTER the ingestion job so it picks up fresh headlines.
"""

import logging
import sys
import time

from database.session import SessionLocal
from intelligence.sentiment_analyzer import SentimentAnalyzer
from services.feature_service import NewsFeatureService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> int:
    for attempt in range(1, 4):
        logger.info(
            "Sentiment run started | attempt %d/3",
            attempt,
        )

        db = SessionLocal()

        try:
            stats = SentimentAnalyzer(db).update_unsentimental_headlines()

            logger.info(
                "Sentiment run complete | attempt %d/3 | "
                "status=%s processed=%d updated=%d skipped=%d failed=%d "
                "avg=%.4f duration=%.2fs",
                attempt,
                stats["status"],
                stats["processed"],
                stats["updated"],
                stats["skipped"],
                stats["failed"],
                stats["average_sentiment"],
                stats["execution_time"],
            )

            from services.feature_service.NewsFeatureService import NewsFeatureService

            # replace: if stats["failed"] == 0: return 0
            if stats["failed"] == 0:
                result = NewsFeatureService(db).process_all()
                logger.info(
                    "News features complete: status=%s updated=%d skipped=%d",
                    result["status"], result["rows_updated"], result["rows_skipped"],
                )
                return 0 if result["status"] != "failed" else 1

            logger.error(
                "Sentiment run reported %d failed headlines | attempt %d/3",
                stats["failed"],
                attempt,
            )

        except Exception as e:
            logger.exception(
                "Sentiment run failed | attempt %d/3 | Error: %s",
                attempt,
                e,
            )

        finally:
            db.close()

        if attempt < 3:
            logger.info(
                "Retrying sentiment run in 5 minutes..."
            )
            time.sleep(300)

    logger.error(
        "Sentiment run permanently failed after 3 attempts"
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
