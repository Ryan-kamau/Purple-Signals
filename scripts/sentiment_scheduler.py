"""
Sentiment scheduler: scores every headline where sentiment_score IS NULL.
Triggered by Windows Task Scheduler via scripts/run_sentiment.bat.
Schedule it AFTER the ingestion job so it picks up fresh headlines.
"""

import logging
import sys

from database.session import SessionLocal
from intelligence.sentiment_analyzer import SentimentAnalyzer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> int:
    db = SessionLocal()
    try:
        stats = SentimentAnalyzer(db).update_unsentimental_headlines()
        logger.info(
            "Sentiment run complete: status=%s processed=%d updated=%d "
            "skipped=%d failed=%d avg=%.4f duration=%.2fs",
            stats["status"], stats["processed"], stats["updated"],
            stats["skipped"], stats["failed"],
            stats["average_sentiment"], stats["execution_time"],
        )
        return 0 if stats["failed"] == 0 else 1
    except Exception:
        logger.exception("Sentiment run failed")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())