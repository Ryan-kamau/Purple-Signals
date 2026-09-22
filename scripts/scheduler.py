# scripts/run_market_refresh.py
"""
Standalone market data refresh — intended to be triggered by an external
scheduler (Windows Task Scheduler / cron), NOT run inside the FastAPI process.

Architectural decision: external scheduling was chosen over in-process
APScheduler so refreshes survive app restarts/crashes and this machine
can run the job even when the API server isn't up. Do not also enable
services.scheduler.start_scheduler() in main.py — pick one trigger source.
"""

import logging
import sys

from database.session import SessionLocal
from services.market_service import MarketService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> int:
    db = SessionLocal()
    try:
        result = MarketService(db).get_all_market_data()
        logger.info(
            "Market refresh complete: total=%d saved=%d skipped=%d source=%s",
            result.total, result.saved, result.skipped, result.source,
        )
        return 0
    except Exception:
        logger.exception("Market refresh failed")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())