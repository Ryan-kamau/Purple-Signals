"""
Alert scheduler: evaluates stored combined signals and maintains alerts.

Triggered by Windows Task Scheduler via scripts/run_alerts.bat.
Schedule it AFTER scripts/signal_scheduler.py (run_signals.bat) has succeeded.
Evaluation is idempotent and never resolves alerts on stale/missing data, so a
run against old signals is harmless.

- Up to 3 attempts if a run fails (failed tickers or a scheduler-level error).
- 5-minute delay between retries.
- Non-zero exit code if all attempts fail.
"""

import logging
import sys
import time

from database.session import SessionLocal
from services.alert_service.AlertEvaluationService import AlertEvaluationService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 300


def evaluate_alerts(db) -> None:
    result = AlertEvaluationService(db).evaluate()

    logger.info(
        "Alert evaluation complete: status=%s evaluated=%d created=%d updated=%d "
        "transitioned=%d resolved=%d unchanged=%d skipped=%d failed=%d",
        result["status"], result["evaluated"], result["created"], result["updated"],
        result["transitioned"], result["resolved"], result["unchanged"],
        result["skipped"], result["failed"],
    )
    for issue in result["errors"]:
        logger.warning("Alert issue | %s %s | %s | %s",
                       issue["ticker"], issue["trading_date"], issue["reason"], issue["detail"])

    if result["status"] == "failed" or result["failed"] > 0:
        raise RuntimeError(
            f"Alert evaluation reported {result['failed']} failed ticker(s), status={result['status']}"
        )


def main() -> int:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        logger.info("Alert evaluation started | attempt %d/%d", attempt, MAX_ATTEMPTS)
        db = None
        try:
            db = SessionLocal()
            evaluate_alerts(db)
            logger.info("Alert evaluation succeeded | attempt %d/%d", attempt, MAX_ATTEMPTS)
            return 0
        except Exception as exc:
            logger.exception("Alert evaluation failed | attempt %d/%d | Error: %s",
                             attempt, MAX_ATTEMPTS, exc)
        finally:
            if db is not None:
                try:
                    db.close()
                except Exception:
                    logger.exception("Failed to close the database session.")

        if attempt < MAX_ATTEMPTS:
            logger.warning("Retrying alert evaluation in %d seconds...", RETRY_DELAY_SECONDS)
            time.sleep(RETRY_DELAY_SECONDS)

    logger.error("Alert evaluation permanently failed after %d attempts.", MAX_ATTEMPTS)
    return 1


if __name__ == "__main__":
    sys.exit(main())