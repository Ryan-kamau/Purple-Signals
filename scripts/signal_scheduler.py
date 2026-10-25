"""
Signal scheduler: generates and persists market analysis signals.

Triggered by Windows Task Scheduler via scripts/run_signal.bat.

Features:
- Up to 3 attempts if a run fails.
- 5-minute delay between retries.
- Detailed logging for successful and failed runs.
- Proper database session cleanup.
- Non-zero exit code if all attempts fail.
"""

import logging
import sys
import time

from database.session import SessionLocal
from services.signal_service import SignalService


# ------------------------------------------------------------------
# Logging configuration
# ------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 300


# ------------------------------------------------------------------
# Main processing
# ------------------------------------------------------------------

def generate_signals(db) -> None:
    """
    Generate signals using the signal-generation service.

    Raises:
        RuntimeError: If the service reports a failed run.
    """

    result = SignalService(db).generate_signals()

    # Log structured results if the service returns a dictionary.
    if isinstance(result, dict):

        logger.info(
            "Signal generation complete: "
            "status=%s generated=%s updated=%s skipped=%s failed=%s",
            result.get("status", "unknown"),
            result.get("generated", 0),
            result.get("updated", 0),
            result.get("skipped", 0),
            result.get("failed", 0),
        )

        # Treat reported failures as a failed scheduler run.
        if result.get("status") == "failed":
            raise RuntimeError(
                f"Signal generation failed: "
                f"{result.get('errors', ['No error details provided'])}"
            )

        # A service may report individual failures without marking
        # the entire run as failed.
        if result.get("failed", 0) > 0:
            raise RuntimeError(
                f"Signal generation reported "
                f"{result['failed']} failed operations."
            )

    else:
        # Support services that return None or another result object.
        logger.info(
            "Signal generation completed. "
            "The service did not return a statistics dictionary."
        )


# ------------------------------------------------------------------
# Retry handler
# ------------------------------------------------------------------

def main() -> int:
    """
    Execute signal generation with retries.

    Returns:
        0: Successful run.
        1: All attempts failed.
    """

    for attempt in range(1, MAX_ATTEMPTS + 1):

        logger.info(
            "Signal generation started | attempt %d/%d",
            attempt,
            MAX_ATTEMPTS,
        )

        db = None

        try:
            # Create a fresh session for every attempt.
            db = SessionLocal()

            generate_signals(db)

            logger.info(
                "Signal generation succeeded | attempt %d/%d",
                attempt,
                MAX_ATTEMPTS,
            )

            return 0

        except Exception as exc:
            logger.exception(
                "Signal generation failed | attempt %d/%d | Error: %s",
                attempt,
                MAX_ATTEMPTS,
                exc,
            )

        finally:
            if db is not None:
                try:
                    db.close()

                except Exception:
                    logger.exception(
                        "Failed to close the database session."
                    )

        # Wait before retrying, but not after the final attempt.
        if attempt < MAX_ATTEMPTS:
            logger.warning(
                "Retrying signal generation in %d seconds...",
                RETRY_DELAY_SECONDS,
            )

            time.sleep(RETRY_DELAY_SECONDS)

    logger.error(
        "Signal generation permanently failed after %d attempts.",
        MAX_ATTEMPTS,
    )

    return 1


if __name__ == "__main__":
    sys.exit(main())
