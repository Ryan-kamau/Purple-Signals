"""
app/main.py

FastAPI application entry point.

Startup sequence:
  1. Initialise the database (create tables if missing).
  2. Register all routers.
  3. Expose a health-check endpoint.

Run locally:
    uvicorn app.main:app --reload --port 8000
"""

import logging

from fastapi import FastAPI

from api import market
from database.session import init_db

# ---------------------------------------------------------------------------
# Logging — structured, one-liner format good for terminal and future log
# aggregators (Loki, CloudWatch, etc.)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="KPLC Intelligence Engine",
    description=(
        "Terminal-first financial intelligence system for monitoring "
        "NSE stocks — starting with KPLC."
    ),
    version="0.1.0",
)


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
@app.on_event("startup")
def on_startup() -> None:
    logger.info("Initialising database…")
    init_db()
    logger.info("Database ready.")


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(market.router)


# ---------------------------------------------------------------------------
# Health check — useful for Docker / k8s probes and quick smoke tests.
# ---------------------------------------------------------------------------
@app.get("/health", tags=["System"])
def health():
    return {"status": "ok", "service": "KPLC Intelligence Engine"}