"""Shared guard for destructive standalone MySQL testers."""

import os
from dotenv import dotenv_values, load_dotenv
from sqlalchemy.engine import make_url


def require_test_database() -> str:
    """Validate the test target before creating an engine or dropping tables."""
    load_dotenv()
    test_url = os.environ.get("TEST_DATABASE_URL")
    production_url = os.environ.get("DATABASE_URL") or dotenv_values().get("DATABASE_URL")

    if not test_url:
        raise RuntimeError("Refusing to run: TEST_DATABASE_URL must be configured.")
    if not production_url:
        raise RuntimeError("Refusing to run: DATABASE_URL is required for the safety comparison.")

    try:
        test = make_url(test_url)
        production = make_url(production_url)
    except Exception as exc:
        raise RuntimeError("Refusing to run: invalid test or production database URL.") from exc

    if test.get_backend_name() != "mysql" or not (test.database or "").lower().endswith("_test"):
        raise RuntimeError("Refusing to run: TEST_DATABASE_URL must target a MySQL database ending in _test.")

    # Compare parsed connection targets, not just raw strings (driver spelling may differ).
    def target(url):
        return (
            url.get_backend_name(),
            (url.host or "localhost").lower(),
            url.port or 3306,
            url.database or "",
        )

    if target(test) == target(production):
        raise RuntimeError("Refusing to run: test and production URLs target the same database.")

    return test_url