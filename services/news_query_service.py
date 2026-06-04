"""
services/news_query_service.py

Read-only query service for stored news intelligence.

Pipeline position:
    DB  →  NewsQueryService  →  API Router  →  Client

This service ONLY reads from the database.  It does NOT:
  - Call NewsAPI or any external data source
  - Perform scraping
  - Store or update records
  - Enrich articles with keywords or sentiment
  - Contain FastAPI route logic

All methods accept a SQLAlchemy Session injected by the caller (typically
via FastAPI Depends(get_db)) and return Headline ORM objects that the router
layer converts into HeadlineResponse Pydantic schemas.

The Headline model is resolved lazily so this module stays importable even
before all project dependencies are wired up.
"""

import importlib
import logging
from typing import Any, Optional

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy model resolution helpers
# The Headline ORM class lives in models/headline_data.py.  We resolve it
# once and cache it so every method doesn't repeat the import dance.
# ---------------------------------------------------------------------------

_HEADLINE_MODEL_CANDIDATES = (
    "models.headline_data",
    "models.news",
    "models",
    "db.models",
)

_headline_model_cache: Any = None


def _get_headline_model() -> Any:
    """
    Resolve the Headline ORM class lazily and cache it.

    Tries each path in _HEADLINE_MODEL_CANDIDATES in order.

    Returns:
        Headline SQLAlchemy model class.

    Raises:
        ImportError: If the model cannot be found in any candidate module.
    """
    global _headline_model_cache  # noqa: PLW0603

    if _headline_model_cache is not None:
        return _headline_model_cache

    for module_path in _HEADLINE_MODEL_CANDIDATES:
        try:
            module = importlib.import_module(module_path)
        except ModuleNotFoundError:
            continue

        if hasattr(module, "Headline"):
            _headline_model_cache = module.Headline
            logger.debug("Resolved Headline model from %s", module_path)
            return _headline_model_cache

    raise ImportError(
        "Could not resolve the Headline ORM model. "
        f"Tried: {', '.join(_HEADLINE_MODEL_CANDIDATES)}. "
        "Pass it explicitly or adjust _HEADLINE_MODEL_CANDIDATES."
    )


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_pagination(page: int, page_size: int) -> None:
    """
    Validate standard pagination parameters.

    Args:
        page:      Current page number. Must be >= 1.
        page_size: Records per page. Must be between 1 and 100 inclusive.

    Raises:
        ValueError: If any parameter is out of range.
    """
    if page < 1:
        raise ValueError(f"page must be >= 1, got {page}.")
    if page_size < 1:
        raise ValueError(f"page_size must be >= 1, got {page_size}.")
    if page_size > 100:
        raise ValueError(f"page_size must be <= 100, got {page_size}.")


def _validate_limit(limit: int) -> None:
    """
    Validate a limit parameter.

    Args:
        limit: Maximum records to return. Must be >= 1.

    Raises:
        ValueError: If limit is less than 1.
    """
    if limit < 1:
        raise ValueError(f"limit must be >= 1, got {limit}.")


# ---------------------------------------------------------------------------
# NSE-relevant category constants
# Centralised here so they stay in sync with the KeywordEngine categories
# and don't need to be duplicated across methods.
# ---------------------------------------------------------------------------

NSE_RELEVANT_CATEGORIES = (
    "macro_economy",
    "energy_sector",
    "geopolitics",
    "market_stress",
    "kenya_policy",
)

NSE_MIN_IMPACT_SCORE = 1


# ---------------------------------------------------------------------------
# Service class
# ---------------------------------------------------------------------------

class NewsQueryService:
    """
    Read-only query service for stored Headline intelligence records.

    All methods delegate pagination, filtering, and sorting to SQLAlchemy
    and return raw Headline ORM objects.  The router layer is responsible
    for schema conversion (ORM → HeadlineResponse).

    Design principles:
      - Zero external I/O: only reads from the injected Session.
      - Every public method validates its inputs before touching the DB.
      - Private helpers eliminate duplicated filtering logic.
      - The Headline model is resolved lazily so this module is importable
        before the ORM layer is fully configured.

    Example usage (in a FastAPI route)::

        from services.news_query_service import NewsQueryService

        service = NewsQueryService()

        @router.get("/news")
        def get_news(db: Session = Depends(get_db)):
            return service.get_news(db, keyword="fuel", page=1, page_size=20)
    """

    # ------------------------------------------------------------------
    # Public query methods
    # ------------------------------------------------------------------

    def get_news(
        self,
        db_session: Session,
        *,
        keyword: Optional[str] = None,
        category: Optional[str] = None,
        source: Optional[str] = None,
        min_impact_score: Optional[int] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> list[Any]:
        """
        General-purpose news search with optional filtering and pagination.

        All filters are additive (AND logic).  If no filters are supplied
        the method returns all headlines paginated by recency.

        Args:
            db_session:       Active SQLAlchemy session.
            keyword:          Case-insensitive substring match against
                              ``keywords_detected``.
                              Example: "fuel", "interest rates".
            category:         Case-insensitive substring match against
                              ``categories``.
                              Example: "energy_sector", "macro_economy".
            source:           Case-insensitive exact-or-contains match
                              against ``source``.
                              Example: "Reuters", "Business Daily".
            min_impact_score: Return only records where
                              ``impact_score >= min_impact_score``.
            page:             Page number, 1-indexed. Defaults to 1.
            page_size:        Records per page. 1–100. Defaults to 20.

        Returns:
            Paginated list of Headline ORM objects, sorted by
            ``published_at DESC``.

        Raises:
            ValueError: If pagination parameters are invalid.
        """
        _validate_pagination(page, page_size)

        Headline = _get_headline_model()
        query = db_session.query(Headline)

        query = self._apply_keyword_filter(query, Headline, keyword)
        query = self._apply_category_filter(query, Headline, category)
        query = self._apply_source_filter(query, Headline, source)
        query = self._apply_min_impact_filter(query, Headline, min_impact_score)

        query = query.order_by(Headline.published_at.desc())
        query = self._apply_pagination(query, page, page_size)

        results = query.all()
        logger.debug(
            "get_news: keyword=%r category=%r source=%r min_impact=%r "
            "page=%d page_size=%d → %d results",
            keyword, category, source, min_impact_score, page, page_size, len(results),
        )
        return results

    def get_latest_news(
        self,
        db_session: Session,
        *,
        limit: int = 10,
    ) -> list[Any]:
        """
        Return the most recently published headlines.

        Args:
            db_session: Active SQLAlchemy session.
            limit:      Maximum records to return. 1–100. Defaults to 10.

        Returns:
            List of Headline ORM objects sorted by ``published_at DESC``.

        Raises:
            ValueError: If limit is less than 1.
        """
        _validate_limit(limit)

        Headline = _get_headline_model()
        results = (
            db_session.query(Headline)
            .order_by(Headline.published_at.desc())
            .limit(limit)
            .all()
        )

        logger.debug("get_latest_news: limit=%d → %d results", limit, len(results))
        return results

    def get_high_impact_news(
        self,
        db_session: Session,
        *,
        limit: int = 20,
    ) -> list[Any]:
        """
        Return headlines ranked by intelligence impact score.

        High impact scores indicate articles that triggered multiple
        high-weight keywords (e.g. "war", "oil", "interest rates").

        Args:
            db_session: Active SQLAlchemy session.
            limit:      Maximum records to return. Defaults to 20.

        Returns:
            List of Headline ORM objects sorted by
            ``impact_score DESC, published_at DESC``.

        Raises:
            ValueError: If limit is less than 1.
        """
        _validate_limit(limit)

        Headline = _get_headline_model()
        results = (
            db_session.query(Headline)
            .order_by(
                Headline.impact_score.desc(),
                Headline.published_at.desc(),
            )
            .limit(limit)
            .all()
        )

        logger.debug("get_high_impact_news: limit=%d → %d results", limit, len(results))
        return results

    def get_news_by_source(
        self,
        db_session: Session,
        source: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> list[Any]:
        """
        Return all headlines published by a specific source.

        Args:
            db_session: Active SQLAlchemy session.
            source:     Source name, e.g. "Reuters", "Business Daily Africa".
                        Matched case-insensitively via LIKE.
            page:       Page number, 1-indexed.
            page_size:  Records per page. 1–100.

        Returns:
            Paginated list of Headline ORM objects sorted by
            ``published_at DESC``.

        Raises:
            ValueError: If source is empty or pagination parameters are invalid.
        """
        if not source or not source.strip():
            raise ValueError("source must be a non-empty string.")

        _validate_pagination(page, page_size)

        Headline = _get_headline_model()
        query = (
            db_session.query(Headline)
            .filter(Headline.source.ilike(f"%{source.strip()}%"))
            .order_by(Headline.published_at.desc())
        )
        query = self._apply_pagination(query, page, page_size)

        results = query.all()
        logger.debug(
            "get_news_by_source: source=%r page=%d → %d results",
            source, page, len(results),
        )
        return results

    def get_news_by_category(
        self,
        db_session: Session,
        category: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> list[Any]:
        """
        Return headlines tagged with a specific intelligence category.

        Categories are stored as comma-separated strings in the ``categories``
        column by NewsService (e.g. ``"energy_sector, geopolitics"``).

        Supported categories (from KeywordEngine):
            - macro_economy
            - energy_sector
            - geopolitics
            - market_stress
            - kenya_policy

        Args:
            db_session: Active SQLAlchemy session.
            category:   Category name. Matched case-insensitively via LIKE.
            page:       Page number, 1-indexed.
            page_size:  Records per page. 1–100.

        Returns:
            Paginated list of Headline ORM objects sorted by
            ``impact_score DESC, published_at DESC``.

        Raises:
            ValueError: If category is empty or pagination parameters are invalid.
        """
        if not category or not category.strip():
            raise ValueError("category must be a non-empty string.")

        _validate_pagination(page, page_size)

        Headline = _get_headline_model()
        query = (
            db_session.query(Headline)
            .filter(Headline.categories.ilike(f"%{category.strip()}%"))
            .order_by(
                Headline.impact_score.desc(),
                Headline.published_at.desc(),
            )
        )
        query = self._apply_pagination(query, page, page_size)

        results = query.all()
        logger.debug(
            "get_news_by_category: category=%r page=%d → %d results",
            category, page, len(results),
        )
        return results

    def get_news_by_keyword(
        self,
        db_session: Session,
        keyword: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> list[Any]:
        """
        Return headlines that matched a specific financial keyword during
        ingestion.

        Keywords are stored as comma-separated strings in the
        ``keywords_detected`` column (e.g. ``"fuel, oil, tariff"``).

        Example keywords (from KeywordEngine.KEYWORDS):
            inflation, fuel, interest rates, debt, tax, imf, cbk,
            electricity, tariff, epra, subsidy, power outage, war,
            sanctions, oil, middle east, losses, profit warning, crisis,
            treasury, parliament, finance bill, budget, kra, ministry

        Args:
            db_session: Active SQLAlchemy session.
            keyword:    Keyword string. Matched case-insensitively via LIKE.
            page:       Page number, 1-indexed.
            page_size:  Records per page. 1–100.

        Returns:
            Paginated list of Headline ORM objects sorted by
            ``impact_score DESC, published_at DESC``.

        Raises:
            ValueError: If keyword is empty or pagination parameters are invalid.
        """
        if not keyword or not keyword.strip():
            raise ValueError("keyword must be a non-empty string.")

        _validate_pagination(page, page_size)

        Headline = _get_headline_model()
        query = (
            db_session.query(Headline)
            .filter(Headline.keywords_detected.ilike(f"%{keyword.strip()}%"))
            .order_by(
                Headline.impact_score.desc(),
                Headline.published_at.desc(),
            )
        )
        query = self._apply_pagination(query, page, page_size)

        results = query.all()
        logger.debug(
            "get_news_by_keyword: keyword=%r page=%d → %d results",
            keyword, page, len(results),
        )
        return results

    def get_nse_relevant_news(
        self,
        db_session: Session,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> list[Any]:
        """
        Return headlines most likely to affect Nairobi Securities Exchange stocks.

        A headline is considered NSE-relevant if it meets EITHER condition:
          - ``impact_score >= NSE_MIN_IMPACT_SCORE`` (currently 5), OR
          - ``categories`` contains any of: macro_economy, energy_sector,
            geopolitics, market_stress, kenya_policy.

        This broad filter intentionally errs on the side of inclusion —
        a low-scoring article about EPRA tariffs is still NSE-relevant even
        if its weighted impact is below the threshold.

        Results are sorted by impact_score DESC, then published_at DESC so
        the most actionable intelligence surfaces first.

        Args:
            db_session: Active SQLAlchemy session.
            page:       Page number, 1-indexed.
            page_size:  Records per page. 1–100.

        Returns:
            Paginated list of NSE-relevant Headline ORM objects.

        Raises:
            ValueError: If pagination parameters are invalid.
        """
        _validate_pagination(page, page_size)

        Headline = _get_headline_model()

        # Build the category OR conditions dynamically from the constant
        # so adding a new NSE_RELEVANT_CATEGORIES entry auto-expands this query.
        from sqlalchemy import or_

        category_conditions = [
            Headline.categories.ilike(f"%{cat}%")
            for cat in NSE_RELEVANT_CATEGORIES
        ]

        query = (
            db_session.query(Headline)
            .filter(
                or_(
                    Headline.impact_score >= NSE_MIN_IMPACT_SCORE,
                    *category_conditions,
                )
            )
            .order_by(
                Headline.impact_score.desc(),
                Headline.published_at.desc(),
            )
        )
        query = self._apply_pagination(query, page, page_size)

        results = query.all()
        logger.debug(
            "get_nse_relevant_news: page=%d page_size=%d → %d results",
            page, page_size, len(results),
        )
        return results

    def get_sources(self, db_session: Session) -> list[str]:
        """
        Return a sorted list of all distinct news sources in the database.

        Useful for populating filter dropdowns in the dashboard and for
        understanding which outlets are feeding the intelligence pipeline.

        Args:
            db_session: Active SQLAlchemy session.

        Returns:
            Alphabetically sorted list of unique source strings.
            Example: ["BBC News", "Business Daily", "CNBC", "Reuters"]
        """
        Headline = _get_headline_model()

        rows = (
            db_session.query(Headline.source)
            .distinct()
            .filter(Headline.source.isnot(None))
            .order_by(Headline.source.asc())
            .all()
        )

        sources = sorted(
            {row[0].strip() for row in rows if row[0] and row[0].strip()}
        )

        logger.debug("get_sources: returned %d distinct sources", len(sources))
        return sources

    def get_categories(self, db_session: Session) -> list[str]:
        """
        Return all unique intelligence categories present in stored records.

        The ``categories`` column stores comma-separated strings produced by
        KeywordEngine (e.g. ``"energy_sector, geopolitics"``).  This method
        unpacks those strings into a flat, deduplicated, sorted list.

        Args:
            db_session: Active SQLAlchemy session.

        Returns:
            Alphabetically sorted list of unique category strings.
            Example: ["energy_sector", "geopolitics", "kenya_policy", "macro_economy"]
        """
        Headline = _get_headline_model()

        rows = (
            db_session.query(Headline.categories)
            .filter(Headline.categories.isnot(None))
            .all()
        )

        categories = self._unpack_comma_column(rows)
        logger.debug("get_categories: returned %d distinct categories", len(categories))
        return categories

    def get_keywords(self, db_session: Session) -> list[str]:
        """
        Return all unique financial keywords present in stored records.

        The ``keywords_detected`` column stores comma-separated strings
        produced by KeywordEngine (e.g. ``"fuel, oil, tariff"``).  This
        method unpacks those strings into a flat, deduplicated, sorted list.

        Useful for:
          - Populating filter autocomplete in the dashboard
          - Auditing which keywords are actually firing
          - Building keyword frequency analytics

        Args:
            db_session: Active SQLAlchemy session.

        Returns:
            Alphabetically sorted list of unique keyword strings.
            Example: ["budget", "cbk", "debt", "electricity", "fuel", ...]
        """
        Headline = _get_headline_model()

        rows = (
            db_session.query(Headline.keywords_detected)
            .filter(Headline.keywords_detected.isnot(None))
            .all()
        )

        keywords = self._unpack_comma_column(rows)
        logger.debug("get_keywords: returned %d distinct keywords", len(keywords))
        return keywords

    # ------------------------------------------------------------------
    # Private filtering helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_keyword_filter(query: Any, Headline: Any, keyword: Optional[str]) -> Any:
        """
        Apply a case-insensitive LIKE filter on ``keywords_detected``.

        Args:
            query:    Active SQLAlchemy query object.
            Headline: Headline ORM class.
            keyword:  Keyword string or None (no-op when None).

        Returns:
            Query with filter applied (or unchanged if keyword is None).
        """
        if keyword and keyword.strip():
            query = query.filter(
                Headline.keywords_detected.ilike(f"%{keyword.strip()}%")
            )
        return query

    @staticmethod
    def _apply_category_filter(query: Any, Headline: Any, category: Optional[str]) -> Any:
        """
        Apply a case-insensitive LIKE filter on ``categories``.

        Args:
            query:    Active SQLAlchemy query object.
            Headline: Headline ORM class.
            category: Category string or None (no-op when None).

        Returns:
            Query with filter applied (or unchanged if category is None).
        """
        if category and category.strip():
            query = query.filter(
                Headline.categories.ilike(f"%{category.strip()}%")
            )
        return query

    @staticmethod
    def _apply_source_filter(query: Any, Headline: Any, source: Optional[str]) -> Any:
        """
        Apply a case-insensitive LIKE filter on ``source``.

        Args:
            query:    Active SQLAlchemy query object.
            Headline: Headline ORM class.
            source:   Source string or None (no-op when None).

        Returns:
            Query with filter applied (or unchanged if source is None).
        """
        if source and source.strip():
            query = query.filter(
                Headline.source.ilike(f"%{source.strip()}%")
            )
        return query

    @staticmethod
    def _apply_min_impact_filter(
        query: Any, Headline: Any, min_impact_score: Optional[int]
    ) -> Any:
        """
        Apply a minimum impact score filter.

        Args:
            query:            Active SQLAlchemy query object.
            Headline:         Headline ORM class.
            min_impact_score: Integer threshold or None (no-op when None).

        Returns:
            Query with filter applied (or unchanged if min_impact_score is None).
        """
        if min_impact_score is not None:
            query = query.filter(Headline.impact_score >= min_impact_score)
        return query

    @staticmethod
    def _apply_pagination(query: Any, page: int, page_size: int) -> Any:
        """
        Apply OFFSET/LIMIT pagination to a query.

        Args:
            query:     Active SQLAlchemy query object.
            page:      Current page (1-indexed).
            page_size: Records per page.

        Returns:
            Query with offset and limit applied.
        """
        offset = (page - 1) * page_size
        return query.offset(offset).limit(page_size)

    @staticmethod
    def _unpack_comma_column(rows: list[Any]) -> list[str]:
        """
        Unpack a list of single-value DB rows containing comma-separated
        strings into a flat, deduplicated, sorted list of trimmed tokens.

        Used by get_categories() and get_keywords() to unpack Text columns
        that store multiple values as comma-separated strings.

        Args:
            rows: List of single-element tuples from a SQLAlchemy .all() call,
                  e.g. [("fuel, oil",), ("tariff",), ("fuel",)].

        Returns:
            Sorted, deduplicated list of clean token strings.
        """
        tokens: set[str] = set()

        for row in rows:
            raw = row[0] if isinstance(row, tuple) else getattr(row, "categories", "")
            if not raw:
                continue
            for token in raw.split(","):
                cleaned = token.strip().lower()
                if cleaned:
                    tokens.add(cleaned)

        return sorted(tokens)