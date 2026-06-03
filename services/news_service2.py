"""
services/news_service.py

Business service for NewsAPI-backed headline ingestion.

Pipeline position:
    news_fetcher.py  →  keyword_engine.py  →  news_service.py  →  DB  →  API

Responsibilities:
  - Accept raw articles from NewsFetcher
  - Run KeywordEngine enrichment (keywords, categories, impact_score)
  - Validate and normalise field values
  - Prevent duplicate URLs
  - Construct SQLAlchemy Headline ORM objects
  - Persist to MySQL with proper session management
  - Return structured, typed results to callers (API routes / schedulers)

This module intentionally does NOT contain:
  - FastAPI routes
  - HTTP fetch logic
  - Raw SQL
  - Sentiment analysis or NLP
  - Analytics / statistical logic
"""

import importlib
import json
import logging
import re
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

try:
    from sqlalchemy.exc import SQLAlchemyError
except ModuleNotFoundError:  # pragma: no cover
    class SQLAlchemyError(Exception):  # type: ignore[no-redef]
        """Fallback when SQLAlchemy is not installed locally."""

from scrapers.news_fetcher import NewsFetcher
from intelligence.keywords_engine import KeywordEngine


logger = logging.getLogger(__name__)

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

MAX_SOURCE_LENGTH = 255
MAX_HEADLINE_LENGTH = 1000
MAX_URL_LENGTH = 1000

SESSIONLOCAL_MODULE_CANDIDATES = (
    "database",
    "database.session",
)

HEADLINE_MODEL_MODULE_CANDIDATES = (
    "models",
    "models.headline_data",
    "models.news",
    "db.models",
)


class NewsServiceConfigurationError(RuntimeError):
    """Raised when required database/model dependencies cannot be resolved."""


class NewsService:
    """
    Business-layer service for fetching, enriching, and storing news headlines.

    The enrichment pipeline runs KeywordEngine on every article before
    persistence, so every stored Headline row carries:

        keywords_detected       — comma-joined string of matched keywords
        categories              — comma-joined string of matched categories
        matched_keywords_count  — integer count
        impact_score            — weighted integer score

    Public methods are designed to be called by FastAPI routes, cron jobs,
    APScheduler/Celery workers, or data pipelines.

    Example usage::

        service = NewsService()
        result = service.fetch_and_store_everything(
            query="Kenya Power KPLC",
            language="en",
            db_session=db,
        )
        print(result["saved"], "headlines stored")
    """

    def __init__(
        self,
        *,
        fetcher: Optional[NewsFetcher] = None,
        keyword_engine: Optional[KeywordEngine] = None,
        session_factory: Optional[Callable[[], Any]] = None,
        headline_model: Optional[Any] = None,
        persist_fallback_articles: bool = False,
    ) -> None:
        """
        Args:
            fetcher:                   NewsFetcher instance (or default).
            keyword_engine:            KeywordEngine instance (or default).
                                       Inject a custom subclass to override
                                       keywords/weights without touching this file.
            session_factory:           Callable returning a SQLAlchemy session.
                                       Resolved lazily from database.session if omitted.
            headline_model:            SQLAlchemy Headline ORM class.
                                       Resolved lazily from models.headline_data if omitted.
            persist_fallback_articles: If True, fallback (mock) articles are
                                       persisted.  Defaults to False so development
                                       runs don't pollute production tables.
        """
        self.fetcher = fetcher or NewsFetcher()
        self.keyword_engine = keyword_engine or KeywordEngine()
        self._session_factory = session_factory
        self._headline_model = headline_model
        self.persist_fallback_articles = persist_fallback_articles

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch_and_store_top_headlines(
        self,
        *,
        db_session: Optional[Any] = None,
        country: Optional[str] = None,
        category: Optional[str] = None,
        sources: Optional[str] = None,
        query: Optional[str] = None,
        q: Optional[str] = None,
        page_size: int = 20,
        limit: Optional[int] = None,
        page: int = 1,
        **extra_params: Any,
    ) -> Dict[str, Any]:
        """
        Fetch top headlines from NewsAPI, enrich them, and persist valid rows.

        Args:
            db_session: Optional external SQLAlchemy session (caller owns lifecycle).
            country:    2-letter ISO country code, e.g. "ke".
            category:   NewsAPI category, e.g. "business".
            sources:    Comma-separated NewsAPI source IDs.
            query/q:    Keyword query string.
            page_size:  Results per page (max 100).
            limit:      Override page_size.
            page:       Pagination page.

        Returns:
            Service result dict — see :meth:`_result`.
        """
        fetch_response = self.fetcher.fetch_top_headlines(
            country=country,
            category=category,
            sources=sources,
            query=query,
            q=q,
            page_size=page_size,
            limit=limit,
            page=page,
            **extra_params,
        )
        return self._store_fetch_response(
            fetch_response=fetch_response,
            endpoint="top-headlines",
            db_session=db_session,
        )

    def fetch_and_store_everything(
        self,
        *,
        db_session: Optional[Any] = None,
        query: Optional[str] = None,
        q: Optional[str] = None,
        search_in: Optional[str] = None,
        sources: Optional[str] = None,
        domains: Optional[str] = None,
        exclude_domains: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        language: Optional[str] = None,
        sort_by: Optional[str] = None,
        page_size: int = 20,
        limit: Optional[int] = None,
        page: int = 1,
        **extra_params: Any,
    ) -> Dict[str, Any]:
        """
        Search NewsAPI everything endpoint, enrich results, and persist valid rows.

        Args:
            db_session:      Optional external SQLAlchemy session.
            query/q:         Keywords, e.g. "Kenya Power KPLC electricity".
            search_in:       Restrict field search: "title", "description", "content".
            sources:         Comma-separated source IDs.
            domains:         Restrict to domains, e.g. "businessdailyafrica.com".
            exclude_domains: Exclude domains.
            from_date:       Oldest article date, e.g. "2026-05-01".
            to_date:         Newest article date.
            language:        ISO language code, e.g. "en".
            sort_by:         "relevancy" | "popularity" | "publishedAt".
            page_size:       Results per page (max 100).
            limit:           Override page_size.
            page:            Pagination page.

        Returns:
            Service result dict — see :meth:`_result`.
        """
        fetch_response = self.fetcher.search_everything(
            query=query,
            q=q,
            search_in=search_in,
            sources=sources,
            domains=domains,
            exclude_domains=exclude_domains,
            from_date=from_date,
            to_date=to_date,
            language=language,
            sort_by=sort_by,
            page_size=page_size,
            limit=limit,
            page=page,
            **extra_params,
        )
        return self._store_fetch_response(
            fetch_response=fetch_response,
            endpoint="everything",
            db_session=db_session,
        )

    # ------------------------------------------------------------------
    # Core pipeline
    # ------------------------------------------------------------------

    def _store_fetch_response(
        self,
        *,
        fetch_response: Mapping[str, Any],
        endpoint: str,
        db_session: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Full pipeline: raw fetch response → enrich → validate → normalise → save.

        Steps:
          1. Extract raw article list from the fetcher envelope.
          2. Guard against fallback-only runs (configurable).
          3. Run KeywordEngine.enrich_articles() on the full batch.
          4. Validate each enriched article.
          5. Normalise fields into Headline-ready dicts.
          6. Deduplicate within the batch and against the DB.
          7. Build ORM objects and persist in one transaction.

        Args:
            fetch_response: Raw dict returned by NewsFetcher.
            endpoint:       Human-readable endpoint name for logging/results.
            db_session:     Optional caller-owned SQLAlchemy session.

        Returns:
            Service result dict.
        """
        raw_articles = self._extract_articles(fetch_response)
        fetched_count = fetch_response.get("total_results", 0)
        fetch_success = bool(fetch_response.get("success"))
        fallback_used = bool(fetch_response.get("fallback_used"))

        logger.info(
            "News fetch completed: endpoint=%s success=%s fetched=%s fallback=%s",
            endpoint, fetch_success, fetched_count, fallback_used,
        )

        # Skip persisting fallback articles unless explicitly enabled.
        if not fetch_success and fallback_used and not self.persist_fallback_articles:
            error = str(fetch_response.get("error") or "News fetch failed")
            logger.warning(
                "Skipping fallback articles: endpoint=%s fetched=%s error=%s",
                endpoint, fetched_count, error,
            )
            return self._result(
                success=False, endpoint=endpoint, fetched=fetched_count,
                saved=0, duplicates=0, invalid=0, errors=[error],
                error=error, fallback_used=fallback_used,
            )

        # ----------------------------------------------------------
        # Step 1 — Keyword enrichment (runs on the whole batch at once
        # for efficiency; KeywordEngine.enrich_articles is O(n)).
        # ----------------------------------------------------------
        enriched_articles = self.keyword_engine.enrich_articles(raw_articles)

        logger.info(
            "Keyword enrichment complete: endpoint=%s articles=%d",
            endpoint, len(enriched_articles),
        )

        session, should_close = self._get_session(db_session)
        invalid_count = 0
        duplicate_count = 0
        saved_count = 0
        errors: List[str] = []

        try:
            # ----------------------------------------------------------
            # Step 2 — Validate → normalise → deduplicate within batch
            # ----------------------------------------------------------
            normalised_articles: List[Dict[str, Any]] = []
            seen_urls: Set[str] = set()

            for article in enriched_articles:
                is_valid, validation_error = self._validate_article(article)
                if not is_valid:
                    invalid_count += 1
                    errors.append(validation_error)
                    logger.debug("Skipping invalid article: %s", validation_error)
                    continue

                normalised = self._normalize_article(article)
                url = normalised["url"]

                if url in seen_urls:
                    duplicate_count += 1
                    continue

                seen_urls.add(url)
                normalised_articles.append(normalised)

            # ----------------------------------------------------------
            # Step 3 — Deduplicate against existing DB rows
            # ----------------------------------------------------------
            existing_urls = self._existing_urls(
                session=session,
                urls=[a["url"] for a in normalised_articles],
            )

            headline_objects = []
            for normalised in normalised_articles:
                if normalised["url"] in existing_urls:
                    duplicate_count += 1
                    continue
                headline_objects.append(self._create_headline_object(normalised))

            # ----------------------------------------------------------
            # Step 4 — Persist
            # ----------------------------------------------------------
            saved_count = self._save_articles(session, headline_objects)

            if invalid_count:
                logger.warning("Skipped invalid articles: count=%d", invalid_count)

            logger.info(
                "News persistence complete: endpoint=%s fetched=%s "
                "saved=%d duplicates=%d invalid=%d",
                endpoint, fetched_count, saved_count, duplicate_count, invalid_count,
            )

            return self._result(
                success=True, endpoint=endpoint, fetched=fetched_count,
                saved=saved_count, duplicates=duplicate_count, invalid=invalid_count,
                errors=errors, fallback_used=fallback_used,
            )

        except SQLAlchemyError as exc:
            self._rollback_safely(session)
            logger.exception("Database failure while saving news articles")
            return self._result(
                success=False, endpoint=endpoint, fetched=fetched_count,
                saved=saved_count, duplicates=duplicate_count, invalid=invalid_count,
                errors=errors + [str(exc)], error="Database commit failed",
                fallback_used=fallback_used,
            )

        except Exception as exc:  # noqa: BLE001
            self._rollback_safely(session)
            logger.exception("Unexpected failure while saving news articles")
            return self._result(
                success=False, endpoint=endpoint, fetched=fetched_count,
                saved=saved_count, duplicates=duplicate_count, invalid=invalid_count,
                errors=errors + [str(exc)], error=str(exc),
                fallback_used=fallback_used,
            )

        finally:
            if should_close:
                session.close()

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_article(self, article: Any) -> Tuple[bool, str]:
        """
        Validate the minimum NewsAPI article shape required for storage.

        Checks for title, url, and source — the three fields the Headline
        model marks as NOT NULL.

        Args:
            article: Enriched article dict from KeywordEngine.

        Returns:
            Tuple of (is_valid, error_message).
        """
        if not isinstance(article, Mapping):
            return False, "Invalid article shape"

        title = self._clean_string(article.get("title"), none_if_empty=True)
        url = self._clean_string(article.get("url"), none_if_empty=True)
        source = self._extract_source_name(article.get("source"))

        if not title:
            return False, "Missing article title"
        if not url:
            return False, "Missing article URL"
        if not source:
            return False, "Missing article source"

        return True, ""

    # ------------------------------------------------------------------
    # Normalisation
    # ------------------------------------------------------------------

    def _normalize_article(self, article: Mapping[str, Any]) -> Dict[str, Any]:
        """
        Convert an enriched article dict into Headline ORM-ready field values.

        Intelligence fields produced by KeywordEngine are serialised here:
          - ``keywords``   list[str]  →  ``keywords_detected``  comma-joined str
          - ``categories`` list[str]  →  ``categories``         comma-joined str
          - ``matched_keywords_count`` and ``impact_score`` pass through as-is.

        This conversion matches the Headline model column definitions:
            keywords_detected  = Column(Text)
            categories         = Column(Text)
            matched_keywords_count = Column(Integer)
            impact_score       = Column(Integer)

        Args:
            article: Enriched article dict (original fields + intelligence fields).

        Returns:
            Dict ready to be passed to :meth:`_create_headline_object`.
        """
        # -- Intelligence fields from KeywordEngine ---------------------
        raw_keywords: List[str] = article.get("keywords_detected") or []
        raw_categories: List[str] = article.get("categories") or []
        matched_keywords_count: int = article.get("matched_keywords_count") or 0
        impact_score: int = article.get("impact_score") or 0

        # Serialise lists to comma-joined strings for Text columns.
        keywords_detected: Optional[str] = (
            ", ".join(raw_keywords) if raw_keywords else None
        )
        categories_str: Optional[str] = (
            ", ".join(raw_categories) if raw_categories else None
        )

        return {
            # -- Core NewsAPI fields ------------------------------------
            "source": self._truncate(
                self._extract_source_name(article.get("source")) or "Unknown",
                MAX_SOURCE_LENGTH,
            ),
            "headline": self._truncate(
                self._clean_string(article.get("title"), default="Untitled"),
                MAX_HEADLINE_LENGTH,
            ),
            "description": self._clean_string(
                article.get("description"), none_if_empty=True
            ),
            "content": self._clean_string(
                article.get("content"), none_if_empty=True
            ),
            "url": self._truncate(
                self._clean_string(article.get("url"), default=""),
                MAX_URL_LENGTH,
            ),
            "published_at": self._parse_datetime(article.get("publishedAt")),
            "timestamp": self._now_nairobi(),

            # -- Intelligence fields ------------------------------------
            "keywords_detected": keywords_detected,
            "categories": categories_str,
            "matched_keywords_count": matched_keywords_count,
            "impact_score": impact_score,

            # -- Downstream enrichment slots (sentiment engine, Phase 2)
            "sentiment_score": None,
        }

    # ------------------------------------------------------------------
    # ORM construction
    # ------------------------------------------------------------------

    def _create_headline_object(self, normalised: Mapping[str, Any]) -> Any:
        """
        Build a SQLAlchemy Headline ORM instance from normalised field values.

        Maps directly to the Headline model columns defined in
        models/headline_data.py:

            source, headline, description, content, sentiment_score,
            published_at, keywords_detected, categories,
            matched_keywords_count, impact_score, url, timestamp

        Args:
            normalised: Dict produced by :meth:`_normalize_article`.

        Returns:
            Unpersisted Headline ORM object.
        """
        headline_model = self._get_headline_model()
        return headline_model(
            source=normalised["source"],
            headline=normalised["headline"],
            description=normalised["description"],
            content=normalised["content"],
            sentiment_score=normalised["sentiment_score"],
            published_at=normalised["published_at"],
            keywords_detected=normalised["keywords_detected"],
            categories=normalised["categories"],
            matched_keywords_count=normalised["matched_keywords_count"],
            impact_score=normalised["impact_score"],
            url=normalised["url"],
            timestamp=normalised["timestamp"],
        )

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------

    def _save_articles(self, session: Any, articles: Sequence[Any]) -> int:
        """Persist ORM objects in a single transaction."""
        if not articles:
            return 0
        session.add_all(list(articles))
        session.commit()
        return len(articles)

    def _existing_urls(self, session: Any, urls: Sequence[str]) -> Set[str]:
        """
        Return the subset of *urls* already present in the headlines table.

        Runs a single IN query — O(1) round-trips regardless of batch size.

        Args:
            session: Active SQLAlchemy session.
            urls:    List of URL strings to check.

        Returns:
            Set of URLs already stored.
        """
        if not urls:
            return set()

        headline_model = self._get_headline_model()
        unique_urls = list(dict.fromkeys(urls))  # preserve order, remove dups

        rows = (
            session.query(headline_model.url)
            .filter(headline_model.url.in_(unique_urls))
            .all()
        )

        existing: Set[str] = set()
        for row in rows:
            existing.add(row[0] if isinstance(row, tuple) else getattr(row, "url", row))
        return existing

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    def _extract_articles(
        self, fetch_response: Mapping[str, Any]
    ) -> List[Mapping[str, Any]]:
        """Safely extract article dicts from a NewsFetcher response envelope."""
        articles = fetch_response.get("articles") or []
        if not isinstance(articles, list):
            logger.warning("Fetcher returned non-list articles payload")
            return []
        return [a for a in articles if isinstance(a, Mapping)]

    def _extract_source_name(self, source: Any) -> Optional[str]:
        """Extract source.name (or source.id) from a NewsAPI source object."""
        if isinstance(source, Mapping):
            return self._clean_string(
                source.get("name") or source.get("id"),
                none_if_empty=True,
            )
        return self._clean_string(source, none_if_empty=True)

    # ------------------------------------------------------------------
    # String / datetime utilities
    # ------------------------------------------------------------------

    def _parse_datetime(self, value: Any) -> datetime:
        """
        Parse a NewsAPI ISO-8601 timestamp string into a Nairobi-aware datetime.

        Falls back to the current Nairobi time on any parse failure so the
        Headline row is always stored with a valid timestamp.

        Args:
            value: Raw publishedAt string, e.g. "2026-05-18T09:00:00Z".

        Returns:
            Timezone-aware datetime in Africa/Nairobi.
        """
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str) and value.strip():
            try:
                parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            except ValueError:
                logger.debug("Invalid publishedAt timestamp; using fallback: %r", value)
                return self._now_nairobi()
        else:
            return self._now_nairobi()

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=NAIROBI_TZ)

        return parsed.astimezone(NAIROBI_TZ)

    def _clean_string(
        self,
        value: Any,
        *,
        default: Optional[str] = None,
        none_if_empty: bool = False,
    ) -> Optional[str]:
        """Normalise whitespace and unicode noise without altering meaning."""
        if value is None:
            return None if none_if_empty else default

        cleaned = (
            str(value)
            .replace("\xa0", " ")
            .replace("\u200b", "")
            .replace("\ufeff", "")
            .strip()
        )
        cleaned = re.sub(r"\s+", " ", cleaned)

        if not cleaned:
            return None if none_if_empty else default
        return cleaned

    def _truncate(self, value: Optional[str], max_length: int) -> Optional[str]:
        """Truncate to fit ORM column size limits."""
        if value is None:
            return None
        return value[:max_length].rstrip() if len(value) > max_length else value

    def _now_nairobi(self) -> datetime:
        """Current timezone-aware Nairobi timestamp."""
        return datetime.now(NAIROBI_TZ)

    # ------------------------------------------------------------------
    # Session / model resolution
    # ------------------------------------------------------------------

    def _get_session(self, db_session: Optional[Any]) -> Tuple[Any, bool]:
        """
        Return a (session, should_close) pair.

        If a session is passed in by the caller (e.g. FastAPI Depends), we
        use it and return should_close=False so the caller keeps ownership.
        Otherwise we open one from the factory and return should_close=True.
        """
        if db_session is not None:
            return db_session, False
        return self._get_session_factory()(), True

    def _get_session_factory(self) -> Callable[[], Any]:
        """Resolve SessionLocal lazily."""
        if self._session_factory is None:
            self._session_factory = self._resolve_import(
                SESSIONLOCAL_MODULE_CANDIDATES, "SessionLocal"
            )
        return self._session_factory

    def _get_headline_model(self) -> Any:
        """Resolve the Headline ORM model lazily."""
        if self._headline_model is None:
            self._headline_model = self._resolve_import(
                HEADLINE_MODEL_MODULE_CANDIDATES, "Headline"
            )
        return self._headline_model

    def _resolve_import(
        self, module_candidates: Sequence[str], attr_name: str
    ) -> Any:
        """
        Try each module path in order and return the first match for attr_name.

        Args:
            module_candidates: Ordered list of module paths to try.
            attr_name:         Attribute to look for in each module.

        Raises:
            NewsServiceConfigurationError if nothing resolves.
        """
        attempted = []
        for module_path in module_candidates:
            attempted.append(f"{module_path}.{attr_name}")
            try:
                module = importlib.import_module(module_path)
            except ModuleNotFoundError as exc:
                missing = exc.name or ""
                if module_path == missing or module_path.startswith(f"{missing}."):
                    continue
                raise

            if hasattr(module, attr_name):
                return getattr(module, attr_name)

        raise NewsServiceConfigurationError(
            f"Could not resolve {attr_name}. Tried: {', '.join(attempted)}. "
            "Pass it explicitly to NewsService(...) if your project uses a "
            "different module path."
        )

    def _rollback_safely(self, session: Any) -> None:
        """Rollback without masking the original exception."""
        try:
            session.rollback()
        except Exception as exc:  # noqa: BLE001
            logger.error("Database rollback failed: %s", exc)

    # ------------------------------------------------------------------
    # Result builder
    # ------------------------------------------------------------------

    def _result(
        self,
        *,
        success: bool,
        endpoint: str,
        fetched: int,
        saved: int,
        duplicates: int,
        invalid: int,
        errors: Sequence[str],
        fallback_used: bool,
        error: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build a stable, typed service-layer result payload."""
        result: Dict[str, Any] = {
            "success": success,
            "endpoint": endpoint,
            "fetched": fetched,
            "saved": saved,
            "duplicates": duplicates,
            "invalid": invalid,
            "errors": list(errors),
            "fallback_used": fallback_used,
            "processed_at": self._now_nairobi().isoformat(),
        }
        if error:
            result["error"] = error
        return result


# ---------------------------------------------------------------------------
# CLI smoke-test — python services/news_service.py
# ---------------------------------------------------------------------------

def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    try:
        service = NewsService()
        result = service.fetch_and_store_top_headlines(
            country="us",
            category="business",
            limit=20,
        )
    except Exception as exc:
        logger.exception("News service example failed")
        result = {
            "success": False,
            "error": str(exc),
            "hint": (
                "Ensure SessionLocal and the Headline ORM model are reachable, "
                "or pass them explicitly to NewsService(...)."
            ),
        }

    print(json.dumps(result, indent=2, default=str, ensure_ascii=True))
    return 0 if result.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())