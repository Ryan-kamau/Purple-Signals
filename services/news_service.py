"""
Business service for NewsAPI-backed headline ingestion.

This module sits above news_fetcher.py. It owns validation, normalization,
duplicate prevention, ORM object construction, and persistence. It intentionally
does not contain FastAPI routes, HTTP fetch logic, raw SQL, sentiment analysis,
NLP, or analytics logic.

Expected project dependencies:
    - services.news_fetcher.NewsFetcher
    - SQLAlchemy SessionLocal
    - SQLAlchemy ORM model: Headline
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
except ModuleNotFoundError:  # pragma: no cover - depends on project deps
    class SQLAlchemyError(Exception):  # type: ignore[no-redef]
        """Fallback used only when SQLAlchemy is not installed locally."""

from scrapers.news_fetcher import NewsFetcher



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
    "models",
)


class NewsServiceConfigurationError(RuntimeError):
    """Raised when required database/model dependencies cannot be resolved."""


class NewsService:
    """
    Business-layer service for fetching, validating, and storing news headlines.

    Public methods are designed to be called by FastAPI routes, cron jobs,
    APScheduler/Celery workers, or data pipelines.
    """

    def __init__(
        self,
        *,
        fetcher: Optional[NewsFetcher] = None,
        session_factory: Optional[Callable[[], Any]] = None,
        headline_model: Optional[Any] = None,
        persist_fallback_articles: bool = False,
    ) -> None:
        self.fetcher = fetcher or NewsFetcher()
        self._session_factory = session_factory
        self._headline_model = headline_model
        self.persist_fallback_articles = persist_fallback_articles

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
        Fetch top headlines from NewsAPI and persist valid, non-duplicate rows.
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
        Search NewsAPI everything endpoint and persist valid, non-duplicate rows.
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

    def _store_fetch_response(
        self,
        *,
        fetch_response: Mapping[str, Any],
        endpoint: str,
        db_session: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Persist articles from a NewsFetcher response."""
        articles = self._extract_articles(fetch_response)
        fetched_count = fetch_response.get("total_results")
        fetch_success = bool(fetch_response.get("success"))
        fallback_used = bool(fetch_response.get("fallback_used"))

        logger.info(
            "News fetch completed: endpoint=%s success=%s fetched=%s fallback=%s",
            endpoint,
            fetch_success,
            fetched_count,
            fallback_used,
        )

        if not fetch_success and fallback_used and not self.persist_fallback_articles:
            error = str(fetch_response.get("error") or "News fetch failed")
            logger.warning(
                "Skipping fallback articles for persistence: endpoint=%s fetched=%s error=%s",
                endpoint,
                fetched_count,
                error,
            )
            return self._result(
                success=False,
                endpoint=endpoint,
                fetched=fetched_count,
                saved=0,
                duplicates=0,
                invalid=0,
                errors=[error],
                error=error,
                fallback_used=fallback_used,
            )

        session, should_close = self._get_session(db_session)
        invalid_count = 0
        duplicate_count = 0
        saved_count = 0
        errors: List[str] = []

        try:
            normalized_articles: List[Dict[str, Any]] = []
            seen_urls: Set[str] = set()

            for article in articles:
                is_valid, validation_error = self._validate_article(article)
                if not is_valid:
                    invalid_count += 1
                    errors.append(validation_error)
                    logger.debug("Skipping invalid article: %s", validation_error)
                    continue

                normalized = self._normalize_article(article)
                url = normalized["url"]
                if url in seen_urls:
                    duplicate_count += 1
                    continue

                seen_urls.add(url)
                normalized_articles.append(normalized)

            existing_urls = self._existing_urls(
                session=session,
                urls=[article["url"] for article in normalized_articles],
            )

            headline_objects = []
            for normalized in normalized_articles:
                if normalized["url"] in existing_urls:
                    duplicate_count += 1
                    continue
                headline_objects.append(self._create_headline_object(normalized))

            saved_count = self._save_articles(session, headline_objects)

            if invalid_count:
                logger.warning("Skipped invalid articles: count=%s", invalid_count)
            logger.info(
                "News persistence completed: endpoint=%s fetched=%s saved=%s duplicates=%s invalid=%s",
                endpoint,
                fetched_count,
                saved_count,
                duplicate_count,
                invalid_count,
            )

            return self._result(
                success=True,
                endpoint=endpoint,
                fetched=fetched_count,
                saved=saved_count,
                duplicates=duplicate_count,
                invalid=invalid_count,
                errors=errors,
                fallback_used=fallback_used,
            )
        except SQLAlchemyError as exc:
            self._rollback_safely(session)
            logger.exception("Database failure while saving news articles")
            return self._result(
                success=False,
                endpoint=endpoint,
                fetched=fetched_count,
                saved=saved_count,
                duplicates=duplicate_count,
                invalid=invalid_count,
                errors=errors + [str(exc)],
                error="Database commit failed",
                fallback_used=fallback_used,
            )
        except Exception as exc:
            self._rollback_safely(session)
            logger.exception("Unexpected failure while saving news articles")
            return self._result(
                success=False,
                endpoint=endpoint,
                fetched=fetched_count,
                saved=saved_count,
                duplicates=duplicate_count,
                invalid=invalid_count,
                errors=errors + [str(exc)],
                error=str(exc),
                fallback_used=fallback_used,
            )
        finally:
            if should_close:
                session.close()

    def _validate_article(self, article: Any) -> Tuple[bool, str]:
        """Validate the minimum raw NewsAPI article shape required for storage."""
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

        published_at = article.get("publishedAt")
        if published_at:
            self._parse_datetime(published_at)

        return True, ""

    def _normalize_article(self, article: Mapping[str, Any]) -> Dict[str, Any]:
        """
        Convert raw NewsAPI article fields into Headline-ready field values.
        """
        return {
            "source": self._truncate(
                self._extract_source_name(article.get("source")) or "Unknown",
                MAX_SOURCE_LENGTH,
            ),
            "headline": self._truncate(
                self._clean_string(article.get("title"), default="Untitled"),
                MAX_HEADLINE_LENGTH,
            ),
            "description": self._clean_string(article.get("description"), none_if_empty=True),
            "content": self._clean_string(article.get("content"), none_if_empty=True),
            "url": self._truncate(
                self._clean_string(article.get("url"), default=""),
                MAX_URL_LENGTH,
            ),
            "published_at": self._parse_datetime(article.get("publishedAt")),
            "timestamp": self._now_nairobi(),
            "sentiment_score": None,
            "keyword_detected": None,
        }

    def _create_headline_object(self, normalized: Mapping[str, Any]) -> Any:
        """Create a SQLAlchemy Headline ORM instance from normalized data."""
        headline_model = self._get_headline_model()
        return headline_model(
            source=normalized["source"],
            headline=normalized["headline"],
            description=normalized["description"],
            content=normalized["content"],
            sentiment_score=normalized["sentiment_score"],
            published_at=normalized["published_at"],
            keyword_detected=normalized["keyword_detected"],
            url=normalized["url"],
            timestamp=normalized["timestamp"],
        )

    def _save_articles(self, session: Any, articles: Sequence[Any]) -> int:
        """Persist ORM objects in one transaction."""
        if not articles:
            return 0

        session.add_all(list(articles))
        session.commit()
        return len(articles)

    def _existing_urls(self, session: Any, urls: Sequence[str]) -> Set[str]:
        """Return URLs already stored in the headlines table."""
        if not urls:
            return set()

        headline_model = self._get_headline_model()
        unique_urls = list(dict.fromkeys(urls))

        rows = (
            session.query(headline_model.url)
            .filter(headline_model.url.in_(unique_urls))
            .all()
        )

        existing = set()
        for row in rows:
            if isinstance(row, tuple):
                existing.add(row[0])
            else:
                existing.add(getattr(row, "url", row))
        return existing

    def _article_exists(self, session: Any, url: str) -> bool:
        """Check whether an article URL is already present."""
        headline_model = self._get_headline_model()
        return (
            session.query(headline_model)
            .filter(headline_model.url == url)
            .first()
            is not None
        )

    def _extract_articles(self, fetch_response: Mapping[str, Any]) -> List[Mapping[str, Any]]:
        """Safely extract article dictionaries from a fetcher response."""
        articles = fetch_response.get("articles") or []
        if not isinstance(articles, list):
            logger.warning("Fetcher returned non-list articles payload")
            return []

        return [article for article in articles if isinstance(article, Mapping)]

    def _extract_source_name(self, source: Any) -> Optional[str]:
        """Extract source.name from a NewsAPI source object."""
        if isinstance(source, Mapping):
            return self._clean_string(
                source.get("name") or source.get("id"),
                none_if_empty=True,
            )
        return self._clean_string(source, none_if_empty=True)

    def _parse_datetime(self, value: Any) -> datetime:
        """
        Parse NewsAPI timestamps safely.

        Invalid or missing timestamps fall back to the current Africa/Nairobi
        time, as required by the ingestion rules.
        """
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str) and value.strip():
            cleaned = value.strip().replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(cleaned)
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
        """Trim and normalize whitespace without changing article meaning."""
        if value is None:
            return None if none_if_empty else default

        cleaned = str(value)
        cleaned = (
            cleaned.replace("\xa0", " ")
            .replace("\u200b", "")
            .replace("\ufeff", "")
            .strip()
        )
        cleaned = re.sub(r"\s+", " ", cleaned)

        if not cleaned:
            return None if none_if_empty else default
        return cleaned

    def _truncate(self, value: Optional[str], max_length: int) -> Optional[str]:
        """Safely truncate values to fit ORM column sizes."""
        if value is None:
            return None
        if len(value) <= max_length:
            return value
        return value[:max_length].rstrip()

    def _now_nairobi(self) -> datetime:
        """Return the current timezone-aware Nairobi timestamp."""
        return datetime.now(NAIROBI_TZ)

    def _get_session(self, db_session: Optional[Any]) -> Tuple[Any, bool]:
        """Return a database session and whether this service should close it."""
        if db_session is not None:
            return db_session, False

        session_factory = self._get_session_factory()
        return session_factory(), True

    def _get_session_factory(self) -> Callable[[], Any]:
        """Resolve SessionLocal lazily so the module remains importable."""
        if self._session_factory is None:
            self._session_factory = self._resolve_import(
                SESSIONLOCAL_MODULE_CANDIDATES,
                "SessionLocal",
            )
        return self._session_factory

    def _get_headline_model(self) -> Any:
        """Resolve the Headline ORM model lazily."""
        if self._headline_model is None:
            self._headline_model = self._resolve_import(
                HEADLINE_MODEL_MODULE_CANDIDATES,
                "Headline",
            )
        return self._headline_model

    def _resolve_import(self, module_candidates: Sequence[str], attr_name: str) -> Any:
        """Resolve a project attribute from common module locations."""
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

        attempted_text = ", ".join(attempted)
        raise NewsServiceConfigurationError(
            f"Could not resolve {attr_name}. Tried: {attempted_text}. "
            "Pass it explicitly to NewsService(...) if your project uses a "
            "different module path."
        )

    def _rollback_safely(self, session: Any) -> None:
        """Rollback without masking the original exception."""
        try:
            session.rollback()
        except Exception as exc:  # pragma: no cover - defensive cleanup
            logger.error("Database rollback failed: %s", exc)

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
        """Build a stable service-layer result payload."""
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


def main() -> int:
    """Small CLI example for manual ingestion checks."""
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
                "Ensure SessionLocal and the Headline ORM model are configured, "
                "or pass them explicitly to NewsService."
            ),
        }

    print(json.dumps(result, indent=2, default=str, ensure_ascii=True))
    return 0 if result.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())
