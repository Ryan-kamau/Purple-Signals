"""
NewsAPI fetcher service.

This module is intentionally limited to external NewsAPI communication and
raw response retrieval. It does not contain FastAPI routes, database logic,
sentiment analysis, keyword extraction, or application-specific transforms.

Run directly:
    python app/services/news_fetcher.py

Or import into schedulers, scrapers, pipelines, or FastAPI services:
    from app.services.news_fetcher import NewsFetcher
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

import requests
from requests import Response, Session

try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - depends on installed deps
    load_dotenv = None  # type: ignore[assignment]
    DOTENV_AVAILABLE = False


if load_dotenv is not None:
    load_dotenv()

logger = logging.getLogger(__name__)

NEWS_API_KEY_ENV = "NEWS_API_KEY"
NEWS_API_BASE_URL = "https://newsapi.org"
DEFAULT_TIMEOUT_SECONDS = 15
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 100


class NewsAPIError(Exception):
    """Base exception for NewsAPI communication failures."""


class NewsFetcher:
    """
    Service-layer client for NewsAPI.

    Responsibilities:
    - Build NewsAPI HTTP requests.
    - Authenticate with the X-Api-Key header.
    - Send requests with timeout and error handling.
    - Return predictable dictionaries with raw NewsAPI article payloads.
    - Provide realistic fallback data for local development and resilient jobs.
    """

    TOP_HEADLINES_ENDPOINT = "/v2/top-headlines"
    EVERYTHING_ENDPOINT = "/v2/everything"

    ENDPOINT_NAMES = {
        TOP_HEADLINES_ENDPOINT: "top-headlines",
        EVERYTHING_ENDPOINT: "everything",
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = NEWS_API_BASE_URL,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        session: Optional[Session] = None,
        fallback_enabled: bool = True,
    ) -> None:
        self.api_key = api_key or os.getenv(NEWS_API_KEY_ENV)
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self.fallback_enabled = fallback_enabled

        if not DOTENV_AVAILABLE:
            logger.warning("python-dotenv is not installed; .env loading was skipped.")

        if not self.api_key:
            logger.warning(
                "%s is not configured. NewsFetcher will return fallback data "
                "unless an API key is supplied.",
                NEWS_API_KEY_ENV,
            )

    def fetch_top_headlines(
        self,
        *,
        query: Optional[str] = None,
        country: Optional[str] = None,
        category: Optional[str] = None,
        sources: Optional[str] = None,
        q: Optional[str] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        limit: Optional[int] = None,
        page: int = 1,
        **extra_params: Any,
    ) -> Dict[str, Any]:
        """
        Fetch raw top headlines from NewsAPI /v2/top-headlines.

        Supports NewsAPI parameters such as country, category, sources, q,
        pageSize, and page. Additional keyword arguments are passed through as
        query parameters after removing empty values.
        """
        resolved_page_size = limit if limit is not None else page_size
        params = {
            "country": country,
            "category": category,
            "sources": sources,
            "q": q or query,
            "pageSize": self._normalize_page_size(resolved_page_size),
            "page": page,
            **extra_params,
        }
        return self._make_request(self.TOP_HEADLINES_ENDPOINT, params)

    def search_everything(
        self,
        *,
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
        page_size: int = DEFAULT_PAGE_SIZE,
        limit: Optional[int] = None,
        page: int = 1,
        **extra_params: Any,
    ) -> Dict[str, Any]:
        """
        Search raw articles from NewsAPI /v2/everything.

        Supports NewsAPI parameters such as q, searchIn, sources, domains,
        excludeDomains, from, to, language, sortBy, pageSize, and page.
        Additional keyword arguments are passed through as query parameters
        after removing empty values.
        """
        resolved_page_size = limit if limit is not None else page_size
        params = {
            "q": q or query,
            "searchIn": search_in,
            "sources": sources,
            "domains": domains,
            "excludeDomains": exclude_domains,
            "from": from_date,
            "to": to_date,
            "language": language,
            "sortBy": sort_by,
            "pageSize": self._normalize_page_size(resolved_page_size),
            "page": page,
            **extra_params,
        }
        return self._make_request(self.EVERYTHING_ENDPOINT, params)

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute a GET request to NewsAPI and return a stable response dict.

        The article payloads are returned exactly as NewsAPI provides them,
        apart from wrapping them in predictable service metadata.
        """
        endpoint_name = self._endpoint_name(endpoint)
        request_params = self._clean_params(params or {})

        if not self.api_key:
            message = f"Missing {NEWS_API_KEY_ENV}"
            logger.error(message)
            return self._failure_response(
                endpoint=endpoint_name,
                error=message,
                status_code=None,
                params=request_params,
            )

        url = self._build_url(endpoint)
        logger.info(
            "NewsAPI request started: endpoint=%s params=%s",
            endpoint_name,
            request_params,
        )

        try:
            response = self.session.get(
                url,
                headers=self._build_headers(),
                params=request_params,
                timeout=self.timeout,
            )
            return self._handle_response(
                response=response,
                endpoint=endpoint_name,
                params=request_params,
            )
        except requests.Timeout:
            logger.error("NewsAPI request timed out: endpoint=%s", endpoint_name)
            return self._failure_response(
                endpoint=endpoint_name,
                error="Request timeout",
                status_code=None,
                params=request_params,
            )
        except requests.ConnectionError as exc:
            logger.error(
                "NewsAPI connection failed: endpoint=%s error=%s",
                endpoint_name,
                exc,
            )
            return self._failure_response(
                endpoint=endpoint_name,
                error="Connection error",
                status_code=None,
                params=request_params,
            )
        except requests.RequestException as exc:
            logger.error(
                "NewsAPI request failed: endpoint=%s error=%s",
                endpoint_name,
                exc,
            )
            return self._failure_response(
                endpoint=endpoint_name,
                error=str(exc),
                status_code=None,
                params=request_params,
            )

    def _handle_response(
        self,
        response: Response,
        endpoint: str,
        params: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """Normalize a NewsAPI HTTP response into the service return shape."""
        status_code = response.status_code

        try:
            payload = response.json()
        except ValueError:
            logger.error(
                "NewsAPI returned non-JSON response: endpoint=%s status_code=%s",
                endpoint,
                status_code,
            )
            return self._failure_response(
                endpoint=endpoint,
                error="Invalid JSON response from NewsAPI",
                status_code=status_code,
                params=params,
            )

        if response.ok and payload.get("status") == "ok":
            articles = payload.get("articles") or []
            logger.info(
                "NewsAPI request succeeded: endpoint=%s status_code=%s articles=%s",
                endpoint,
                status_code,
                len(articles),
            )
            return {
                "success": True,
                "endpoint": endpoint,
                "status": payload.get("status"),
                "total_results": payload.get("totalResults", len(articles)),
                "articles": articles,
                "fetched_at": self._utc_now_iso(),
                "status_code": status_code,
                "params": dict(params),
            }

        error_message = self._extract_error_message(payload, status_code)
        logger.warning(
            "NewsAPI request returned an error: endpoint=%s status_code=%s error=%s",
            endpoint,
            status_code,
            error_message,
        )
        return self._failure_response(
            endpoint=endpoint,
            error=error_message,
            status_code=status_code,
            params=params,
            raw=payload,
        )

    def _failure_response(
        self,
        *,
        endpoint: str,
        error: str,
        status_code: Optional[int],
        params: Mapping[str, Any],
        raw: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build a predictable failure payload with optional fallback articles."""
        fallback = (
            self._generate_fallback_response(endpoint=endpoint, params=params)
            if self.fallback_enabled
            else {"articles": [], "totalResults": 0}
        )

        return {
            "success": False,
            "endpoint": endpoint,
            "status": raw.get("status") if raw else "error",
            "status_code": status_code,
            "error": error,
            "total_results": fallback.get("totalResults", 0),
            "articles": fallback.get("articles", []),
            "fetched_at": self._utc_now_iso(),
            "params": dict(params),
            "fallback_used": self.fallback_enabled,
            "raw": dict(raw) if raw else None,
        }

    def _build_headers(self) -> Dict[str, str]:
        """Build NewsAPI authentication headers."""
        return {
            "X-Api-Key": self.api_key or "",
            "Accept": "application/json",
            "User-Agent": "PurpleStacksNewsFetcher/1.0",
        }

    def _build_url(self, endpoint: str) -> str:
        """Return an absolute NewsAPI endpoint URL."""
        if endpoint not in self.ENDPOINT_NAMES:
            raise NewsAPIError(f"Unsupported NewsAPI endpoint: {endpoint}")
        return f"{self.base_url}{endpoint}"

    def _clean_params(self, params: Mapping[str, Any]) -> Dict[str, Any]:
        """Remove empty parameters while preserving falsey valid values."""
        cleaned: Dict[str, Any] = {}
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            if isinstance(value, (list, tuple, set)):
                if not value:
                    continue
                cleaned[key] = ",".join(str(item) for item in value if item is not None)
                continue
            cleaned[key] = value
        return cleaned

    def _endpoint_name(self, endpoint: str) -> str:
        """Return a stable public endpoint name for response payloads."""
        if endpoint not in self.ENDPOINT_NAMES:
            raise NewsAPIError(f"Unsupported NewsAPI endpoint: {endpoint}")
        return self.ENDPOINT_NAMES[endpoint]

    def _normalize_page_size(self, page_size: int) -> int:
        """Keep page size within NewsAPI's documented maximum."""
        try:
            numeric_page_size = int(page_size)
        except (TypeError, ValueError):
            logger.warning("Invalid page_size=%r; using default", page_size)
            return DEFAULT_PAGE_SIZE

        if numeric_page_size < 1:
            return 1
        if numeric_page_size > MAX_PAGE_SIZE:
            return MAX_PAGE_SIZE
        return numeric_page_size

    def _extract_error_message(
        self,
        payload: Mapping[str, Any],
        status_code: int,
    ) -> str:
        """Convert common NewsAPI failures into clear service errors."""
        code = str(payload.get("code") or "").strip()
        message = str(payload.get("message") or "").strip()

        if code:
            known_messages = {
                "apiKeyDisabled": "Invalid or disabled NewsAPI key",
                "apiKeyExhausted": "NewsAPI rate limit or quota exceeded",
                "apiKeyInvalid": "Invalid NewsAPI key",
                "apiKeyMissing": "Missing NewsAPI key",
                "parameterInvalid": "Invalid NewsAPI query parameter",
                "parametersMissing": "Required NewsAPI query parameters are missing",
                "rateLimited": "NewsAPI rate limit exceeded",
                "sourcesTooMany": "Too many NewsAPI sources requested",
                "sourceDoesNotExist": "Requested NewsAPI source does not exist",
            }
            return known_messages.get(code, message or code)

        if status_code == 401:
            return "Invalid NewsAPI key"
        if status_code == 429:
            return "NewsAPI rate limit exceeded"
        if status_code == 400:
            return "Invalid NewsAPI query parameters"
        if status_code >= 500:
            return "NewsAPI server error"

        return message or f"NewsAPI request failed with status code {status_code}"

    def _generate_fallback_response(
        self,
        *,
        endpoint: str,
        params: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """
        Generate realistic NewsAPI-shaped fallback data.

        Fallback articles are only for local development, testing, and resilient
        scheduled jobs when NewsAPI is unavailable. They intentionally keep the
        NewsAPI article field names: source, author, title, description, url,
        urlToImage, publishedAt, and content.
        """
        query_hint = (
            params.get("q")
            or params.get("category")
            or params.get("country")
            or params.get("sources")
            or "markets"
        )
        fetched_at = self._utc_now_iso()

        articles = [
            {
                "source": {"id": "market-watch", "name": "Market Watch"},
                "author": "NewsAPI fallback desk",
                "title": f"Global markets track policy signals around {query_hint}",
                "description": (
                    "Investors monitored central-bank guidance, currency moves, "
                    "and commodity prices as analysts assessed the latest risk tone."
                ),
                "url": "https://example.com/fallback/global-markets-policy-signals",
                "urlToImage": None,
                "publishedAt": fetched_at,
                "content": (
                    "Market participants focused on policy direction, liquidity "
                    "conditions, and cross-asset volatility. This fallback item is "
                    "provided for local development when NewsAPI is unavailable."
                ),
            },
            {
                "source": {"id": "business-daily", "name": "Business Daily"},
                "author": "NewsAPI fallback desk",
                "title": f"Companies review exposure as {query_hint} remains in focus",
                "description": (
                    "Corporate finance teams reviewed supply chains, financing "
                    "costs, and consumer demand against a mixed macro backdrop."
                ),
                "url": "https://example.com/fallback/companies-review-exposure",
                "urlToImage": None,
                "publishedAt": fetched_at,
                "content": (
                    "Executives said planning assumptions remain sensitive to "
                    "interest rates, exchange rates, and policy changes. This "
                    "fallback item mimics NewsAPI article structure for testing."
                ),
            },
            {
                "source": {"id": "financial-post", "name": "Financial Post"},
                "author": "NewsAPI fallback desk",
                "title": f"Analysts flag currency and inflation risks linked to {query_hint}",
                "description": (
                    "Research desks highlighted inflation expectations, foreign "
                    "exchange liquidity, and trade flows as key variables to watch."
                ),
                "url": "https://example.com/fallback/analysts-flag-currency-risks",
                "urlToImage": None,
                "publishedAt": fetched_at,
                "content": (
                    "Analysts expect market attention to remain on inflation data, "
                    "fiscal policy, and currency stability. This is synthetic "
                    "fallback content for development and pipeline testing."
                ),
            },
        ]

        logger.info(
            "Generated NewsAPI fallback response: endpoint=%s articles=%s",
            endpoint,
            len(articles),
        )
        return {
            "status": "ok",
            "totalResults": len(articles),
            "articles": articles,
            "fetchedAt": fetched_at,
        }

    def _utc_now_iso(self) -> str:
        """Return the current UTC timestamp in ISO 8601 format."""
        return datetime.now(timezone.utc).isoformat()


def main() -> int:
    """Small CLI example for manual local verification."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    fetcher = NewsFetcher()

    top_headlines = fetcher.fetch__top_headlines(
        country="us",
        category="business",
        page_size=1,
    )
    everything = fetcher.search_everything(
        q="central bank OR inflation OR markets",
        language="en",
        sort_by="publishedAt",
        page_size=5,
    )

    print(
        json.dumps(
            {
                "top_headlines": top_headlines,
                "everything": everything,
            },
            indent=2,
            ensure_ascii=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
