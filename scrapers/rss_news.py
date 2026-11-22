"""
scrapers/rss_news_ingestor.py

RSS News Ingestion Service — Kenya Intelligence Platform

Pipeline position:
    RSS Feeds  →  RSSNewsIngestor  →  KeywordEngine  →  Headline DB  →  API

This module mirrors the NewsService pipeline exactly:
  1. Fetch RSS/Atom feeds via requests and parse entries with feedparser
  2. Normalise raw feedparser entries into article dicts
  3. Run KeywordEngine.enrich_articles() on the full batch
  4. Filter articles using RelevanceFilter
  5. Validate required fields (headline, url, source)
  6. Deduplicate against existing DB rows (chunked IN query)
  7. Build Headline ORM objects from enriched, normalised dicts
  8. Bulk-insert new articles in a single transaction
  9. Return a structured IngestionResponse compatible with NewsService

KeywordEngine output fields persisted per row:
    keywords_detected       — comma-joined string of matched keywords
    categories              — comma-joined string of matched intelligence categories
    matched_keywords_count  — integer count of unique matched keywords
    impact_score            — weighted integer score

This module does NOT:
  - Perform sentiment analysis (sentiment_score remains None; Phase 2)
  - Make async calls
  - Schedule jobs
  - Contain FastAPI routes

Usage:
    from scrapers.rss_news_ingestor import RSSNewsIngestor

    service = RSSNewsIngestor(session)

    # Ingest one feed:
    result = service.ingest_feed(
        "https://www.businessdailyafrica.com/rss/266",
        source_label="Business Daily Africa",
        region = "kenya".
    )

    # Ingest all default Kenya-relevant feeds:
    results = RSSNewsIngestor.ingest_all_feeds(session)
    total_saved = sum(r.saved for r in results)
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo
import hashlib
import feedparser
import requests
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from intelligence.keywords_engine import KeywordEngine
from intelligence.relevance_filter import RelevanceFilter


from models.headline_data import Headline
from models.article_security import ArticleSecurity
from schemas.headline import IngestionResponse
from sqlalchemy import select


logger = logging.getLogger(__name__)

_FEED_HEADERS = {
    "User-Agent": "KPLCIntelligenceEngine/1.0",
    "Accept": "application/rss+xml, application/xml, text/xml",
}

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

# Maximum number of URLs to pass in a single SQL IN clause to avoid
# database parameter-count limits (e.g. SQLite's 999-variable limit).
_URL_CHUNK_SIZE: int = 500

# ---------------------------------------------------------------------------
# Default feed registry
#
# Grouped by region for easy scanning.  Add/remove feeds here without
# touching any other code.  `label` is stored as the article `source` when
# the RSS entry does not carry its own source name.
# ---------------------------------------------------------------------------

DEFAULT_FEEDS: List[Dict[str, str]] = [
    # ── Local Kenya ─────────────────────────────────────────────────────────
    # region="kenya" tells RelevanceFilter to trust the Kenya anchor for these feeds.
    {
        "label": "Business Daily Africa",
        "region": "kenya",
        # site: filter so we only get Business Daily's own articles, not any
        # article that merely mentions the phrase (same pattern as Nation below).
        "url": "https://news.google.com/rss/search?q=site:businessdailyafrica.com&hl=en-KE&gl=KE&ceid=KE:en",
    },
    {
        "label": "The Standard Business",
        "region": "kenya",
        "url": "https://www.standardmedia.co.ke/rss/business.php",
    },
    {
        "label": "Nation Business",
        "region": "kenya",
        "url": "https://news.google.com/rss/search?q=site:nation.africa+business&hl=en-KE&gl=KE&ceid=KE:en",
    },
    {
        "label": "Capital FM Business",
        "region": "kenya",
        "url": "https://www.capitalfm.co.ke/business/feed/",
    },
    # ── Global ───────────────────────────────────────────────────────────────
    # region="global" means the strict gate applies: the title must hit an
    # oil / shipping / Fed / Eurobond channel, or mention Kenya or an NSE company.
    {
        "label": "CNBC Markets",
        "region": "global",
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839135",
    },
    {
        "label": "IMF News",
        "region": "global",
        "url": "https://www.imf.org/en/News/rss?language=eng",
    },
    {
        "label": "World Bank News",
        "region": "global",
        # "Kenya" added to the query: the old one returned Thailand, Egypt, Nigeria.
        "url": "https://news.google.com/rss/search?q=%22World+Bank%22+Kenya&hl=en-KE&gl=KE&ceid=KE:en",
    },
    {
    "label": "Oil Markets",
    "region": "global",
    "url": "https://news.google.com/rss/search?q=Brent+crude+OR+OPEC+OR+Hormuz&hl=en-KE&gl=KE&ceid=KE:en",
    },
]


# ---------------------------------------------------------------------------
# RSSNewsIngestor
# ---------------------------------------------------------------------------


class RSSNewsIngestor:
    """
    Fetches one RSS feed, enriches entries with KeywordEngine, deduplicates,
    and bulk-inserts new Headline rows in a single database transaction.


    This ensures that RSS-sourced headlines produce identical DB records to
    headlines ingested via the NewsAPI path — both are queryable via the same
    /news API endpoints and filtered by the same KeywordEngine output.

    Args:
        session:                 SQLAlchemy session (caller owns the lifecycle).
        keyword_engine:          KeywordEngine instance. Inject a custom subclass
                                 to override keywords/weights without touching this
                                 file. Defaults to KeywordEngine().
        request_timeout:         HTTP read timeout in seconds. Default: 15.

    Example::

        service = RSSNewsIngestor(db)
        result  = service.ingest_feed(
        "url": "https://www.standardmedia.co.ke/rss/business.php",
        "source_label": "The Standard Business"
        )
        print(result.saved, "new articles stored")
    """

    def __init__(
        self,
        session: Session,
        keyword_engine: Optional[KeywordEngine] = None,
        request_timeout: int = 15,
        relevance_filter: Optional[RelevanceFilter] = None,   # new kwarg
    ) -> None:
        self._session = session
        self._keyword_engine = keyword_engine or KeywordEngine()
        self._timeout = request_timeout
        self._relevance = relevance_filter or RelevanceFilter()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def ingest_feed(
        self,
        feed_url: str,
        source_label: Optional[str] = None,
        region: str = "kenya"
    ) -> IngestionResponse:
        """
        Fetch, enrich, validate, deduplicate, and persist articles from one RSS feed.

        Pipeline steps:
          1. Fetch the feed via requests and parse with feedparser.
          2. Validate that the feed has parseable entries.
          3. Normalise each entry into an article dict (NewsAPI-compatible shape).
          4. Run KeywordEngine.enrich_articles() on the full normalised batch.
          5. Filter articles with RelevanceFilter.
          6. Build Headline-ready dicts from enriched articles.
          7. Deduplicate against existing DB rows (chunked IN query).
          8. Bulk-insert new articles in a single transaction.

        Args:
            feed_url:     Full RSS/Atom feed URL.
            source_label: Human-readable source name used when the RSS entry
                          does not carry its own feed title.
                          Falls back to the feed's own ``feed.title`` or URL.
            region:       Region of the feed, either "kenya" or "global".

        Returns:
            IngestionResponse with counts of fetched / saved / duplicates / invalid.
        """
        start_time = time.monotonic()
        errors: List[str] = []

        logger.info("RSS ingest started: url=%s", feed_url)

        # ── Step 1: Fetch ───────────────────────────────────────────────────
        feed, fetch_error = self._fetch_feed(feed_url)

        if fetch_error or feed is None:
            error_msg = fetch_error or f"Unknown fetch failure for {feed_url}"
            logger.error("RSS fetch failed: url=%s error=%s", feed_url, error_msg)

            return self._failure_response(
                feed_url=feed_url,
                error=error_msg,
                errors=[error_msg],
            )

        # ── Step 2: Validate ────────────────────────────────────────────────
        is_valid, validation_error = self._validate_feed(feed, feed_url)

        if not is_valid:
            logger.warning(
                "RSS feed validation failed: url=%s reason=%s",
                feed_url,
                validation_error,
            )

            return self._empty_response(feed_url=feed_url, fetched=0)

        # Resolve the source name: argument → feed title → URL
        resolved_source = (
            source_label
            or feed.get("feed", {}).get("title")
            or feed_url
        )

        raw_entries = feed.get("entries", [])
        fetched_count = len(raw_entries)

        logger.info(
            "RSS feed fetched: source=%s url=%s entries=%d",
            resolved_source,
            feed_url,
            fetched_count,
        )

        # ── Step 3: Normalise to NewsAPI-compatible article dicts ───────────
        # Produces dicts with title / description / content / url / publishedAt
        # so that KeywordEngine.enrich_articles() can operate on them
        # identically to how it processes raw NewsAPI articles.
        raw_articles, invalid_count, normalise_errors = self._normalize_entries(
            raw_entries, resolved_source
        )
        errors.extend(normalise_errors)

        # ── Step 4: KeywordEngine enrichment ────────────────────────────────
        # Mirrors NewsService._store_fetch_response() Step 1.
        # Adds: keywords_detected (list), categories (list),
        #       matched_keywords_count (int), impact_score (int)
        enriched_articles = self._keyword_engine.enrich_articles(raw_articles)

        logger.info(
            "RSS keyword enrichment complete: source=%s articles=%d",
            resolved_source,
            len(enriched_articles),
        )

        # ── Step 5: Filter by relevance ──────────────────────────────────
        filtered_articles: List[Dict[str, Any]] = []
        rejected_count = 0

        for article in enriched_articles:
            decision = self._relevance.evaluate(article, is_local=(region == "kenya"))
            if not decision.keep:
                logger.debug("RSS rejected: %r", str(article.get("title", ""))[:80])
                rejected_count += 1
                continue
            article["relevance_tier"] = decision.tier
            article["matched_tickers"] = decision.tickers
            filtered_articles.append(article)

        if rejected_count:
            logger.info(
                "RSS relevance filter: source=%s rejected=%d",
                resolved_source,
                rejected_count,
            )

        # ── Step 6: Build Headline-ready dicts from enriched articles ───────
        normalised_for_db = [
            self._build_headline_dict(article) for article in filtered_articles
        ]

        # ── Step 7: Deduplicate within batch + against DB ───────────────────
        try:
            new_articles, duplicate_count = self._deduplicate(normalised_for_db)
        except SQLAlchemyError as exc:
            self._rollback_safely()
            msg = f"Dedup lookup failed: {exc}"
            logger.error("RSS %s url=%s", msg, feed_url)
            return self._failure_response(feed_url=feed_url, error=msg, errors=errors + [msg])

        # ── Step 8: Bulk insert ─────────────────────────────────────────────
        saved_count = 0
        stored_ids: List[int] = []

        if new_articles:
            saved_count, stored_ids, insert_error = self._bulk_insert(new_articles)
            if insert_error:
                errors.append(insert_error)

        elapsed = round(time.monotonic() - start_time, 3)

        logger.info(
            "RSS ingest complete: source=%s url=%s fetched=%d enriched=%d "
            "rejected=%d saved=%d duplicates=%d invalid=%d duration=%.3fs",
            resolved_source,
            feed_url,
            fetched_count,
            len(enriched_articles),
            rejected_count,
            saved_count,
            duplicate_count,
            invalid_count,
            elapsed,
        )

        return self._success_response(
            feed_url=feed_url,
            fetched=fetched_count,
            saved=saved_count,
            duplicates=duplicate_count,
            invalid=invalid_count + rejected_count,
            stored_ids=stored_ids,
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Class-level convenience: ingest all default feeds
    # ------------------------------------------------------------------

    @classmethod
    def ingest_all_feeds(
        cls,
        session: Session,
        feeds: Optional[List[Dict[str, str]]] = None,
        keyword_engine: Optional[KeywordEngine] = None,
        request_timeout: int = 15,
    ) -> List[IngestionResponse]:
        """
        Ingest every feed in the default registry (or a custom list).

        A single KeywordEngine instance is shared across all feeds to avoid
        re-building the keyword lookup table for each feed 

        Args:
            session:                Active SQLAlchemy session.
            feeds:                  Optional custom feed list. Each item must have
                                    ``url`` and ``label`` keys.
                                    Defaults to DEFAULT_FEEDS.
            keyword_engine:         Optional shared KeywordEngine instance.
            request_timeout:        HTTP timeout per feed in seconds.

        Returns:
            List of IngestionResponse objects — one per feed, in order.

        Example::

            results = RSSNewsIngestor.ingest_all_feeds(db)
            total_saved = sum(r.saved for r in results)
        """
        feed_list = feeds or DEFAULT_FEEDS
        shared_engine = keyword_engine or KeywordEngine()

        service = cls(
            session,
            keyword_engine=shared_engine,
            request_timeout=request_timeout,
        )
        results: List[IngestionResponse] = []

        for feed_def in feed_list:
            url = feed_def.get("url", "")
            label = feed_def.get("label")
            region = feed_def.get("region", "kenya")

            if not url:
                logger.warning("Skipping feed with missing URL: %s", feed_def)
                continue
            result = service.ingest_feed(url, source_label=label, region=region)
            results.append(result)

        total_saved = sum(r.saved for r in results)
        total_fetched = sum(r.fetched for r in results)

        logger.info(
            "All feeds ingestion complete: feeds=%d fetched=%d saved=%d",
            len(results),
            total_fetched,
            total_saved,
        )

        return results

    # ------------------------------------------------------------------
    # Private: fetch
    # ------------------------------------------------------------------

    def _fetch_feed(
        self, feed_url: str
    ) -> Tuple[Optional[feedparser.FeedParserDict], Optional[str]]:
        """
        Fetch and parse an RSS/Atom feed via feedparser.

        Handles all known failure modes without raising:
          - Network errors (DNS, connection refused, timeouts)
          - SSL certificate failures
          - Malformed or invalid XML
          - Completely empty/unparseable responses

        Args:
            feed_url: Full RSS feed URL.

        Returns:
            Tuple of (parsed_feed | None, error_message | None).
            On success: (feed, None).
            On failure: (None, error_string).
        """
        logger.info("RSS fetch started: url=%s timeout=%ds", feed_url, self._timeout)

        try:
            resp = requests.get(feed_url, headers=_FEED_HEADERS, timeout=(5, self._timeout))
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.error("RSS request failed: url=%s error=%s", feed_url, exc)
            return None, f"RSS request failed for {feed_url}: {exc}"

        try:
            parsed = feedparser.parse(resp.content)
        except Exception as exc:
            return None, f"feedparser raised unexpected exception for {feed_url}: {exc}"

        # feedparser sets bozo=True for malformed feeds but still returns
        # partial data. Only fatal if entries are absent too.
        entries = parsed.get("entries", [])
        if parsed.get("bozo"):
            bozo_exc = parsed.get("bozo_exception")
            if not entries:
                error = f"feedparser could not parse {feed_url}: {bozo_exc}"
                logger.error(
                    "RSS feed unparseable: url=%s bozo_exception=%s",
                    feed_url,
                    bozo_exc,
                )
                return None, error
            logger.warning(
                "RSS feed bozo (partial parse): url=%s bozo_exception=%s entries=%d",
                feed_url,
                bozo_exc,
                len(entries),
            )

        logger.info(
            "RSS fetch succeeded: url=%s entries=%d",
            feed_url,
            len(entries),
        )
        return parsed, None

    # ------------------------------------------------------------------
    # Private: validation
    # ------------------------------------------------------------------

    def _validate_feed(
        self,
        feed: feedparser.FeedParserDict,
        feed_url: str,
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate that the parsed feed has usable content.

        Args:
            feed:     Parsed feedparser dict.
            feed_url: URL for logging context.

        Returns:
            Tuple of (is_valid, error_message | None).
        """
        if feed is None:
            return False, f"Feed object is None for {feed_url}"

        entries = feed.get("entries")

        if entries is None:
            return False, f"Feed has no 'entries' key: {feed_url}"

        if not hasattr(entries, "__iter__"):
            return False, f"Feed 'entries' is not iterable: {feed_url}"

        if len(entries) == 0:
            return False, f"Feed returned 0 entries: {feed_url}"

        return True, None

    # ------------------------------------------------------------------
    # Private: normalise to NewsAPI-compatible shape
    # ------------------------------------------------------------------

    def _normalize_entries(
        self,
        entries: list,
        source: str,
    ) -> Tuple[List[Dict[str, Any]], int, List[str]]:
        """
        Normalise raw feedparser entries into NewsAPI-compatible article dicts.

        The output dicts use the same field names as raw NewsAPI articles:
            title, description, content, url, publishedAt, source

        This is deliberate: KeywordEngine.enrich_articles() calls
        _build_combined_text() which looks for "title", "description",
        and "content" — matching NewsAPI's exact field names. Keeping the
        same shape means KeywordEngine operates identically on both
        RSS and NewsAPI article batches.

        Args:
            entries: Raw feedparser entry list.
            source:  Resolved source name for this feed.

        Returns:
            Tuple of (article_dicts, invalid_count, error_messages).
        """
        articles: List[Dict[str, Any]] = []
        errors: List[str] = []
        invalid_count = 0

        for entry in entries:
            try:
                article = self._normalise_entry_to_newsapi_shape(entry, source)
            except Exception as exc:
                error_msg = (
                    f"Normalisation error for entry in source={source}: {exc}"
                )
                logger.debug(error_msg)
                errors.append(error_msg)
                invalid_count += 1
                continue

            if article is None:
                invalid_count += 1
                continue

            articles.append(article)

        logger.debug(
            "Normalisation complete: source=%s total=%d valid=%d invalid=%d",
            source,
            len(entries),
            len(articles),
            invalid_count,
        )

        return articles, invalid_count, errors

    def _normalise_entry_to_newsapi_shape(
        self,
        entry: feedparser.FeedParserDict,
        source: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Convert one raw feedparser entry into a NewsAPI-compatible article dict.

        Output field names match NewsAPI's article envelope exactly so
        KeywordEngine.enrich_articles() and _build_combined_text() work
        without any changes.

        Required fields (returns None if either is absent):
            title  → required (maps to NewsAPI "title")
            link   → required (maps to NewsAPI "url")

        Args:
            entry:  Raw feedparser entry dict.
            source: Resolved source name for this feed.

        Returns:
            NewsAPI-shaped article dict or None if required fields are missing.
        """
        title = self._clean_str(
            entry.get("title") or entry.get("summary", "")
        )
        url = self._clean_str(entry.get("link") or entry.get("id", ""))

        if not title:
            logger.debug("Skipping entry: missing title. source=%s", source)
            return None
        if not url:
            logger.debug("Skipping entry: missing URL. source=%s", source)
            return None

        description = self._clean_str(
            entry.get("summary") or entry.get("description", "")
        )
        # Avoid the headline being duplicated as the description
        if description == title:
            description = None

        # feedparser returns content as a list of content objects
        content_raw = entry.get("content")
        content: Optional[str] = None
        if content_raw and isinstance(content_raw, list):
            content = self._clean_str(content_raw[0].get("value", ""))

        published_at = self._parse_published(entry)

        # Use NewsAPI field names so KeywordEngine._build_combined_text()
        # picks up "title", "description", "content" without modification.
        return {
            "title": title,
            "description": description,
            "content": content,
            "url": url,
            "publishedAt": published_at.isoformat() if published_at else None,
            # source dict mirrors NewsAPI shape — not used by KeywordEngine
            # but available for downstream services / logging.
            "source": {"id": None, "name": source},
        }

    # ------------------------------------------------------------------
    # Private: build Headline-ready dict from enriched article
    # ------------------------------------------------------------------

    def _build_headline_dict(
        self, enriched_article: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Convert a KeywordEngine-enriched article dict into a Headline ORM-ready dict.

        Maps NewsAPI field names back to Headline column names and serialises
        KeywordEngine's list outputs (keywords_detected, categories) into
        comma-joined strings.

        Intelligence field mapping for Headline ORM columns:
            enriched["keywords_detected"] list[str] → comma-joined str  → keywords_detected column
            enriched["categories"]        list[str] → comma-joined str  → categories column
            enriched["matched_keywords_count"] int  → int               → matched_keywords_count column
            enriched["impact_score"]           int  → int               → impact_score column

        Args:
            enriched_article: Dict produced by KeywordEngine.enrich_article()
                              (contains all original fields + intelligence fields).

        Returns:
            Dict with all Headline model columns populated, ready for ORM construction.
        """
        # -- Intelligence fields from KeywordEngine -------------------------
        raw_keywords: List[str] = enriched_article.get("keywords_detected") or []
        raw_categories: List[str] = enriched_article.get("categories") or []
        matched_keywords_count: int = enriched_article.get("matched_keywords_count") or 0
        impact_score: int = enriched_article.get("impact_score") or 0

        # Serialise lists to comma-joined strings
        keywords_detected: Optional[str] = (
            ", ".join(raw_keywords) if raw_keywords else None
        )
        categories_str: Optional[str] = (
            ", ".join(raw_categories) if raw_categories else None
        )

        # -- Source name resolution -----------------------------------------
        source_field = enriched_article.get("source")
        if isinstance(source_field, dict):
            source_name = (
                source_field.get("name") or source_field.get("id") or "Unknown"
            )
        else:
            source_name = str(source_field) if source_field else "Unknown"

        # -- Published timestamp -------------------------------------------
        published_at = self._parse_iso_timestamp(
            enriched_article.get("publishedAt")
        )

        return {
            # Core fields mapping NewsAPI names → Headline column names
            "source": self._truncate(source_name, 255),
            "headline": self._truncate(
                enriched_article.get("title") or "Untitled", 1000
            ),
            "description": enriched_article.get("description"),
            "content": enriched_article.get("content"),
            "url": enriched_article.get("url") or "",
            "url_hash": self._hash_url(enriched_article.get("url") or ""),
            "published_at": published_at,
            "timestamp": self._current_time(),

            # Intelligence fields — populated by KeywordEngine
            "keywords_detected": keywords_detected,
            "categories": categories_str,
            "matched_keywords_count": matched_keywords_count,
            "impact_score": impact_score,

            # Sentiment placeholder — populated by sentiment engine in Phase 2
            "sentiment_score": None,
            "matched_tickers": tuple(enriched_article.get("matched_tickers") or ()),
        }

    # ------------------------------------------------------------------
    # Private: deduplication
    # ------------------------------------------------------------------
    @staticmethod
    def _hash_url(url: str) -> str:
        """Stable fixed-length key for a URL of any length."""
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def _deduplicate(
        self, articles: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], int]:
        if not articles:
            return [], 0

        existing_hashes = self._fetch_existing_hashes(
            [a["url_hash"] for a in articles if a.get("url")]
        )

        new_articles: List[Dict[str, Any]] = []
        duplicate_count = 0
        seen_in_batch: Set[str] = set()

        for article in articles:
            h = article.get("url_hash", "")
            if h in existing_hashes or h in seen_in_batch:
                duplicate_count += 1
                continue
            seen_in_batch.add(h)
            new_articles.append(article)

        return new_articles, duplicate_count

    def _fetch_existing_hashes(self, hashes: List[str]) -> Set[str]:
        """
        Return the subset of *urls* already present in the headlines table.

        Runs chunked IN queries (max _URL_CHUNK_SIZE per chunk) to avoid
        database parameter explosion on large feeds.

        Args:
            urls: List of URL strings to check (may contain duplicates).

        Returns:
            Set of URLs already stored in the database.
        """
        if not hashes:
            return set()

        unique_hashes = list(dict.fromkeys(hashes))
        existing: Set[str] = set()

        for start in range(0, len(unique_hashes), _URL_CHUNK_SIZE):
            chunk = unique_hashes[start:start + _URL_CHUNK_SIZE]
            rows = self._session.scalars(
                select(Headline.url_hash).where(Headline.url_hash.in_(chunk))
            ).all()
            existing.update(rows)

        return existing

    
    # ------------------------------------------------------------------
    # Private: bulk insert
    # ------------------------------------------------------------------

    def _bulk_insert(
        self, articles: List[Dict[str, Any]]
    ) -> Tuple[int, List[int], Optional[str]]:
        """
        Insert a batch of Headline-ready dicts in a single transaction.

        Uses session.flush() to assign database IDs before committing,
        so we can return ``stored_ids`` in the response

        On failure: rolls back cleanly and returns (0, [], error_message).

        Args:
            articles: List of Headline-ready dicts from _build_headline_dict().

        Returns:
            Tuple of (saved_count, stored_ids, error_message | None).
        """
        if not articles:
            return 0, [], None

        try:
            orm_objects = []
            ticker_groups = []
            for article in articles:
                data = dict(article)
                ticker_groups.append(tuple(dict.fromkeys(data.pop("matched_tickers", ()))))
                orm_objects.append(Headline(**data))
            self._session.add_all(orm_objects)
            self._session.flush()  # assigns IDs before commit
            self._session.add_all([
                ArticleSecurity(article_id=obj.id, ticker=ticker)
                for obj, tickers in zip(orm_objects, ticker_groups)
                for ticker in tickers
            ])

            stored_ids = [
                obj.id
                for obj in orm_objects
                if getattr(obj, "id", None) is not None
            ]

            self._session.commit()

            logger.debug(
                "Bulk insert committed: count=%d ids=%s",
                len(orm_objects),
                stored_ids,
            )
            return len(orm_objects), stored_ids, None

        except IntegrityError:
            self._rollback_safely()
            return self._insert_row_by_row(articles)
        except Exception as exc:
            self._rollback_safely()
            error_msg = f"Database bulk insert failed: {exc}"
            logger.error(error_msg)
            return 0, [], error_msg

    def _insert_row_by_row(
        self, articles: List[Dict[str, Any]]
    ) -> Tuple[int, List[int], Optional[str]]:
        """Retry after a bulk uniqueness collision, isolating each row in a savepoint."""
        stored_ids: List[int] = []
        try:
            for article in articles:
                data = dict(article)
                tickers = tuple(dict.fromkeys(data.pop("matched_tickers", ())))
                try:
                    with self._session.begin_nested():
                        obj = Headline(**data)
                        self._session.add(obj)
                        self._session.flush()
                        self._session.add_all([
                            ArticleSecurity(article_id=obj.id, ticker=ticker)
                            for ticker in tickers
                        ])
                        self._session.flush()
                    stored_ids.append(obj.id)
                except IntegrityError:
                    logger.info("RSS duplicate skipped during row retry: url=%s", data.get("url"))
            self._session.commit()
            return len(stored_ids), stored_ids, None
        except Exception as exc:
            self._rollback_safely()
            msg = f"Database row-by-row retry failed: {exc}"
            logger.error(msg)
            return 0, [], msg

    def _rollback_safely(self) -> None:
        """Attempt a session rollback without masking the original exception."""
        try:
            self._session.rollback()
        except Exception as exc:
            logger.error("Session rollback failed: %s", exc)

    # ------------------------------------------------------------------
    # Private: response builders
    # ------------------------------------------------------------------

    def _success_response(
        self,
        *,
        feed_url: str,
        fetched: int,
        saved: int,
        duplicates: int,
        invalid: int,
        stored_ids: List[int],
        errors: List[str],
    ) -> IngestionResponse:
        return IngestionResponse(
            success=True,
            endpoint=feed_url,
            fetched=fetched,
            saved=saved,
            duplicates=duplicates,
            invalid=invalid,
            errors=errors,
            fallback_used=False,
            processed_at=self._current_time(),
            stored_ids=stored_ids,
            error=None,
        )

    def _failure_response(
        self,
        *,
        feed_url: str,
        error: str,
        errors: List[str],
    ) -> IngestionResponse:
        return IngestionResponse(
            success=False,
            endpoint=feed_url,
            fetched=0,
            saved=0,
            duplicates=0,
            invalid=0,
            errors=errors,
            fallback_used=False,
            processed_at=self._current_time(),
            stored_ids=[],
            error=error,
        )

    def _empty_response(
        self,
        *,
        feed_url: str,
        fetched: int,
    ) -> IngestionResponse:
        return IngestionResponse(
            success=True,
            endpoint=feed_url,
            fetched=fetched,
            saved=0,
            duplicates=0,
            invalid=0,
            errors=[],
            fallback_used=False,
            processed_at=self._current_time(),
            stored_ids=[],
            error=None,
        )

    # ------------------------------------------------------------------
    # Private: string / datetime utilities
    # ------------------------------------------------------------------

    def _current_time(self) -> datetime:
        """Return the current timezone-aware Nairobi timestamp."""
        return datetime.now(NAIROBI_TZ)

    def _parse_iso_timestamp(self, value: Any) -> Optional[datetime]:
        """
        Parse an ISO-8601 timestamp string into a Nairobi-aware datetime.

        Mirrors NewsService._parse_datetime() for consistent timestamp
        handling regardless of whether the article arrived via RSS or NewsAPI.

        Args:
            value: ISO timestamp string (may be None).

        Returns:
            Timezone-aware datetime in Africa/Nairobi, or None on failure.
        """
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str) and value.strip():
            try:
                parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            except ValueError:
                logger.debug("Invalid publishedAt timestamp; using None: %r", value)
                return None
        else:
            return None

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=NAIROBI_TZ)

        return parsed.astimezone(NAIROBI_TZ)

    @staticmethod
    def _parse_published(
        entry: feedparser.FeedParserDict,
    ) -> Optional[datetime]:
        """
        Extract a timezone-aware Nairobi datetime from the RSS entry's
        published_parsed struct_time (always UTC from feedparser).

        Args:
            entry: Raw feedparser entry.

        Returns:
            Timezone-aware datetime (Africa/Nairobi) or None.
        """
        struct = entry.get("published_parsed") or entry.get("updated_parsed")

        if struct is None:
            return None

        try:
            utc_dt = datetime(*struct[:6], tzinfo=timezone.utc)
            return utc_dt.astimezone(ZoneInfo("Africa/Nairobi"))
        except Exception as exc:
            logger.debug("Could not parse published date: %s", exc)
            return None

    @staticmethod
    def _clean_str(value: object) -> Optional[str]:
        """
        Strip whitespace and unicode noise from a field value.

        Returns None for empty strings so callers can use simple
        ``if not field`` guards without also checking for blank strings.
        """
        if not value:
            return None
        cleaned = (
            str(value)
            .replace("\xa0", " ")
            .replace("\u200b", "")
            .replace("\ufeff", "")
            .strip()
        )
        return cleaned if cleaned else None

    @staticmethod
    def _truncate(value: Optional[str], max_length: int) -> Optional[str]:
        """Trim a string to fit a database column's max length."""
        if value is None:
            return None
        return value[:max_length].rstrip() if len(value) > max_length else value


# ---------------------------------------------------------------------------
# CLI smoke-test:  python scrapers/rss_news_ingestor.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    print("\nRSSNewsIngestor — feed registry preview\n")
    print(f"Total configured feeds: {len(DEFAULT_FEEDS)}\n")

    for i, feed in enumerate(DEFAULT_FEEDS, 1):
        print(f"  {i:>2}. [{feed['label']:<30}]  {feed['url']}")

    print(
        "\nTo run a live ingestion test:\n"
        "    from database.session import SessionLocal\n"
        "    from scrapers.rss_news_ingestor import RSSNewsIngestor\n"
        "\n"
        "    db = SessionLocal()\n"
        "    result = RSSNewsIngestor(db).ingest_feed(\n"
        "        \"https://www.standardmedia.co.ke/rss/business.php\",\n"
        "        # source_label=\"The Standard Business\",\n",
        "         region=\"kenya\",\n   "
        "    )\n"
        "    print(result)\n"
        "\n"
        "To ingest all default feeds:\n"
        "    results = RSSNewsIngestor.ingest_all_feeds(db)\n"
        "    print(sum(r.saved for r in results), 'articles saved')\n"
    )