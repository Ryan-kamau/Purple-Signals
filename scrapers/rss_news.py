"""
scrapers/rss_news_ingestor.py

RSS News Ingestion Service — Kenya Intelligence Platform

Pipeline position:
    RSS Feeds  →  RSSNewsIngestor  →  KeywordEngine  →  Headline DB  →  API

This module mirrors the NewsService pipeline exactly:
  1. Fetch raw entries from RSS/Atom feeds via feedparser
  2. Normalise raw feedparser entries into article dicts
  3. Run KeywordEngine.enrich_articles() on the full batch
  4. Filter out low-impact articles (impact_score < IMPACT_SCORE_THRESHOLD)
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

import feedparser
from sqlalchemy.orm import Session

from intelligence.keywords_engine import KeywordEngine
from models.headline_data import Headline
from schemas.headline import IngestionResponse

logger = logging.getLogger(__name__)

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

# Minimum weighted impact score required for an article to be persisted.
# Mirrors the threshold used in NewsService._store_fetch_response().
# Articles scoring below this are logged and silently discarded.
IMPACT_SCORE_THRESHOLD: int = 2

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
    {
        "label": "Business Daily Africa",
        "url": "https://news.google.com/rss/search?q=Business+Daily+Africa&hl=en-KE&gl=KE&ceid=KE:en",
    },
    {
        "label": "The Standard Business",
        "url": "https://www.standardmedia.co.ke/rss/business.php",
    },
    {
        "label": "Nation Business",
        "url": "https://news.google.com/rss/search?q=site:nation.africa+business&hl=en-KE&gl=KE&ceid=KE:en",
    },
    {
        "label": "Capital FM Business",
        "url": "https://www.capitalfm.co.ke/business/feed/",
    },
    # ── Global ───────────────────────────────────────────────────────────────
    {
        "label": "Reuters Business",
        "url": "https://feeds.reuters.com/reuters/businessNews",
    },
    {
        "label": "CNBC Markets",
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839135",
    },
    {
        "label": "Financial Times World",
        "url": "https://www.ft.com/world?format=rss",
    },
    {
        "label": "IMF News",
        "url": "https://www.imf.org/en/News/rss?language=eng",
    },
    {
        "label": "World Bank News",
        "url": "https://news.google.com/rss/search?q=World+Bank&hl=en-KE&gl=KE&ceid=KE:en",
    },
]


# ---------------------------------------------------------------------------
# RSSNewsIngestor
# ---------------------------------------------------------------------------


class RSSNewsIngestor:
    """
    Fetches one RSS feed, enriches entries with KeywordEngine, deduplicates,
    and bulk-inserts new Headline rows in a single database transaction.

    The enrichment pipeline mirrors NewsService._store_fetch_response() exactly:
      - KeywordEngine.enrich_articles() runs on the normalised batch
      - Articles below IMPACT_SCORE_THRESHOLD are discarded (default: 2)
      - Intelligence fields (keywords_detected, categories, matched_keywords_count,
        impact_score) are persisted on every stored Headline row

    This ensures that RSS-sourced headlines produce identical DB records to
    headlines ingested via the NewsAPI path — both are queryable via the same
    /news API endpoints and filtered by the same KeywordEngine output.

    Args:
        session:                 SQLAlchemy session (caller owns the lifecycle).
        keyword_engine:          KeywordEngine instance. Inject a custom subclass
                                 to override keywords/weights without touching this
                                 file. Defaults to KeywordEngine().
        request_timeout:         HTTP timeout in seconds for feedparser. Default: 15.
        fallback_enabled:        Return synthetic articles when a feed is unavailable.
                                 Default: True.
        impact_score_threshold:  Minimum impact score for an article to be stored.
                                 Default: IMPACT_SCORE_THRESHOLD (2).

    Example::

        service = RSSNewsIngestor(db)
        result  = service.ingest_feed(
            "https://www.businessdailyafrica.com/rss/266",
            source_label="Business Daily Africa",
        )
        print(result.saved, "new articles stored")
    """

    def __init__(
        self,
        session: Session,
        keyword_engine: Optional[KeywordEngine] = None,
        request_timeout: int = 15,
        fallback_enabled: bool = True,
        impact_score_threshold: int = IMPACT_SCORE_THRESHOLD,
    ) -> None:
        self._session = session
        self._keyword_engine = keyword_engine or KeywordEngine()
        self._timeout = request_timeout
        self._fallback_enabled = fallback_enabled
        self._impact_score_threshold = impact_score_threshold

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def ingest_feed(
        self,
        feed_url: str,
        source_label: Optional[str] = None,
    ) -> IngestionResponse:
        """
        Fetch, enrich, validate, deduplicate, and persist articles from one RSS feed.

        Pipeline steps:
          1. Fetch the feed via feedparser.
          2. Validate that the feed has parseable entries.
          3. Normalise each entry into an article dict (NewsAPI-compatible shape).
          4. Run KeywordEngine.enrich_articles() on the full normalised batch.
          5. Filter articles below IMPACT_SCORE_THRESHOLD.
          6. Build Headline-ready dicts from enriched articles.
          7. Deduplicate against existing DB rows (chunked IN query).
          8. Bulk-insert new articles in a single transaction.

        Args:
            feed_url:     Full RSS/Atom feed URL.
            source_label: Human-readable source name used when the RSS entry
                          does not carry its own feed title.
                          Falls back to the feed's own ``feed.title`` or URL.

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

            if self._fallback_enabled:
                logger.warning("RSS fallback activated: url=%s", feed_url)
                return self._store_fallback(
                    feed_url=feed_url,
                    source_label=source_label or feed_url,
                    error_msg=error_msg,
                    errors=errors,
                )

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

            if self._fallback_enabled:
                logger.warning(
                    "RSS fallback activated after empty feed: url=%s", feed_url
                )
                return self._store_fallback(
                    feed_url=feed_url,
                    source_label=source_label or feed_url,
                    error_msg=validation_error or "Empty or invalid feed",
                    errors=errors,
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

        # ── Step 5: Filter by impact score ──────────────────────────────────
        # Mirrors NewsService threshold logic exactly.
        filtered_articles: List[Dict[str, Any]] = []
        low_impact_count = 0

        for article in enriched_articles:
            if article.get("impact_score", 0) < self._impact_score_threshold:
                logger.debug(
                    "RSS low-impact article skarded: headline=%r impact_score=%d",
                    str(article.get("title", ""))[:80],
                    article.get("impact_score", 0),
                )
                low_impact_count += 1
                continue
            filtered_articles.append(article)

        if low_impact_count:
            logger.info(
                "RSS impact filter: source=%s discarded=%d threshold=%d",
                resolved_source,
                low_impact_count,
                self._impact_score_threshold,
            )

        # ── Step 6: Build Headline-ready dicts from enriched articles ───────
        normalised_for_db = [
            self._build_headline_dict(article) for article in filtered_articles
        ]

        # ── Step 7: Deduplicate within batch + against DB ───────────────────
        new_articles, duplicate_count = self._deduplicate(normalised_for_db)

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
            "low_impact=%d saved=%d duplicates=%d invalid=%d duration=%.3fs",
            resolved_source,
            feed_url,
            fetched_count,
            len(enriched_articles),
            low_impact_count,
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
            invalid=invalid_count + low_impact_count,
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
        fallback_enabled: bool = True,
        impact_score_threshold: int = IMPACT_SCORE_THRESHOLD,
    ) -> List[IngestionResponse]:
        """
        Ingest every feed in the default registry (or a custom list).

        A single KeywordEngine instance is shared across all feeds to avoid
        re-building the keyword lookup table for each feed — same pattern
        used in NewsService where one engine instance handles all batches.

        Args:
            session:                Active SQLAlchemy session.
            feeds:                  Optional custom feed list. Each item must have
                                    ``url`` and ``label`` keys.
                                    Defaults to DEFAULT_FEEDS.
            keyword_engine:         Optional shared KeywordEngine instance.
            request_timeout:        HTTP timeout per feed in seconds.
            fallback_enabled:       Passed to each RSSNewsIngestor instance.
            impact_score_threshold: Minimum impact score to persist. Default: 2.

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
            fallback_enabled=fallback_enabled,
            impact_score_threshold=impact_score_threshold,
        )
        results: List[IngestionResponse] = []

        for feed_def in feed_list:
            url = feed_def.get("url", "")
            label = feed_def.get("label")

            if not url:
                logger.warning("Skipping feed with missing URL: %s", feed_def)
                continue

            result = service.ingest_feed(url, source_label=label)
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
            parsed = feedparser.parse(
                feed_url,
                agent="KPLCIntelligenceEngine/1.0",
                request_headers={
                    "Accept": "application/rss+xml, application/xml, text/xml",
                },
            )
        except Exception as exc:
            error = f"feedparser raised unexpected exception for {feed_url}: {exc}"
            logger.error("RSS fetch exception: url=%s error=%s", feed_url, exc)
            return None, error

        # feedparser sets bozo=True for malformed feeds but still returns
        # partial data. Only fatal if entries are absent too.
        if parsed.get("bozo"):
            bozo_exc = parsed.get("bozo_exception")
            if not parsed.get("entries"):
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
                len(parsed.get("entries", [])),
            )

        status = parsed.get("status")
        if status and status >= 400:
            error = f"RSS feed returned HTTP {status}: {feed_url}"
            logger.error("RSS fetch HTTP error: url=%s status=%s", feed_url, status)
            return None, error

        logger.info(
            "RSS fetch succeeded: url=%s entries=%d",
            feed_url,
            len(parsed.get("entries", [])),
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
        comma-joined strings — identical to how NewsService._normalize_article()
        handles this conversion.

        Intelligence field mapping (mirrors NewsService._normalize_article()):
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

        # Serialise lists to comma-joined strings — identical to NewsService
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
            "url": self._truncate(enriched_article.get("url") or "", 1000),
            "published_at": published_at,
            "timestamp": self._current_time(),

            # Intelligence fields — populated by KeywordEngine
            "keywords_detected": keywords_detected,
            "categories": categories_str,
            "matched_keywords_count": matched_keywords_count,
            "impact_score": impact_score,

            # Sentiment placeholder — populated by sentiment engine in Phase 2
            "sentiment_score": None,
        }

    # ------------------------------------------------------------------
    # Private: fallback
    # ------------------------------------------------------------------

    def _generate_fallback_articles(
        self,
        source: str,
        feed_url: str,
    ) -> List[Dict[str, Any]]:
        """
        Generate NewsAPI-shaped fallback articles for development and resilient
        scheduled jobs when a live feed is unavailable.

        Fallback articles use the same NewsAPI-compatible shape as
        _normalise_entry_to_newsapi_shape() output so KeywordEngine can
        enrich them identically to live articles.

        Args:
            source:   Source label for the unavailable feed.
            feed_url: Feed URL used in fallback URLs to keep them unique.

        Returns:
            List of three NewsAPI-shaped article dicts.
        """
        now_iso = self._current_time().isoformat()
        url_base = feed_url.replace("https://", "").split("/")[0]

        articles = [
            {
                "title": f"Markets monitor macro signals — {source} feed unavailable",
                "description": (
                    "Investors tracked central-bank guidance, currency moves, and "
                    "commodity prices as analysts assessed the latest risk tone. "
                    "Fallback article generated while the feed was unreachable."
                ),
                "content": (
                    "Market participants focused on policy direction, liquidity "
                    "conditions, and cross-asset volatility. Inflation and interest "
                    "rate expectations remained key variables driving sentiment."
                ),
                "url": f"https://example.com/rss-fallback/{url_base}/macro-signals",
                "publishedAt": now_iso,
                "source": {"id": None, "name": source},
            },
            {
                "title": f"Companies review exposure amid uncertainty — {source} fallback",
                "description": (
                    "Corporate finance teams reviewed supply chains, financing "
                    "costs, and consumer demand against a mixed macro backdrop."
                ),
                "content": (
                    "Executives said planning assumptions remain sensitive to "
                    "interest rates, exchange rates, and fuel prices. IMF projections "
                    "and CBK policy guidance were cited as key inputs."
                ),
                "url": f"https://example.com/rss-fallback/{url_base}/company-exposure",
                "publishedAt": now_iso,
                "source": {"id": None, "name": source},
            },
            {
                "title": f"Analysts flag inflation and currency risks — {source} fallback",
                "description": (
                    "Research desks highlighted inflation expectations, foreign "
                    "exchange liquidity, and trade flows as key variables to watch."
                ),
                "content": (
                    "Analysts expect market attention to remain on inflation data, "
                    "fiscal policy, budget announcements, and currency stability. "
                    "EPRA tariff reviews and energy sector developments also flagged."
                ),
                "url": f"https://example.com/rss-fallback/{url_base}/inflation-risks",
                "publishedAt": now_iso,
                "source": {"id": None, "name": source},
            },
        ]

        logger.info(
            "RSS fallback articles generated: source=%s count=%d",
            source,
            len(articles),
        )
        return articles

    def _store_fallback(
        self,
        feed_url: str,
        source_label: str,
        error_msg: str,
        errors: List[str],
    ) -> IngestionResponse:
        """
        Enrich fallback articles through KeywordEngine, then persist them.

        Fallback articles go through the full enrichment + threshold pipeline
        so they produce properly tagged Headline rows with keywords_detected,
        categories, and impact_score populated — not empty stubs.

        Args:
            feed_url:     Original feed URL (used as endpoint reference).
            source_label: Resolved source name.
            error_msg:    Human-readable reason for fallback activation.
            errors:       Accumulated error list from earlier pipeline steps.

        Returns:
            IngestionResponse with fallback_used=True.
        """
        raw_fallback = self._generate_fallback_articles(source_label, feed_url)

        # Run through KeywordEngine — fallback articles are keyword-rich
        # by design so they reliably pass the impact_score threshold.
        enriched_fallback = self._keyword_engine.enrich_articles(raw_fallback)

        filtered_fallback = [
            a for a in enriched_fallback
            if a.get("impact_score", 0) >= self._impact_score_threshold
        ]

        normalised_for_db = [
            self._build_headline_dict(a) for a in filtered_fallback
        ]

        new_articles, duplicate_count = self._deduplicate(normalised_for_db)

        saved_count = 0
        stored_ids: List[int] = []

        # Do NOT persist fallback articles.
        # They are synthetic records used only to indicate feed failure.

        logger.warning(
            "Fallback articles generated but NOT stored: source=%s count=%d",
            source_label,
            len(filtered_fallback),
        )

        return IngestionResponse(
            success=True,
            endpoint=feed_url,
            fetched=len(raw_fallback),
            saved=0,
            duplicates=0,
            invalid=0,
            errors=errors + [error_msg],
            fallback_used=True,
            processed_at=self._current_time(),
            stored_ids=[],
            error=error_msg,
        )

    # ------------------------------------------------------------------
    # Private: deduplication
    # ------------------------------------------------------------------

    def _deduplicate(
        self, articles: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Remove articles whose URLs already exist in the database or appear
        more than once within the current batch.

        Uses a chunked IN query to stay within database parameter-count limits.

        Args:
            articles: Headline-ready article dicts (post-enrichment).

        Returns:
            Tuple of (new_articles, duplicate_count).
        """
        if not articles:
            return [], 0

        all_urls = [a["url"] for a in articles if a.get("url")]
        existing_urls = self._fetch_existing_urls(all_urls)

        new_articles: List[Dict[str, Any]] = []
        duplicate_count = 0
        seen_in_batch: Set[str] = set()

        for article in articles:
            url = article.get("url", "")
            if url in existing_urls or url in seen_in_batch:
                duplicate_count += 1
                continue
            seen_in_batch.add(url)
            new_articles.append(article)

        logger.debug(
            "Deduplication complete: total=%d new=%d duplicates=%d",
            len(articles),
            len(new_articles),
            duplicate_count,
        )

        return new_articles, duplicate_count

    def _fetch_existing_urls(self, urls: List[str]) -> Set[str]:
        """
        Return the subset of *urls* already present in the headlines table.

        Runs chunked IN queries (max _URL_CHUNK_SIZE per chunk) to avoid
        database parameter explosion on large feeds.

        Args:
            urls: List of URL strings to check (may contain duplicates).

        Returns:
            Set of URLs already stored in the database.
        """
        if not urls:
            return set()

        unique_urls = list(dict.fromkeys(urls))
        existing: Set[str] = set()

        for chunk_start in range(0, len(unique_urls), _URL_CHUNK_SIZE):
            chunk = unique_urls[chunk_start: chunk_start + _URL_CHUNK_SIZE]
            try:
                rows = (
                    self._session.query(Headline.url)
                    .filter(Headline.url.in_(chunk))
                    .all()
                )
                for row in rows:
                    url = row[0] if isinstance(row, tuple) else getattr(row, "url", None)
                    if url:
                        existing.add(url)
            except Exception as exc:
                logger.error(
                    "URL dedup query failed for chunk starting at %d: %s",
                    chunk_start,
                    exc,
                )
                # Don't abort — skip dedup for this chunk; worst case a
                # duplicate URL gets re-inserted which the unique index will catch.

        logger.debug(
            "Existing URL check: queried=%d existing=%d",
            len(unique_urls),
            len(existing),
        )
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
        so we can return ``stored_ids`` in the response — same approach
        as NewsService._save_articles().

        On failure: rolls back cleanly and returns (0, [], error_message).

        Args:
            articles: List of Headline-ready dicts from _build_headline_dict().

        Returns:
            Tuple of (saved_count, stored_ids, error_message | None).
        """
        if not articles:
            return 0, [], None

        try:
            orm_objects = [Headline(**article) for article in articles]
            self._session.add_all(orm_objects)
            self._session.flush()  # assigns IDs before commit

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

        except Exception as exc:
            self._rollback_safely()
            error_msg = f"Database bulk insert failed: {exc}"
            logger.error(error_msg)
            return 0, [], error_msg

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
        '        "https://www.businessdailyafrica.com/rss/266",\n'
        '        source_label="Business Daily Africa"\n'
        "    )\n"
        "    print(result)\n"
        "\n"
        "To ingest all default feeds:\n"
        "    results = RSSNewsIngestor.ingest_all_feeds(db)\n"
        "    print(sum(r.saved for r in results), 'articles saved')\n"
    )