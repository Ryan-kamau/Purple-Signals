"""
intelligence/keyword_engine.py

Pure intelligence layer — keyword detection, category tagging, and impact
scoring for financial news articles sourced from NewsFetcher.

Pipeline position:
    news_fetcher.py  →  keyword_engine.py  →  news_service.py  →  DB  →  API

This module is intentionally stateless and side-effect-free:
  - No HTTP requests
  - No database access
  - No FastAPI routes
  - No sentiment analysis
  - No machine learning
  - No LLMs
  - No predictions

All behaviour is deterministic and fully testable without external
dependencies.  It enriches article dicts produced by NewsFetcher and passes
them downstream to NewsService for storage and further processing.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class KeywordEngine:
    """
    Deterministic keyword intelligence layer for financial news articles.

    Detects domain-relevant keywords, assigns intelligence categories, and
    computes weighted impact scores.  The output fields are appended to
    article dicts without touching any original NewsAPI fields — making the
    enrichment completely non-destructive.

    Class-level constants (KEYWORDS, WEIGHTS) are designed to be overridden
    by subclasses or extended at runtime, keeping the engine flexible as the
    platform expands to new markets and signal types.

    Example usage::

        from scrapers.news_fetcher import NewsFetcher
        from intelligence.keyword_engine import KeywordEngine

        fetcher = NewsFetcher()
        engine  = KeywordEngine()

        response = fetcher.fetch_top_headlines(country="ke", category="business")
        enriched = engine.enrich_articles(response["articles"])

        for article in enriched:
            print(article["keywords"], article["impact_score"])
    """

    # ------------------------------------------------------------------
    # Keyword category definitions
    # All keywords are stored in lower-case for O(1) membership tests and
    # case-insensitive matching.
    # ------------------------------------------------------------------

    KEYWORDS: Dict[str, List[str]] = {
        "macro_economy": [
            "inflation",
            "interest rates",
            "debt",
            "tax",
            "imf",
            "cbk",
        ],
        "energy_sector": [
            "fuel",
            "electricity",
            "tariff",
            "epra",
            "subsidy",
            "power outage",
        ],
        "geopolitics": [
            "war",
            "sanctions",
            "china",
            "usa",
            "russia",
            "oil",
            "middle east",
            "shipping",
        ],
        "market_stress": [
            "losses",
            "profit warning",
            "decline",
            "shortage",
            "crisis",
        ],
        "kenya_policy": [
            "treasury",
            "parliament",
            "finance bill",
            "budget",
            "cbk",
            "kra",
            "epra",
            "ministry",
        ],
    }

    # ------------------------------------------------------------------
    # Impact weight table
    # Keywords not listed here receive the default weight of 1.
    # Weights reflect expected market-moving potential; calibrate over
    # time as the platform accumulates signal-to-noise data.
    # ------------------------------------------------------------------

    WEIGHTS: Dict[str, int] = {
        "war": 5,
        "interest rates": 5,
        "oil": 4,
        "inflation": 4,
        "cbk": 4,
        "fuel": 3,
        "sanctions": 3,
        "crisis": 3,
        "profit warning": 3,
        "finance bill": 3,
        "budget": 2,
        "tariff": 2,
        "debt": 2,
        "imf": 2,
        "electricity": 2,
        "epra": 2,
    }

    DEFAULT_WEIGHT: int = 1

    # ------------------------------------------------------------------
    # Internal flat keyword lookup built once at class definition time.
    # Avoids re-iterating KEYWORDS on every article.
    # Maps keyword → category (last write wins for keywords that appear
    # in multiple categories — e.g. "cbk" appears in macro_economy and
    # kenya_policy; the lookup is only used for fast membership tests so
    # the exact category-per-keyword mapping is not critical here).
    # ------------------------------------------------------------------

    _KEYWORD_TO_CATEGORIES: Dict[str, List[str]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._build_lookup()

    @classmethod
    def _build_lookup(cls) -> None:
        """Rebuild the internal keyword→categories lookup from KEYWORDS."""
        mapping: Dict[str, List[str]] = {}
        for category, keywords in cls.KEYWORDS.items():
            for kw in keywords:
                mapping.setdefault(kw.lower(), []).append(category)
        cls._KEYWORD_TO_CATEGORIES = mapping

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def extract_keywords(self, text: str) -> List[str]:
        """
        Detect and return unique financial keywords present in *text*.

        Matching is case-insensitive and duplicate-safe: a keyword that
        appears multiple times is returned only once.

        Args:
            text: Raw combined article text (title + description + content).

        Returns:
            Alphabetically sorted list of unique matched keywords.

        Example::

            engine.extract_keywords(
                "Fuel prices rise as oil markets react to Middle East war fears."
            )
            # → ["fuel", "middle east", "oil", "war"]
        """
        if not text or not isinstance(text, str):
            return []

        normalised = text.lower()
        found: Set[str] = set()

        for category_keywords in self.KEYWORDS.values():
            for keyword in category_keywords:
                kw_lower = keyword.lower()
                # Use word-boundary-aware search for single-word keywords;
                # multi-word phrases are matched as substrings (sufficient
                # for the current keyword set and avoids false negatives).
                if " " in kw_lower:
                    if kw_lower in normalised:
                        found.add(kw_lower)
                else:
                    pattern = r"\b" + re.escape(kw_lower) + r"\b"
                    if re.search(pattern, normalised):
                        found.add(kw_lower)

        matched = sorted(found)
        logger.debug("extract_keywords: found %d keyword(s): %s", len(matched), matched)
        return matched

    def extract_categories(self, text: str) -> List[str]:
        """
        Return intelligence categories that have at least one keyword match
        in *text*.

        Args:
            text: Raw combined article text.

        Returns:
            Alphabetically sorted list of unique matched category names.

        Example::

            engine.extract_categories(
                "Fuel prices rise as oil markets react to Middle East war fears."
            )
            # → ["energy_sector", "geopolitics"]
        """
        if not text or not isinstance(text, str):
            return []

        matched_keywords = self.extract_keywords(text)
        matched_keyword_set = set(matched_keywords)

        categories: Set[str] = set()
        for category, keywords in self.KEYWORDS.items():
            for kw in keywords:
                if kw.lower() in matched_keyword_set:
                    categories.add(category)
                    break  # one match is enough for the category

        result = sorted(categories)
        logger.debug("extract_categories: matched categories %s", result)
        return result

    def calculate_impact_score(self, keywords: List[str]) -> int:
        """
        Compute a weighted impact score from a list of detected keywords.

        Each keyword contributes its value from WEIGHTS, or DEFAULT_WEIGHT
        (1) if it is not explicitly weighted.

        Args:
            keywords: List of keyword strings as returned by
                      :meth:`extract_keywords`.

        Returns:
            Non-negative integer impact score.

        Example::

            engine.calculate_impact_score(["fuel", "oil", "middle east", "war"])
            # → 13  (fuel=3, oil=4, middle east=1, war=5)
        """
        if not keywords:
            return 0

        score: int = 0
        for kw in keywords:
            weight = self.WEIGHTS.get(kw.lower(), self.DEFAULT_WEIGHT)
            score += weight

        logger.debug(
            "calculate_impact_score: keywords=%s → score=%d", keywords, score
        )
        return score

    def count_keyword_matches(self, keywords: List[str]) -> int:
        """
        Return the count of unique matched keywords.

        Kept as a dedicated method (rather than inlining ``len(keywords)``)
        because future ranking algorithms may apply additional filters —
        for example, counting only high-weight keywords — without changing
        the public interface.

        Args:
            keywords: List of keyword strings.

        Returns:
            Integer count.
        """
        return len(keywords)

    def enrich_article(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single NewsAPI article dict with keyword intelligence fields.

        The original article fields are never modified.  Intelligence fields
        are appended as new keys:

        - ``keywords_detected``              – list of detected keyword strings
        - ``categories``            – list of matched category names
        - ``matched_keywords_count``– integer count of matched keywords
        - ``impact_score``          – weighted integer impact score

        Invalid or non-dict inputs are returned with zeroed intelligence
        fields so downstream code never receives ``None`` values.

        Args:
            article: A raw NewsAPI article dict (may contain title,
                     description, content, source, url, publishedAt, etc.).

        Returns:
            New dict containing all original fields plus intelligence fields.

        Example::

            enriched = engine.enrich_article({
                "title": "Fuel prices rise",
                "description": "Oil markets rally after war fears.",
                "content": "Analysts warned of sustained pressure...",
            })
            # enriched["keywords"]   → ["fuel", "oil", "war"]
            # enriched["categories"] → ["energy_sector", "geopolitics"]
            # enriched["impact_score"] → 12
        """
        empty_intelligence = {
            "keywords_detected": [],
            "categories": [],
            "matched_keywords_count": 0,
            "impact_score": 0,
        }

        if not isinstance(article, dict):
            logger.warning(
                "enrich_article: received non-dict input (%s); returning with "
                "zeroed intelligence fields.",
                type(article).__name__,
            )
            return {**(article if isinstance(article, dict) else {}), **empty_intelligence}

        combined_text = self._build_combined_text(article)

        keywords = self.extract_keywords(combined_text)
        categories = self.extract_categories(combined_text)
        impact_score = self.calculate_impact_score(keywords)
        keyword_count = self.count_keyword_matches(keywords)

        logger.debug(
            "enrich_article: title=%r keywords=%d impact=%d",
            str(article.get("title", ""))[:60],
            keyword_count,
            impact_score,
        )

        return {
            **article,
            "keywords_detected": keywords,
            "categories": categories,
            "matched_keywords_count": keyword_count,
            "impact_score": impact_score,
        }

    def enrich_articles(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich a batch of NewsAPI article dicts with keyword intelligence.

        Internally calls :meth:`enrich_article` for each item.  Invalid
        entries (non-dict) are included in the output with zeroed
        intelligence fields so the list length is always preserved and
        callers can rely on index correspondence.

        Args:
            articles: List of raw NewsAPI article dicts.

        Returns:
            List of enriched article dicts in the same order as the input.

        Example::

            enriched_batch = engine.enrich_articles(response["articles"])
        """
        if not articles:
            logger.debug("enrich_articles: received empty article list.")
            return []

        if not isinstance(articles, list):
            logger.warning(
                "enrich_articles: expected list, got %s; returning empty list.",
                type(articles).__name__,
            )
            return []

        enriched = [self.enrich_article(article) for article in articles]

        total_keywords = sum(a.get("matched_keywords_count", 0) for a in enriched)
        total_impact = sum(a.get("impact_score", 0) for a in enriched)

        logger.info(
            "enrich_articles: processed %d article(s) | "
            "total_keywords=%d | total_impact_score=%d",
            len(enriched),
            total_keywords,
            total_impact,
        )

        return enriched

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_combined_text(self, article: Dict[str, Any]) -> str:
        """
        Safely combine title, description, and content into a single
        lower-case searchable string.

        Handles None values, missing keys, and non-string field values
        without raising exceptions.

        Args:
            article: Raw NewsAPI article dict.

        Returns:
            Single combined string (may be empty if all fields are absent).
        """
        parts: List[str] = []

        for field in ("title", "description", "content"):
            value = article.get(field)
            if value and isinstance(value, str):
                parts.append(value.strip())

        combined = " ".join(parts)
        logger.debug(
            "_build_combined_text: assembled %d char(s) from article fields.",
            len(combined),
        )
        return combined


# ---------------------------------------------------------------------------
# Rebuild the internal lookup on the base class now that the class body has
# been fully evaluated.  (The classmethod call in the class body runs before
# KEYWORDS is fully defined when Python evaluates class-level expressions, so
# we re-run it here to guarantee correctness.)
# ---------------------------------------------------------------------------
KeywordEngine._build_lookup()


# ---------------------------------------------------------------------------
# Quick smoke-test — run directly:   python intelligence/keyword_engine.py
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    engine = KeywordEngine()

    sample_articles = [
        {
            "title": "Fuel prices rise as oil markets react to Middle East war fears",
            "description": "CBK holds interest rates steady amid inflation concerns.",
            "content": "Analysts cited debt pressures and IMF projections as key risks.",
        },
        {
            "title": "Kenya parliament debates Finance Bill amid treasury pressure",
            "description": "EPRA reviews electricity tariff ahead of subsidy review.",
            "content": "The ministry confirmed no immediate changes to the budget.",
        },
        {
            "title": "Global shipping disrupted by Russia sanctions and China trade slowdown",
            "description": "Oil futures fell on demand fears.",
            "content": "Market stress indicators rose after profit warning from major carriers.",
        },
    ]

    enriched = engine.enrich_articles(sample_articles)

    print("\n" + "=" * 70)
    print("KEYWORD ENGINE — ENRICHED OUTPUT")
    print("=" * 70)

    for i, article in enumerate(enriched, 1):
        print(f"\n[Article {i}]")
        print(f"  Title      : {article['title']}")
        print(f"  Keywords   : {article['keywords_detected']}")
        print(f"  Categories : {article['categories']}")
        print(f"  KW Count   : {article['matched_keywords_count']}")
        print(f"  Impact     : {article['impact_score']}")