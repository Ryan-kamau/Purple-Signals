"""
intelligence/relevance_filter.py

Stateless relevance gate: decides whether an enriched article is worth storing.
Pipeline: RSS -> KeywordEngine -> RelevanceFilter -> Headline DB

No I/O. NSE_COMPANY_ALIASES is a stopgap until the `securities` table exists
(PRD section 10); swap find_tickers() to read from it without changing callers.
"""
import re
from dataclasses import dataclass
from typing import Any, Dict, Tuple

# ticker -> lowercase aliases.
# Avoid generic or highly ambiguous words that can cause false company matches.
NSE_COMPANY_ALIASES: Dict[str, Tuple[str, ...]] = {
    "SCOM": (
        "safaricom",
        "safaricom plc",
    ),
    "EQTY": (
        "equity group",
        "equity group holdings",
        "equity bank",
        "equity",
    ),
    "KCB": (
        "kcb",
        "kcb group",
        "kenya commercial bank",
    ),
    "EABL": (
        "eabl",
        "east african breweries",
        "east african breweries limited",
    ),
    "COOP": (
        "co-op bank",
        "co-operative bank",
        "cooperative bank",
        "coop bank",
        "co-op",
    ),
    "ABSA": (
        "absa",
        "absa bank",
        "absa bank kenya",
        "absa kenya",
    ),
    "KPC": (
        "kenya pipeline",
        "kenya pipeline company",
        "kpc",
    ),
    "IMH": (
        "i&m",
        "i&m group",
        "i&m holdings",
        "i&m bank",
    ),
    "NCBA": (
        "ncba",
        "ncba group",
        "ncba bank",
    ),
    "SCBK": (
        "standard chartered",
        "standard chartered kenya",
        "standard chartered bank kenya",
        "stanchart",
        "stanchart kenya",
    ),
    "SBIC": (
        "stanbic",
        "stanbic holdings",
        "stanbic bank kenya",
    ),
    "KEGN": (
        "kengen",
        "kengen plc",
        "kenya electricity generating company",
    ),
    "KPLC": (
        "kplc",
        "kenya power",
        "kenya power and lighting",
        "kenya power & lighting",
    ),
    "BAT": (
        "bat",
        "bat kenya",
        "british american tobacco",
        "british american tobacco kenya",
    ),
    "KQ": (
        "kenya airways",
        "kenya airways plc",
        "kq",
    ),
    "BAMB": (
        "bamburi",
        "bamburi cement",
        "bamburi cement plc",
    ),
    "DTK": (
        "diamond trust bank",
        "diamond trust bank kenya",
        "dtb",
        "dtb kenya",
    ),
    "HFCK": (
        "hf group",
        "housing finance",
        "housing finance group",
        "housing finance company of kenya",
    ),
    "FMLY": (
        "family bank",
        "family bank kenya",
    ),
    "CGEN": (
        "car and general",
        "car & general",
        "car and general kenya",
    ),
    "NMG": (
        "nation media group",
        "nation media",
        "nmg",
    ),
    "SASN": (
        "sasini",
        "sasini plc",
    ),
    "KUKZ": (
        "kakuzi",
        "kakuzi plc",
    ),
    "KAPC": (
        "kapchorua",
        "kapchorua tea",
        "kapchorua tea kenya",
    ),
    "LIMT": (
        "limuru tea",
        "limuru tea company",
    ),
    "WTK": (
        "williamson tea",
        "williamson tea kenya",
    ),
    "EGAD": (
        "eaagads",
        "eaagads limited",
    ),
    "UNGA": (
        "unga",
        "unga group",
    ),
    "CARB": (
        "carbacid",
        "carbacid investments",
    ),
    "BOC": (
        "boc kenya",
        "boc",
    ),
    "EVRD": (
        "eveready",
        "eveready east africa",
    ),
    "ORCH": (
        "kenya orchards",
        "kenya orchards limited",
    ),
    "MSC": (
        "mumias sugar",
        "mumias sugar company",
        "mumias sugar co",
    ),
    "BAUM": (
        "baumann",
        "a. baumann",
        "a baumann",
    ),
    "CIC": (
        "cic insurance",
        "cic insurance group",
        "cic group",
    ),
    "BRIT": (
        "britam",
        "britam holdings",
    ),
    "JUB": (
        "jubilee holdings",
        "jubilee",
    ),
    "KNRE": (
        "kenya re",
        "kenya reinsurance",
        "kenya reinsurance corporation",
    ),
    "CFCI": (
        "liberty kenya",
        "liberty kenya holdings",
        "liberty life kenya",
    ),
    "CTUM": (
        "centum",
        "centum investment",
        "centum investment company",
    ),
    "OCH": (
        "olympia capital",
        "olympia capital holdings",
    ),
    "TCL": (
        "transcentury",
        "trans-century",
        "trans-century investments",
    ),
    "XPRS": (
        "express kenya",
        "express kenya plc",
    ),
    "HBE": (
        "homeboyz entertainment",
        "homeboyz",
    ),
    "FTGH": (
        "flame tree",
        "flame tree group",
        "flame tree group holdings",
    ),
    "HAFR": (
        "home africa",
        "home afrika",
        "home afrika limited",
    ),
}



_COMPANY_PATTERNS = {
    ticker: re.compile(r"\b(?:" + "|".join(re.escape(a) for a in aliases) + r")\b", re.IGNORECASE)
    for ticker, aliases in NSE_COMPANY_ALIASES.items()
}

# Institutions/places only -- no politicians' names (they go stale).
_KENYA_ANCHOR = re.compile(
    r"\b(?:kenya|kenyan|kenyans|nairobi|mombasa|lamu|nse|cbk|kra|epra|ketraco|kengen|ksh|shilling)\b|\bsh\d",
    re.IGNORECASE,
)

# Main transmission channels from global events to Kenya.
_GLOBAL_CHANNEL = re.compile(
    r"\b(?:"
    # Oil / energy
    r"oil|brent|crude|opec|opec\+|"
    r"fuel prices?|energy prices?|"
    r"hormuz|strait of hormuz|red sea|suez canal|"
    # US rates / dollar
    r"federal reserve|fed|fed rate|rate cut|rate hike|"
    r"interest rates?|"
    r"us dollar|dollar index|dxy|dollar strength|"
    # Global capital / emerging markets
    r"emerging markets?|frontier markets?|"
    r"capital flows?|foreign capital|"
    r"risk[- ]on|risk[- ]off|"
    # Global borrowing
    r"eurobonds?|sovereign bonds?|"
    r"bond yields?|treasury yields?|"
    # Major central banks
    r"ecb|european central bank|"
    r"bank of england|boe|"
    r"bank of japan|boj|"
    # China / global growth
    r"china economy|chinese economy|"
    r"china growth|chinese growth|"
    r"global growth|global slowdown|"
    r"recession|"
    # Geopolitical / trade shocks
    r"geopolitical tensions?|"
    r"trade war|tariffs?|sanctions?|"
    r"russia[- ]ukraine|ukraine war|"
    r"middle east conflict|"
    # Commodity shocks
    r"commodity prices?|"
    r"food prices?|"
    r"wheat prices?|"
    r"fertilizer prices?"
    r")\b",
    re.IGNORECASE,
)

@dataclass(frozen=True)
class RelevanceResult:
    keep: bool
    tier: str                      # company | kenya_macro | global_channel | rejected
    tickers: Tuple[str, ...] = ()  # feeds the future article_securities table


class RelevanceFilter:
    KENYA_CATEGORIES = frozenset({"macro_economy", "energy_sector", "kenya_policy"})
    MIN_KENYA_IMPACT = 3
    MIN_KENYA_KEYWORDS = 2
    MIN_GLOBAL_IMPACT = 4

    def find_tickers(self, text: str) -> Tuple[str, ...]:
        return tuple(t for t, p in _COMPANY_PATTERNS.items() if p.search(text))

    def evaluate(self, article: Dict[str, Any], *, is_local: bool) -> RelevanceResult:
        title = str(article.get("title") or "")
        scope = f"{title} {article.get('description') or ''}"

        tickers = self.find_tickers(scope)
        if tickers:
            return RelevanceResult(True, "company", tickers)

        impact = int(article.get("impact_score") or 0)
        count = int(article.get("matched_keywords_count") or 0)
        categories = set(article.get("categories") or [])

        anchored = is_local or bool(_KENYA_ANCHOR.search(scope))
        if (
            anchored
            and categories & self.KENYA_CATEGORIES
            and (impact >= self.MIN_KENYA_IMPACT or count >= self.MIN_KENYA_KEYWORDS)
        ):
            return RelevanceResult(True, "kenya_macro")

        if not is_local and _GLOBAL_CHANNEL.search(title) and impact >= self.MIN_GLOBAL_IMPACT:
            return RelevanceResult(True, "global_channel")

        return RelevanceResult(False, "rejected")