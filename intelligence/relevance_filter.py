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

from config.nse_companies import NSE_COMPANIES, get_company_aliases

# Built once from the central registry; no separate alias definitions here.
_COMPANY_PATTERNS = {}
_AMBIGUOUS_TICKERS = {"ARM", "BAT", "BOC", "NSE", "PORT", "SCAN", "CIC", "OCH", "KQ"}
for ticker in NSE_COMPANIES:
    ticker_pattern = (
        None if ticker in _AMBIGUOUS_TICKERS
        else re.compile(rf"(?<!\w){re.escape(ticker)}(?!\w)")
    )
    aliases = get_company_aliases(ticker)
    name_pattern = re.compile(
        r"(?<!\w)(?:" + "|".join(re.escape(a) for a in sorted(aliases, key=len, reverse=True)) + r")(?!\w)",
        re.IGNORECASE,
    ) if aliases else None
    _COMPANY_PATTERNS[ticker] = (ticker_pattern, name_pattern)

# Institutions/places only -- no politicians' names (they go stale).
_KENYA_ANCHOR = re.compile(
    r"\b(?:kenya|kenyan|kenyans|nairobi|mombasa|lamu|nse|cbk|kra|kengen|ksh|shilling)\b|\bsh\d",
    re.IGNORECASE,
)

# Terms that signal a story moves Kenya's economy. Large Sh amounts (bn/tn) count too.
_KENYA_ECON = re.compile(
    r"\b(?:"
    
    # Monetary policy / Central Bank
    r"cbk|central bank(?: of kenya)?|monetary policy|"
    r"central bank rate|cbr|interest rates?|policy rate|"
    r"cash reserve ratio|crr|"

    # Government / fiscal policy / taxation
    r"treasury|national treasury|budget|budget deficit|"
    r"public debt|government debt|fiscal deficit|"
    r"tax(?:es|ation)?|vat|excise duty|tax revenue|"
    r"finance bill|appropriation bill|"

    # Government securities / debt markets
    r"t-?bills?|treasury bills?|treasury bonds?|"
    r"government bonds?|eurobonds?|bond yields?|"
    r"debt restructuring|debt servicing|sovereign debt|"
    
    # Currency / FX
    r"kenyan shilling|shilling|kes|ksh|"
    r"exchange rate|forex|foreign exchange|"
    r"currency depreciation|currency appreciation|"
    
    # Inflation / cost of living
    r"inflation|consumer prices?|cpi|"
    r"cost of living|food prices?|"
    r"fuel prices?|energy prices?|"

    # Trade / external sector
    r"exports?|imports?|trade deficit|trade surplus|"
    r"current account|balance of payments|"
    r"foreign reserves?|forex reserves?|"
    r"remittances?|diaspora remittances?|"
    r"foreign direct investment|fdi|"

    # Energy / fuel
    r"electricity|power tariffs?|"
    r"kplc|kenya power|"
    r"fuel|petroleum|crude oil|oil prices?|"
    r"refinery|power supply|blackouts?|"
    
    # Agriculture / food economy
    r"agriculture|agricultural|"
    r"maize|tea prices?|coffee prices?|"
    r"fertili[sz]er|food production|"
    r"drought|floods?|crop yields?|"

    # Financial sector
    r"banks?|lenders?|"
    r"banking sector|credit growth|"
    r"loans?|mortgages?|"
    r"non[- ]performing loans?|npl|"
    r"bad loans?|"
    
    # Capital markets
    r"nse|nairobi securities exchange|"
    r"stock market|shares?|equities?|"
    r"market capitalization|"
    r"stockbrokers?|"
    r"ipo|initial public offering|"
    
    # Major economic institutions / external lenders
    r"imf|international monetary fund|"
    r"world bank|afdb|african development bank|"
    r"credit rating|sovereign rating|"
    r"moodys|moody's|fitch|s&p|"

    # Business / competition / regulation
    r"kepsa|competition authority|cak|"
    r"cartel(?:s)?|"
    r"regulation|regulator|"
    
    # Employment / wages
    r"unemployment|employment|job creation|"
    r"wages?|minimum wage|payroll|"
    
    # Large Kenyan economic amounts
    r")\b"
    r"|\bsh\.?\s?\d[\d,.]*\s?(?:bn|billion|tn|trn|trillion)\b"
    r"|\bkes\s?\d[\d,.]*\s?(?:bn|billion|tn|trn|trillion)\b",
    re.IGNORECASE,
)

# Main transmission channels from global events to Kenya.
_GLOBAL_CHANNEL = re.compile(
    r"\b(?:"
    # Oil / energy
    r"oil|brent|crude|opec|opec\+|"
    r"fuel prices?|energy prices?|fuel costs?|fuel (?:spike|bill|surge)|jet fuel|"
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
    reason: str = ""                # optional human-readable explanation


class RelevanceFilter:
    MIN_KENYA_IMPACT = 2
    MIN_KENYA_KEYWORDS = 2
    MIN_GLOBAL_IMPACT = 3

    def find_tickers(self, text: str) -> Tuple[str, ...]:
        matches = []

        for ticker, (ticker_pattern, name_pattern) in _COMPANY_PATTERNS.items():
            if ticker_pattern is not None and ticker_pattern.search(text):
                matches.append(ticker)
            elif name_pattern is not None and name_pattern.search(text):
                matches.append(ticker)

        return tuple(matches)
    def evaluate(self, article: Dict[str, Any], *, is_local: bool) -> RelevanceResult:
        title = str(article.get("title") or "")
        scope = f"{title} {article.get('description') or ''}"

        tickers = self.find_tickers(scope)
        if tickers:
            return RelevanceResult(True, "company", tickers)

        impact = int(article.get("impact_score") or 0)
        count = int(article.get("matched_keywords_count") or 0)
        categories = set(article.get("categories") or [])
        econ = bool(_KENYA_ECON.search(scope))

        anchored = is_local or bool(_KENYA_ANCHOR.search(scope))
        if (
            anchored
            and categories 
            and econ
            and (impact >= self.MIN_KENYA_IMPACT or count >= self.MIN_KENYA_KEYWORDS)
        ):
            return RelevanceResult(True, "kenya_macro")

        if not is_local and _GLOBAL_CHANNEL.search(title) and impact >= self.MIN_GLOBAL_IMPACT:
            return RelevanceResult(True, "global_channel")
        
        reason = (f"anchored={anchored} econ={econ} cats={sorted(categories)} impact={impact} "
          f"count={count} global_hit={bool(_GLOBAL_CHANNEL.search(title))} local={is_local}")
        return RelevanceResult(False, "rejected", reason=reason)