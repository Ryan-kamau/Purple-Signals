"""
Central Bank of Kenya scraper service.
"""

import logging
import re
from datetime import datetime, timedelta, timezone
from io import BytesIO, StringIO
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from fake_useragent import UserAgent
except Exception:
    UserAgent = None

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None

try:
    import pdfplumber
except Exception:
    pdfplumber = None


logger = logging.getLogger(__name__)

CBK_BASE_URL = "https://www.centralbank.go.ke"
FOREX_URL = f"{CBK_BASE_URL}/rates/forex-exchange-rates/"
CBR_URL = f"{CBK_BASE_URL}/rates/central-bank-rate/"
PRESS_URL = f"{CBK_BASE_URL}/press/"
HOME_URL = f"{CBK_BASE_URL}/"

SOURCE_NAME = "CBK"

EAT = timezone(timedelta(hours=3))

TARGET_FOREX_CURRENCIES = {
    "US DOLLAR": ("USD", "USD/KES"),
    "USD": ("USD", "USD/KES"),
    "STG POUND": ("GBP", "GBP/KES"),
    "STERLING POUND": ("GBP", "GBP/KES"),
    "POUND STERLING": ("GBP", "GBP/KES"),
    "BRITISH POUND": ("GBP", "GBP/KES"),
    "GBP": ("GBP", "GBP/KES"),
    "EURO": ("EUR", "EUR/KES"),
    "EUR": ("EUR", "EUR/KES"),
}


def _build_headers() -> Dict[str, str]:

    user_agent = "Mozilla/5.0"

    if UserAgent is not None:
        try:
            user_agent = UserAgent().random
        except Exception:
            pass

    return {
        "User-Agent": user_agent,
        "Accept": (
            "text/html,application/xhtml+xml,"
            "application/xml;q=0.9,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }


def create_session() -> Session:

    session = requests.Session()

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry)

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(_build_headers())

    return session


_DEFAULT_SESSION: Optional[Session] = None
_PAGE_CACHE: Dict[str, Optional[Response]] = {}


def get_default_session() -> Session:

    global _DEFAULT_SESSION

    if _DEFAULT_SESSION is None:
        _DEFAULT_SESSION = create_session()

    return _DEFAULT_SESSION


def now_iso() -> str:
    return datetime.now(EAT).isoformat()


def clean_text(value: Any) -> str:

    if value is None:
        return ""

    text = str(value)

    text = (
        text.replace("\xa0", " ")
        .replace("\u200b", "")
        .replace("\ufeff", "")
        .replace("\r", " ")
        .replace("\n", " ")
        .replace("\t", " ")
    )

    text = re.sub(r"\s+", " ", text)

    return text.strip()


def clean_float(value: Any) -> Optional[float]:

    if value is None:
        return None

    text = clean_text(value)

    if not text:
        return None

    text = text.replace(",", "")

    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)

    if not match:
        return None

    try:
        return float(match.group(0))
    except Exception:
        return None


def normalize_date(
    value: Any,
    default_tz: timezone = EAT,
) -> Optional[str]:

    text = clean_text(value)

    if not text:
        return None

    try:
        parsed = parser.parse(text, dayfirst=True, fuzzy=True)
    except Exception:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=default_tz)

    return parsed.isoformat()


def is_recent_date(
    date_string: Optional[str],
    months: int = 5,
) -> bool:

    if not date_string:
        return False

    try:
        parsed = parser.parse(date_string)
    except Exception:
        return False

    cutoff = datetime.now(EAT) - timedelta(days=30 * months)

    return parsed >= cutoff


def safe_request(
    url: str,
    session: Optional[Session] = None,
    timeout: int = 20,
    use_cache: bool = True,
) -> Optional[Response]:

    resolved_session = session or get_default_session()

    if use_cache and url in _PAGE_CACHE:
        return _PAGE_CACHE[url]

    logger.info("Request started: %s", url)

    try:
        response = resolved_session.get(url, timeout=timeout)
    except requests.RequestException as exc:
        logger.error("Request failed for %s: %s", url, exc)
        return None

    if response.status_code >= 400:
        logger.error(
            "HTTP %s for %s",
            response.status_code,
            url,
        )
        return None

    logger.info("Request succeeded: %s", url)

    if use_cache:
        _PAGE_CACHE[url] = response

    return response


def _make_soup(
    response: Optional[Response],
) -> Optional[BeautifulSoup]:

    if response is None:
        return None

    try:
        return BeautifulSoup(response.text, "lxml")
    except Exception:
        return BeautifulSoup(response.text, "html.parser")


def _tables_from_html(html: str) -> List[pd.DataFrame]:

    try:
        return pd.read_html(StringIO(html))
    except Exception as exc:
        logger.warning("No parseable HTML tables found: %s", exc)
        return []


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:

    normalized = df.copy()

    if isinstance(normalized.columns, pd.MultiIndex):

        normalized.columns = [
            " ".join(
                clean_text(part)
                for part in column
                if clean_text(part)
            )
            for column in normalized.columns
        ]

    else:
        normalized.columns = [
            clean_text(column)
            for column in normalized.columns
        ]

    return normalized


def _extract_pdf_text(response: Response) -> str:

    content = BytesIO(response.content)

    if PdfReader is not None:
        try:
            reader = PdfReader(content)

            text = " ".join(
                clean_text(page.extract_text())
                for page in reader.pages
            )

            if text:
                return clean_text(text)

        except Exception:
            pass

    if pdfplumber is not None:
        try:
            content.seek(0)

            with pdfplumber.open(content) as pdf:
                text = " ".join(
                    clean_text(page.extract_text())
                    for page in pdf.pages
                )

            if text:
                return clean_text(text)

        except Exception:
            pass

    return ""


def _extract_publication_date_from_text(
    text: str,
) -> Optional[str]:

    patterns = (
        r"Posted\s+On\s*:?\s*([0-9]{1,2}[-/][0-9]{1,2}[-/][0-9]{2,4})",
        r"([0-9]{1,2}[-/][0-9]{1,2}[-/][0-9]{4})",
    )

    for pattern in patterns:

        match = re.search(
            pattern,
            text,
            flags=re.IGNORECASE,
        )

        if match:
            return normalize_date(match.group(1))

    return None


def _currency_info(
    currency_name: Any,
) -> Optional[Tuple[str, str, str]]:

    cleaned = clean_text(currency_name).upper()

    if cleaned in TARGET_FOREX_CURRENCIES:
        code, pair = TARGET_FOREX_CURRENCIES[cleaned]
        return code, pair, cleaned

    for label, (code, pair) in TARGET_FOREX_CURRENCIES.items():
        if label in cleaned:
            return code, pair, label

    return None


def _format_forex_row(
    currency_name: Any,
    mean_rate: Any,
    published_at: Optional[str],
    scraped_at: str,
) -> Optional[Dict[str, Any]]:

    info = _currency_info(currency_name)

    if not info:
        return None

    code, pair, canonical_name = info

    mean = clean_float(mean_rate)

    if mean is None:
        return None

    return {
        "currency_pair": pair,
        "currency_code": code,
        "currency_name": canonical_name,
        "mean_rate": mean,
        "buy_rate": None,
        "sell_rate": None,
        "source": SOURCE_NAME,
        "scraped_at": scraped_at,
        "published_at": published_at,
    }


def _dedupe_forex_rates(
    rates: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:

    unique: Dict[str, Dict[str, Any]] = {}

    for rate in rates:

        pair = rate.get("currency_pair")

        if pair and pair not in unique:
            unique[pair] = rate

    ordered_pairs = (
        "USD/KES",
        "EUR/KES",
        "GBP/KES",
    )

    return [
        unique[pair]
        for pair in ordered_pairs
        if pair in unique
    ]


def _parse_forex_from_tables(
    html: str,
    published_at: Optional[str],
    scraped_at: str,
) -> List[Dict[str, Any]]:

    rates: List[Dict[str, Any]] = []

    for table in _tables_from_html(html):

        table = _flatten_columns(table)

        table_text = clean_text(
            table.to_string()
        ).lower()

        if (
            "exchange" not in table_text
            and "dollar" not in table_text
        ):
            continue

        for _, row in table.iterrows():

            values = [
                clean_text(v)
                for v in row.tolist()
            ]

            if len(values) < 2:
                continue

            currency = values[0]
            rate = values[-1]

            parsed = _format_forex_row(
                currency_name=currency,
                mean_rate=rate,
                published_at=published_at,
                scraped_at=scraped_at,
            )

            if parsed:
                rates.append(parsed)

    return _dedupe_forex_rates(rates)


def scrape_forex_rates(
    session: Optional[Session] = None,
) -> List[Dict[str, Any]]:

    logger.info("CBK forex scraping started")

    scraped_at = now_iso()

    resolved_session = session or get_default_session()

    forex_response = safe_request(
        FOREX_URL,
        session=resolved_session,
    )

    if forex_response is not None:

        soup = _make_soup(forex_response)

        text = clean_text(
            soup.get_text(" ")
            if soup else ""
        )

        published_at = _extract_publication_date_from_text(text)

        rates = _parse_forex_from_tables(
            forex_response.text,
            published_at,
            scraped_at,
        )

        if rates:
            return rates

    logger.warning(
        "Forex page did not yield target rates; trying homepage fallback"
    )

    home_response = safe_request(
        HOME_URL,
        session=resolved_session,
    )

    if home_response is None:
        return []

    soup = _make_soup(home_response)

    text = clean_text(
        soup.get_text(" ")
        if soup else ""
    )

    published_at = _extract_publication_date_from_text(text)

    rates = _parse_forex_from_tables(
        home_response.text,
        published_at,
        scraped_at,
    )

    return rates


def _parse_cbr_rows_from_text(
    text: str,
) -> List[Dict[str, Any]]:

    rows = []

    pattern = (
        r"Central Bank Rate\s+(\d+(?:\.\d+)?)%\s+([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})"
    )

    matches = re.findall(
        pattern,
        text,
        flags=re.IGNORECASE,
    )

    for rate_text, date_text in matches:

        rows.append(
            {
                "rate": clean_float(rate_text),
                "published_at": normalize_date(date_text),
            }
        )

    return rows


def scrape_central_bank_rate(
    session: Optional[Session] = None,
) -> Dict[str, Any]:

    logger.info("CBK Central Bank Rate scraping started")

    scraped_at = now_iso()

    response = safe_request(
        CBR_URL,
        session=session or get_default_session(),
    )

    soup = _make_soup(response)

    text = clean_text(
        soup.get_text(" ")
        if soup else ""
    )

    rows = _parse_cbr_rows_from_text(text)

    if not rows:
        return {
            "current_rate": None,
            "previous_rate": None,
            "change": None,
            "direction": "unknown",
            "published_at": None,
            "source_title": "Central Bank Rate",
            "source": SOURCE_NAME,
            "url": CBR_URL,
            "scraped_at": scraped_at,
        }

    current = rows[0]

    previous = rows[1] if len(rows) > 1 else None

    current_rate = current["rate"]

    previous_rate = (
        previous["rate"]
        if previous else None
    )

    change = None

    if (
        current_rate is not None
        and previous_rate is not None
    ):
        change = round(
            current_rate - previous_rate,
            4,
        )

    direction = "unknown"

    if change is not None:
        if change > 0:
            direction = "increase"
        elif change < 0:
            direction = "decrease"
        else:
            direction = "unchanged"

    return {
        "current_rate": current_rate,
        "previous_rate": previous_rate,
        "change": change,
        "direction": direction,
        "published_at": current["published_at"],
        "source_title": "Central Bank Rate",
        "source": SOURCE_NAME,
        "url": CBR_URL,
        "scraped_at": scraped_at,
    }


def _extract_release_links(
    soup: Optional[BeautifulSoup],
    limit: int,
) -> List[Dict[str, str]]:

    if soup is None:
        return []

    candidates = []

    for anchor in soup.find_all("a", href=True):

        title = clean_text(
            anchor.get_text(" ")
        )

        href = urljoin(
            CBK_BASE_URL,
            anchor["href"],
        )

        searchable = f"{title} {href}".lower()

        if not any(
            keyword in searchable
            for keyword in (
                "mpc",
                "monetary policy",
                "committee",
                "cbr",
            )
        ):
            continue

        parent = (
            anchor.find_parent("tr")
            or anchor.find_parent("li")
            or anchor.parent
        )

        row_text = clean_text(
            parent.get_text(" ")
            if parent else ""
        )

        published_at = (
            _extract_publication_date_from_text(row_text)
        )

        cells = parent.find_all(["td", "th"]) if parent else []

        if cells:

            cell_text = clean_text(
                cells[0].get_text(" ")
            )

            if cell_text.lower() != "date":

                parsed_date = normalize_date(cell_text)

                if parsed_date:
                    published_at = parsed_date

        candidates.append(
            {
                "title": title,
                "url": href,
                "published_at": published_at,
                "row_text": row_text,
            }
        )

    unique: Dict[str, Dict[str, str]] = {}

    for item in candidates:

        url = item.get("url")

        if url and url not in unique:
            unique[url] = item

    recent_candidates = [
        item
        for item in unique.values()
        if is_recent_date(item.get("published_at"))
    ]

    sorted_candidates = sorted(
        recent_candidates,
        key=lambda item: parser.parse(
            item["published_at"]
        ),
        reverse=True,
    )

    return sorted_candidates[:limit]


def _extract_article_text(
    soup: Optional[BeautifulSoup],
) -> str:

    if soup is None:
        return ""

    for noisy in soup(["script", "style", "noscript"]):
        noisy.decompose()

    article = (
        soup.find("article")
        or soup.find("main")
        or soup.find("body")
        or soup
    )

    return clean_text(
        article.get_text(" ")
    )


def _parse_release_page(
    release: Dict[str, str],
    session: Optional[Session],
    scraped_at: str,
) -> Dict[str, Any]:

    url = release["url"]

    response = safe_request(
        url,
        session=session,
    )

    title = clean_text(
        release.get("title")
    )

    published_at = release.get("published_at")

    text = clean_text(
        release.get("row_text")
    )

    if response is not None:

        content_type = response.headers.get(
            "Content-Type",
            "",
        ).lower()

        if (
            "pdf" in content_type
            or url.lower().endswith(".pdf")
        ):

            pdf_text = _extract_pdf_text(response)

            if pdf_text:
                text = pdf_text

        else:

            soup = _make_soup(response)

            text = _extract_article_text(soup)

    summary = text[:450]

    cbr_match = re.search(
        r"(\d+(?:\.\d+)?)\s*(?:percent|%)",
        text,
        flags=re.IGNORECASE,
    )

    cbr = None

    if cbr_match:
        cbr = clean_float(
            cbr_match.group(1)
        )

    return {
        "title": title,
        "published_at": published_at,
        "summary": summary,
        "policy_stance": "neutral",
        "cbr": cbr,
        "inflation_signal": "unknown",
        "inflation_outlook": None,
        "economic_outlook": None,
        "source": SOURCE_NAME,
        "url": url,
        "scraped_at": scraped_at,
    }


def scrape_mpc_releases(
    session: Optional[Session] = None,
    limit: int = 5,
) -> List[Dict[str, Any]]:

    logger.info("CBK MPC release scraping started")

    scraped_at = now_iso()

    resolved_session = (
        session or get_default_session()
    )

    response = safe_request(
        PRESS_URL,
        session=resolved_session,
    )

    soup = _make_soup(response)

    release_links = _extract_release_links(
        soup,
        limit=limit,
    )

    if not release_links:
        logger.error(
            "No MPC release links found on CBK press page"
        )
        return []

    releases = []

    for release in release_links:

        try:

            parsed_release = _parse_release_page(
                release,
                resolved_session,
                scraped_at,
            )

            releases.append(parsed_release)

        except Exception as exc:

            logger.error(
                "Failed parsing MPC release %s: %s",
                release.get("url"),
                exc,
            )

    logger.info(
        "CBK MPC parsing completed with %s releases",
        len(releases),
    )

    return releases


def run_cbk_scraper() -> Dict[str, Any]:

    logger.info(
        "CBK scraper orchestration started"
    )

    session = create_session()

    scraped_at = now_iso()

    output = {
        "forex_rates": [],
        "central_bank_rate": {},
        "mpc_releases": [],
        "scraped_at": scraped_at,
    }

    try:
        output["forex_rates"] = scrape_forex_rates(
            session=session
        )
    except Exception as exc:
        logger.error(
            "Forex scraper failed: %s",
            exc,
        )

    try:
        output["central_bank_rate"] = (
            scrape_central_bank_rate(
                session=session
            )
        )
    except Exception as exc:
        logger.error(
            "CBR scraper failed: %s",
            exc,
        )

    try:
        output["mpc_releases"] = scrape_mpc_releases(
            session=session
        )
    except Exception as exc:
        logger.error(
            "MPC scraper failed: %s",
            exc,
        )

    logger.info(
        "CBK scraper orchestration completed"
    )

    return output


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    print("\n" + "=" * 70)
    print("FULL CBK SCRAPER OUTPUT")
    print("=" * 70)

    result = run_cbk_scraper()

    import json

    print(
        json.dumps(
            result,
            indent=4,
        )
    )