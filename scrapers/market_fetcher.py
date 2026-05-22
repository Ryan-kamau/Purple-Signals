"""
app/scrapers/market_fetcher.py
 
Fetch Layer — communicates ONLY with the external RapidAPI NSE market API.
 
Responsibilities:
  - Build and send HTTP requests
  - Handle headers / auth
  - Support query parameters (search, sort, limit, order)
  - Return RAW API responses with zero transformation
  - Provide realistic fallback data on failure
 
No business logic. No DB logic. No FastAPI routes.
"""
 
import logging
import os
from typing import Any
 
import requests
from dotenv import load_dotenv
 
load_dotenv()

RAPIDAPI_HOST = "nairobi-stock-exchange-nse.p.rapidapi.com"
RAPIDAPI_BASE_URL = "https://nairobi-stock-exchange-nse.p.rapidapi.com"
RAPID_API_KEY = os.getenv("RAPID_API_KEY")
_DEFAULT_TIMEOUT: int = 10  # seconds

 
logger = logging.getLogger(__name__)
 
# ---------------------------------------------------------------------------
# Fallback data — realistic NSE stocks used when the live API is unavailable.
# Mirrors the exact shape the RapidAPI endpoint returns so the service layer
# never has to branch on "is this real or mocked?".
# ---------------------------------------------------------------------------
_FALLBACK_STOCKS: list[dict[str, Any]] = [
    {
        "ticker": "KPLC",
        "name": "Kenya Power & Lighting Co",
        "volume": "1330000",
        "price": "15.40",
        "change": "+0.33%",
    },
    {
        "ticker": "EQTY",
        "name": "Equity Group Holdings",
        "volume": "4200000",
        "price": "42.00",
        "change": "-1.18%",
    },
    {
        "ticker": "SCOM",
        "name": "Safaricom PLC",
        "volume": "8750000",
        "price": "17.85",
        "change": "+0.56%",
    },
    {
        "ticker": "KCB",
        "name": "KCB Group PLC",
        "volume": "2100000",
        "price": "28.40",
        "change": "-0.70%",
    },
    {
        "ticker": "COOP",
        "name": "Co-operative Bank of Kenya",
        "volume": "1560000",
        "price": "13.90",
        "change": "+1.09%",
    },
]
 
 
class MarketFetcher:
    """
    Thin HTTP client for the RapidAPI NSE market data endpoint.
 
    All methods return the raw list of stock dicts exactly as the API
    delivers them.  Callers (the service layer) own all parsing/cleaning.
 
    Environment variables expected:
        RAPIDAPI_KEY      — your RapidAPI subscription key
        RAPIDAPI_HOST     — e.g. "nse-stocks.p.rapidapi.com"
        RAPIDAPI_BASE_URL — e.g. "https://nse-stocks.p.rapidapi.com"
    """
 
 
    def __init__(self) -> None:
        self._api_key: str = RAPID_API_KEY
        self._host: str = RAPIDAPI_HOST
        self._base_url: str = RAPIDAPI_BASE_URL
 
        if not self._api_key:
            logger.warning(
                "RAPIDAPI_KEY is not set — fetch calls will fail; "
                "fallback data will be returned."
            )
 
    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------
 
    def fetch_all_stocks(self) -> list[dict[str, Any]]:
        """
        Fetch every available NSE stock from the API.
 
        Returns:
            List of raw stock dicts from the API (or fallback on failure).
        """
        params: dict[str, Any] = {}
        return self._get_stocks(params=params)
 
    def search_stocks(self, query: str) -> list[dict[str, Any]]:
        """
        Search NSE stocks by ticker or company name fragment.
 
        Args:
            query: Free-text search string, e.g. "EQTY" or "safaricom".
 
        Returns:
            Filtered list of raw stock dicts.
        """
        if not query or not query.strip():
            logger.warning("search_stocks called with empty query; fetching all.")
            return self.fetch_all_stocks()
 
        params: dict[str, Any] = {"search": query.strip()}
        return self._get_stocks(params=params)
 
    def get_top_stocks(
        self,
        limit: int = 5,
        sort: str = "price",
        order: str = "desc",
    ) -> list[dict[str, Any]]:
        """
        Retrieve a ranked subset of NSE stocks.
 
        Args:
            limit: Maximum number of results (default 5).
            sort:  Field to sort by — e.g. "price", "volume" (default "price").
            order: Sort direction — "asc" or "desc" (default "desc").
 
        Returns:
            Ranked list of raw stock dicts.
        """
        params: dict[str, Any] = {
            "limit": limit,
            "sort": sort,
            "order": order,
        }
        return self._get_stocks(params=params)
 
    def fetch_stock_by_ticker(self, ticker: str) -> dict[str, Any] | None:
        """
        Convenience wrapper — search by ticker and return the first match.
 
        Args:
            ticker: Exact ticker symbol, e.g. "KPLC".
 
        Returns:
            Single raw stock dict or None if not found.
        """
        results = self.search_stocks(query=ticker.upper())
        for stock in results:
            if stock.get("ticker", "").upper() == ticker.upper():
                return stock
        logger.warning("Ticker %s not found in API response.", ticker)
        return None
    
    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------
 
    def fetch_all_stocks(self) -> list[dict[str, Any]]:
        """
        Fetch every available NSE stock from the API.
 
        Returns:
            List of raw stock dicts from the API (or fallback on failure).
        """
        params: dict[str, Any] = {}
        return self._get_stocks(params=params)
 
    def search_stocks(self, query: str) -> list[dict[str, Any]]:
        """
        Search NSE stocks by ticker or company name fragment.
 
        Args:
            query: Free-text search string, e.g. "EQTY" or "safaricom".
 
        Returns:
            Filtered list of raw stock dicts.
        """
        if not query or not query.strip():
            logger.warning("search_stocks called with empty query; fetching all.")
            return self.fetch_all_stocks()
 
        params: dict[str, Any] = {"search": query.strip()}
        return self._get_stocks(params=params)
 
    def get_top_stocks(
        self,
        limit: int = 10,
        sort: str = "price",
        order: str = "desc",
    ) -> list[dict[str, Any]]:
        """
        Retrieve a ranked subset of NSE stocks.
 
        Args:
            limit: Maximum number of results (default 10).
            sort:  Field to sort by — e.g. "price", "volume" (default "price").
            order: Sort direction — "asc" or "desc" (default "desc").
 
        Returns:
            Ranked list of raw stock dicts.
        """
        params: dict[str, Any] = {
            "limit": limit,
            "sort": sort,
            "order": order,
        }
        return self._get_stocks(params=params)
 
    def fetch_stock_by_ticker(self, ticker: str) -> dict[str, Any] | None:
        """
        Convenience wrapper — search by ticker and return the first match.
 
        Args:
            ticker: Exact ticker symbol, e.g. "KPLC".
 
        Returns:
            Single raw stock dict or None if not found.
        """
        results = self.search_stocks(query=ticker.upper())
        for stock in results:
            if stock.get("ticker", "").upper() == ticker.upper():
                return stock
        logger.warning("Ticker %s not found in API response.", ticker)
        return None
 
    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------
 
    def _get_stocks(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Core request method.  Builds headers, fires the GET, and returns
        the raw data list.  Falls back to mocked data on any failure.
 
        Args:
            params: Query-string parameters to forward to the API.
 
        Returns:
            List of raw stock dicts.
        """
        url = f"{self._base_url}/stocks"
 
        try:
            response = requests.get(
                url,
                headers=self._build_headers(),
                params=params,
                timeout=_DEFAULT_TIMEOUT,
            )
            response.raise_for_status()
            return self._extract_data(response.json())
 
        except requests.exceptions.Timeout:
            logger.error("RapidAPI request timed out after %ss.", self._DEFAULT_TIMEOUT)
            return self._fallback()
 
        except requests.exceptions.ConnectionError as exc:
            logger.error("Network error connecting to RapidAPI: %s", exc)
            return self._fallback()
 
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response else "unknown"
            logger.error("HTTP %s from RapidAPI: %s", status, exc)
            return self._fallback()
 
        except (ValueError, KeyError) as exc:
            logger.error("Failed to parse RapidAPI response: %s", exc)
            return self._fallback()
 
        except Exception as exc:  # noqa: BLE001
            logger.error("Unexpected error in fetch layer: %s", exc)
            return self._fallback()
 
    def _build_headers(self) -> dict[str, str]:
        """Assemble the RapidAPI authentication headers."""
        return {
            "x-rapidapi-key": self._api_key,
            "x-rapidapi-host": self._host,
            "Content-Type": "application/json",
        }
 
    @staticmethod
    def _extract_data(response_json: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Pull the 'data' list out of the API envelope.
 
        Expected shape:
            { "success": true, "data": [...], "meta": {...} }
 
        Args:
            response_json: Parsed JSON from the API response.
 
        Returns:
            The raw list of stock dicts, or [] if the shape is unexpected.
        """
        if not response_json.get("success", False):
            logger.warning("API returned success=false: %s", response_json)
            return []
 
        data = response_json.get("data", [])
 
        if not isinstance(data, list):
            logger.warning("Unexpected 'data' shape in API response: %s", type(data))
            return []
 
        logger.debug("Fetched %d raw stock records from API.", len(data))
        return data
 
    def _fallback(self) -> list[dict[str, Any]]:
        """Return the static fallback dataset and log a clear warning."""
        logger.warning(
            "Returning fallback NSE data (%d records). "
            "Live API unavailable.",
            len(_FALLBACK_STOCKS),
        )
        return _FALLBACK_STOCKS