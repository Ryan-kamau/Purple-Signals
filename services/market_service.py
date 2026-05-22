"""
app/services/market_service.py
 
Service Layer — owns ALL business logic for market data.
 
Responsibilities:
  - Call the fetch layer (MarketFetcher)
  - Validate incoming raw records
  - Clean / normalise field values
  - Compute derived analytics (daily_change, volatility)
  - Construct SQLAlchemy MarketData ORM objects
  - Persist to MySQL with proper session management
  - Return structured, typed results to callers (API routes)
 
No FastAPI routes live here.
"""
 
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from decimal import Decimal
from zoneinfo import ZoneInfo
 
from sqlalchemy.orm import Session
 
from models.market_data import MarketData
from scrapers.market_fetcher import MarketFetcher
 
logger = logging.getLogger(__name__)
 
NAIROBI_TZ = ZoneInfo("Africa/Nairobi")
 
 
# ---------------------------------------------------------------------------
# Result container — gives callers a typed, stable interface.
# Using a dataclass (not Pydantic) to keep the service layer free of FastAPI
# concerns; Pydantic schemas live in app/api/schemas/.
# ---------------------------------------------------------------------------
 
@dataclass
class MarketDataResult:
    """
    Structured result returned by every public MarketService method.
 
    Attributes:
        records:     Persisted MarketData ORM objects.
        total:       Count of records processed (including skipped).
        saved:       Count of records actually saved to the DB.
        skipped:     Count of records that failed validation.
        source:      "live" | "fallback" — useful for monitoring/alerting.
    """
 
    records: list[MarketData]
    total: int
    saved: int
    skipped: int
    source: str = "live"
 
 
class MarketService:
    """
    Core business logic layer for NSE market data.
 
    Designed for easy extension:
      - Swap the fetcher implementation without touching DB logic.
      - Add Redis caching as a thin wrapper around _fetch_raw().
      - Plug in Celery by calling public methods from task functions.
      - Extend _compute_volatility() with proper rolling std-dev later.
 
    Usage:
        service = MarketService(db_session)
        result  = service.get_all_market_data()
    """
 
    def __init__(self, db: Session, fetcher: MarketFetcher | None = None) -> None:
        """
        Args:
            db:      Active SQLAlchemy session (injected via FastAPI Depends).
            fetcher: Optional fetcher override (useful for testing / DI).
        """
        self._db = db
        self._fetcher = fetcher or MarketFetcher()
 
    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------
 
    def get_all_market_data(self) -> MarketDataResult:
        """
        Fetch, process, and persist all available NSE stocks.
 
        Returns:
            MarketDataResult with every successfully saved record.
        """
        raw_records = self._fetcher.fetch_all_stocks()
        return self._process_and_save(raw_records)
 
    def search_market_data(self, query: str) -> MarketDataResult:
        """
        Fetch, process, and persist stocks matching a search query.
 
        Args:
            query: Ticker symbol or company name fragment, e.g. "KPLC".
 
        Returns:
            MarketDataResult for matching stocks.
        """
        raw_records = self._fetcher.search_stocks(query=query)
        return self._process_and_save(raw_records)
 
    def get_top_market_data(
        self,
        limit: int = 5,
        sort: str = "price",
        order: str = "desc",
    ) -> MarketDataResult:
        """
        Fetch, process, and persist a ranked subset of NSE stocks.
 
        Args:
            limit: Maximum number of stocks.
            sort:  API sort field (e.g. "price", "volume").
            order: "asc" or "desc".
 
        Returns:
            MarketDataResult for the top-ranked stocks.
        """
        raw_records = self._fetcher.get_top_stocks(limit=limit, sort=sort, order=order)
        return self._process_and_save(raw_records)
 
    def get_kplc(self) -> MarketData | None:
        """
        Convenience method — fetch and persist the latest KPLC record only.
 
        Returns:
            Saved MarketData object or None if KPLC was not found / invalid.
        """
        raw = self._fetcher.fetch_stock_by_ticker("KPLC")
        if raw is None:
            logger.warning("KPLC record not found in API response.")
            return None
 
        market_data = self._build_orm_object(raw)
        if market_data is None:
            return None
 
        return self._save_single(market_data)
    
    # ------------------------------------------------------------------
    # Core pipeline
    # ------------------------------------------------------------------
 
    def _process_and_save(
        self, raw_records: list[dict[str, Any]]
    ) -> MarketDataResult:
        """
        Full pipeline:  raw list → validate → clean → compute → save.
 
        Args:
            raw_records: List of raw dicts from the fetch layer.
 
        Returns:
            MarketDataResult summarising the pipeline run.
        """
        total = len(raw_records)
        saved_objects: list[MarketData] = []
        skipped = 0
 
        # Detect fallback source by checking if API data is stale/mocked.
        # The fetcher logs this internally; we track it here for the result.
        source = "live" if total > 0 else "fallback"
 
        for raw in raw_records:
            orm_object = self._build_orm_object(raw)
            if orm_object is None:
                skipped += 1
                continue
 
            saved = self._save_single(orm_object)
            if saved:
                saved_objects.append(saved)
            else:
                skipped += 1
 
        logger.info(
            "Pipeline complete — total=%d  saved=%d  skipped=%d",
            total, len(saved_objects), skipped,
        )
 
        return MarketDataResult(
            records=saved_objects,
            total=total,
            saved=len(saved_objects),
            skipped=skipped,
            source=source,
        )
 
    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
 
    def _validate_raw(self, raw: dict[str, Any]) -> bool:
        """
        Validate that a raw stock dict has the minimum required fields
        and that price / volume / change are present and non-empty.
 
        Args:
            raw: Single raw stock dict from the fetch layer.
 
        Returns:
            True if the record is safe to process; False otherwise.
        """
        ticker = raw.get("ticker", "<unknown>")
 
        required_fields = ("price", "volume", "change", "ticker", "name")
        for field in required_fields:
            if field not in raw or raw[field] in (None, "", "N/A"):
                logger.warning(
                    "Skipping %s — missing or empty field: '%s'", ticker, field
                )
                return False
 
        # Sanity-check that price and volume look numeric before committing
        # to a full parse (cheap guard before the heavier _parse_* methods).
        for field in ("price", "volume"):
            try:
                float(str(raw[field]).replace(",", ""))
            except ValueError:
                logger.warning(
                    "Skipping %s — non-numeric value for '%s': %s",
                    ticker, field, raw[field],
                )
                return False
 
        return True
 
    # ------------------------------------------------------------------
    # Cleaning / normalisation
    # ------------------------------------------------------------------
 
    @staticmethod
    def _parse_price(raw_price: Any) -> float | None:
        """
        Coerce the raw price string to a float.
 
        Args:
            raw_price: e.g. "15.40" or 15.40.
 
        Returns:
            Float price or None on failure.
        """
        try:
            return round(Decimal(str(raw_price).replace(",", "").strip()), 3)
        except (ValueError, TypeError) as exc:
            logger.warning("Could not parse price '%s': %s", raw_price, exc)
            return None
 
    @staticmethod
    def _parse_volume(raw_volume: Any) -> int | None:
        """
        Coerce the raw volume string to an integer.
 
        Args:
            raw_volume: e.g. "1330000".
 
        Returns:
            Integer volume or None on failure.
        """
        try:
            return int(float(str(raw_volume).replace(",", "").strip()))
        except (ValueError, TypeError) as exc:
            logger.warning("Could not parse volume '%s': %s", raw_volume, exc)
            return None
 
    @staticmethod
    def _parse_change_pct(raw_change: Any) -> float | None:
        """
        Convert the API's change string to a plain float percentage.
 
        The API delivers:  "+0.33%"  →  0.33
                           "-2.26%"  →  -2.26
 
        Args:
            raw_change: e.g. "+0.33%" or "-2.26%".
 
        Returns:
            Float percentage (NOT divided by 100) or None on failure.
        """
        try:
            cleaned = str(raw_change).replace("%", "").replace("+", "").strip()
            return float(cleaned)
        except (ValueError, TypeError) as exc:
            logger.warning("Could not parse change '%s': %s", raw_change, exc)
            return None

    # ------------------------------------------------------------------
    # Analytics computation
    # ------------------------------------------------------------------
 
    @staticmethod
    def _compute_daily_change(price: float, daily_change_pct: float) -> float:
        """
        Derive the absolute daily price change from percentage and price.
 
        Formula:  daily_change = (price * daily_change_pct) / 100
 
        Example:  15.40 * 0.33 / 100  → 0.0508
 
        Args:
            price:            Current stock price.
            daily_change_pct: Percentage change (e.g. 0.33 for +0.33%).
 
        Returns:
            Absolute price movement rounded to 4 decimal places.
        """
        return round((price * Decimal(daily_change_pct)) / Decimal(100), 4)
 
    @staticmethod
    def _compute_volatility(daily_change_pct: float) -> float:
        """
        MVP volatility proxy — absolute value of the daily % change.
 
        Upgrade path (no interface change needed):
          - Replace with rolling std-dev once enough history is stored.
          - Accept a list of historical returns for proper σ calculation.
 
        Args:
            daily_change_pct: Today's percentage change (e.g. -2.26).
 
        Returns:
            Non-negative float representing relative volatility.
        """
        return round(abs(daily_change_pct), 4)
 
    # ------------------------------------------------------------------
    # ORM construction
    # ------------------------------------------------------------------
 
    def _build_orm_object(self, raw: dict[str, Any]) -> MarketData | None:
        """
        Validate → clean → compute → return a ready-to-save MarketData object.
 
        Args:
            raw: Single raw stock dict from the fetch layer.
 
        Returns:
            Populated MarketData instance or None if validation fails.
        """
        if not self._validate_raw(raw):
            return None
 
        price = self._parse_price(raw["price"])
        volume = self._parse_volume(raw["volume"])
        daily_change_pct = self._parse_change_pct(raw["change"])
 
        # Belt-and-suspenders: parsing can still return None even after
        # _validate_raw passes (e.g. edge-case strings like "N/A" slipping
        # through a lenient validator in future).
        if price is None or volume is None or daily_change_pct is None:
            logger.warning(
                "Skipping %s — a required field failed numeric conversion.",
                raw.get("ticker", "<unknown>"),
            )
            return None
 
        daily_change = self._compute_daily_change(price, daily_change_pct)
        volatility = self._compute_volatility(daily_change_pct)
        timestamp = self._resolve_timestamp(raw)
 
        return MarketData(
            ticker=raw["ticker"].upper().strip(),
            company=raw["name"].strip(),
            price=price,
            volume=volume,
            daily_change=daily_change,
            daily_change_pct=daily_change_pct,
            volatility=volatility,
            timestamp=timestamp,
        )
 
    # ------------------------------------------------------------------
    # Timezone handling
    # ------------------------------------------------------------------
 
    @staticmethod
    def _resolve_timestamp(raw: dict[str, Any]) -> datetime:
        """
        Resolve a Nairobi-aware datetime for a record.
 
        Priority:
          1. Use the API's own timestamp field if present and parseable.
          2. Fall back to the current Nairobi time.
 
        The API delivers ISO-8601 UTC strings like:
            "2026-05-18T23:00:47.305Z"
 
        We store all timestamps in Nairobi time (Africa/Nairobi / EAT, UTC+3)
        to align with the NSE trading day and the existing DB model.
 
        Args:
            raw: Single raw stock dict which may contain a "timestamp" key.
 
        Returns:
            timezone-aware datetime in Africa/Nairobi.
        """
        api_ts = raw.get("lastUpdated") or raw.get("timestamp")
 
        if api_ts:
            try:
                # Parse the ISO string; Python 3.11+ handles 'Z' natively.
                # For 3.9/3.10 compatibility we normalise the suffix.
                normalised = api_ts.replace("Z", "+00:00")
                utc_dt = datetime.fromisoformat(normalised)
                return utc_dt.astimezone(NAIROBI_TZ)
            except (ValueError, TypeError) as exc:
                logger.debug("Could not parse API timestamp '%s': %s", api_ts, exc)
 
        return datetime.now(tz=NAIROBI_TZ)
 
    # ------------------------------------------------------------------
    # Database persistence
    # ------------------------------------------------------------------
 
    def _save_single(self, market_data: MarketData) -> MarketData | None:
        """
        Persist a single MarketData record to the database.
 
        Uses a savepoint-style try/except so one bad record cannot corrupt
        the session for the records that follow it.
 
        Args:
            market_data: Fully populated ORM object ready to persist.
 
        Returns:
            The refreshed (DB-populated id, etc.) object on success, or None.
        """
        try:
            self._db.add(market_data)
            self._db.commit()
            self._db.refresh(market_data)
            logger.debug(
                "Saved %s | price=%.2f | change=%.2f%% | vol=%d",
                market_data.ticker,
                market_data.price,
                market_data.daily_change_pct,
                market_data.volume,
            )
            return market_data
 
        except Exception as exc:  # noqa: BLE001
            self._db.rollback()
            logger.error(
                "DB error saving %s: %s — rolled back.",
                getattr(market_data, "ticker", "<unknown>"),
                exc,
            )
            return None
 
    def _save_batch(
        self, objects: list[MarketData]
    ) -> tuple[list[MarketData], int]:
        """
        Persist a batch of MarketData records in a single transaction.
 
        Preferred over _save_single in bulk-fetch scenarios for performance.
        Falls back to row-by-row on failure so one bad row doesn't wipe the
        entire batch.
 
        Args:
            objects: List of validated, populated MarketData ORM objects.
 
        Returns:
            Tuple of (saved_objects, skipped_count).
        """
        try:
            self._db.add_all(objects)
            self._db.commit()
            for obj in objects:
                self._db.refresh(obj)
            logger.info("Batch-saved %d records.", len(objects))
            return objects, 0
 
        except Exception as exc:  # noqa: BLE001
            self._db.rollback()
            logger.warning(
                "Batch save failed (%s) — falling back to row-by-row.", exc
            )
 
            saved: list[MarketData] = []
            skipped = 0
            for obj in objects:
                result = self._save_single(obj)
                if result:
                    saved.append(result)
                else:
                    skipped += 1
 
            return saved, skipped
 