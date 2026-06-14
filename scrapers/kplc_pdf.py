"""
services/fundamental_extractor.py

Production-ready service for extracting KPLC fundamental metrics from
audited financial result PDFs, calculating key ratios, and persisting
results to the Fundamentals table.

Pipeline:
    PDF URL
     ↓
    Download PDF (requests)
     ↓
    Extract text pages (pdfplumber)
     ↓
    Split into lines
     ↓
    Keyword matching
     ↓
    Extract numeric values
     ↓
    Fetch latest KPLC price from DB
     ↓
    Calculate ratios
     ↓
    Save to Fundamentals table

Usage:
    extractor = KPLCFundamentalExtractor()
    raw       = extractor.extract(pdf_url)
    ratios    = extractor.calculate_ratios(raw, stock_price=15.45)
    record    = extractor.save_to_db(db, ratios)
"""

from __future__ import annotations

import io
import logging
import re
from typing import Optional

import pdfplumber
import requests
from sqlalchemy.orm import Session

from models.fundamental_data import Fundamentals
from models.market_data import MarketData
from database.session import  SessionLocal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

KPLC_TICKER: str = "KPLC"
_REQUEST_TIMEOUT: int = 30  # seconds

# Keyword → attribute name mapping for single-line numeric extractions.
# The extractor searches for the FIRST line containing the keyword and pulls
# the first numeric token from that line.
_LINE_KEYWORDS: dict[str, str] = {
    "Basic and diluted earnings per share": "eps",
    "Revenue from contracts with customers": "revenue",
    "Profit After Tax": "profit_after_tax",
    "Shareholders' equity": "shareholders_equity",
    "Non-current liabilities": "non_current_liabilities",
    "Current liabilities": "current_liabilities",
}

# Dividend keywords searched individually because two values are needed.
_INTERIM_DIVIDEND_KW: str = "Interim dividend"
_FINAL_DIVIDEND_KW: str = "Final dividend"

# Regex: match the first decimal or integer number in a string.
_FIRST_NUMBER_RE = re.compile(r"[\d,]+(?:\.\d+)?")


# ---------------------------------------------------------------------------
# KPLCFundamentalExtractor
# ---------------------------------------------------------------------------


class KPLCFundamentalExtractor:
    """
    Extracts KPLC financial fundamentals from an audited PDF report,
    computes key ratios, and persists them to the database.

    Methods are intentionally small and single-purpose so each step is
    independently testable and swappable without touching the others.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(self, pdf_url: str) -> dict:
        """
        Download a KPLC audited results PDF and extract raw financial figures.

        Args:
            pdf_url: Direct URL to the PDF file.

        Returns:
            Dict with keys: eps, revenue, profit_after_tax, shareholders_equity,
            non_current_liabilities, current_liabilities, dividend_per_share.

        Raises:
            requests.exceptions.RequestException: If the download fails.
            RuntimeError: If the PDF cannot be parsed or required values are missing.
        """
        pdf_bytes = self._download_pdf(pdf_url)
        lines = self._extract_lines(pdf_bytes)
        raw = self._parse_lines(lines)
        return raw

    def calculate_ratios(
        self,
        extracted_data: dict,
        stock_price: float,
    ) -> dict:
        """
        Compute investment ratios from raw extracted figures and the current
        KPLC stock price.

        Args:
            extracted_data: Dict returned by :meth:`extract`.
            stock_price:    Latest KPLC share price in KSh.

        Returns:
            Dict with all fields required by FundamentalsCreate:
            eps, pe_ratio, dividend_yield, revenue, debt_ratio, net_profit_margin.

        Raises:
            ValueError: If stock_price <= 0 or required raw figures are absent.
        """
        if stock_price <= 0:
            raise ValueError(f"stock_price must be positive, got {stock_price}")

        eps: float = extracted_data["eps"]
        revenue: float = extracted_data["revenue"]
        profit_after_tax: float = extracted_data["profit_after_tax"]
        shareholders_equity: float = extracted_data["shareholders_equity"]
        non_current_liabilities: float = extracted_data["non_current_liabilities"]
        current_liabilities: float = extracted_data["current_liabilities"]
        dividend_per_share: float = extracted_data["dividend_per_share"]

        pe_ratio = self._safe_divide(stock_price, eps, "P/E ratio")
        dividend_yield = self._safe_divide(dividend_per_share, stock_price, "dividend yield")

        total_liabilities = current_liabilities + non_current_liabilities
        debt_ratio = self._safe_divide(total_liabilities, shareholders_equity, "debt ratio")

        net_profit_margin = self._safe_divide(profit_after_tax, revenue, "net profit margin")

        ratios: dict = {
            "eps": eps,
            "pe_ratio": round(pe_ratio, 4),
            "dividend_yield": round(dividend_yield, 6),
            "revenue": revenue,
            "debt_ratio": round(debt_ratio, 4),
            "net_profit_margin": round(net_profit_margin, 6),
        }

        logger.info(
            "Ratios calculated | pe_ratio=%.2f dividend_yield=%.4f "
            "debt_ratio=%.2f net_profit_margin=%.4f",
            ratios["pe_ratio"],
            ratios["dividend_yield"],
            ratios["debt_ratio"],
            ratios["net_profit_margin"],
        )
        return ratios

    def save_to_db(self, db: Session, metrics: dict) -> Fundamentals:
        """
        Persist a Fundamentals record to the database.

        Args:
            db:      Active SQLAlchemy session.
            metrics: Dict returned by :meth:`calculate_ratios`.

        Returns:
            The committed and refreshed Fundamentals ORM object.

        Raises:
            RuntimeError: If the database write fails.
        """
        record = Fundamentals(
            eps=metrics["eps"],
            pe_ratio=metrics["pe_ratio"],
            dividend_yield=metrics["dividend_yield"],
            revenue=metrics["revenue"],
            debt_ratio=metrics["debt_ratio"],
            net_profit_margin=metrics["net_profit_margin"],
        )

        try:
            db.add(record)
            db.commit()
            db.refresh(record)
        except Exception as exc:
            db.rollback()
            logger.error("Database save failed: %s", exc)
            raise RuntimeError(f"Failed to save Fundamentals record: {exc}") from exc

        logger.info("Fundamentals saved | id=%d eps=%.2f pe_ratio=%.2f", record.id, record.eps, record.pe_ratio)
        return record

    # ------------------------------------------------------------------
    # Convenience: full pipeline in one call
    # ------------------------------------------------------------------

    def run(self, pdf_url: str, db: Session) -> Fundamentals:
        """
        Execute the full pipeline: download → extract → fetch price → calculate → save.

        Args:
            pdf_url: Direct URL to the KPLC audited results PDF.
            db:      Active SQLAlchemy session.

        Returns:
            Committed Fundamentals ORM record.
        """
        raw = self.extract(pdf_url)
        stock_price = self._fetch_latest_price(db)
        metrics = self.calculate_ratios(raw, stock_price)
        return self.save_to_db(db, metrics)

    # ------------------------------------------------------------------
    # Private: download
    # ------------------------------------------------------------------

    def _download_pdf(self, pdf_url: str) -> bytes:
        """
        Download the PDF from *pdf_url* and return raw bytes.

        Args:
            pdf_url: Direct HTTPS URL to the PDF.

        Returns:
            PDF file bytes.

        Raises:
            requests.exceptions.RequestException: On any network or HTTP error.
        """
        logger.info("PDF download started: %s", pdf_url)
        try:
            response = requests.get(pdf_url, timeout=_REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            logger.error("PDF fetch failed: %s — %s", pdf_url, exc)
            raise

        logger.info("PDF download successful: %d bytes", len(response.content))
        return response.content

    # ------------------------------------------------------------------
    # Private: text extraction
    # ------------------------------------------------------------------

    def _extract_lines(self, pdf_bytes: bytes) -> list[str]:
        """
        Open a PDF from bytes with pdfplumber and return all non-empty lines
        across every page as a flat list.

        Args:
            pdf_bytes: Raw PDF content.

        Returns:
            Flat list of stripped text lines.

        Raises:
            RuntimeError: If pdfplumber cannot open or read the PDF.
        """
        logger.info("Extraction started")
        lines: list[str] = []

        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    for line in page_text.splitlines():
                        stripped = line.strip()
                        if stripped:
                            lines.append(stripped)
        except Exception as exc:
            logger.error("PDF parsing failed: %s", exc)
            raise RuntimeError(f"pdfplumber could not parse PDF: {exc}") from exc

        return lines

    @staticmethod
    def _normalize_text(text: str) -> str:
        return (
            text.lower()
            .replace("’", "'")
            .replace("‘", "'")
            .replace("“", '"')
            .replace("”", '"')
        )

    # ------------------------------------------------------------------
    # Private: parsing
    # ------------------------------------------------------------------

    def _parse_lines(self, lines: list[str]) -> dict:
        """
        Run keyword matching over the line list and extract all required
        financial figures.

        Args:
            lines: Flat list of text lines from the PDF.

        Returns:
            Raw extracted dict with keys matching _LINE_KEYWORDS values plus
            dividend_per_share.

        Raises:
            RuntimeError: If any required metric cannot be found.
        """
        raw: dict[str, Optional[float]] = {attr: None for attr in _LINE_KEYWORDS.values()}
        raw["dividend_per_share"] = None

        for line in lines:
            # Single-value keyword matches
            for keyword, attr in _LINE_KEYWORDS.items():
                normalized_line = self._normalize_text(line)

                if raw[attr] is None and self._normalize_text(keyword) in normalized_line:
                    value = self._first_number(line)
                    if value is not None:
                        raw[attr] = value

            # Dividend: interim
            if raw.get("_interim") is None and self._normalize_text(_INTERIM_DIVIDEND_KW) in normalized_line:
                raw["_interim"] = self._first_number(line) or 0.0

            # Dividend: final
            if raw.get("_final") is None and self._normalize_text(_FINAL_DIVIDEND_KW) in normalized_line:
                raw["_final"] = self._first_number(line) or 0.0

        # Compute dividend_per_share from interim + final
        interim = raw.pop("_interim", None) or 0.0
        final_ = raw.pop("_final", None) or 0.0
        raw["dividend_per_share"] = round(interim + final_, 4)

        # Validate all required fields are present
        missing = [k for k, v in raw.items() if v is None]
        for attr in missing:
            logger.warning("Expected metric not found: %s", attr)

        required = list(_LINE_KEYWORDS.values()) + ["dividend_per_share"]
        still_missing = [k for k in required if raw.get(k) is None]
        if still_missing:
            raise RuntimeError(
                f"Could not extract required metrics from PDF: {still_missing}"
            )

        logger.info(
            "Metrics extracted | eps=%.2f revenue=%.0f profit_after_tax=%.0f "
            "shareholders_equity=%.0f non_current_liabilities=%.0f "
            "current_liabilities=%.0f dividend_per_share=%.2f",
            raw["eps"],
            raw["revenue"],
            raw["profit_after_tax"],
            raw["shareholders_equity"],
            raw["non_current_liabilities"],
            raw["current_liabilities"],
            raw["dividend_per_share"],
        )
        return raw  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Private: DB helpers
    # ------------------------------------------------------------------

    def _fetch_latest_price(self, db: Session) -> float:
        """
        Fetch the most recent KPLC close price from the market_data table.

        Args:
            db: Active SQLAlchemy session.

        Returns:
            Latest KPLC stock price as a float.

        Raises:
            RuntimeError: If no KPLC record exists in the database.
        """
        record: Optional[MarketData] = (
            db.query(MarketData)
            .filter(MarketData.ticker == KPLC_TICKER)
            .order_by(MarketData.timestamp.desc())
            .first()
        )

        if record is None:
            raise RuntimeError(
                f"No MarketData found for ticker '{KPLC_TICKER}'. "
                "Run the market data fetch endpoint first."
            )

        price = float(record.price)
        logger.info("Latest KPLC price fetched: %.3f", price)
        return price

    # ------------------------------------------------------------------
    # Private: numeric helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _first_number(line: str) -> Optional[float]:
        """
        Extract and return the first numeric value from *line*.

        Handles comma-formatted numbers (e.g. "219,285" → 219285.0).

        Args:
            line: A single text line from the PDF.

        Returns:
            Float value or None if no number is found.
        """
        match = _FIRST_NUMBER_RE.search(line)
        if not match:
            return None
        try:
            return float(match.group(0).replace(",", ""))
        except ValueError:
            return None

    @staticmethod
    def _safe_divide(
        numerator: float,
        denominator: float,
        ratio_name: str,
    ) -> float:
        """
        Divide numerator by denominator with a zero-division guard.

        Args:
            numerator:    Dividend value.
            denominator:  Divisor value.
            ratio_name:   Human-readable name used in the warning log.

        Returns:
            Result of division, or 0.0 if denominator is zero.
        """
        if denominator == 0:
            logger.warning(
                "Cannot compute %s — denominator is zero. Returning 0.0.",
                ratio_name,
            )
            return 0.0
        return numerator / denominator


# ---------------------------------------------------------------------------
# CLI smoke-test:  python services/fundamental_extractor.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    # ── Offline test using the PDF text pasted directly ─────────────────────
    # Replace with a real URL when running against a live PDF.
    SAMPLE_PDF_URL = "https://www.nse.co.ke/wp-content/uploads/The-Kenya-Power-Lighting-Company-Plc-Audited-Financial-Results-for-the-Year-Ended-30-Jun-2025.pdf"

    print("\n" + "=" * 70)
    print("KPLCFundamentalExtractor —  demo")
    print("=" * 70)

    # extract values frmpdf.
    db = SessionLocal()
    try:
        extractor = KPLCFundamentalExtractor()
        raw = extractor.extract(SAMPLE_PDF_URL)
    except Exception as e:
        logger.error("Error occurred while extracting PDF data: %s", e)
        sys.exit(1)

    EXTRACTED: dict = {
        "eps": raw["eps"],
        "revenue": raw["revenue"],
        "profit_after_tax": raw["profit_after_tax"],
        "shareholders_equity": raw["shareholders_equity"],
        "non_current_liabilities": raw["non_current_liabilities"],
        "current_liabilities": raw["current_liabilities"],
        "dividend_per_share": raw.get("dividend_per_share"),
    }

    Fundamental_PRICE = extractor._fetch_latest_price(db)
    ratios = extractor.calculate_ratios(EXTRACTED, stock_price=Fundamental_PRICE)

    print("\nInput price : KSh", Fundamental_PRICE)
    print("\nCalculated ratios:")
    print(json.dumps(ratios, indent=4))

    print("\nExpected approximate values:")
    print(f"  P/E ratio        : {Fundamental_PRICE / EXTRACTED['eps']:.4f}")
    print(f"  Dividend yield   : {EXTRACTED['dividend_per_share'] / Fundamental_PRICE:.6f}")
    total_liab = EXTRACTED["current_liabilities"] + EXTRACTED["non_current_liabilities"]
    print(f"  Debt ratio       : {total_liab / EXTRACTED['shareholders_equity']:.4f}")
    print(f"  Net profit margin: {EXTRACTED['profit_after_tax'] / EXTRACTED['revenue']:.6f}")