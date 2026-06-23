"""
scrapers/macro_scraper.py

KNBS Leading Economic Indicators PDF Scraper.

Pipeline position:
    KNBS PDF URL  →  MacroPdfExtractor  →  MacroIndicators  →  MacroService  →  DB

Responsibilities:
  - Download the KNBS LEI PDF from a URL
  - Extract structured tables using pdfplumber (NOT raw text / OCR)
  - Parse each indicator from the correct table via pandas DataFrames
  - Determine a policy signal from combined indicators
  - Return a typed MacroIndicators dataclass ready for the service layer

This module does NOT:
  - Access the database
  - Contain FastAPI routes
  - Call any other scrapers
  - Perform sentiment analysis

The MacroIndicators dataclass maps directly to the macro_data table columns
defined in models/macro_data.py. The service layer owns persistence.

Usage:
    extractor = MacroPdfExtractor(pdf_url="https://...")
    result: MacroIndicators = extractor.extract()
    # Pass result to MacroService.save(db, result)
"""

from __future__ import annotations

import io
import logging
import re
import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import pdfplumber
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REQUEST_TIMEOUT: int = 60
_MONTH_MAP: dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}
_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*",
}

# ---------------------------------------------------------------------------
# Output dataclass — maps 1:1 to macro_data model columns.
# The service layer converts this into a MacroData ORM object.
# ---------------------------------------------------------------------------

@dataclass
class MacroIndicators:
    """
    Structured extraction result from the KNBS LEI PDF.

    All numeric fields are Optional[float] so the service layer can decide
    whether to skip, warn, or reject records with missing values — rather
    than forcing the scraper to raise on every missing cell.

    Fields align with models/macro_data.py. Extended fields (beyond the MVP
    table) are included here so the DB model can be expanded without touching
    this scraper.
    """

    report_date: Optional[datetime]

    # Prices / rates
    inflation: Optional[float]
    usd_kes: Optional[float]
    cbk_rate: Optional[float]

    # Fuel
    premium_petrol: Optional[float]
    diesel: Optional[float]
    kerosene: Optional[float]

    # NSE
    nse20: Optional[float]
    nasi: Optional[float]
    market_cap: Optional[float]

    # Money supply (KES billions)
    m1: Optional[float]
    m2: Optional[float]
    m3: Optional[float]

    # External sector
    foreign_reserves: Optional[float]
    exports: Optional[float]
    imports: Optional[float]
    trade_volume: Optional[float]

    # Derived intelligence signal
    policy_signal: str = "Neutral"

    def to_dict(self) -> dict[str, Any]:
        """Convenience — useful for logging and API serialisation."""
        return {
            "report_date": self.report_date.isoformat() if self.report_date else None,
            "inflation": self.inflation,
            "usd_kes": self.usd_kes,
            "cbk_rate": self.cbk_rate,
            "premium_petrol": self.premium_petrol,
            "diesel": self.diesel,
            "kerosene": self.kerosene,
            "nse20": self.nse20,
            "nasi": self.nasi,
            "market_cap": self.market_cap,
            "m1": self.m1,
            "m2": self.m2,
            "m3": self.m3,
            "foreign_reserves": self.foreign_reserves,
            "exports": self.exports,
            "imports": self.imports,
            "trade_volume": self.trade_volume,
            "policy_signal": self.policy_signal,
        }


# ---------------------------------------------------------------------------
# MacroPdfExtractor
# ---------------------------------------------------------------------------


class MacroPdfExtractor:
    """
    Downloads and parses the KNBS Leading Economic Indicators PDF.

    Each public method has a single responsibility. Private helpers are
    reused across parsers. The extract() method orchestrates the full
    pipeline and returns a MacroIndicators dataclass.

    Args:
        pdf_url: Direct URL to the KNBS LEI PDF.
        timeout: HTTP request timeout in seconds. Default: 60.

    Example::

        extractor = MacroPdfExtractor(
            pdf_url="https://www.knbs.or.ke/wp-content/uploads/2026/05/"
                    "Leading-Economic-Indicators-March-2026.pdf"
        )
        result = extractor.extract()
        # result is a MacroIndicators dataclass
    """

    def __init__(self, pdf_url: str, timeout: int = _REQUEST_TIMEOUT) -> None:
        self._pdf_url: str = pdf_url
        self._timeout: int = timeout

        # Set by pipeline steps — not available at construction time.
        self._raw_bytes: Optional[bytes] = None
        self._tables: dict[str, dict[str, Any]] = {}
        self._report_date: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Public pipeline entry point
    # ------------------------------------------------------------------

    def extract(self) -> MacroIndicators:
        """
        Execute the full extraction pipeline.

        Steps:
          1. Download PDF
          2. Extract tables into DataFrames
          3. Detect report date
          4. Parse each indicator category
          5. Determine policy signal
          6. Return MacroIndicators

        Returns:
            MacroIndicators dataclass with all available fields populated.
        """
        logger.info("MacroPdfExtractor pipeline started: url=%s", self._pdf_url)

        self._raw_bytes = self.download_pdf()
        self._tables = self.extract_tables()
        self._report_date = self.extract_report_date()

        inflation = self.extract_inflation()
        usd_kes = self.extract_usd_kes_rate()
        cbk_rate = self.extract_cbk_rate()
        petrol, diesel, kerosene = self.extract_fuel_prices()
        nse20, nasi, market_cap = self.extract_nse_market_data()
        m1, m2, m3 = self.extract_money_supply()
        foreign_reserves = self.extract_foreign_reserves()
        exports, imports, trade_volume = self.extract_trade_data()

        policy_signal = self.determine_policy_signal(
            inflation=inflation,
            cbk_rate=cbk_rate,
        )

        result = MacroIndicators(
            report_date=self._report_date,
            inflation=inflation,
            usd_kes=usd_kes,
            cbk_rate=cbk_rate,
            premium_petrol=petrol,
            diesel=diesel,
            kerosene=kerosene,
            nse20=nse20,
            nasi=nasi,
            market_cap=market_cap,
            m1=m1,
            m2=m2,
            m3=m3,
            foreign_reserves=foreign_reserves,
            exports=exports,
            imports=imports,
            trade_volume=trade_volume,
            policy_signal=policy_signal,
        )

        logger.info(
            "MacroPdfExtractor pipeline complete: "
            "report_date=%s inflation=%s usd_kes=%s cbk_rate=%s "
            "petrol=%s diesel=%s kerosene=%s "
            "nse20=%s nasi=%s market_cap=%s policy_signal=%s",
            result.report_date,
            result.inflation,
            result.usd_kes,
            result.cbk_rate,
            result.premium_petrol,
            result.diesel,
            result.kerosene,
            result.nse20,
            result.nasi,
            result.market_cap,
            result.policy_signal,
        )
        return result

    # ------------------------------------------------------------------
    # Step 1 — Download
    # ------------------------------------------------------------------

    def download_pdf(self) -> bytes:
        """
        Download the PDF from the configured URL.

        Attempts with SSL verification first; retries without if a
        certificate error is raised (common with government sites).

        Returns:
            Raw PDF bytes.

        Raises:
            requests.exceptions.RequestException: If both attempts fail.
        """
        logger.info("PDF download started: url=%s timeout=%ds", self._pdf_url, self._timeout)

        try:
            response = requests.get(
                self._pdf_url,
                headers=_BROWSER_HEADERS,
                timeout=self._timeout,
                verify=True,
            )
            response.raise_for_status()

        except requests.exceptions.SSLError as ssl_exc:
            logger.warning(
                "SSL verification failed; retrying without verification: %s", ssl_exc
            )
            warnings.warn(
                "SSL verification disabled for KNBS PDF download — "
                "consider installing the site's certificate.",
                UserWarning,
                stacklevel=2,
            )
            response = requests.get(
                self._pdf_url,
                headers=_BROWSER_HEADERS,
                timeout=self._timeout,
                verify=False,
            )
            response.raise_for_status()

        logger.info(
            "PDF download complete: %d bytes content_type=%s",
            len(response.content),
            response.headers.get("Content-Type", "unknown"),
        )
        return response.content

    # ------------------------------------------------------------------
    # Step 2 — Extract tables
    # ------------------------------------------------------------------

    def extract_tables(self) -> dict[str, dict[str, Any]]:
        """
        Open the PDF and extract every table across all pages into
        named DataFrames.

        Table names follow the pattern: ``page_{page_number}_{table_index}``
        e.g. ``page_6_0``, ``page_7_1``.

        Returns:
            Dict mapping table name → DataFrame.
        """
        if self._raw_bytes is None:
            raise RuntimeError("download_pdf() must be called before extract_tables().")

        tables: dict[str, dict[str, Any]] = {}
        total_tables = 0

        with pdfplumber.open(io.BytesIO(self._raw_bytes)) as pdf:
            page_count = len(pdf.pages)

            logger.info("PDF opened: pages=%d", page_count)

            for page_num, page in enumerate(pdf.pages, start=1):
                table_settings = {
                    "vertical_strategy": "text",
                    "horizontal_strategy": "text",
                    "snap_tolerance": 3,
                    "join_tolerance": 3,
                }

                raw_tables = page.extract_tables(
                    table_settings
                )

                if not raw_tables:
                    continue

                for table_idx, raw_table in enumerate(raw_tables):
                    if not raw_table or len(raw_table) < 2:
                        logger.debug(
                            "Skipping empty/trivial table: page=%d idx=%d",
                            page_num,
                            table_idx,
                        )
                        continue

                    try:
                        df = self._raw_table_to_dataframe(raw_table)
                        df = self._normalize_dataframe(df)
                    except Exception as exc:
                        logger.warning(
                            "DataFrame conversion failed: page=%d idx=%d error=%s",
                            page_num, table_idx, exc,
                        )
                        continue

                    page_text = (
                        page.extract_text() or ""
                    ).lower()

                    name = f"page_{page_num}_{table_idx}"

                    tables[name] = {
                        "df": df,
                        "page_text": page_text
                    }
                    total_tables += 1

                    logger.debug(
                        "Table extracted: %s rows=%d cols=%d",
                        name, len(df), len(df.columns),
                    )
                    logger.info(
                        "Extracted table %s rows=%d cols=%d",
                        name,
                        len(df),
                        len(df.columns)
                    )

                    logger.debug(
                        "\n%s",
                        df.head(3).to_string()
                    )

        logger.info(
            "Table extraction complete: pages_scanned=%d tables_found=%d",
            page_count, total_tables,
        )
        return tables

    # ------------------------------------------------------------------
    # Step 3 — Report date
    # ------------------------------------------------------------------

    def extract_report_date(self) -> Optional[datetime]:
        """
        Detect the report period from the first page of the PDF.

        Looks for patterns like ``Leading Economic Indicators March 2026``
        across the first two pages' text content.

        Returns:
            datetime set to the first day of the report month, or None.
        """
        if self._raw_bytes is None:
            logger.warning("extract_report_date: raw bytes not available.")
            return None

        with pdfplumber.open(io.BytesIO(self._raw_bytes)) as pdf:
            # Check first two pages — title is always near the top
            for page in pdf.pages[:2]:
                text = page.extract_text() or ""
                date = self._parse_month_year_from_text(text)
                if date:
                    logger.info(
                        "Report date detected: %s", date.strftime("%B %Y")
                    )
                    return date

        logger.warning("Could not detect report date from PDF.")
        return None

    # ------------------------------------------------------------------
    # Step 4 — Individual indicator parsers
    # ------------------------------------------------------------------

    def extract_inflation(self) -> Optional[float]:
        """
        Extract Kenya CPI inflation rate from Table 1(b).

        Target: Row containing 'Kenya' in the inflation table.
        Returns the most recent month's value (last numeric column).

        Expected: ~4.39 (March 2026)
        """
        df = self._find_table_by_keywords(
            ["inflation"]
        )

        if df is None:
            return None

        row = self._find_row_by_month(
            df,
            self._report_date.strftime("%B")
        )

        if row is None:
            return None

        values = self._extract_numeric_values(row)

        if len(values) >= 6:

            inflation = values[-1]

            logger.info(
                "Inflation=%s",
                inflation
            )

            return inflation

        return None

    def extract_usd_kes_rate(self) -> Optional[float]:
        """
        Extract the USD/KES exchange rate from Table 2.

        Targets the row for '1 US Dollar'. Deliberately avoids the
        OPEC oil price row which also appears in the exchange rate section.

        Expected: ~129.43 (March 2026)
        """
        df = self._find_table_by_keywords(
            ["us dollar"]
        )

        if df is None:
            return None

        row = self._find_row_by_month(
            df,
            self._report_date.strftime("%B")
        )

        if row is None:
            return None

        nums = self._extract_numeric_values(row)

        if nums:

            usd = nums[0]

            logger.info(
                "USD/KES=%s",
                usd
            )

            return usd

        return None

    def extract_cbk_rate(self) -> Optional[float]:
        """
        Extract the Central Bank Rate (CBR) from Table 3.

        Expected: ~8.75 (March 2026)
        """
        df = self._find_table_by_keywords(
            ["interest rate", "cbr"]
        )

        if df is None:
            return None

        row = self._find_row_by_month(
            df,
            self._report_date.strftime("%B")
        )

        if row is None:
            return None

        nums = self._extract_numeric_values(row)

        if len(nums) >= 4:

            cbr = nums[3]

            logger.info(
                "CBR=%s",
                cbr
            )

            return cbr

        return None
    
    def extract_fuel_prices(self) -> tuple[Optional[float], Optional[float], Optional[float]]:
        """
        Extract retail fuel prices from the energy/pump prices section.

        Returns:
            Tuple of (premium_petrol, diesel, kerosene). Any may be None.

        Expected: (179.35, 167.72, 153.96)
        """
        df = self._find_table_by_keywords(
            keywords=["average retail prices", "petroleum","premium motor gasoline"
                    ],
            min_matches=3,
        )
        if df is None:
            logger.warning("extract_fuel_prices: energy/fuel table not found.")
            return None, None, None

        petrol: Optional[float] = None
        diesel: Optional[float] = None
        kerosene: Optional[float] = None

        for _, row in df.iterrows():
            row_lower = self._row_to_text(row).lower()

            if petrol is None and (
                "premium" in row_lower or "petrol" in row_lower or "super" in row_lower
            ):
                petrol = self._extract_last_numeric(row)
                if petrol:
                    logger.info("Premium petrol extracted: %.4f", petrol)

            elif diesel is None and "diesel" in row_lower:
                diesel = self._extract_last_numeric(row)
                if diesel:
                    logger.info("Diesel extracted: %.4f", diesel)

            elif kerosene is None and (
                "kerosene" in row_lower or "paraffin" in row_lower
            ):
                kerosene = self._extract_last_numeric(row)
                if kerosene:
                    logger.info("Kerosene extracted: %.4f", kerosene)

        return petrol, diesel, kerosene

    def extract_nse_market_data(self) -> tuple[Optional[float], Optional[float], Optional[float]]:
        """
        Extract NSE market indicators from Table 4.

        Returns:
            Tuple of (nse20, nasi, market_cap_billion_kes). Any may be None.

        Expected: (3432, 195, 3231)
        """
        df = self._find_table_by_keywords(
            ["nse", "market capitalization"]
        )

        if df is None:
            return None,None,None

        row = self._find_row_by_month(
            df,
            self._report_date.strftime("%B")
        )

        if row is None:
            return None,None,None

        nums = self._extract_numeric_values(row)

        if len(nums)>=7:

            nse20=nums[0]
            nasi=nums[1]
            market_cap=nums[-1]

            return (
                nse20,
                nasi,
                market_cap
            )

        return None,None,None

    def extract_money_supply(self) -> tuple[Optional[float], Optional[float], Optional[float]]:
        """
        Extract M1, M2, M3 money supply aggregates from Table 5(a).

        Returns:
            Tuple of (m1, m2, m3) in KES billions. Any may be None.
        """
        df = self._find_table_by_keywords(
            keywords=["money supply", "m1", "m2", "m3", "broad money", "narrow money"],
            min_matches=3,
        )
        if df is None:
            logger.warning("extract_money_supply: money supply table not found.")
            return None, None, None

        m1: Optional[float] = None
        m2: Optional[float] = None
        m3: Optional[float] = None

        for _, row in df.iterrows():
            row_lower = self._row_to_text(row).lower()

            if m1 is None and re.search(r"\bm1\b", row_lower):
                m1 = self._extract_last_numeric(row)
                if m1:
                    logger.info("M1 extracted: %.2f", m1)

            elif m2 is None and re.search(r"\bm2\b", row_lower):
                m2 = self._extract_last_numeric(row)
                if m2:
                    logger.info("M2 extracted: %.2f", m2)

            elif m3 is None and re.search(r"\bm3\b", row_lower):
                m3 = self._extract_last_numeric(row)
                if m3:
                    logger.info("M3 extracted: %.2f", m3)

        return m1, m2, m3

    def extract_foreign_reserves(self) -> Optional[float]:
        """
        Extract gross foreign exchange reserves from Table 5(b).

        Returns:
            Gross reserves in USD billions, or None.
        """
        df = self._find_table_by_keywords(
            keywords=["foreign reserve", "gross reserve", "foreign exchange reserve"],
            min_matches=2,
        )
        if df is None:
            logger.warning("extract_foreign_reserves: reserves table not found.")
            return None

        for _, row in df.iterrows():
            row_lower = self._row_to_text(row).lower()
            if "gross" in row_lower or "total" in row_lower or "reserve" in row_lower:
                value = self._extract_last_numeric(row)
                if value is not None:
                    logger.info("Foreign reserves extracted: %.4f", value)
                    return value

        logger.warning("extract_foreign_reserves: gross reserves row not found.")
        return None

    def extract_trade_data(self) -> tuple[Optional[float], Optional[float], Optional[float]]:
        """
        Extract exports, imports, and trade volume from Table 12.

        Returns:
            Tuple of (exports, imports, trade_volume). Any may be None.
        """
        df = self._find_table_by_keywords(
            keywords=["export", "import", "trade", "merchandise"],
            min_matches=3,
        )
        if df is None:
            logger.warning("extract_trade_data: trade table not found.")
            return None, None, None

        exports: Optional[float] = None
        imports: Optional[float] = None

        for _, row in df.iterrows():
            row_lower = self._row_to_text(row).lower()

            if exports is None and (
                "export" in row_lower and "import" not in row_lower
            ):
                exports = self._extract_last_numeric(row)
                if exports:
                    logger.info("Exports extracted: %.2f", exports)

            elif imports is None and "import" in row_lower:
                imports = self._extract_last_numeric(row)
                if imports:
                    logger.info("Imports extracted: %.2f", imports)

        trade_volume: Optional[float] = None
        if exports is not None and imports is not None:
            trade_volume = round(exports + imports, 2)
            logger.info("Trade volume computed: %.2f", trade_volume)

        return exports, imports, trade_volume

    # ------------------------------------------------------------------
    # Step 5 — Policy signal
    # ------------------------------------------------------------------

    @staticmethod
    def determine_policy_signal(
        inflation: Optional[float],
        cbk_rate: Optional[float],
    ) -> str:
        """
        Derive a simple macro policy signal from inflation and CBK rate.

        Signal rules:
          - Hawkish: inflation > 5% (above CBK target band)
          - Dovish:  inflation <= 3% and cbk_rate is available (room to cut)
          - Neutral: all other cases or insufficient data

        Args:
            inflation: Latest CPI inflation rate (%).
            cbk_rate:  Latest Central Bank Rate (%).

        Returns:
            One of: "Hawkish", "Neutral", "Dovish"
        """
        if inflation is None:
            logger.debug("determine_policy_signal: inflation not available → Neutral")
            return "Neutral"

        if inflation > 5.0:
            signal = "Hawkish"
        elif inflation <= 3.0 and cbk_rate is not None:
            signal = "Dovish"
        else:
            signal = "Neutral"

        logger.info(
            "Policy signal: %s (inflation=%.2f cbk_rate=%s)",
            signal,
            inflation,
            f"{cbk_rate:.2f}" if cbk_rate is not None else "N/A",
        )
        return signal

    # ------------------------------------------------------------------
    # Private helpers — table search
    # ------------------------------------------------------------------

    def _find_table_by_keywords(
            self,
            keywords:list[str],
            min_matches:int=2
        )->Optional[pd.DataFrame]:

            best_df=None
            best_score=0

            for name,data in self._tables.items():

                df=data["df"]
                page_text=data["page_text"]

                table_text=self._dataframe_header_text(df)

                combined=(
                    table_text
                    + " "
                    + page_text
                ).lower()

                score = sum(
                    1
                    for keyword in keywords
                    if keyword.lower() in combined
                )

                logger.info(
                    "%s score=%d",
                    name,
                    score
                )

                if score > best_score and score >= min_matches:
                    best_score = score
                    best_df = df


            if best_score>=min_matches:

                logger.info(
                    "Selected table score=%d keywords=%s",
                    best_score,
                    keywords
                )

                return best_df

            return None
    
    def _find_row_by_month(
        self,
        df: pd.DataFrame,
        month: str
    ) -> Optional[pd.Series]:

        month = month.lower()

        for _, row in df.iterrows():

            row_text = " ".join(
                str(x).lower()
                for x in row.values
            )

            if month in row_text:
                return row

        return None
    
    def _extract_month_row(
        self,
        df: pd.DataFrame,
        target_month: str
    ) -> Optional[pd.Series]:

        month = target_month.lower()

        for _, row in df.iterrows():

            text = self._row_to_text(row).lower()

            if month in text:

                return row

        return None

    # ------------------------------------------------------------------
    # Private helpers — numeric extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_last_numeric(row: pd.Series) -> Optional[float]:
        """
        Extract the last parseable float from a DataFrame row.

        Iterates cells in reverse order (right-to-left = most recent period
        in KNBS tables) and returns the first successfully parsed number.

        Handles: "1,234.56", "-0.45", "4.39", "N/A", None, empty strings.

        Args:
            row: A single pandas Series (one table row).

        Returns:
            Float value or None if no numeric cell is found.
        """
        cells = list(row.values)

        for cell in reversed(cells):
            value = _parse_float(str(cell) if cell is not None else "")
            if value is not None:
                return value

        return None
    
    def _extract_numeric_values(
        self,
        data: pd.Series
    )->list[float]:

        text = " ".join(
            str(x)
            for x in data
        )

        nums = re.findall(
            r"(?<!\d)(\d{2,6}(?:,\d{3})*(?:\.\d+)?)",
            text
        )

        cleaned=[]

        for n in nums:

            try:

                value=float(
                    n.replace(",","")
                )

                # Remove obvious garbage
                if value > 10:
                    cleaned.append(value)

            except Exception:
                pass

        return cleaned

    @staticmethod
    def _extract_first_numeric(row: pd.Series) -> Optional[float]:
        """
        Extract the first parseable float from a row (left-to-right).

        Useful when the label is not in the row text and the value
        is always in the first numeric column.
        """
        for cell in row.values:
            value = _parse_float(str(cell) if cell is not None else "")
            if value is not None:
                return value
        return None

    # ------------------------------------------------------------------
    # Private helpers — text utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _raw_table_to_dataframe(raw_table: list[list]) -> pd.DataFrame:
        """
        Convert pdfplumber's raw table (list of lists) to a pandas DataFrame.

        Promotes the first row to column headers. Fills None cells with
        empty strings to avoid dtype inference issues.

        Args:
            raw_table: List-of-lists from pdfplumber page.extract_tables().

        Returns:
            pandas DataFrame.
        """
        if not raw_table or len(raw_table) < 2:
            raise ValueError("Table has fewer than 2 rows — cannot build DataFrame.")

        headers = [str(h) if h is not None else "" for h in raw_table[0]]
        rows = [
            [str(cell) if cell is not None else "" for cell in row]
            for row in raw_table[1:]
        ]

        # Handle duplicate column names — pdfplumber sometimes produces them
        seen: dict[str, int] = {}
        deduped: list[str] = []
        for col in headers:
            if col in seen:
                seen[col] += 1
                deduped.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                deduped.append(col)

        return pd.DataFrame(rows, columns=deduped)

    @staticmethod
    def _normalize_dataframe(
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Clean KNBS table artifacts.
        """

        df = df.copy()

        df = df.fillna("")

        df = df.apply(
            lambda col: col.astype(str)
            .str.replace("\n", " ", regex=False)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

        return df

    @staticmethod
    def _dataframe_header_text(df: pd.DataFrame, rows: int = 4) -> str:
        """
        Flatten column headers + first N data rows into a lower-case string.

        Used for keyword matching to identify the correct table.

        Args:
            df:   Source DataFrame.
            rows: Number of data rows to include (default 4).

        Returns:
            Lower-case combined text string.
        """
        parts: list[str] = list(df.columns)
        for _, row in df.head(rows).iterrows():
            parts.extend(str(cell) for cell in row.values if cell)
        return " ".join(parts).lower()

    @staticmethod
    def _row_to_text(row: pd.Series) -> str:
        """
        Flatten all cells in a row into a single space-joined string.

        Args:
            row: pandas Series.

        Returns:
            Combined row text.
        """
        return " ".join(str(v) for v in row.values if v)

    # ------------------------------------------------------------------
    # Private helpers — date parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_month_year_from_text(text: str) -> Optional[datetime]:
        """
        Extract a ``Month YYYY`` pattern from text and return a datetime
        set to the first day of that month.

        Handles: ``March 2026``, ``MARCH 2026``, ``march 2026``

        Args:
            text: Raw page text from pdfplumber.

        Returns:
            datetime or None.
        """
        pattern = re.compile(
            r"\b(January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+(\d{4})\b",
            re.IGNORECASE,
        )
        match = pattern.search(text)
        if not match:
            return None

        month_name = match.group(1).lower()
        year = int(match.group(2))
        month = _MONTH_MAP.get(month_name)

        if month is None:
            return None

        return datetime(year, month, 1)


# ---------------------------------------------------------------------------
# Module-level utilities
# ---------------------------------------------------------------------------

def _parse_float(text: str) -> Optional[float]:
    """
    Parse a float from a cell string, handling common PDF formatting:
      - Comma thousands separators: "1,234.56" → 1234.56
      - Parenthetical negatives: "(1,234.56)" → -1234.56
      - Percentage signs: "4.39%" → 4.39
      - Leading/trailing whitespace
      - "N/A", "-", "", "None", "nan" → None

    Args:
        text: Raw cell string from a PDF table.

    Returns:
        Float or None.
    """
    cleaned = text.strip()

    if not cleaned or cleaned.lower() in ("n/a", "-", "none", "nan", "—", "*"):
        return None

    # Parenthetical negatives: (1,234.56)
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    if negative:
        cleaned = "-" + cleaned[1:-1]

    # Strip non-numeric characters except digits, dot, minus, comma
    cleaned = re.sub(r"[^\d.,\-]", "", cleaned)

    # Remove thousands commas
    cleaned = cleaned.replace(",", "")

    if not cleaned or cleaned in (".", "-", "-."):
        return None

    try:
        return float(cleaned)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------

def smoke_test() -> None:
    """
    Run a live extraction against the March 2026 KNBS LEI PDF and print
    all extracted indicators to stdout.

    Run directly:
        python scrapers/macro_scraper.py
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    pdf_url = (
        "https://www.knbs.or.ke/wp-content/uploads/2026/05/Leading-Economic-Indicators-March-2026.pdf"
    )

    print("\n" + "=" * 65)
    print("MacroPdfExtractor — KNBS LEI Smoke Test")
    print("=" * 65)
    print(f"URL: {pdf_url}\n")

    extractor = MacroPdfExtractor(pdf_url=pdf_url)
    result: MacroIndicators = extractor.extract()

    def fmt(value: Optional[float], decimals: int = 2) -> str:
        return f"{value:.{decimals}f}" if value is not None else "NOT FOUND"

    print(f"Report date      : {result.report_date.strftime('%B %Y') if result.report_date else 'NOT FOUND'}")
    print(f"Inflation        : {fmt(result.inflation, 2)} %")
    print(f"USD/KES          : {fmt(result.usd_kes, 2)}")
    print(f"CBK Rate         : {fmt(result.cbk_rate, 2)} %")
    print(f"Premium Petrol   : KES {fmt(result.premium_petrol, 2)}")
    print(f"Diesel           : KES {fmt(result.diesel, 2)}")
    print(f"Kerosene         : KES {fmt(result.kerosene, 2)}")
    print(f"NSE20            : {fmt(result.nse20, 0)}")
    print(f"NASI             : {fmt(result.nasi, 0)}")
    print(f"Market Cap       : KES {fmt(result.market_cap, 0)} bn")
    print(f"M1               : KES {fmt(result.m1, 2)} bn")
    print(f"M2               : KES {fmt(result.m2, 2)} bn")
    print(f"M3               : KES {fmt(result.m3, 2)} bn")
    print(f"Foreign Reserves : USD {fmt(result.foreign_reserves, 2)} bn")
    print(f"Exports          : KES {fmt(result.exports, 2)} bn")
    print(f"Imports          : KES {fmt(result.imports, 2)} bn")
    print(f"Trade Volume     : KES {fmt(result.trade_volume, 2)} bn")
    print(f"Policy Signal    : {result.policy_signal}")
    print("=" * 65)


if __name__ == "__main__":
    smoke_test()