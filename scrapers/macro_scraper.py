"""
knbs_extractor.py

Reusable extraction engine for KNBS "Leading Economic Indicators" PDFs.

This module provides a generic, table-agnostic pipeline for locating and
extracting tables from KNBS PDF reports. It is designed so that new
table-specific extractors (CBR, inflation, exchange rates, fuels, stock
market data, etc.) can be added later WITHOUT modifying the reusable
pipeline itself.

Pipeline overview:

    PDF (local file or URL)
     -> KNBSExtractor.get_pdf()            [cached]
     -> KNBSExtractor.get_report_metadata() [cached]
     -> KNBSExtractor.get_toc()             [cached: toc_page + toc_entries]
     -> PipelineEngine.run()                [table-specific work only]
        -> find_table
        -> locate_candidate_pages
        -> extract_candidate_tables
        -> select_best_table
     -> (table-specific parser)
     -> validate_output
     -> return results

CACHING ARCHITECTURE (this revision):
    KNBSExtractor is the single source of truth for everything that is
    document-level and does not change between tables in the same PDF:

        * local PDF path              (get_pdf)
        * report metadata             (get_report_metadata)
        * opened pdfplumber.PDF       (get_pdf_object)
        * TOC page index              (get_toc_page)
        * parsed TOC entries          (get_toc)

    Every one of the above is computed exactly once per unique pdf_url and
    reused by every table extractor (CBR / Inflation / Exchange / Fuel /
    Stock Market) run against that same report. A KNBSContext dataclass
    bundles all of this together and is what extractors actually consume.

    PipelineEngine.run() no longer downloads a PDF or rebuilds the TOC --
    it receives toc_entries + pdf_path and only does the table-specific
    steps (find target table -> candidate pages -> extract -> score).

    No table-parsing logic (parse_cbr_table, parse_inflation_table,
    parse_exchange_dataframe, parse_fuel_table, scoring, validation,
    TABLE_CONFIG) has been changed. This is purely a caching / plumbing
    refactor.

Architecture:

    KNBSExtractor                     <- owns the cache + shared collaborators
    │
    ├── PDFManager                    <- dumb I/O only, no caching
    │     ├── download_pdf()
    │     ├── load_pdf()
    │     ├── extract_report_date_from_url()
    │
    ├── PipelineEngine                <- table-specific work only
    │     ├── find_toc()              (still exists; called by KNBSExtractor)
    │     ├── parse_toc()             (still exists; called by KNBSExtractor)
    │     ├── find_table()
    │     ├── locate_candidate_pages()
    │     ├── extract_candidate_tables()
    │     ├── score_table()
    │     ├── select_best_table()
    │     └── run()                   <- no longer touches TOC/PDF loading
    │
    ├── Validator
    │
    ├── KNBSContext (dataclass)       <- cached bundle handed to extractors
    │
    └── Extractors
          ├── CBRExtractor
          ├── InflationExtractor
          ├── ExchangeExtractor
          ├── FuelExtractor
          └── StockMarketExtractor

FUEL EXTRACTOR NOTE (see FuelExtractor below):
    Unlike the other extractors, FuelExtractor does NOT call
    PipelineEngine.run() end-to-end. It reuses PipelineEngine's individual
    reusable steps (find_table, extract_candidate_tables) but replaces the
    generic candidate-page window and generic scoring with fuel-specific
    logic, because the generic 5-page window and generic keyword scoring
    were drifting into neighboring tables such as "Consumption of
    Petroleum Fuels" (Table 15(c)) instead of "National Average Retail
    Prices for Selected Fuels in Kenya". PipelineEngine itself is not
    modified, and no other extractor is affected. It now sources its TOC
    page / TOC entries / page count from the shared KNBSExtractor cache
    instead of recomputing them.
"""

import logging
import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlparse, unquote

import pdfplumber
import camelot
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from rapidfuzz import fuzz

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("knbs_extractor")


# ---------------------------------------------------------------------------
# CLASS: PDFManager
# ---------------------------------------------------------------------------

class PDFManager:
    """
    Owns all PDF acquisition and metadata logic.

    Responsibilities:
        * validate URL
        * download PDF
        * load PDF
        * extract report date metadata

    Deliberately stateless / cache-free. Every call does real work.
    Caching (so the same PDF isn't downloaded twice) is owned by
    KNBSExtractor, not here -- this class stays a thin, reusable I/O
    utility that has no opinion about "have I done this before".
    """

    def __init__(self, temp_directory="temp"):
        self.temp_directory = temp_directory

    # -----------------------------------------------------------------
    # STEP 0: PDF acquisition (URL download support)
    # -----------------------------------------------------------------

    def download_pdf(self, pdf_url, download_dir=None):
        """
        Download a PDF from a URL and save it to a local temporary directory.

        Args:
            pdf_url (str): Fully qualified URL pointing to a PDF file.
            download_dir (str): Directory to save the downloaded file into.
                Defaults to self.temp_directory (created relative to the
                current working directory if it does not already exist).

        Responsibilities:
            * Validate the URL
            * Download the PDF using requests
            * Save it into a temporary directory, preserving the original
              filename where possible
            * Log progress and errors

        Returns:
            str: Local filesystem path to the downloaded PDF.

        Raises:
            ValueError: If the URL is invalid or does not appear to point to
                a PDF file.
            RuntimeError: If the download fails for any reason.
        """
        if download_dir is None:
            download_dir = self.temp_directory

        logger.info("Downloading PDF...")

        # --- Validate URL -----------------------------------------------------
        parsed_url = urlparse(pdf_url)
        if not parsed_url.scheme or not parsed_url.netloc:
            logger.error("Could not download PDF")
            raise ValueError(f"Invalid PDF URL: {pdf_url}")

        # Derive a filename from the URL path, preserving the original name.
        filename = os.path.basename(unquote(parsed_url.path))
        if not filename:
            filename = "downloaded_report.pdf"
        if not filename.lower().endswith(".pdf"):
            filename = f"{filename}.pdf"

        # --- Prepare temporary directory --------------------------------------
        os.makedirs(download_dir, exist_ok=True)
        local_pdf_path = os.path.join(download_dir, filename)

        # --- Download -----------------------------------------------------------
        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        logger.warning("SSL verification disabled for PDF download")

        try:
            response = session.get(
                pdf_url,
                timeout=30,
                verify=False,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.error("Could not download PDF")
            raise RuntimeError(f"Failed to download PDF from {pdf_url}: {exc}") from exc

        with open(local_pdf_path, "wb") as f:
            f.write(response.content)

        logger.info("Saved PDF to %s", local_pdf_path)
        return local_pdf_path

    def extract_report_date_from_url(self, pdf_url):
        """
        Extract the report month/year from a KNBS PDF URL or filename.

        Example:
            Input:
                "https://www.knbs.or.ke/wp-content/uploads/2026/05/
                 Leading-Economic-Indicators-March-2026.pdf"
            Output:
                {
                    "month": "March",
                    "year": 2026,
                    "report_date": "March 2026"
                }

        Args:
            pdf_url (str): URL (or plain filename) referencing the KNBS report.

        Returns:
            dict | None: Dictionary with "month", "year", and "report_date"
                keys if extraction succeeds, otherwise None.
        """
        parsed_url = urlparse(pdf_url)
        filename = os.path.basename(unquote(parsed_url.path)) or pdf_url

        # Strip extension, e.g. ".pdf"
        filename_no_ext = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)

        month_pattern = (
            r"(January|February|March|April|May|June|July|August|"
            r"September|October|November|December)"
        )
        year_pattern = r"(20\d{2})"

        match = re.search(
            rf"{month_pattern}[-_ ]?{year_pattern}",
            filename_no_ext,
            flags=re.IGNORECASE,
        )

        if not match:
            logger.warning(
                "Could not extract report date from URL: %s", pdf_url
            )
            return None

        month = match.group(1).capitalize()
        year = int(match.group(2))
        report_date = f"{month} {year}"

        logger.info("Report date extracted: %s", report_date)

        return {
            "month": month,
            "year": year,
            "report_date": report_date,
        }

    # -----------------------------------------------------------------
    # STEP 1: PDF handling
    # -----------------------------------------------------------------

    def load_pdf(self, path):
        """
        Validate and open a local PDF file using pdfplumber.

        Args:
            path (str): Local filesystem path to the PDF file.

        Responsibilities:
            * Validate that the file exists
            * Open the PDF using pdfplumber
            * Log the page count
            * Return the opened PDF object

        Returns:
            pdfplumber.PDF: The opened PDF object.

        Raises:
            FileNotFoundError: If the file does not exist at the given path.
        """
        if not os.path.exists(path):
            logger.error("PDF file not found: %s", path)
            raise FileNotFoundError(f"PDF file not found: {path}")

        pdf = pdfplumber.open(path)
        logger.info("Loaded PDF '%s' with %d pages", path, len(pdf.pages))
        return pdf


# ---------------------------------------------------------------------------
# CLASS: PipelineEngine
# ---------------------------------------------------------------------------

class PipelineEngine:
    """
    Holds the reusable, table-agnostic extraction backbone.

    find_toc() / parse_toc() still live here (KNBSExtractor's caching
    getters call them exactly once per PDF). run() itself, however, no
    longer touches PDF loading or TOC parsing -- it is handed toc_entries
    and pdf_path by the caller and only performs table-specific work.
    """

    def __init__(self, pdf_manager):
        self.pdf_manager = pdf_manager

    # -----------------------------------------------------------------
    # STEP 2: TOC detection
    # -----------------------------------------------------------------

    def find_toc(self, pdf, max_pages_to_search=10):
        """
        Locate the "List of Tables" (Table of Contents) page within a PDF.

        Args:
            pdf (pdfplumber.PDF): An opened PDF object (from load_pdf).
            max_pages_to_search (int): Number of leading pages to search
                through before giving up. Defaults to 10.

        Responsibilities:
            * Search only the first few pages of the document
            * Find the page containing "List of Tables"
            * Return the page index

        Returns:
            int | None: Zero-based page index of the TOC page, or None if
                no TOC page was found.
        """
        search_limit = min(max_pages_to_search, len(pdf.pages))

        for page_index in range(search_limit):
            page = pdf.pages[page_index]
            text = page.extract_text() or ""

            if re.search(r"list of tables", text, flags=re.IGNORECASE):
                logger.info("Found TOC ('List of Tables') on page index %d", page_index)
                return page_index

        logger.warning("Could not locate 'List of Tables' in first %d pages", search_limit)
        return None

    # -----------------------------------------------------------------
    # STEP 3: TOC parsing
    # -----------------------------------------------------------------

    def parse_toc(self, pdf, toc_page):
        """
        Parse table-of-contents entries from the TOC page(s).

        Handles common KNBS TOC formatting variations:
            * "Table 1" / "Table 1(a)"
            * Variable spacing between title and page number
            * Dot leaders (e.g. "Table 1: CBR Rates ..... 6")

        Args:
            pdf (pdfplumber.PDF): An opened PDF object.
            toc_page (int): Zero-based page index where the TOC begins
                (typically the result of find_toc()).

        Responsibilities:
            * Extract TOC entries in the form:
                [{"title": "...", "document_page": 6}, ...]

        Returns:
            list[dict]: Parsed TOC entries. Empty list if none found or if
                toc_page is None.
        """
        if toc_page is None:
            logger.warning("No TOC page provided; cannot parse TOC entries")
            return []

        page = pdf.pages[toc_page]
        text = page.extract_text() or ""
        lines = text.splitlines()

        # Matches lines like:
        #   "Table 1: CBR and Interest Rates ........... 6"
        #   "Table 1(a) Exchange Rates   12"
        #   "Table 12  Fuel Prices...... 20"
        entry_pattern = re.compile(
            r"^\s*Table\s+\d+[a-zA-Z]?(?:\([a-zA-Z]\))?\s*[:.\-]?\s*"
            r"(?P<title>.+?)\s*[\.\s]{2,}\s*(?P<page>\d+)\s*$",
            flags=re.IGNORECASE,
        )

        entries = []
        for line in lines:
            line = line.strip()
            if not line.lower().startswith("table"):
                continue

            match = entry_pattern.match(line)
            if match:
                title = match.group("title").strip(" .")
                document_page = int(match.group("page"))
                entries.append({"title": title, "document_page": document_page})
            else:
                logger.warning("Could not parse TOC line: '%s'", line)

        logger.info("Parsed %d TOC entries from page index %d", len(entries), toc_page)
        return entries

    # -----------------------------------------------------------------
    # STEP 4: Target table search
    # -----------------------------------------------------------------

    def find_table(self, entries, target, score_cutoff=50):
        """
        Find the TOC entry that best matches a target table name using
        fuzzy string matching.

        Args:
            entries (list[dict]): Parsed TOC entries from parse_toc().
            target (str): The target table name/description to search for,
                e.g. "Interest rate".
            score_cutoff (int): Minimum fuzzy match score (0-100) required
                to consider a match valid. Defaults to 50.

        Responsibilities:
            * Use fuzzy matching to compare `target` against each entry title
            * Select the best matching entry

        Returns:
            dict | None: {"title": "...", "document_page": x} for the best
                match, or None if no entry meets the score cutoff.
        """
        if not entries:
            logger.warning("No TOC entries available to search for target '%s'", target)
            return None

        best_entry = None
        best_score = -1

        for entry in entries:
            score = fuzz.partial_ratio(target.lower(), entry["title"].lower())
            logger.info("TOC candidate '%s' scored %d against target '%s'",
                        entry["title"], score, target)
            if score > best_score:
                best_score = score
                best_entry = entry

        if best_entry is None or best_score < score_cutoff:
            logger.warning(
                "No suitable match found for target '%s' (best score=%s)",
                target, best_score,
            )
            return None

        logger.info(
            "Selected TOC entry '%s' (page %d) for target '%s' with score %d",
            best_entry["title"], best_entry["document_page"], target, best_score,
        )
        return best_entry

    # -----------------------------------------------------------------
    # STEP 5: Candidate page locator
    # -----------------------------------------------------------------

    def locate_candidate_pages(self, document_page, window_size=5):
        """
        Generate a forward-only search window of candidate pages, starting
        from the page referenced in the TOC.

        Note: This does NOT attempt to calculate any offset between the
        TOC's printed page numbers and actual PDF/document page indices.
        It simply returns a forward window starting at document_page.

        Example:
            locate_candidate_pages(9) -> [9, 10, 11, 12, 13]

        Args:
            document_page (int): Starting page number, as referenced in the TOC.
            window_size (int): Number of pages to include in the forward
                search window. Defaults to 5.

        Returns:
            list[int]: List of candidate page numbers.
        """
        candidate_pages = list(range(document_page, document_page + window_size))
        logger.info(
            "Generated candidate page window starting at %d: %s",
            document_page, candidate_pages,
        )
        return candidate_pages

    # -----------------------------------------------------------------
    # STEP 6: Camelot extraction
    # -----------------------------------------------------------------

    def extract_candidate_tables(self, pdf_path, candidate_pages):
        """
        Run Camelot table extraction (stream flavor) across a set of
        candidate pages.

        Args:
            pdf_path (str): Local filesystem path to the PDF file.
            candidate_pages (list[int]): Page numbers to attempt extraction on.

        Responsibilities:
            * For each candidate page, run Camelot with flavor="stream"
            * Log page number and number of tables found
            * Collect results

        Returns:
            list[dict]: [{"page": page_number, "table": dataframe}, ...]
                One entry per table found (a single page may yield multiple
                tables, or none).
        """
        candidate_tables = []

        for page_number in candidate_pages:
            try:
                tables = camelot.read_pdf(
                    pdf_path,
                    pages=str(page_number),
                    flavor="stream",
                )
            except Exception as exc:  # noqa: BLE001 - log and continue on any Camelot failure
                logger.warning(
                    "Camelot extraction failed on page %d: %s", page_number, exc
                )
                continue

            logger.info("Page %d: found %d table(s)", page_number, len(tables))

            for table in tables:
                candidate_tables.append({
                    "page": page_number,
                    "table": table.df,
                })

        logger.info(
            "Extracted %d total candidate table(s) across %d page(s)",
            len(candidate_tables), len(candidate_pages),
        )
        return candidate_tables

    # -----------------------------------------------------------------
    # STEP 7: Table scoring
    # -----------------------------------------------------------------

    def score_table(self, dataframe, keywords):
        """
        Score a dataframe based on how many times each keyword appears in
        its flattened text content, weighted by the keyword's configured
        importance.

        Args:
            dataframe (pandas.DataFrame): Candidate table to score.
            keywords (dict): Mapping of keyword -> weight, e.g.
                {"interest": 2, "cbr": 3, "deposit": 1}

        Responsibilities:
            * Flatten all dataframe cell text into a single lowercase string
            * Count keyword occurrences weighted by their configured score

        Returns:
            int: Total numeric score for the table.
        """
        if dataframe is None or dataframe.empty:
            return 0

        flattened_text = " ".join(
            str(cell) for cell in dataframe.values.flatten()
        ).lower()

        total_score = 0
        for keyword, weight in keywords.items():
            occurrences = flattened_text.count(keyword.lower())
            total_score += occurrences * weight

        return total_score

    # -----------------------------------------------------------------
    # STEP 8: Best table selector
    # -----------------------------------------------------------------

    def select_best_table(self, candidate_tables, keywords):
        """
        Score every candidate table and select the highest scoring one.

        Args:
            candidate_tables (list[dict]): Output of extract_candidate_tables().
            keywords (dict): Keyword weight mapping used for scoring.

        Responsibilities:
            * Score all candidate tables
            * Select the highest scoring dataframe
            * Log each table's score and the final selection

        Returns:
            pandas.DataFrame | None: The best matching dataframe, or None if
                no candidate tables were provided.
        """
        if not candidate_tables:
            logger.warning("No candidate tables available to select from")
            return None

        best_table = None
        best_score = -1
        best_index = -1

        for index, candidate in enumerate(candidate_tables):
            score = self.score_table(candidate["table"], keywords)
            logger.info("table %d (page %d) score=%d", index, candidate["page"], score)

            if score > best_score:
                best_score = score
                best_table = candidate["table"]
                best_index = index

        logger.info("selected table=%d (score=%d)", best_index, best_score)
        return best_table

    # -----------------------------------------------------------------
    # STEP 9: Main reusable extraction engine (table-specific work only)
    # -----------------------------------------------------------------

    def run(self, pdf_path, toc_entries, target, keywords):
        """
        Execute the table-specific portion of the extraction pipeline for
        a given target table.

        Unlike the original implementation, this method does NOT load the
        PDF or rebuild the TOC -- both are supplied by the caller
        (KNBSExtractor's cached getters), since they are identical for
        every table extracted from the same report. This method only does
        the work that genuinely differs per target table:

            find_table -> locate_candidate_pages -> extract_candidate_tables
            -> select_best_table

        This function does NOT parse table contents into structured records.
        It only locates and returns the best-matching raw dataframe for the
        requested target table. Table-specific parsing is the responsibility
        of the individual extractor classes.

        Args:
            pdf_path (str): Local filesystem path to the PDF file (from
                KNBSExtractor.get_pdf()).
            toc_entries (list[dict]): Already-parsed TOC entries (from
                KNBSExtractor.get_toc()).
            target (str): Target table name/description to search for.
            keywords (dict): Keyword weight mapping used for scoring
                candidate tables.

        Returns:
            pandas.DataFrame | None: The selected raw dataframe, or None if
                the pipeline could not locate a suitable table.
        """
        logger.info("Running pipeline for target='%s'", target)

        matched_entry = self.find_table(toc_entries, target)

        if matched_entry is None:
            logger.error("Pipeline aborted: no matching TOC entry for target '%s'", target)
            return None

        candidate_pages = self.locate_candidate_pages(matched_entry["document_page"])
        candidate_tables = self.extract_candidate_tables(pdf_path, candidate_pages)
        selected_dataframe = self.select_best_table(candidate_tables, keywords)

        return selected_dataframe


# ---------------------------------------------------------------------------
# CLASS: Validator
# ---------------------------------------------------------------------------

class Validator:
    """
    Owns generic, table-agnostic output validation.
    """

    def validate_output(self, data):
        """
        Perform generic, table-agnostic validation checks on extracted data.

        Checks performed:
            * Output is not empty
            * Records are well-formed (dict-like, if data is a list of records)
            * No obviously missing/blank values in top-level fields

        NOTE: Table-specific validation (e.g. checking that a CBR value is a
        valid percentage) is NOT implemented here. This is intentionally
        generic and will be extended by table-specific extractors later.

        Args:
            data (Any): The data to validate. Typically a list of records or
                a pandas.DataFrame.

        Returns:
            bool: True if the data passes generic validation, False otherwise.
        """
        if data is None:
            logger.error("Validation failed: output is None")
            return False

        # Handle pandas DataFrame
        if hasattr(data, "empty"):
            if data.empty:
                logger.error("Validation failed: dataframe is empty")
                return False
            logger.info("Validation passed: dataframe has %d row(s)", len(data))
            return True

        # Handle list-like output
        if isinstance(data, list):
            if len(data) == 0:
                logger.warning("Validation warning: output list is empty")
                return False

            for i, record in enumerate(data):
                if not isinstance(record, dict):
                    logger.error("Malformed record at index %d: not a dict", i)
                    return False
                if any(value in (None, "", []) for value in record.values()):
                    logger.warning("Record at index %d has missing value(s)", i)

            logger.info("Validation passed: %d record(s) checked", len(data))
            return True

        logger.warning("Validation skipped: unrecognized data type %s", type(data))
        return False


# ---------------------------------------------------------------------------
# Standardized extractor response helper
# ---------------------------------------------------------------------------
#
# This helper ONLY shapes the final return value of each extractor's
# extract() method into a common structure. It performs no extraction,
# parsing, scoring, or validation of its own, and does not alter any of
# the existing business logic above -- it is purely a formatting step
# applied to already-produced results.
# ---------------------------------------------------------------------------

def build_extractor_response(table_name, metadata, pdf_url, data, status=None):
    """
    Build the standardized top-level response object returned by every
    table-specific extractor.

    Args:
        table_name (str): Human-readable table name (e.g. "Interest Rate").
        metadata (dict | None): Report-level metadata dict containing at
            least a "report_date" key. May be None if report date
            extraction failed; in that case "report_date" in the response
            will be None.
        pdf_url (str): The original PDF URL passed into the extractor.
        data (list): The already-parsed records for this table, exactly as
            produced by the extractor's existing parsing logic.
        status (str | None): Explicit status override, either "success" or
            "failed". If not provided, status is inferred: "success" if
            `data` is non-empty, otherwise "failed".

    Returns:
        dict: {
            "table_name": str,
            "report_date": str | None,
            "status": "success" | "failed",
            "source_url": str,
            "data": list,
        }
    """
    if data is None:
        data = []

    if status is None:
        status = "success" if data else "failed"

    return {
        "table_name": table_name,
        "report_date": metadata.get("report_date") if metadata else None,
        "status": status,
        "source_url": pdf_url,
        "data": data,
    }


# ---------------------------------------------------------------------------
# Configuration storage
# ---------------------------------------------------------------------------

@dataclass
class TableConfig:
    """Configuration for a single target table: what to search for and
    which keywords to use when scoring candidate tables."""
    target: str
    keywords: dict


TABLE_CONFIG = {
    "cbr": TableConfig(
        target="Interest rate",
        keywords={
            "interest": 2,
            "cbr": 3,
            "deposit": 1,
        },
    ),
    "inflation": TableConfig(
        target="Inflation rate",
        keywords={
            "inflation": 3,
            "cpi": 2,
            "index": 1,
        },
    ),
    "exchange_rates": TableConfig(
        target="Mean Monthly Foreign Exchange Rates of Kenyan Shilling against Selected Major Currencies",
        keywords={
            "foreign": 4,
            "exchange": 5,
            "currency": 4,
            "dollar": 4,
            "sterling": 4,
            "euro": 4,
            "yen": 2,
            "rand": 2,
        },
    ),
    "fuels": TableConfig(
        # Matched by title/semantic meaning against the TOC, never by table
        # number (table numbers such as "15(e)" can shift between reports).
        target="National Average Retail Prices for Selected Fuels in Kenya",
        keywords={
            "fuel": 3,
            "diesel": 4,
            "gasoline": 3,
            "kerosene": 2,
            "lpg": 2,
            "charcoal": 1,
        },
    ),
    "stock_market": TableConfig(
        target="Stock market",
        keywords={
            "stock": 3,
            "nse": 2,
            "share": 1,
            "index": 1,
        },
    ),
}


# ---------------------------------------------------------------------------
# KNBSContext -- the reusable, cached extraction context
# ---------------------------------------------------------------------------
#
# Bundles everything that is identical for every table extracted from the
# same KNBS report. Built lazily by KNBSExtractor and handed to every
# table-specific extractor so none of them ever re-download the PDF or
# re-parse the TOC.
# ---------------------------------------------------------------------------

@dataclass
class KNBSContext:
    """
    Cached, document-level extraction context for a single KNBS PDF.

    Attributes:
        pdf_path (str): Local filesystem path to the downloaded PDF.
        pdf (pdfplumber.PDF): The opened pdfplumber PDF object.
        report_metadata (dict): {"source_url", "report_date", "month", "year"}.
        toc_page (int | None): Zero-based page index of the TOC.
        toc_entries (list[dict]): Parsed TOC entries.
        source_url (str): The original PDF URL this context was built for.
    """
    pdf_path: str
    pdf: "pdfplumber.PDF"
    report_metadata: dict
    toc_page: Optional[int]
    toc_entries: list
    source_url: str


# ---------------------------------------------------------------------------
# Placeholder table-specific extractors
# ---------------------------------------------------------------------------
#
# Extractors now accept a `knbs` reference (the owning KNBSExtractor
# instance) instead of a raw PDFManager. They pull everything document-level
# (pdf_path, metadata, TOC) from `knbs.get_context(pdf_url)`, which is
# computed once and cached for the lifetime of that KNBSExtractor instance.
#
# No extractor calls pdf_manager.download_pdf(), pdf_manager
# .extract_report_date_from_url(), pipeline.find_toc(), or
# pipeline.parse_toc() directly anymore -- only KNBSExtractor's cached
# getters do that.
#
# (FuelExtractor is the exception in terms of *table-selection* strategy --
# see its class docstring below -- but it too now reads TOC/page-count
# information from the shared cache instead of recomputing it.)
# ---------------------------------------------------------------------------

class BaseExtractor(ABC):
    """
    Shared base for all table-specific extractors.

    Holds a reference to the owning KNBSExtractor (`knbs`) for cached,
    document-level context, plus the shared PipelineEngine and Validator
    instances for table-specific work.
    """

    def __init__(self, knbs, pipeline, validator):
        self.knbs = knbs
        self.pipeline = pipeline
        self.validator = validator

    @abstractmethod
    def extract(self, pdf_url):
        pass


# ---------------------------------------------------------------------------
# CBR-specific parsing utilities
# ---------------------------------------------------------------------------
#
# The constants and helper function below are scoped ONLY to CBR table
# parsing. They convert the raw Camelot dataframe (already located and
# selected by the reusable PipelineEngine.run() pipeline) into structured
# CBR records. No reusable pipeline logic is touched or duplicated here.
# ---------------------------------------------------------------------------

MONTH_NAMES_SET = {
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
}

# Zero-based index, within a row's cleaned tokens, where the CBR value
# is expected to live for KNBS "Interest rate" tables.
CBR_POSITION = 4

# Rows containing any of these phrases are footnotes/labels, not data rows,
# and should be skipped.
_SKIP_ROW_PHRASES = [
    "source",
    "notes",
    "commercial banks",
    "government treasury",
    "central bank rates",
]


def parse_cbr_table(dataframe, metadata):
    """
    Parse a raw CBR/interest-rate Camelot dataframe (as returned by
    PipelineEngine.run()) into a list of structured CBR records.

    Args:
        dataframe (pandas.DataFrame): Raw candidate table selected by the
            reusable pipeline for the CBR/"Interest rate" target.
        metadata (dict): Report-level metadata already assembled by
            CBRExtractor.extract() -- expects "report_date", "month",
            "year", and "source_url" keys.

    Returns:
        list[dict]: Structured CBR records, one per recognized month row.
            Returns an empty list if the dataframe is None/empty or no
            valid month rows are found.
    """
    records = []
    current_year = None

    if dataframe is None or dataframe.empty:
        logger.warning("parse_cbr_table: received empty or None dataframe")
        return records

    for _, row in dataframe.iterrows():
        # --- Clean and tokenize the row --------------------------------
        raw_values = [str(cell) for cell in row.tolist()]
        tokens = [
            value.replace("*", "").strip()
            for value in raw_values
            if value is not None and value.strip() not in ("", "None")
        ]
        tokens = [token for token in tokens if token != ""]

        if not tokens:
            continue

        logger.info("RAW TOKENS: %s", tokens)

        flattened_row_text = " ".join(tokens).lower()

        # --- Skip footnote / label rows ---------------------------------
        if any(phrase in flattened_row_text for phrase in _SKIP_ROW_PHRASES):
            logger.info("Skipping row (matched skip phrase): %s", tokens)
            continue

       # --- Year header row (e.g. "2024", "2025", "2026") ----------------
        if re.fullmatch(r"20\d{2}", tokens[0].strip()):
            current_year = int(tokens[0])

            logger.info("Current parsing year set to %d", current_year)

            continue

        # --- Determine whether this is a month row ----------------------
        month = tokens[0].strip(":").capitalize()

        if month not in MONTH_NAMES_SET:
            logger.info("Skipping non-month row: %s", tokens)
            continue

        # --- Validate token count ----------------------------------------
        if len(tokens) < 5:
            logger.warning("Skipping %s: not enough tokens (%d)", month, len(tokens))
            continue

        # --- Extract and validate the CBR value ---------------------------
        cbr_raw = tokens[CBR_POSITION]

        try:
            cbr_value = float(cbr_raw)
        except (TypeError, ValueError):
            logger.warning(
                "Skipping %s: CBR value '%s' invalid", month, cbr_raw
            )
            continue

        logger.info("Extracted CBR -> %s = %.2f", month, cbr_value)

        record = {
            "month": month,
            "year" : current_year,
            "cbr": cbr_value
        }
        records.append(record)

    logger.info("parse_cbr_table: parsed %d CBR record(s)", len(records))
    return records


class CBRExtractor(BaseExtractor):
    """
    Extractor for the CBR (Central Bank Rate) / interest rates table.
    """

    def extract(self, pdf_url):
        """
        Args:
            pdf_url (str): URL to the KNBS PDF report.

        Returns:
            dict: Standardized extractor response (see
                build_extractor_response()), with "data" containing the
                structured CBR records.
        """
        context = self.knbs.get_context(pdf_url)

        config = TABLE_CONFIG["cbr"]
        table_df = self.pipeline.run(
            pdf_path=context.pdf_path,
            toc_entries=context.toc_entries,
            target=config.target,
            keywords=config.keywords,
        )

        logger.info("extract_cbr metadata: %s", context.report_metadata)

        if table_df is None or table_df.empty:
            logger.error("extract_cbr: no candidate table found; returning empty list")
            return build_extractor_response("Interest Rate", context.report_metadata, pdf_url, [])

        results = parse_cbr_table(table_df, context.report_metadata)

        if not self.validator.validate_output(results):
            logger.warning("extract_cbr: output failed validation")

        return build_extractor_response("Interest Rate", context.report_metadata, pdf_url, results)


# ---------------------------------------------------------------------------
# Inflation-specific parsing utilities
# ---------------------------------------------------------------------------
#
# The constants and helper function below are scoped ONLY to inflation
# table parsing. They convert the raw Camelot dataframe (already located
# and selected by the reusable PipelineEngine.run() pipeline) into
# structured inflation records. No reusable pipeline logic is touched or
# duplicated here. MONTH_NAMES_SET is reused from the CBR section above.
# ---------------------------------------------------------------------------

# Kenya inflation is always the last token in a valid month row
# (Lower Income, Middle Income, Upper Income, Nairobi, Combined, Kenya).
INFLATION_POSITION = -1

# Rows containing any of these phrases are headers/footnotes/labels, not
# data rows, and should be skipped.
_INFLATION_SKIP_ROW_PHRASES = [
    "source",
    "notes",
    "inflation rates",
    "income",
    "nairobi",
    "combined",
]


def parse_inflation_table(dataframe, metadata):
    """
    Parse a raw inflation-rate Camelot dataframe (as returned by
    PipelineEngine.run()) into a list of structured inflation records.

    Args:
        dataframe (pandas.DataFrame): Raw candidate table selected by the
            reusable pipeline for the inflation/"Inflation rate" target.
        metadata (dict): Report-level metadata already assembled by
            InflationExtractor.extract() -- expects "report_date", "month",
            "year", and "source_url" keys.

    Returns:
        list[dict]: Structured inflation records, one per recognized month
            row. Returns an empty list if the dataframe is None/empty or no
            valid month rows are found.
    """
    records = []
    current_year = None

    if dataframe is None or dataframe.empty:
        logger.warning("parse_inflation_table: received empty or None dataframe")
        return records

    for _, row in dataframe.iterrows():
        # --- Clean and tokenize the row --------------------------------
        raw_values = [str(cell) for cell in row.tolist()]
        tokens = [
            value.replace("*", "").strip()
            for value in raw_values
            if value is not None and value.strip() not in ("", "None")
        ]
        tokens = [token for token in tokens if token != ""]

        if not tokens:
            continue

        logger.info("RAW INFLATION TOKENS: %s", tokens)

        flattened_row_text = " ".join(tokens).lower()

        # --- Skip header / footnote / label rows -------------------------
        if any(phrase in flattened_row_text for phrase in _INFLATION_SKIP_ROW_PHRASES):
            logger.info("Skipping row (matched skip phrase): %s", tokens)
            continue

        # --- Year header row (e.g. "2024", "2025", "2026") ----------------
        if re.fullmatch(r"20\d{2}", tokens[0].strip()):
            current_year = int(tokens[0])

            logger.info("Current parsing year set to %d", current_year)

            continue

        # --- Determine whether this is a month row ----------------------
        month = tokens[0].strip(":").capitalize()

        if month not in MONTH_NAMES_SET:
            logger.info("Skipping non-month row: %s", tokens)
            continue

        # --- Extract and validate the Kenya inflation value ---------------
        inflation_raw = tokens[INFLATION_POSITION]

        try:
            inflation_value = float(inflation_raw)
        except (TypeError, ValueError):
            logger.warning(
                "Skipping %s: inflation value '%s' invalid", month, inflation_raw
            )
            continue

        logger.info(
            "Extracted Kenya inflation -> %s = %.2f",
            month,
            inflation_value,
        )

        record = {
            "month": month,
            "year" : current_year,
            "kenya_inflation": inflation_value
        }
        records.append(record)

    logger.info(
        "parse_inflation_table: parsed %d inflation record(s)",
        len(records)
    )
    return records


class InflationExtractor(BaseExtractor):
    """
    Extractor for the inflation rate table.
    """

    def extract(self, pdf_url):
        """
        Args:
            pdf_url (str): URL to the KNBS PDF report.

        Returns:
            dict: Standardized extractor response (see
                build_extractor_response()), with "data" containing the
                structured inflation records.
        """
        context = self.knbs.get_context(pdf_url)

        config = TABLE_CONFIG["inflation"]
        table_df = self.pipeline.run(
            pdf_path=context.pdf_path,
            toc_entries=context.toc_entries,
            target=config.target,
            keywords=config.keywords,
        )

        logger.info("extract_inflation metadata: %s", context.report_metadata)

        if table_df is None or table_df.empty:
            logger.error(
                "extract_inflation: no candidate table found"
            )
            return build_extractor_response(
                "Consumer Price Indices and Inflation Rates", context.report_metadata, pdf_url, []
            )

        results = parse_inflation_table(
            table_df,
            context.report_metadata,
        )

        if not self.validator.validate_output(results):
            logger.warning(
                "extract_inflation: output failed validation"
            )

        return build_extractor_response(
            "Consumer Price Indices and Inflation Rates", context.report_metadata, pdf_url, results
        )


# ---------------------------------------------------------------------------
# Fuel-specific parsing utilities
# ---------------------------------------------------------------------------
#
# The constants and helper function below are scoped ONLY to fuel table
# parsing. They convert the raw Camelot dataframe (already located and
# selected by the fuel-specific selection logic further below) into
# structured fuel records. No reusable pipeline logic is touched or
# duplicated here. MONTH_NAMES_SET is reused from the CBR section above.
# ---------------------------------------------------------------------------

# Zero-based index, within a row's cleaned tokens, where the Light Diesel
# Oil (KSh per Litre) value is expected to live for the KNBS
# "National Average Retail Prices for Selected Fuels in Kenya" rows.
#
# After cleaning/tokenization:
#   ["January", "177.25", "167.84", "152.18", "3122.16", "86.22"]
# positions become:
#   0 -> month
#   1 -> gasoline
#   2 -> diesel
#   3 -> kerosene
#   4 -> lpg
#   5 -> charcoal
FUEL_DIESEL_POSITION = 2

# Rows containing any of these phrases are headers/footnotes/labels, not
# data rows, and should be skipped.
_FUEL_SKIP_ROW_PHRASES = [
    "source",
    "notes",
    "national average retail prices",
    "motor gasoline",
    "light diesel",
    "illuminating",
    "charcoal",
    "descriptions",
    "period",
]


def parse_fuel_table(dataframe, metadata):
    """
    Parse a raw fuel-prices Camelot dataframe (as returned by the
    fuel-specific table selection logic) into a list of structured fuel
    records.

    Args:
        dataframe (pandas.DataFrame): Raw candidate table selected for the
            fuels/"National Average Retail Prices for Selected Fuels in
            Kenya" target.
        metadata (dict): Report-level metadata already assembled by
            FuelExtractor.extract() -- expects "report_date", "month",
            "year", and "source_url" keys.

    Returns:
        list[dict]: Structured fuel records, one per recognized month row.
            Returns an empty list if the dataframe is None/empty or no
            valid month rows are found.
    """
    records = []
    current_year = None

    if dataframe is None or dataframe.empty:
        logger.warning("parse_fuel_table: received empty or None dataframe")
        return records

    for _, row in dataframe.iterrows():
        # --- Clean and tokenize the row --------------------------------
        raw_values = [str(cell) for cell in row.tolist()]
        tokens = [
            value.replace("*", "").replace(",", "").strip()
            for value in raw_values
            if value is not None and value.strip() not in ("", "None")
        ]
        tokens = [token for token in tokens if token != ""]

        if not tokens:
            continue

        logger.info("RAW FUEL TOKENS: %s", tokens)

        flattened_row_text = " ".join(tokens).lower()

        # --- Skip header / footnote / label rows -------------------------
        if any(phrase in flattened_row_text for phrase in _FUEL_SKIP_ROW_PHRASES):
            logger.info("Skipping row (matched skip phrase): %s", tokens)
            continue

        # --- Year header row (e.g. "2024", "2025", "2026") ----------------
        if re.fullmatch(r"20\d{2}", tokens[0].strip()):
            current_year = int(tokens[0])

            logger.info("Current parsing year set to %d", current_year)

            continue

        # --- Determine whether this is a month row ----------------------
        month = tokens[0].strip(":").capitalize()

        if month not in MONTH_NAMES_SET:
            logger.info("Skipping non-month row: %s", tokens)
            continue

        # --- Validate token count ----------------------------------------
        if len(tokens) < 6:
            logger.warning(
                "Skipping %s: not enough tokens (%d)",
                month,
                len(tokens)
            )
            continue

        # --- Extract and validate the diesel value -------------------------
        diesel_raw = tokens[FUEL_DIESEL_POSITION]

        try:
            diesel_price = float(diesel_raw)
        except (TypeError, ValueError):
            logger.warning(
                "Skipping %s: diesel value '%s' invalid",
                month,
                diesel_raw
            )
            continue

        logger.info(
            "Extracted Diesel price -> %s = %.2f",
            month,
            diesel_price
        )

        record = {
            "month": month,
            "year": current_year,
            "diesel_price": diesel_price,
        }
        records.append(record)

    logger.info(
        "parse_fuel_table: parsed %d fuel record(s)",
        len(records)
    )
    return records


# ---------------------------------------------------------------------------
# Fuel-specific table IDENTIFICATION utilities
# ---------------------------------------------------------------------------
#
# These are deliberately separate from parse_fuel_table() above:
# parse_fuel_table() turns an ALREADY-SELECTED dataframe into records, while
# the functions below are responsible for SELECTING the correct dataframe
# out of several candidates in the first place. They exist because the
# generic PipelineEngine.select_best_table()/score_table() logic was
# drifting into neighboring tables (e.g. "Consumption of Petroleum Fuels",
# "OPEC Reference Basket Prices"). None of this touches PipelineEngine or
# any other extractor.
# ---------------------------------------------------------------------------

# Strong positive signals that a candidate table IS the fuel retail-price
# table. Weighted higher for phrases that are essentially unique to this
# table's row/column labels.
FUEL_POSITIVE_KEYWORDS = {
    "national": 8,
    "retail": 8,
    "selected fuels": 10,
    "kenya": 8,
    "motor gasoline": 10,
    "premium": 10,
    "light diesel": 10,
    "illuminating kerosene": 10,
    "l.p.g": 10,
    "charcoal": 10,
    "ksh per litre": 6,
    "ksh per 13 kg": 6,
    "descriptions": 3,
    "period": 3,
    "january": 2,
    "february": 2,
    "march": 2,
    "april": 2,
    "may": 2,
    "june": 2,
    "july": 2,
    "august": 2,
    "september": 2,
    "october": 2,
    "november": 2,
    "december": 2,
}

# Strong negative signals that a candidate table is a DIFFERENT, nearby
# fuel-related table that should be rejected instead.
FUEL_PENALTY_KEYWORDS = {
    "aviation": -15,
    "jet": -15,
    "fuel oil": -15,
    "consumption": -20,
    "petroleum fuels": -20,
    "opec": -20,
}

# A correctly identified fuel retail-price table must mention at least this
# many of the five core fuel categories somewhere in its cells.
FUEL_REQUIRED_KEYWORDS = ["motor", "diesel", "kerosene", "l.p.g", "charcoal"]
FUEL_REQUIRED_MIN_MATCHES = 4


def _flatten_table_text(dataframe):
    """Flatten every cell of a dataframe into one lowercase string."""
    if dataframe is None or dataframe.empty:
        return ""
    return " ".join(str(cell) for cell in dataframe.values.flatten()).lower()


def score_fuel_table(dataframe):
    """
    Score a candidate dataframe specifically for the fuel retail-price
    table, combining weighted positive keyword matches with weighted
    penalties for keywords that indicate a different, nearby table.

    Args:
        dataframe (pandas.DataFrame): Candidate table to score.

    Returns:
        int: Total score. Higher is a stronger match for the fuel retail
            price table; strongly negative scores indicate a different
            table (e.g. petroleum consumption, OPEC prices).
    """
    flattened_text = _flatten_table_text(dataframe)
    if not flattened_text:
        return 0

    score = 0
    for keyword, weight in FUEL_POSITIVE_KEYWORDS.items():
        occurrences = flattened_text.count(keyword.lower())
        score += occurrences * weight

    for keyword, weight in FUEL_PENALTY_KEYWORDS.items():
        occurrences = flattened_text.count(keyword.lower())
        score += occurrences * weight

    return score


def validate_fuel_table(dataframe):
    """
    Confirm a candidate dataframe actually contains the core fuel
    categories expected in the retail-price table before it is accepted.

    Args:
        dataframe (pandas.DataFrame): Candidate table to validate.

    Returns:
        bool: True if at least FUEL_REQUIRED_MIN_MATCHES of
            FUEL_REQUIRED_KEYWORDS are present, False otherwise.
    """
    flattened_text = _flatten_table_text(dataframe)
    if not flattened_text:
        return False

    matches = sum(1 for keyword in FUEL_REQUIRED_KEYWORDS if keyword in flattened_text)
    return matches >= FUEL_REQUIRED_MIN_MATCHES


def select_fuel_table(candidate_tables):
    """
    Select the correct fuel retail-price table out of several narrowly
    windowed candidates, using fuel-specific scoring and a required-keyword
    validation gate so petroleum-consumption/OPEC tables are rejected even
    if they happen to score reasonably well.

    Args:
        candidate_tables (list[dict]): [{"page": int, "table": DataFrame}, ...]

    Returns:
        pandas.DataFrame | None: The selected dataframe, or None if no
            candidate passed validation.
    """
    if not candidate_tables:
        logger.warning("select_fuel_table: no candidate tables available")
        return None

    scored_candidates = []
    for index, candidate in enumerate(candidate_tables):
        table_df = candidate["table"]
        page = candidate["page"]
        score = score_fuel_table(table_df)

        logger.info("Table %d page=%d score=%d", index, page, score)
        if table_df is not None and not table_df.empty:
            logger.info("TABLE PREVIEW:\n%s", table_df.head(10))

        scored_candidates.append((score, index, page, table_df))

    # Highest score first; ties broken by original (page) order.
    scored_candidates.sort(key=lambda item: item[0], reverse=True)

    for score, index, page, table_df in scored_candidates:
        if validate_fuel_table(table_df):
            logger.info(
                "Selected table title=%s page=%d score=%d",
                TABLE_CONFIG["fuels"].target, page, score,
            )
            return table_df

        logger.warning(
            "Rejected candidate table %d (page=%d, score=%d): "
            "failed required fuel-keyword validation",
            index, page, score,
        )

    logger.error("select_fuel_table: no candidate table passed validation")
    return None


# ---------------------------------------------------------------------------
# Exchange-rate-specific parsing utilities
# ---------------------------------------------------------------------------
#
# The constants and helper function below are scoped ONLY to exchange-rate
# table parsing. They convert the raw Camelot dataframe (already located
# and selected by the reusable PipelineEngine.run() pipeline) into
# structured exchange-rate records. No reusable pipeline logic is touched
# or duplicated here. MONTH_NAMES_SET is reused from the CBR section above.
# ---------------------------------------------------------------------------

# Column positions (within a cleaned month-row's tokens) for the three
# currencies extracted from Table 2 ("Mean Monthly Foreign Exchange Rates
# of Kenyan Shilling against Selected Major Currencies"). Japanese Yen, SA
# Rand, USHS/KSh, and TSHS/KSh columns are intentionally ignored.
EXCHANGE_COLUMNS = {
    "usd": 1,
    "pound_sterling": 2,
    "euro": 3,
}

# Rows containing any of these phrases are headers/footnotes/labels, not
# data rows, and should be skipped.
_EXCHANGE_SKIP_ROW_PHRASES = [
    "currency",
    "period",
    "source",
    "figure",
]

# A valid month row must contain at least this many numeric values
# (beyond the leading month token) to be considered well-formed.
_EXCHANGE_MIN_NUMERIC_VALUES = 4


def parse_exchange_dataframe(dataframe):
    """
    Parse a raw exchange-rate Camelot dataframe (as returned by
    PipelineEngine.run()) into a list of structured exchange-rate records.

    Only three currencies are extracted per KNBS Table 2: 1 US Dollar,
    1 Pound Sterling, and 1 Euro. Japanese Yen, SA Rand, USHS/KSh, and
    TSHS/KSh columns are ignored.

    Args:
        dataframe (pandas.DataFrame): Raw candidate table selected by the
            reusable pipeline for the exchange_rates/"Mean Monthly Foreign
            Exchange Rates..." target.

    Returns:
        list[dict]: Structured exchange-rate records, one per recognized
            month row, each with "month", "year", "usd", "pound_sterling",
            and "euro" keys. Returns an empty list if the dataframe is
            None/empty or no valid month rows are found.
    """
    records = []

    if dataframe is None or dataframe.empty:
        logger.warning("parse_exchange_dataframe: received empty or None dataframe")
        return records

    current_year = None
    rows = dataframe.values.tolist()

    for raw_row in rows:
        # --- Clean and tokenize the row --------------------------------
        raw_values = [str(cell) for cell in raw_row]
        tokens = [
            value.replace("*", "").replace(",", "").strip()
            for value in raw_values
            if value is not None and value.strip() not in ("", "None")
        ]
        tokens = [token for token in tokens if token != ""]

        if not tokens:
            continue

        logger.info("RAW EXCHANGE TOKENS: %s", tokens)

        flattened_row_text = " ".join(tokens).lower()

        # --- Skip header / footnote / label rows -------------------------
        if any(phrase in flattened_row_text for phrase in _EXCHANGE_SKIP_ROW_PHRASES):
            logger.info("Skipping row (matched skip phrase): %s", tokens)
            continue

        # --- Detect year rows (e.g. "2025", "2026") ----------------------
        if re.fullmatch(r"20\d{2}", tokens[0].strip()):
            current_year = int(tokens[0].strip())
            logger.info("Detected year row: current_year=%s", current_year)
            continue

        # --- Determine whether this is a month row ----------------------
        month = tokens[0].strip(":").capitalize()

        if month not in MONTH_NAMES_SET:
            logger.info("Skipping non-month row: %s", tokens)
            continue

        # --- Validate numeric value count ---------------------------------
        numeric_tokens = tokens[1:]
        if len(numeric_tokens) < _EXCHANGE_MIN_NUMERIC_VALUES:
            logger.warning(
                "Skipping %s: not enough numeric tokens (%d)",
                month, len(numeric_tokens),
            )
            continue

        # --- Extract and validate USD / Pound Sterling / Euro values ------
        try:
            usd = float(tokens[EXCHANGE_COLUMNS["usd"]])
            pound_sterling = float(tokens[EXCHANGE_COLUMNS["pound_sterling"]])
            euro = float(tokens[EXCHANGE_COLUMNS["euro"]])
        except (TypeError, ValueError, IndexError):
            logger.warning(
                "Skipping %s: could not parse currency values from tokens %s",
                month, tokens,
            )
            continue

        logger.info(
            "Parsed exchange row: %s %s USD=%s GBP=%s EUR=%s",
            month, current_year, usd, pound_sterling, euro,
        )

        record = {
            "month": month,
            "year": current_year,
            "usd": usd,
            "pound_sterling": pound_sterling,
            "euro": euro,
        }
        records.append(record)

    logger.info(
        "parse_exchange_dataframe: parsed %d exchange record(s)",
        len(records),
    )
    return records


class ExchangeExtractor(BaseExtractor):
    """
    Extractor for the exchange rates table ("Mean Monthly Foreign Exchange
    Rates of Kenyan Shilling against Selected Major Currencies").

    Like CBRExtractor and InflationExtractor, this extractor calls
    PipelineEngine.run() end-to-end and does not touch any reusable
    pipeline logic. Only exchange-specific parsing
    (parse_exchange_dataframe) is added.
    """

    def extract(self, pdf_url):
        """
        Args:
            pdf_url (str): URL to the KNBS PDF report.

        Returns:
            dict: Standardized extractor response (see
                build_extractor_response()), with "data" containing the
                structured exchange-rate records.
        """
        context = self.knbs.get_context(pdf_url)

        config = TABLE_CONFIG["exchange_rates"]

        dataframe = self.pipeline.run(
            pdf_path=context.pdf_path,
            toc_entries=context.toc_entries,
            target=config.target,
            keywords=config.keywords,
        )

        if dataframe is None:
            logger.error(
                "Exchange extraction failed"
            )
            return build_extractor_response(
                "Exchange Rates", context.report_metadata, pdf_url, []
            )

        logger.info(
            "Raw exchange dataframe:\n%s",
            dataframe.to_string()
        )

        parsed_data = parse_exchange_dataframe(
            dataframe
        )

        logger.info(
            "Extracted %d exchange records",
            len(parsed_data)
        )

        if not self.validator.validate_output(
            parsed_data
        ):
            return build_extractor_response(
                "Exchange Rates", context.report_metadata, pdf_url, [], status="failed"
            )

        return build_extractor_response(
            "Exchange Rates", context.report_metadata, pdf_url, parsed_data
        )


class FuelExtractor(BaseExtractor):
    """
    Extractor for the fuel prices table ("National Average Retail Prices
    for Selected Fuels in Kenya").

    Unlike the other extractors, this one does NOT call
    PipelineEngine.run() end-to-end. It reuses PipelineEngine's individual
    reusable steps (find_table, extract_candidate_tables) directly, but:

      * Looks the target table up by its title only (never by a table
        number like "15(e)"), via PipelineEngine.find_table(), so the
        lookup survives KNBS renumbering tables between reports.
      * Replaces the generic 5-page forward window
        (PipelineEngine.locate_candidate_pages) with a widened forward
        window computed off a real PDF page, since the fuel-related tables
        are sequential (15(c) -> 15(d) -> 15(e) -> 15(f)) and a narrow
        window was stopping before reaching 15(e).
      * Replaces the generic keyword scoring
        (PipelineEngine.score_table/select_best_table) with fuel-specific
        scoring plus a required-keyword validation gate (see
        score_fuel_table / validate_fuel_table / select_fuel_table above),
        so tables like "Consumption of Petroleum Fuels" or "OPEC Reference
        Basket Prices" are rejected even if a keyword or two overlaps.

    PipelineEngine itself is not modified by any of this, and no other
    extractor is affected. TOC page / TOC entries / total page count are
    now sourced from the shared KNBSExtractor cache (via KNBSContext and
    knbs.get_page_count()) instead of being recomputed and instead of this
    extractor opening/closing its own pdfplumber.PDF handle.
    """

    # Pages to search before the computed real PDF page.
    PAGE_WINDOW_BEFORE = 1
    # Pages to search after the computed real PDF page. Widened from the
    # original narrow +/-1 window so the forward-only sequence of fuel
    # tables (15(c) -> 15(d) -> 15(e) -> 15(f)) does not get cut off
    # before reaching 15(e).
    PAGE_WINDOW_AFTER = 3

    def _compute_candidate_pages(self, toc_page, document_page, total_pages):
        """
        Convert a TOC's printed page number into a widened window of real
        PDF page numbers to search, instead of the generic broad forward
        window.

        Assumption: printed document page numbering restarts at 1
        immediately after the TOC page, so the (zero-based) index of the
        TOC page itself is the offset between printed page numbers and
        actual PDF page numbers. This offset is computed per-PDF (never
        hardcoded) so it adapts automatically if KNBS changes how many
        front-matter pages precede the numbered content.

        The window is widened forward (rather than kept narrowly centered)
        because the fuel-related tables are sequential in the report
        (15(c) -> 15(d) -> 15(e) -> 15(f)), and the target table, 15(e),
        can land a couple of pages after the TOC-referenced page.

        Args:
            toc_page (int): Zero-based PDF page index of the TOC (from the
                shared KNBSContext).
            document_page (int): Printed page number from the matched TOC
                entry, from PipelineEngine.find_table().
            total_pages (int): Total number of pages in the PDF, used to
                clamp the window to a valid range.

        Returns:
            list[int]: A clamped list of candidate page numbers spanning
                from just before to several pages after the computed real
                PDF page.
        """
        offset = toc_page
        real_pdf_page = document_page + offset

        # Fuel tables are sequential:
        # 15(c) -> 15(d) -> 15(e) -> 15(f)
        # Expand forward search so we do not stop before 15(e)
        candidate_pages = [
            page
            for page in range(
                real_pdf_page - self.PAGE_WINDOW_BEFORE,
                real_pdf_page + self.PAGE_WINDOW_AFTER + 1,
            )
            if 1 <= page <= total_pages
        ]

        logger.info(
            "extract_fuels: document_page=%s "
            "toc_page=%s offset=%s "
            "real_pdf_page=%s "
            "candidate_pages=%s",
            document_page,
            toc_page,
            offset,
            real_pdf_page,
            candidate_pages,
        )
        return candidate_pages

    def extract(self, pdf_url):

        context = self.knbs.get_context(pdf_url)

        config = TABLE_CONFIG["fuels"]

        matched_entry = self.pipeline.find_table(
            context.toc_entries,
            config.target
        )

        if matched_entry is None:
            logger.error(
                "extract_fuels: no matching TOC entry"
            )
            return build_extractor_response(
                "National Average Retail Prices for Selected Fuels",
                context.report_metadata,
                pdf_url,
                [],
            )

        total_pages = self.knbs.get_page_count(pdf_url)

        candidate_pages = self._compute_candidate_pages(
            context.toc_page,
            matched_entry["document_page"],
            total_pages
        )

        candidate_tables = self.pipeline.extract_candidate_tables(
            context.pdf_path,
            candidate_pages
        )

        table_df = select_fuel_table(candidate_tables)

        if table_df is None or table_df.empty:
            logger.error(
                "extract_fuels: no candidate table found"
            )
            return build_extractor_response(
                "National Average Retail Prices for Selected Fuels",
                context.report_metadata,
                pdf_url,
                [],
            )

        results = parse_fuel_table(
            table_df,
            context.report_metadata,
        )

        if not self.validator.validate_output(results):
            logger.warning(
                "extract_fuels: output failed validation"
            )

        return build_extractor_response(
            "National Average Retail Prices for Selected Fuels",
            context.report_metadata,
            pdf_url,
            results,
        )


class StockMarketExtractor(BaseExtractor):
    """
    Placeholder extractor for the stock market data table.
    """

    def extract(self, pdf_url):
        """
        Args:
            pdf_url (str): URL to the KNBS PDF report.

        Returns:
            dict: Standardized extractor response (see
                build_extractor_response()). "data" is currently always
                empty since parsing is not yet implemented, so "status"
                will be "failed".
        """
        context = self.knbs.get_context(pdf_url)

        config = TABLE_CONFIG["stock_market"]
        table_df = self.pipeline.run(
            pdf_path=context.pdf_path,
            toc_entries=context.toc_entries,
            target=config.target,
            keywords=config.keywords,
        )

        logger.info("extract_stock_market metadata: %s", context.report_metadata)

        # TODO:
        # Add stock market parsing logic
        # - Parse table_df into structured records
        # - Attach `context.report_metadata` to each returned record

        return build_extractor_response("Stock Market", context.report_metadata, pdf_url, [])


# ---------------------------------------------------------------------------
# CLASS: KNBSExtractor (top-level orchestrator + cache owner)
# ---------------------------------------------------------------------------

class KNBSExtractor:
    """
    Top-level orchestrator. Owns the shared PDFManager, PipelineEngine,
    Validator, AND the per-report cache (PDF path, report metadata, opened
    pdfplumber object, TOC page, TOC entries).

    Each get_* method wires up the relevant extractor with the shared
    collaborators and delegates to it -- no table-parsing logic lives here.

    Caching contract:
      * The cache is keyed on `source_url`. Calling any get_*() method with
        a *different* pdf_url than the one currently cached transparently
        resets the cache (closes the old pdfplumber handle) and starts
        fresh -- so it is always safe to reuse one KNBSExtractor instance
        across multiple reports, it just won't get the caching benefit
        across different URLs.
      * Within the same pdf_url, download / metadata parse / TOC parse
        each happen exactly once, no matter how many get_cbr() /
        get_inflation() / get_exchange_rates() / get_fuels() /
        get_stock_market() calls follow.
    """

    def __init__(self):
        self.pdf_manager = PDFManager()
        self.pipeline = PipelineEngine(self.pdf_manager)
        self.validator = Validator()

        # --- Cached, per-report state ----------------------------------
        self.source_url: Optional[str] = None
        self.pdf_path: Optional[str] = None
        self.report_metadata: Optional[dict] = None
        self.pdf: Optional["pdfplumber.PDF"] = None
        self.toc_page: Optional[int] = None
        self.toc_entries: Optional[list] = None

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def _reset_cache_if_new_url(self, pdf_url: str) -> None:
        """
        Invalidate the cache if `pdf_url` differs from the currently
        cached `source_url`. Safe to call at the top of every getter.
        """
        if self.source_url == pdf_url:
            return

        if self.source_url is not None:
            logger.info(
                "KNBSExtractor: new source URL detected (old=%s new=%s); "
                "resetting cache",
                self.source_url, pdf_url,
            )

        self.close()

        self.source_url = pdf_url
        self.pdf_path = None
        self.report_metadata = None
        self.pdf = None
        self.toc_page = None
        self.toc_entries = None

    def close(self) -> None:
        """Close the cached pdfplumber handle, if one is open."""
        if self.pdf is not None:
            try:
                self.pdf.close()
            except Exception as exc:  # noqa: BLE001
                logger.warning("KNBSExtractor: error closing cached PDF: %s", exc)
            finally:
                self.pdf = None

    def __del__(self):
        # Best-effort cleanup; never raise from __del__.
        try:
            self.close()
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------
    # Lazy, cached getters
    # ------------------------------------------------------------------

    def get_pdf(self, pdf_url: str) -> str:
        """
        Return the local filesystem path to `pdf_url`, downloading it only
        the first time it's requested for this instance.
        """
        self._reset_cache_if_new_url(pdf_url)

        if self.pdf_path is not None:
            return self.pdf_path

        self.pdf_path = self.pdf_manager.download_pdf(pdf_url)
        return self.pdf_path

    def get_report_metadata(self, pdf_url: str) -> dict:
        """
        Return {"source_url", "report_date", "month", "year"} for
        `pdf_url`, computed only once per instance/URL.
        """
        self._reset_cache_if_new_url(pdf_url)

        if self.report_metadata is not None:
            return self.report_metadata

        raw = self.pdf_manager.extract_report_date_from_url(pdf_url)
        self.report_metadata = {
            "source_url": pdf_url,
            "report_date": raw["report_date"] if raw else None,
            "month": raw["month"] if raw else None,
            "year": raw["year"] if raw else None,
        }
        return self.report_metadata

    def get_pdf_object(self, pdf_url: str) -> "pdfplumber.PDF":
        """
        Return the opened pdfplumber.PDF for `pdf_url`, opening it only
        the first time it's requested for this instance. Caller does NOT
        need to close it -- KNBSExtractor.close() (or a new URL) does.
        """
        self._reset_cache_if_new_url(pdf_url)

        if self.pdf is not None:
            return self.pdf

        pdf_path = self.get_pdf(pdf_url)
        self.pdf = self.pdf_manager.load_pdf(pdf_path)
        return self.pdf

    def get_toc_page(self, pdf_url: str) -> Optional[int]:
        """Return the cached TOC page index for `pdf_url`."""
        self._reset_cache_if_new_url(pdf_url)

        if self.toc_page is not None:
            return self.toc_page

        pdf = self.get_pdf_object(pdf_url)
        self.toc_page = self.pipeline.find_toc(pdf)
        return self.toc_page

    def get_toc(self, pdf_url: str) -> list:
        """Return the cached, parsed TOC entries for `pdf_url`."""
        self._reset_cache_if_new_url(pdf_url)

        if self.toc_entries is not None:
            return self.toc_entries

        pdf = self.get_pdf_object(pdf_url)
        toc_page = self.get_toc_page(pdf_url)
        self.toc_entries = self.pipeline.parse_toc(pdf, toc_page)
        return self.toc_entries

    def get_page_count(self, pdf_url: str) -> int:
        """Return the total page count of the (cached) opened PDF."""
        return len(self.get_pdf_object(pdf_url).pages)

    def get_context(self, pdf_url: str) -> KNBSContext:
        """
        Build (from cache) the full KNBSContext handed to every
        table-specific extractor. Cheap to call repeatedly -- every field
        is resolved through the cached getters above.
        """
        return KNBSContext(
            pdf_path=self.get_pdf(pdf_url),
            pdf=self.get_pdf_object(pdf_url),
            report_metadata=self.get_report_metadata(pdf_url),
            toc_page=self.get_toc_page(pdf_url),
            toc_entries=self.get_toc(pdf_url),
            source_url=pdf_url,
        )

    # ------------------------------------------------------------------
    # Public extraction API (unchanged signatures -- MacroService is
    # unaffected by this refactor)
    # ------------------------------------------------------------------

    def get_cbr(self, pdf_url):
        extractor = CBRExtractor(self, self.pipeline, self.validator)
        return extractor.extract(pdf_url)

    def get_inflation(self, pdf_url):
        extractor = InflationExtractor(self, self.pipeline, self.validator)
        return extractor.extract(pdf_url)

    def get_exchange_rates(self, pdf_url):
        extractor = ExchangeExtractor(self, self.pipeline, self.validator)
        return extractor.extract(pdf_url)

    def get_fuels(self, pdf_url):
        extractor = FuelExtractor(self, self.pipeline, self.validator)
        return extractor.extract(pdf_url)

    def get_stock_market(self, pdf_url):
        extractor = StockMarketExtractor(self, self.pipeline, self.validator)
        return extractor.extract(pdf_url)


# ---------------------------------------------------------------------------
# Manual test entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Example usage (manual smoke test only):
    #
    # sample_url = (
    #     "https://www.knbs.or.ke/wp-content/uploads/2026/05/"
    #     "Leading-Economic-Indicators-March-2026.pdf"
    # )
    # knbs = KNBSExtractor()
    #
    # # PDF is downloaded once here...
    # cbr_results = knbs.get_cbr(sample_url)
    #
    # # ...and reused (no re-download, no re-TOC-parse) for every call below.
    # inflation_results = knbs.get_inflation(sample_url)
    # exchange_results = knbs.get_exchange_rates(sample_url)
    # fuel_results = knbs.get_fuels(sample_url)
    #
    # knbs.close()
    pass