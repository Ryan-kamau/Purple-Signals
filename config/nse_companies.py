
"""
config/nse_companies.py

Per-company registry for fundamentals CSV ingestion.

Single source of truth for:
  - which tickers may be ingested
  - the canonical company name stored in fundamentals.company
  - units_multiplier: factor applied to MONEY columns:
    revenue, net_profit, shareholders_equity, total_liabilities.

The multiplier is NOT applied to EPS, dividend_per_share, or ratios.

IMPORTANT:
    KES_MILLIONS is a provisional default for entries whose reporting
    units have not yet been individually verified. Confirm the units
    in each company's source financial statement before ingestion.
"""

from typing import Optional, TypedDict


class CompanyConfig(TypedDict):
    company: str
    units_multiplier: float


KES_MILLIONS: float = 1_000_000
KES_THOUSANDS: float = 1_000
KES_UNITS: float = 1


# Ordinary equity listings.
# Reporting multipliers must be verified against source statements.
NSE_COMPANIES: dict[str, CompanyConfig] = {
    # Agriculture
    "EGAD": {
        "company": "Eaagads Limited",
        "units_multiplier": KES_MILLIONS,
    },
    "KAPC": {
        "company": "Kapchorua Tea Kenya Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "KUKZ": {
        "company": "Kakuzi Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "LIMT": {
        "company": "The Limuru Tea Company Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "SASN": {
        "company": "Sasini Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "WTK": {
        "company": "Williamson Tea Kenya Plc",
        "units_multiplier": KES_MILLIONS,
    },

    # Automobiles and accessories
    "CGEN": {
        "company": "Car & General (K) Limited",
        "units_multiplier": KES_MILLIONS,
    },

    # Banking
    "ABSA": {
        "company": "Absa Bank Kenya Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "BKG": {
        "company": "BK Group Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "COOP": {
        "company": "The Co-operative Bank of Kenya Limited",
        "units_multiplier": KES_MILLIONS,
    },
    "DTK": {
        "company": "Diamond Trust Bank Kenya Limited",
        "units_multiplier": KES_MILLIONS,
    },
    "EQTY": {
        "company": "Equity Group Holdings Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "HFCK": {
        "company": "HF Group Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "IMH": {
        "company": "I&M Group Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "KCB": {
        "company": "KCB Group Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "NBK": {
        "company": "National Bank of Kenya Limited",
        "units_multiplier": KES_MILLIONS,
    },
    "NCBA": {
        "company": "NCBA Group Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "SBIC": {
        "company": "Stanbic Holdings Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "SCBK": {
        "company": "Standard Chartered Bank Kenya Limited",
        "units_multiplier": KES_MILLIONS,
    },

    # Commercial and services
    "ARM": {
        "company": "ARM Cement Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "BAMB": {
        "company": "Bamburi Cement Limited",
        "units_multiplier": KES_MILLIONS,
    },
    "CABL": {
        "company": "East African Cables Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "CRWN": {
        "company": "Crown Paints Kenya Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "DCON": {
        "company": "Deacons (East Africa) Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "EVRD": {
        "company": "Eveready East Africa Limited",
        "units_multiplier": KES_MILLIONS,
    },
    "HBE": {
        "company": "Homeboyz Entertainment Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "KQ": {
        "company": "Kenya Airways Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "LKL": {
        "company": "Longhorn Publishers Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "NBV": {
        "company": "Nairobi Business Ventures Limited",
        "units_multiplier": KES_MILLIONS,
    },
    "NMG": {
        "company": "Nation Media Group Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "PORT": {
        "company": "East African Portland Cement Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "SCAN": {
        "company": "WPP Scangroup Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "SGL": {
        "company": "The Standard Group Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "SMER": {
        "company": "Sameer Africa Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "TPSE": {
        "company": "TPS Eastern Africa Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "UCHM": {
        "company": "Uchumi Supermarkets Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "XPRS": {
        "company": "Express Kenya Plc",
        "units_multiplier": KES_MILLIONS,
    },

    # Energy and petroleum
    "KEGN": {
        "company": "Kenya Electricity Generating Company Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "KPC": {
        "company": "Kenya Pipeline Company Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "KPLC": {
        "company": "Kenya Power and Lighting Company Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "TOTL": {
        "company": "TotalEnergies Marketing Kenya Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "UMME": {
        "company": "Umeme Limited",
        "units_multiplier": KES_MILLIONS,
    },

    # Insurance and investment
    "BRIT": {
        "company": "Britam Holdings Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "CIC": {
        "company": "CIC Insurance Group Limited",
        "units_multiplier": KES_MILLIONS,
    },
    "CTUM": {
        "company": "Centum Investment Company Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "HAFR": {
        "company": "Home Afrika Limited",
        "units_multiplier": KES_MILLIONS,
    },
    "JUB": {
        "company": "Jubilee Holdings Limited",
        "units_multiplier": KES_MILLIONS,
    },
    "KNRE": {
        "company": "Kenya Re-Insurance Corporation",
        "units_multiplier": KES_MILLIONS,
    },
    "KURV": {
        "company": "Kurwitu Ventures Limited",
        "units_multiplier": KES_MILLIONS,
    },
    "LBTY": {
        "company": "Liberty Kenya Holdings Limited",
        "units_multiplier": KES_MILLIONS,
    },
    "OCH": {
        "company": "Olympia Capital Holdings Limited",
        "units_multiplier": KES_MILLIONS,
    },
    "SLAM": {
        "company": "Sanlam Kenya Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "TCL": {
        "company": "Trans-Century Plc",
        "units_multiplier": KES_MILLIONS,
    },

    # Investment services
    "NSE": {
        "company": "Nairobi Securities Exchange Plc",
        "units_multiplier": KES_MILLIONS,
    },

    # Manufacturing and allied
    "AMAC": {
        "company": "Africa Mega Agricorp Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "BAT": {
        "company": "British American Tobacco Kenya Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "BOC": {
        "company": "BOC Kenya Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "CARB": {
        "company": "Carbacid Investments Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "EABL": {
        "company": "East African Breweries Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "FTGH": {
        "company": "Flame Tree Group Holdings Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "MSC": {
        "company": "Mumias Sugar Company Limited",
        "units_multiplier": KES_MILLIONS,
    },
    "SKL": {
        "company": "Shri Krishana Overseas Plc",
        "units_multiplier": KES_MILLIONS,
    },
    "UNGA": {
        "company": "Unga Group Plc",
        "units_multiplier": KES_MILLIONS,
    },

    # Telecommunications
    "SCOM": {
        "company": "Safaricom Plc",
        "units_multiplier": KES_MILLIONS,
    },
}



# Central company-name aliases used by ALL article/company matching.
# Ticker symbols are matched separately, case-sensitively.
# Avoid generic terms such as "equity", "port", "scan", "arm" and "bat".
NSE_COMPANY_ALIASES: dict[str, tuple[str, ...]] = {
    "SCOM": ("safaricom",),
    "EQTY": ("equity group", "equity group holdings", "equity bank"),
    "KCB": ("kcb group", "kenya commercial bank"),
    "EABL": ("east african breweries",),
    "COOP": ("co-op bank", "co-operative bank", "cooperative bank", "coop bank"),
    "ABSA": ("absa bank", "absa bank kenya", "absa kenya"),
    "KPC": ("kenya pipeline", "kenya pipeline company"),
    "IMH": ("i&m", "i&m group", "i&m holdings", "i&m bank"),
    "NCBA": ("ncba group", "ncba bank"),
    "SCBK": ("standard chartered", "standard chartered kenya", "stanchart"),
    "SBIC": ("stanbic", "stanbic holdings", "stanbic bank kenya"),
    "KEGN": ("kengen", "kenya electricity generating company"),
    "KPLC": ("kenya power", "kenya power and lighting", "kenya power & lighting"),
    "BAT": ("bat kenya", "british american tobacco"),
    "KQ": ("kenya airways",),
    "BAMB": ("bamburi", "bamburi cement"),
    "DTK": ("diamond trust bank", "dtb", "dtb kenya"),
    "HFCK": ("hf group", "housing finance", "housing finance group"),
    "CGEN": ("car and general", "car & general"),
    "NMG": ("nation media group", "nation media"),
    "SASN": ("sasini",), "KUKZ": ("kakuzi",),
    "KAPC": ("kapchorua", "kapchorua tea"),
    "LIMT": ("limuru tea",), "WTK": ("williamson tea",),
    "EGAD": ("eaagads",), "UNGA": ("unga group",),
    "CARB": ("carbacid",), "BOC": ("boc kenya",),
    "EVRD": ("eveready",), "CIC": ("cic insurance", "cic group"),
    "BRIT": ("britam",), "JUB": ("jubilee holdings", "jubilee"),
    "KNRE": ("kenya re", "kenya reinsurance"),
    "LBTY": ("liberty kenya", "liberty life kenya"),
    "CTUM": ("centum", "centum investment"),
    "OCH": ("olympia capital",), "XPRS": ("express kenya",),
    "FTGH": ("flame tree", "flame tree group"),
    "HAFR": ("home africa", "home afrika"),
    "AMAC": ("africa mega agricorp", "african mega agricorp"),
    "BKG": ("bk group",), "CRWN": ("crown paints",),
    "PORT": ("e.a. portland cement", "ea portland cement", "east african portland cement"),
    "NBV": ("nairobi business ventures",),
    "SLAM": ("sanlam kenya",),
    "SKL": ("shri krishana overseas",),
    "SGL": ("standard group",),
    "TOTL": ("totalenergies marketing kenya", "total energies marketing kenya"),
    "UCHM": ("uchumi supermarket", "uchumi",),
    "UMME": ("umeme",),
    "SCAN": ("wpp scangroup", "scangroup", "wpp scan group"),
}


def get_company_aliases(ticker: str) -> tuple[str, ...]:
    """Return canonical company name plus explicit safe aliases."""
    key = ticker.strip().upper()
    entry = NSE_COMPANIES.get(key)
    if entry is None:
        return ()
    names = (entry["company"], *NSE_COMPANY_ALIASES.get(key, ()))
    return tuple(dict.fromkeys(n for n in names if n and n.strip()))


def get_company(ticker: str) -> Optional[CompanyConfig]:
    """Return the registry entry for ticker, case-insensitively."""
    if not ticker:
        return None
    return NSE_COMPANIES.get(ticker.strip().upper())
