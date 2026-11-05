"""
testers/test_MasterIntelligenceService.py

Standalone tester for the Master Intelligence Service and GET /analysis.
Same style as the other testers: plain script, [PASS]/[FAIL]/[SKIP] counters,
no pytest.

    TEST_DATABASE_URL=mysql+pymysql://user:pw@localhost/purple_signals_test \\
        python -m testers.test_MasterIntelligenceService

SAFETY - this script DROPS AND RECREATES the six tables it reads
(market_data, daily_features, analysis_results, alerts, fundamentals,
macro_data) in TEST_DATABASE_URL. It therefore refuses to run unless:
  * TEST_DATABASE_URL is set,
  * it differs from the DATABASE_URL in the environment / .env,
  * the database name contains "test" (sqlite files/memory are exempt).
It also forces DATABASE_URL to the test URL BEFORE importing any project
module, so nothing in this process can touch the real database by accident.

All fixture data is synthetic and fixed (reference time: Wed 2026-11-04
10:00 Africa/Nairobi), so results do not depend on the wall clock.
"""

import os
import sys
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Safety guard - MUST run before any project import
# ---------------------------------------------------------------------------
load_dotenv()
TEST_URL = os.getenv("TEST_DATABASE_URL") or os.environ.get("TEST_DATABASE_URL")


def _guard() -> None:
    from dotenv import dotenv_values
    from sqlalchemy.engine import make_url

    if not TEST_URL:
        print("Refusing to run: set TEST_DATABASE_URL to a DEDICATED test database.")
        sys.exit(2)

    url = make_url(TEST_URL)
    real = os.environ.get("DATABASE_URL") or dotenv_values().get("DATABASE_URL")
    if real and TEST_URL == real:
        print("Refusing to run: TEST_DATABASE_URL equals DATABASE_URL.")
        sys.exit(2)

    is_sqlite = url.get_backend_name() == "sqlite"
    if not is_sqlite and "test" not in (url.database or "").lower():
        print(f"Refusing to run: database name {url.database!r} must contain 'test'.")
        sys.exit(2)

    # Anything in the project that builds an engine from the environment now
    # gets the test database, regardless of what .env says.
    os.environ["DATABASE_URL"] = TEST_URL


_guard()

from datetime import date, datetime, timedelta  # noqa: E402
from decimal import Decimal  # noqa: E402
from typing import Any  # noqa: E402
from zoneinfo import ZoneInfo  # noqa: E402

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import create_engine, delete, event, func, inspect as sa_inspect, select  # noqa: E402
from sqlalchemy.exc import OperationalError  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from api import analysis as analysis_api  # noqa: E402
from config import analysis_config as cfg  # noqa: E402
from database.base import Base  # noqa: E402
from database.session import get_db  # noqa: E402
from models.alert_data import Alert  # noqa: E402
from models.analytics_data import AnalysisResult  # noqa: E402
from models.daily_market_features import DailyMarketFeatures as Features  # noqa: E402
from models.fundamental_data import Fundamentals  # noqa: E402
from models.macro_data import MacroData  # noqa: E402
from models.market_data import MarketData  # noqa: E402
from services.analysis_service.MasterIntelligenceService import (  # noqa: E402
    MasterIntelligenceService,
    TickerNotFoundError,
)

NAIROBI = ZoneInfo("Africa/Nairobi")
NOW = datetime(2026, 11, 4, 10, 0, tzinfo=NAIROBI)          # Wednesday
TABLES = [
    MarketData.__table__, Features.__table__, AnalysisResult.__table__,
    Alert.__table__, Fundamentals.__table__, MacroData.__table__,
]

ENGINE = create_engine(TEST_URL)
Session = sessionmaker(bind=ENGINE, autocommit=False, autoflush=False, expire_on_commit=False)

_PASS = _FAIL = _SKIP = 0


def check(condition: bool, message: str) -> bool:
    global _PASS, _FAIL
    if condition:
        _PASS += 1
        print(f"[PASS] {message}")
    else:
        _FAIL += 1
        print(f"[FAIL] {message}")
    return condition


def skip(message: str) -> None:
    global _SKIP
    _SKIP += 1
    print(f"[SKIP] {message}")


def section(title: str) -> None:
    print("\n" + "-" * 70 + f"\n{title}\n" + "-" * 70)


def raises(exc_type, fn) -> bool:
    try:
        fn()
    except exc_type:
        return True
    except Exception:  # noqa: BLE001
        return False
    return False


# ===========================================================================
# Fixtures (synthetic, deterministic)
# ===========================================================================

def _make(model, **kwargs):
    """Build a model instance keeping only columns that exist (Alert schema may evolve)."""
    columns = {a.key for a in sa_inspect(model).mapper.column_attrs}
    return model(**{k: v for k, v in kwargs.items() if k in columns})


def _ts(y, m, d, h=12):
    return datetime(y, m, d, h, 0, tzinfo=NAIROBI)


def reset_schema() -> None:
    Base.metadata.drop_all(ENGINE, tables=TABLES)
    Base.metadata.create_all(ENGINE, tables=TABLES)


def _market(ticker, company, ts):
    return MarketData(ticker=ticker, company=company, price=Decimal("10.000"), volume=1000,
                      daily_change=Decimal("0.100"), daily_change_pct=1.0, volatility=1.0, timestamp=ts)


def _feat(ticker, d, **kw):
    return Features(ticker=ticker, trading_date=d, **kw)


def _signal(ticker, d, label, strength=None, conf=0.0, **kw):
    return AnalysisResult(ticker=ticker, trading_date=d, signal=label, strength=strength,
                          confidence_score=conf, explanation=f"{ticker} {label}",
                          components=kw.get("components", {"price": 1}), timestamp=_ts(d.year, d.month, d.day, 16))


def _alert(ticker, alert_type, status, **kw):
    return _make(
        Alert, ticker=ticker, alert_type=alert_type, status=status, severity=kw.get("severity", "high"),
        message=f"{alert_type} on {ticker}", trading_date=kw.get("trading_date", date(2026, 11, 2)),
        signal_strength="strong", confidence_score=0.9,
        first_detected_at=_ts(2026, 11, 2, 16), last_detected_at=_ts(2026, 11, 3, 16),
        evidence={"volume_ratio": 3.4}, timestamp=_ts(2026, 11, 3, 16),
    )


def seed_main() -> None:
    s = Session()
    d = date

    # --- market_data: universe source #1 --------------------------------
    s.add_all([
        # newest row inserted FIRST (lower id): "latest" must follow timestamp, not id
        _market("KPLC", "Kenya Power and Lighting Company Plc", _ts(2026, 11, 4)),
        _market("KPLC", "Kenya Power OLD NAME", _ts(2026, 10, 1)),
        _market("SCOM", "Safaricom Plc", _ts(2026, 10, 30)),
        _market("ABSA", "Absa Bank Kenya Plc", _ts(2026, 11, 4)),
        _market("COOP", "The Co-operative Bank of Kenya Limited", _ts(2026, 11, 4)),
        _market("EABL", "East African Breweries Plc", _ts(2026, 11, 3)),
        _market("KCB", "KCB Group Plc", _ts(2026, 11, 4)),
        _market("", "Blank ticker row - must be ignored", _ts(2026, 11, 4)),
    ])

    # --- daily_features: universe source #2 ------------------------------
    s.add_all([
        _feat("KPLC", d(2026, 11, 4), close_price=20.0, daily_return=0.01, volume_ratio_10d=1.5,
              volume_ratio_20d=1.2, volatility_5d=0.02, volatility_20d=0.03, average_sentiment=0.4,
              weighted_sentiment=0.5, sentiment_volatility=0.1, relevant_headline_count=3,
              average_impact_score=3.0),
        _feat("KPLC", d(2026, 11, 2), close_price=19.0, daily_return=0.05, volume_ratio_10d=3.9,
              volume_ratio_20d=3.4, volatility_5d=0.04, volatility_20d=0.05, average_sentiment=0.3,
              weighted_sentiment=0.35, sentiment_volatility=0.2, relevant_headline_count=2,
              average_impact_score=2.5),
        _feat("KPLC", d(2026, 10, 30), close_price=18.0),
        # SCOM: latest 10-30, NO row on the signal date 10-20; sentiment all NULL
        _feat("SCOM", d(2026, 10, 30), close_price=30.0, daily_return=-0.01, volume_ratio_20d=1.0),
        _feat("SCOM", d(2026, 10, 29), close_price=30.3),
        # EQTY: features only (not in market_data) -> proves the UNION
        _feat("EQTY", d(2026, 11, 3), close_price=50.0, daily_return=0.0),
        _feat("ABSA", d(2026, 11, 4), close_price=15.0, daily_return=0.03),
        _feat("ABSA", d(2026, 11, 3), close_price=14.5),
        _feat("EABL", d(2026, 11, 3), close_price=200.0, volume_ratio_20d=4.0),
        _feat("COOP", d(2026, 11, 4), close_price=13.0, daily_return=0.0),
        _feat("", d(2026, 11, 4), close_price=1.0),      # blank ticker - ignored
    ])

    # --- signals (analysis_results) ---------------------------------------
    s.add_all([
        _signal("KPLC", d(2026, 10, 30), "bearish", "moderate", 0.6667),        # older, must lose
        _signal("KPLC", d(2026, 11, 2), "bullish", "strong", 1.0),
        _signal("SCOM", d(2026, 10, 20), "bearish", "moderate", 0.6667),        # stale
        _signal("ABSA", d(2026, 11, 4), "divergence", None, 0.0),
        _signal("EABL", d(2026, 11, 3), "unusual_activity", None, 0.3333),
        _signal("COOP", d(2026, 11, 4), "neutral", None, 0.0),
        _signal("ORPH", d(2026, 11, 4), "bullish", "strong", 1.0),             # not in universe
    ])

    # --- alerts ------------------------------------------------------------
    s.add_all([
        _alert("KPLC", "unusual_volume", "active"),
        _alert("KPLC", "price_move", "resolved"),            # excluded
        _alert("SCOM", "bearish_signal", "active", severity="medium"),
        _alert("ZZZZ", "unusual_volume", "active"),          # not in universe -> not counted
    ])

    # --- fundamentals ------------------------------------------------------
    s.add_all([
        # newest inserted first: latest must follow report_date, not id
        Fundamentals(ticker="KPLC", company="Kenya Power and Lighting Company Plc",
                     report_date=datetime(2026, 6, 30, tzinfo=NAIROBI), eps=Decimal("12.5"),
                     pe_ratio=Decimal("1.6"), dividend_yield=Decimal("0.02"), revenue=Decimal("219285000000"),
                     debt_ratio=Decimal("2.3"),
                     net_profit_margin=Decimal("0.0615"), timestamp=_ts(2026, 10, 1)),
        Fundamentals(ticker="KPLC", company="Kenya Power and Lighting Company Plc",
                     report_date=datetime(2025, 6, 30, tzinfo=NAIROBI), eps=Decimal("10.0")),
        Fundamentals(ticker="EQTY", company="Equity Group Holdings Plc",
                     report_date=datetime(2025, 6, 30, tzinfo=NAIROBI), eps=Decimal("5.0")),
    ])

    # --- macro: inserted out of chronological order ------------------------
    s.add_all([
        MacroData(month="July", year="2026", report_date=d(2026, 7, 1), inflation=6.5, inflation_trend=0.5,
                  fuel_price=214.03, fuel_trend=None, cbk_rate=None, usd_kes_rate=129.4,
                  euro_kes_rate=None, pounds_kes_rate=None),
        MacroData(month="May", year="2026", report_date=d(2026, 5, 1), inflation=7.0, inflation_trend=-0.2,
                  fuel_price=213.0, fuel_trend=0.3, cbk_rate=9.5, usd_kes_rate=129.0,
                  euro_kes_rate=147.0, pounds_kes_rate=None),
        MacroData(month="June", year="2026", report_date=d(2026, 6, 1), inflation=6.0, inflation_trend=-1.0,
                  fuel_price=None, cbk_rate=9.0, usd_kes_rate=None, euro_kes_rate=None, pounds_kes_rate=None),
        MacroData(month="Foo", year="2026", inflation=99.0),      # unusable period - ignored
        MacroData(month="August", year=None, inflation=98.0),     # unusable period - ignored
    ])
    s.commit()
    s.close()


UNIVERSE = ["ABSA", "COOP", "EABL", "EQTY", "KCB", "KPLC", "SCOM"]


def svc(session, now=NOW) -> MasterIntelligenceService:
    return MasterIntelligenceService(session, now=now)


def by_ticker(resp, ticker):
    return next((s for s in resp.securities if s.ticker == ticker), None)


# ===========================================================================
# A. Freshness helpers (pure)
# ===========================================================================

def test_config() -> None:
    section("A. config.analysis_config (pure)")
    bd = cfg.business_days_between
    check(bd(date(2026, 10, 30), date(2026, 11, 2)) == 1, "Friday -> Monday = 1 business day")
    check(bd(date(2026, 11, 2), date(2026, 11, 6)) == 4, "Monday -> Friday = 4")
    check(bd(date(2026, 11, 4), date(2026, 11, 4)) == 0, "same day = 0")
    check(bd(date(2026, 11, 5), date(2026, 11, 4)) == 0, "future-dated record = 0")
    check(bd(date(2026, 11, 2), date(2026, 11, 4)) == 2, "Mon -> Wed = 2")
    check(bd(date(2026, 10, 20), date(2026, 11, 4)) == 11, "Tue 10-20 -> Wed 11-04 = 11")
    check(bd(date(2026, 10, 3), date(2026, 10, 4)) == 0, "Sat -> Sun = 0")
    check(bd(date(2026, 10, 1), date(2026, 10, 29)) == 20, "4 full weeks = 20")

    ref = date(2026, 11, 4)
    check(cfg.is_signal_stale(date(2026, 11, 2), ref) is False, "signal 2 business days old is fresh")
    check(cfg.is_signal_stale(date(2026, 10, 20), ref) is True, "signal 11 business days old is stale")
    check(cfg.is_fundamentals_stale(date(2026, 6, 30), ref) is False, "fundamentals 127 days old fresh")
    check(cfg.is_fundamentals_stale(date(2025, 6, 30), ref) is True, "fundamentals 492 days old stale")
    check(cfg.is_macro_stale(2026, 8, ref) is False, "macro 3 months behind is fresh (boundary)")
    check(cfg.is_macro_stale(2026, 7, ref) is True, "macro 4 months behind is stale")
    check(cfg.is_macro_stale(2026, 12, ref) is False, "future macro period never stale")


# ===========================================================================
# B. NSE-wide
# ===========================================================================

def test_nse_wide() -> None:
    section("B. NSE-wide response")
    s = Session()
    r = svc(s).get_analysis()
    ms = r.market_summary

    check([x.ticker for x in r.securities] == UNIVERSE,
          "universe = UNION(market_data, daily_features), blanks excluded, sorted")
    check(r.ticker_summary is None, "ticker_summary is null without ticker")
    check(r.metadata.scope == "nse_wide" and r.metadata.requested_ticker is None, "metadata scope nse_wide")
    check(r.metadata.pagination is not None and r.metadata.pagination.total == 7
          and r.metadata.pagination.returned == 7, "pagination metadata correct")
    check(r.metadata.reference_date == date(2026, 11, 4) and r.metadata.timezone == "Africa/Nairobi",
          "reference date / timezone in metadata")
    check(r.metadata.generated_at.utcoffset() == timedelta(hours=3), "generated_at is Nairobi-aware")

    check(ms.total_securities == 7, "total_securities == 7 (EQTY from features only, KCB without features)")
    check(ms.securities_with_signals == 5 and ms.securities_without_signals == 2,
          "with/without signals == 5/2 (out-of-universe signal ORPH excluded)")
    check((ms.bullish_count, ms.bearish_count, ms.divergence_count, ms.unusual_activity_count, ms.neutral_count)
          == (1, 1, 1, 1, 1), "classification counts use LATEST signal per ticker (KPLC counted bullish once)")
    check(ms.fresh_signal_count == 4 and ms.stale_signal_count == 1,
          "fresh/stale counts 4/1; stale still counted in classification")
    check(ms.fresh_signal_count + ms.stale_signal_count == ms.securities_with_signals,
          "fresh + stale == with_signals")
    check(ms.active_alert_count == 2, "active alerts == 2 (resolved and out-of-universe excluded)")

    dumped = r.model_dump(mode="json")
    check(isinstance(dumped["securities"], list) and "macroeconomics" in dumped, "response is JSON-serialisable")
    check(not hasattr(ms, "average_return") and "average" not in " ".join(ms.model_dump().keys()),
          "no NSE-wide financial averages invented")
    s.close()


# ===========================================================================
# C. Pagination
# ===========================================================================

def test_pagination() -> None:
    section("C. Pagination")
    s = Session()
    full = svc(s).get_analysis(limit=200)
    base_summary = full.market_summary.model_dump()

    pages = []
    for offset in (0, 3, 6):
        r = svc(s).get_analysis(limit=3, offset=offset)
        pages.append([x.ticker for x in r.securities])
        check(r.market_summary.model_dump() == base_summary,
              f"summary identical on page offset={offset}")
        check(r.metadata.pagination.total == 7, f"total stable on offset={offset}")
    check(pages == [["ABSA", "COOP", "EABL"], ["EQTY", "KCB", "KPLC"], ["SCOM"]],
          "deterministic ordering, pages do not overlap")

    beyond = svc(s).get_analysis(limit=3, offset=7)
    check(beyond.securities == [] and beyond.metadata.pagination.returned == 0
          and beyond.market_summary.model_dump() == base_summary, "offset == total -> empty page, summary intact")
    check(svc(s).get_analysis(limit=1, offset=100).securities == [], "offset far beyond total -> empty")
    check(len(svc(s).get_analysis(limit=1).securities) == 1, "limit=1 accepted")
    check(len(svc(s).get_analysis(limit=200).securities) == 7, "limit=200 accepted")

    check(raises(ValueError, lambda: svc(s).get_analysis(limit=0)), "limit=0 rejected")
    check(raises(ValueError, lambda: svc(s).get_analysis(limit=201)), "limit=201 rejected")
    check(raises(ValueError, lambda: svc(s).get_analysis(offset=-1)), "offset=-1 rejected")
    s.close()


# ===========================================================================
# D. Ticker-specific
# ===========================================================================

def test_ticker_kplc() -> None:
    section("D1. KPLC - latest features newer than signal-date features, alerts, fundamentals")
    s = Session()
    r = svc(s).get_analysis(ticker="kplc", limit=1, offset=50)   # lowercase; paging ignored
    k = r.securities[0]
    ts = r.ticker_summary

    check(r.metadata.scope == "ticker" and r.metadata.requested_ticker == "KPLC"
          and r.metadata.pagination is None, "ticker mode metadata, no pagination")
    check(len(r.securities) == 1 and k.ticker == "KPLC", "single record returned")
    check(r.market_summary.total_securities == 7 and r.market_summary.bullish_count == 1,
          "ticker mode carries the FULL NSE-wide summary")
    check(k.company_name == "Kenya Power and Lighting Company Plc", "company = latest market_data row by timestamp")

    check(k.signal.signal == "bullish" and k.signal.strength == "strong" and k.signal.trading_date == date(2026, 11, 2)
          and k.signal.confidence_score == 1.0 and k.signal.components == {"price": 1}
          and k.signal.explanation == "KPLC bullish", "latest persisted signal returned with all fields")

    check(k.market.latest.trading_date == date(2026, 11, 4) and k.market.latest.close_price == 20.0,
          "latest features = 2026-11-04")
    check(k.market.signal_date.trading_date == date(2026, 11, 2) and k.market.signal_date.close_price == 19.0,
          "signal-date features = 2026-11-02 (kept separate from latest)")
    check(k.market.latest.volume_ratio_10d == 1.5 and k.market.latest.volume_ratio_20d == 1.2
          and k.market.latest.volatility_5d == 0.02 and k.market.latest.volatility_20d == 0.03
          and k.market.latest.daily_return == 0.01, "stored indicators passed through untouched")

    check(k.sentiment.latest.average_sentiment == 0.4 and k.sentiment.signal_date.average_sentiment == 0.3,
          "two sentiment views (latest vs signal date)")
    check(k.sentiment.latest.weighted_sentiment == 0.5 and k.sentiment.latest.relevant_headline_count == 3
          and k.sentiment.latest.average_impact_score == 3.0, "sentiment aggregates present")

    check(len(k.active_alerts) == 1 and k.active_alerts[0].alert_type == "unusual_volume"
          and k.active_alerts[0].status == "active", "only the ACTIVE alert returned")
    det = k.active_alerts[0].details
    check("ticker" not in det and "status" not in det, "alert details exclude ticker/core fields")
    check(det.get("confidence_score") == 0.9 and det.get("evidence") == {"volume_ratio": 3.4}
          and det.get("first_detected_at") is not None, "stored alert fields passed through in details")

    f = k.fundamentals
    check(f.report_date == date(2026, 6, 30) and f.eps == 12.5, "latest fundamentals by report_date (not by id)")
    check(f.pe_ratio == 1.6 and f.net_profit_margin == 0.0615 and f.dividend_per_share is None,
          "Decimal -> float, NULL preserved (dividend_per_share)")

    ds = k.data_status
    check(ds.signal.is_available and ds.signal.is_stale is False and ds.signal.as_of == date(2026, 11, 2),
          "signal fresh")
    check(ds.latest_features.is_stale is False and ds.signal_date_features.is_available is True,
          "features fresh / signal-date features available")
    check(ds.fundamentals.is_available and ds.fundamentals.is_stale is False, "fundamentals fresh")
    check(ds.active_alerts.is_available is True, "active_alerts available")

    check(ts.ticker == "KPLC" and ts.signal == "bullish" and ts.signal_is_stale is False
          and ts.active_alert_count == 1 and ts.has_fundamentals is True
          and ts.latest_features_date == date(2026, 11, 4) and ts.signal_trading_date == date(2026, 11, 2),
          "ticker_summary restates the record")
    s.close()


def test_ticker_scom() -> None:
    section("D2. SCOM - stale signal, missing signal-date feature row, NULL sentiment, no fundamentals")
    s = Session()
    r = svc(s).get_analysis(ticker="SCOM")
    k = r.securities[0]

    check(k.signal.signal == "bearish" and k.data_status.signal.is_stale is True
          and k.data_status.signal.is_available is True, "stale signal still returned, flagged stale")
    check(r.ticker_summary.signal_is_stale is True, "ticker_summary reflects staleness")
    check(k.market.latest.trading_date == date(2026, 10, 30), "latest features present")
    check(k.market.signal_date is None and k.sentiment.signal_date is None,
          "missing signal-date row -> null (latest NOT substituted)")
    check(k.data_status.signal_date_features.is_available is False
          and k.data_status.signal_date_features.as_of == date(2026, 10, 20)
          and k.data_status.signal_date_features.is_stale is None,
          "status: signal-date features unavailable, as_of = signal date, stale not applicable")
    check(k.data_status.latest_features.is_stale is False,
          "SCOM latest features (Fri 10-30 -> Wed 11-04 = 3 biz days) fresh at the boundary")
    snt = k.sentiment.latest
    check(snt is not None and snt.average_sentiment is None and snt.weighted_sentiment is None
          and snt.sentiment_volatility is None and snt.average_impact_score is None,
          "NULL sentiment preserved as null, not 0")
    check(snt.relevant_headline_count == 0, "stored headline count (0) preserved")
    check(k.fundamentals is None and k.data_status.fundamentals.is_available is False
          and k.data_status.fundamentals.is_stale is None, "no fundamentals -> null, not 'stale'")
    check(len(k.active_alerts) == 1 and k.active_alerts[0].severity == "medium", "SCOM active alert returned")
    s.close()


def test_ticker_eqty_kcb_absa() -> None:
    section("D3. EQTY (features only), KCB (market only), ABSA (divergence), no-signal tickers")
    s = Session()

    e = svc(s).get_analysis(ticker="EQTY")
    ek = e.securities[0]
    check(ek.company_name == "Equity Group Holdings Plc", "company falls back to Fundamentals.company")
    check(ek.signal is None and ek.data_status.signal.is_available is False
          and ek.data_status.signal.is_stale is None, "no signal -> null, available False, stale null (not false)")
    check(ek.market.latest is not None and ek.market.signal_date is None, "features exist, no signal-date features")
    check(ek.data_status.signal_date_features.as_of is None, "no signal -> no signal-date as_of")
    check(ek.fundamentals.eps == 5.0 and ek.data_status.fundamentals.is_stale is True, "old fundamentals flagged stale")
    check(e.ticker_summary.signal is None and e.ticker_summary.has_fundamentals is True,
          "ticker_summary for a no-signal ticker")

    k = svc(s).get_analysis(ticker="KCB")
    kk = k.securities[0]
    check(kk.company_name == "KCB Group Plc", "company from market_data")
    check(kk.market.latest is None and kk.market.signal_date is None and kk.signal is None
          and kk.fundamentals is None and kk.active_alerts == [] and kk.sentiment.latest is None,
          "known ticker with no intelligence -> every section null/empty, still 200-shaped")
    check(not any([kk.data_status.latest_features.is_available, kk.data_status.signal.is_available,
                   kk.data_status.fundamentals.is_available, kk.data_status.active_alerts.is_available]),
          "all statuses unavailable")
    check(k.ticker_summary.active_alert_count == 0, "no alerts -> count 0")

    a = svc(s).get_analysis(ticker=" absa ")
    check(a.securities[0].signal.signal == "divergence" and a.securities[0].signal.strength is None,
          "ticker normalised (strip/upper); divergence with null strength preserved")
    check(a.securities[0].market.signal_date.trading_date == date(2026, 11, 4)
          and a.securities[0].market.latest.close_price == a.securities[0].market.signal_date.close_price,
          "latest == signal-date when same day")

    for t in UNIVERSE:
        check(svc(s).get_analysis(ticker=t).securities[0].ticker == t, f"ticker mode works for {t}")
    s.close()


def test_unknown_ticker() -> None:
    section("E. Unknown ticker")
    s = Session()
    for bad in ("NOPE", "ORPH", "ZZZZ", "  ", "K PLC"):
        check(raises(TickerNotFoundError, lambda b=bad: svc(s).get_analysis(ticker=b)),
              f"{bad!r} -> TickerNotFoundError")
    s.close()


# ===========================================================================
# F. Macro
# ===========================================================================

def test_macro() -> None:
    section("F. Macroeconomics")
    s = Session()
    m = svc(s).get_analysis(limit=1).macroeconomics

    check(m.inflation.value == 6.5 and m.inflation.period == "July 2026" and m.inflation.as_of == date(2026, 7, 1),
          "inflation from the newest non-null month (July), independent of insertion order")
    check(m.inflation.trend == 0.5, "inflation trend comes from the SAME (July) row")
    check(m.fuel_price.value == 214.03 and m.fuel_price.period == "July 2026" and m.fuel_price.trend is None,
          "fuel trend NULL on the source row stays NULL (May's 0.3 is NOT borrowed)")
    check(m.cbk_rate.value == 9.0 and m.cbk_rate.period == "June 2026",
          "cbk_rate skips NULL July -> June (newer than May)")
    check(m.usd_kes_rate.value == 129.4 and m.usd_kes_rate.period == "July 2026", "usd from July")
    check(m.euro_kes_rate.value == 147.0 and m.euro_kes_rate.period == "May 2026",
          "euro skips NULL July and June -> May")
    check(m.pounds_kes_rate.is_available is False and m.pounds_kes_rate.value is None
          and m.pounds_kes_rate.period is None and m.pounds_kes_rate.is_stale is None,
          "indicator NULL in every row -> unavailable, stale=null")
    check(m.cbk_rate.report_date == date(2026, 6, 1) and m.euro_kes_rate.report_date == date(2026, 5, 1),
          "each indicator keeps its own source report_date")
    check(len({m.inflation.period, m.cbk_rate.period, m.euro_kes_rate.period}) == 3,
          "indicators legitimately come from different periods")
    check(all(i.is_stale is True for i in (m.inflation, m.cbk_rate, m.euro_kes_rate)),
          "at 2026-11-04 July/June/May are all > 3 months behind -> stale")

    earlier = svc(s, now=datetime(2026, 9, 15, 10, tzinfo=NAIROBI)).get_analysis(limit=1).macroeconomics
    check(earlier.inflation.is_stale is False and earlier.cbk_rate.is_stale is False
          and earlier.euro_kes_rate.is_stale is True,
          "at 2026-09-15: July fresh, June fresh (3 months), May stale (4 months)")

    check(svc(s).get_analysis(ticker="KCB").macroeconomics == m, "same macro section in ticker mode")
    s.close()


# ===========================================================================
# G. Read-only + N+1
# ===========================================================================

def _capture(fn):
    statements, commits = [], []

    def on_exec(conn, cursor, statement, params, context, executemany):
        statements.append(statement)

    def on_commit(conn):
        commits.append(1)

    event.listen(ENGINE, "before_cursor_execute", on_exec)
    event.listen(ENGINE, "commit", on_commit)
    try:
        out = fn()
    finally:
        event.remove(ENGINE, "before_cursor_execute", on_exec)
        event.remove(ENGINE, "commit", on_commit)
    return out, statements, commits


def _table_counts() -> dict[str, int]:
    s = Session()
    try:
        return {t.name: s.execute(select(func.count()).select_from(t)).scalar_one() for t in TABLES}
    finally:
        s.close()


def test_read_only_and_n_plus_one() -> None:
    section("G. Read-only behaviour and absence of N+1")
    before = _table_counts()
    s = Session()

    for label, call in (
        ("NSE-wide", lambda: svc(s).get_analysis(limit=200)),
        ("ticker", lambda: svc(s).get_analysis(ticker="KPLC")),
    ):
        _, stmts, commits = _capture(call)
        check(all(x.lstrip().upper().startswith(("SELECT", "SHOW")) for x in stmts),
              f"{label}: every statement is a SELECT ({len(stmts)} statements)")
        check(not commits, f"{label}: no commit issued")
        check(not (s.new or s.dirty or s.deleted), f"{label}: session has no pending writes")

    check(_table_counts() == before, "row counts of all six tables unchanged")

    _, small, _ = _capture(lambda: svc(s).get_analysis(limit=2))
    _, full, _ = _capture(lambda: svc(s).get_analysis(limit=200))
    _, one, _ = _capture(lambda: svc(s).get_analysis(ticker="SCOM"))
    check(len(small) == len(full), f"statement count independent of page size ({len(small)} vs {len(full)} for 7 tickers)")
    check(len(one) == len(full), f"single-ticker uses the same number of statements ({len(one)})")
    check(len(full) <= 14, f"bounded statement count ({len(full)} <= 14)")
    s.close()


# ===========================================================================
# H. Failure handling
# ===========================================================================

class BrokenSession:
    """Every query fails the way a lost MySQL connection would."""

    def execute(self, *args, **kwargs):
        raise OperationalError("SELECT 1", {}, Exception("secret-host=10.0.0.5 password=hunter2"))


def test_db_failure_service() -> None:
    section("H. Database failure (service)")
    check(raises(OperationalError, lambda: MasterIntelligenceService(BrokenSession(), now=NOW).get_analysis()),
          "DB error propagates (never a misleading empty 200)")


# ===========================================================================
# I. HTTP API
# ===========================================================================

def _client(broken: bool = False) -> TestClient:
    app = FastAPI()
    app.include_router(analysis_api.router)

    def override():
        if broken:
            yield BrokenSession()
            return
        db = Session()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override
    return TestClient(app, raise_server_exceptions=False, follow_redirects=False)


def test_api() -> None:
    section("I. GET /analysis (HTTP)")
    c = _client()

    r = c.get("/analysis")
    check(r.status_code == 200, "GET /analysis -> 200 (no redirect)")
    body = r.json()
    check(set(body) == {"metadata", "market_summary", "ticker_summary", "securities", "macroeconomics"},
          "top-level keys")
    check(len(body["securities"]) == 7 and body["ticker_summary"] is None and body["metadata"]["pagination"]["total"] == 7,
          "NSE-wide payload")
    check(body["market_summary"]["total_securities"] == 7 and body["market_summary"]["active_alert_count"] == 2,
          "summary in payload")

    r = c.get("/analysis?limit=2&offset=2")
    check(r.status_code == 200 and [s["ticker"] for s in r.json()["securities"]] == ["EABL", "EQTY"],
          "limit/offset honoured")

    for kplc in ("KPLC", "kplc"):
        r = c.get(f"/analysis?ticker={kplc}")
        j = r.json()
        check(r.status_code == 200 and j["metadata"]["requested_ticker"] == "KPLC"
              and j["ticker_summary"]["signal"] == "bullish" and len(j["securities"]) == 1,
              f"?ticker={kplc} -> KPLC detail")
    j = c.get("/analysis?ticker=KPLC").json()["securities"][0]
    check(j["fundamentals"]["eps"] == 12.5 and j["market"]["latest"]["trading_date"] == "2026-11-04"
          and j["market"]["signal_date"]["trading_date"] == "2026-11-02", "KPLC JSON values / ISO dates")
    check(isinstance(j["signal"]["components"], dict) and j["active_alerts"][0]["details"]["evidence"] is not None,
          "components / alert evidence serialise")

    for t in ("SCOM", "EQTY", "KCB", "COOP"):
        check(c.get(f"/analysis?ticker={t}").status_code == 200, f"?ticker={t} -> 200")

    r = c.get("/analysis?ticker=SCOM&limit=1&offset=99")
    check(r.status_code == 200 and r.json()["securities"][0]["ticker"] == "SCOM", "paging ignored in ticker mode")

    r = c.get("/analysis?ticker=KCB").json()
    check(r["securities"][0]["signal"] is None and r["securities"][0]["fundamentals"] is None
          and r["securities"][0]["active_alerts"] == [], "known ticker with no data -> 200 with nulls")

    r = c.get("/analysis?ticker=NOPE")
    check(r.status_code == 404 and "NOPE" in r.json()["detail"], "unknown ticker -> 404")
    check(c.get("/analysis?ticker=ORPH").status_code == 404, "ticker only in analysis_results (outside universe) -> 404")

    for q in ("limit=0", "limit=201", "offset=-1", "limit=abc", "ticker="):
        check(c.get(f"/analysis?{q}").status_code == 422, f"?{q} -> 422")
    check(c.get("/analysis?limit=200").status_code == 200 and c.get("/analysis?limit=1").status_code == 200,
          "limit boundaries 1 and 200 accepted")

    check(c.post("/analysis").status_code == 405 and c.put("/analysis").status_code == 405
          and c.delete("/analysis").status_code == 405, "no write methods exposed")

    broken = _client(broken=True).get("/analysis")
    check(broken.status_code == 500, "DB failure -> 500")
    check("hunter2" not in broken.text and "10.0.0.5" not in broken.text and "OperationalError" not in broken.text,
          "DB error details not leaked to the client")


# ===========================================================================
# J. Empty database
# ===========================================================================

def test_empty() -> None:
    section("J. Empty tables")
    s = Session()
    for t in TABLES:
        s.execute(delete(t))
    s.commit()

    r = svc(s).get_analysis()
    check(r.securities == [] and r.market_summary.total_securities == 0
          and r.market_summary.securities_without_signals == 0, "empty universe -> empty page, zero summary")
    m = r.macroeconomics
    check(not any(i.is_available for i in (m.inflation, m.fuel_price, m.cbk_rate,
                                           m.usd_kes_rate, m.euro_kes_rate, m.pounds_kes_rate)),
          "empty macro_data -> section present, every indicator unavailable")
    check(raises(TickerNotFoundError, lambda: svc(s).get_analysis(ticker="KPLC")), "any ticker 404s on empty universe")
    s.close()


# ===========================================================================
# Orchestration
# ===========================================================================

def main() -> int:
    print("=" * 70)
    print("Master Intelligence Service test")
    print(f"database: {ENGINE.url.render_as_string(hide_password=True)}")
    print("=" * 70)

    if not (hasattr(Alert, "ticker") and hasattr(Alert, "status")):
        print("Alert model lacks `ticker`/`status` - adjust config.analysis_config / the service first.")
        return 1

    reset_schema()
    seed_main()

    test_config()
    test_nse_wide()
    test_pagination()
    test_ticker_kplc()
    test_ticker_scom()
    test_ticker_eqty_kcb_absa()
    test_unknown_ticker()
    test_macro()
    test_read_only_and_n_plus_one()
    test_db_failure_service()
    test_api()
    test_empty()

    print("\n" + "=" * 70)
    print(f"Passed: {_PASS}   Failed: {_FAIL}   Skipped: {_SKIP}")
    print("=" * 70)
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())