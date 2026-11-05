"""
testers/test_AlertService.py

Standalone tester for the Alert Engine (rules, lifecycle service, API).
Same style as the other testers: plain script, [PASS]/[FAIL] counters.

    python -m testers.test_AlertService

ISOLATED: requires TEST_DATABASE_URL pointing to a dedicated MySQL test database.
The test tables are reset for each test. Never use a production database.
"""

import os
import sys
import threading
from datetime import date, timedelta
from dotenv import load_dotenv



from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import create_engine, event, func, select  # noqa: E402
from sqlalchemy.engine import make_url  # noqa: E402
from sqlalchemy.exc import IntegrityError, OperationalError  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from api import alerts as alerts_api  # noqa: E402
from database.base import Base  # noqa: E402
from database.session import get_db  # noqa: E402
from intelligence.alert_rules import (  # noqa: E402
    InvalidSignalError, check_freshness, evaluate_alert_rule, severity_for,
)
from models.alert_data import Alert  # noqa: E402
from models.analytics_data import AnalysisResult  # noqa: E402
from models.daily_market_features import DailyMarketFeatures  # noqa: E402
from services.alert_service.AlertEvaluationService import AlertEvaluationService  # noqa: E402

load_dotenv()
_PASS = _FAIL = 0
TODAY = date(2026, 9, 23)
D1, D2, D3 = date(2026, 9, 21), date(2026, 9, 22), date(2026, 9, 23)


def check(cond: bool, msg: str) -> bool:
    global _PASS, _FAIL
    if cond:
        _PASS += 1
        print(f"[PASS] {msg}")
    else:
        _FAIL += 1
        print(f"[FAIL] {msg}")
    return cond


def section(t: str) -> None:
    print("\n" + "-" * 70 + f"\n{t}\n" + "-" * 70)


class Env:
    """Dedicated MySQL test database + project-style sessionmaker for one test."""

    def __init__(self) -> None:
        url = os.environ.get("TEST_DATABASE_URL", "")
        parsed = make_url(url) if url else None
        if (parsed is None or parsed.get_backend_name() != "mysql"
                or not (parsed.database or "").lower().endswith("_test")):
            raise RuntimeError(
                "Set TEST_DATABASE_URL to a dedicated MySQL database ending in _test "
                "(e.g. mysql+pymysql://user:password@localhost/purple_signals_test)."
            )
        self.engine = create_engine(url, pool_pre_ping=True, isolation_level="READ COMMITTED")
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(autocommit=False, autoflush=False,
                                    expire_on_commit=False, bind=self.engine)
        self.db = self.Session()
        self.svc = AlertEvaluationService(self.db)

    def signal(self, ticker, d, signal, strength=None, conf=0.5, **kw):
        row = AnalysisResult(ticker=ticker, trading_date=d, signal=signal, strength=strength,
                             confidence_score=conf, explanation=kw.get("explanation", f"{signal} expl"),
                             components=kw.get("components", {"price": 1}))
        self.db.add(row)
        self.db.commit()
        return row

    def set_signal(self, ticker, d, **fields):
        row = self.db.execute(select(AnalysisResult).where(
            AnalysisResult.ticker == ticker, AnalysisResult.trading_date == d)).scalar_one()
        for k, v in fields.items():
            setattr(row, k, v)
        self.db.commit()

    def alerts(self, ticker=None):
        self.db.expire_all()
        stmt = select(Alert).order_by(Alert.id)
        if ticker:
            stmt = stmt.where(Alert.ticker == ticker)
        return list(self.db.execute(stmt).scalars().all())

    def active(self, ticker):
        return [a for a in self.alerts(ticker) if a.status == "active"]

    def close(self):
        self.db.close()
        self.engine.dispose()



def ev(env, ticker=None):
    return env.svc.evaluate(ticker=ticker, today=TODAY)


# ---------------------------------------------------------------------------
def test_rules() -> None:
    section("Rules: severity, validation, freshness")
    check(severity_for("bullish", "strong") == "high", "strong -> high")
    check(severity_for("bearish", "moderate") == "medium", "moderate -> medium")
    check(severity_for("divergence", None) == "low", "None -> low")
    check(severity_for("unusual_activity", None) == "low", "unusual_activity None -> low")
    check(severity_for("neutral", None) == "info", "neutral -> info")
    check(severity_for("neutral", "strong") == "info", "neutral always info")
    try:
        severity_for("bullish", "weak")
        check(False, "invented 'weak' strength rejected")
    except InvalidSignalError:
        check(True, "invented 'weak' strength rejected")

    e = Env()
    try:
        for sig, strength in [("bullish", "strong"), ("bearish", "moderate"), ("divergence", None),
                              ("unusual_activity", None), ("neutral", None)]:
            row = e.signal("T" + sig[:3].upper(), D2, sig, strength, 0.6667)
            d = evaluate_alert_rule(row)
            check(d.alert_type == sig and d.severity == severity_for(sig, strength)
                  and d.confidence_score == 0.6667 and d.evidence["components"] == {"price": 1}
                  and d.trading_date == D2, f"rule builds decision for {sig}")
        bad_cases = {
            "invalid classification": dict(signal="buy"),
            "missing classification": dict(signal=None),
            "missing confidence": dict(confidence_score=None),
            "confidence out of range": dict(confidence_score=1.5),
            "bad strength": dict(strength="weak"),
            "malformed components": dict(components=[1, 2]),
        }
        for i, (label, fields) in enumerate(bad_cases.items()):
            row = e.signal(f"BAD{i}", D2, "bullish", "strong", 0.9)
            for k, v in fields.items():
                setattr(row, k, v)
            try:
                evaluate_alert_rule(row)
                check(False, f"{label} rejected")
            except InvalidSignalError:
                check(True, f"{label} rejected")
        row = e.signal("NOCOMP", D2, "neutral", None, 0.0, components=None)
        check(evaluate_alert_rule(row).evidence["components"] is None, "components None allowed")
    finally:
        e.close()

    check(check_freshness(D2, D2, TODAY) is None, "fresh when equal to reference")
    check(check_freshness(D2 - timedelta(days=5), D2, TODAY) is None, "5-day lag still fresh")
    check(check_freshness(D2 - timedelta(days=6), D2, TODAY) == "stale_signal", "6-day lag stale")
    check(check_freshness(D2, D2, D2 + timedelta(days=8)) == "market_data_stalled", "stalled market")
    check(check_freshness(D2, D2, D2 + timedelta(days=3)) is None, "weekend gap not stale")


def test_five_classes_and_basic() -> None:
    section("Lifecycle: first creation for all five classes; repeat is idempotent")
    e = Env()
    try:
        spec = [("AAA", "bullish", "strong", "high"), ("BBB", "bearish", "moderate", "medium"),
                ("CCC", "divergence", None, "low"), ("DDD", "unusual_activity", None, "low"),
                ("EEE", "neutral", None, "info")]
        for t, s, st, _ in spec:
            e.signal(t, D2, s, st, 0.6667)
        r = ev(e)
        check(r["status"] == "success" and r["evaluated"] == 5 and r["created"] == 5, f"5 created: {r}")
        for t, s, st, sev in spec:
            a = e.active(t)
            check(len(a) == 1 and a[0].alert_type == s and a[0].severity == sev
                  and a[0].signal_strength == st and a[0].status == "active"
                  and a[0].first_detected_at == a[0].last_detected_at and a[0].resolved_at is None
                  and a[0].trading_date == D2 and a[0].evidence["signal"] == s and a[0].confidence_score == 0.6667,
                  f"{t} {s} alert correct")

        before = {a.id: (a.first_detected_at, a.last_detected_at) for a in e.alerts()}
        for _ in range(3):
            r = ev(e)
        after = {a.id: (a.first_detected_at, a.last_detected_at) for a in e.alerts()}
        check(r["unchanged"] == 5 and r["created"] == r["updated"] == r["transitioned"] == 0,
              "repeat evaluation: all unchanged")
        check(before == after and len(after) == 5, "no duplicates, timestamps untouched")
        check(r["evaluated"] == sum(r[k] for k in
              ("created", "updated", "transitioned", "unchanged", "skipped", "failed")),
              "outcome counts sum to evaluated")
    finally:
        e.close()


def test_updates() -> None:
    section("Lifecycle: same classification (newer date, strength, confidence)")
    e = Env()
    try:
        e.signal("KPLC", D1, "bullish", "moderate", 0.6667)
        ev(e)
        first = e.active("KPLC")[0]
        fid, fdet = first.id, first.first_detected_at

        e.signal("KPLC", D2, "bullish", "moderate", 0.6667)  # newer date, same state
        r = ev(e)
        a = e.active("KPLC")
        check(r["updated"] == 1 and len(a) == 1 and a[0].id == fid and a[0].trading_date == D2
              and a[0].first_detected_at == fdet and a[0].last_detected_at >= fdet
              and a[0].evidence["trading_date"] == D2.isoformat(), "newer date updates same alert")

        e.signal("KPLC", D3, "bullish", "strong", 1.0)  # upgrade
        r = ev(e)
        a = e.active("KPLC")
        check(r["updated"] == 1 and a[0].id == fid and a[0].severity == "high"
              and a[0].signal_strength == "strong" and a[0].confidence_score == 1.0
              and a[0].first_detected_at == fdet and len(e.alerts("KPLC")) == 1,
              "strength upgrade medium->high, same id/first_detected_at")

        e.set_signal("KPLC", D3, strength="moderate", confidence_score=0.6667)  # downgrade same day
        r = ev(e)
        a = e.active("KPLC")
        check(r["updated"] == 1 and a[0].severity == "medium" and a[0].id == fid, "strength downgrade high->medium")

        e.set_signal("KPLC", D3, confidence_score=0.7)
        r = ev(e)
        check(r["updated"] == 1 and e.active("KPLC")[0].confidence_score == 0.7, "confidence-only change updates")
        check(ev(e)["unchanged"] == 1, "then stable")
    finally:
        e.close()


def test_transitions() -> None:
    section("Lifecycle: classification transitions")
    e = Env()
    try:
        e.signal("KCB", D1, "bullish", "strong", 1.0)
        ev(e)
        bull_id = e.active("KCB")[0].id

        e.signal("KCB", D2, "bearish", "moderate", 0.6667)
        r = ev(e)
        al = e.alerts("KCB")
        check(r["transitioned"] == 1 and r["resolved"] == 1 and r["evaluated"] == 1 and r["created"] == 0,
              f"bullish->bearish counted as ONE evaluated ticker, resolved=1: {r}")
        check(len(al) == 2 and al[0].id == bull_id and al[0].status == "resolved"
              and al[0].resolved_at is not None and al[0].active_ticker is None,
              "old bullish resolved and kept")
        check(len(e.active("KCB")) == 1 and e.active("KCB")[0].alert_type == "bearish", "new bearish active")

        e.signal("KCB", D3, "neutral", None, 0.0)
        r = ev(e)
        act = e.active("KCB")
        check(r["transitioned"] == 1 and act[0].alert_type == "neutral" and act[0].severity == "info",
              "bearish->neutral creates info neutral alert")

        # F: previous classification returns -> NEW row, old one stays resolved
        e.signal("KCB", D3 + timedelta(days=1), "bullish", "strong", 1.0)
        ev(e)  # needs today >= date; freshness ok (TODAY=D3, signal D3+1 is future but not stale)
        al = e.alerts("KCB")
        bulls = [a for a in al if a.alert_type == "bullish"]
        check(len(al) == 4 and len(bulls) == 2 and bulls[0].status == "resolved" and bulls[0].id == bull_id
              and bulls[1].status == "active" and bulls[1].id != bull_id,
              "returning classification creates a new alert; history not reactivated")
        check(len(e.active("KCB")) == 1, "exactly one active")

        # neutral -> bullish
        e.signal("OTH", D1, "neutral", None, 0.0)
        ev(e)
        e.signal("OTH", D2, "bullish", "moderate", 0.6667)
        r = ev(e, "OTH")
        check(r["transitioned"] == 1 and e.active("OTH")[0].alert_type == "bullish", "neutral->bullish")

        # same-day recalculation flips classification -> transition (not outdated)
        e.set_signal("OTH", D2, signal="bearish", strength="strong", confidence_score=1.0)
        r = ev(e, "OTH")
        check(r["transitioned"] == 1 and e.active("OTH")[0].alert_type == "bearish",
              "same-day reclassification transitions")
    finally:
        e.close()


def test_missing_stale_outdated_invalid() -> None:
    section("Safety: missing / stale / older / invalid data never resolve alerts")
    e = Env()
    try:
        # build an active bearish alert for STALE while it is fresh
        e.signal("STALE", D1 - timedelta(days=20), "bearish", "strong", 1.0)
        e.svc.evaluate(today=D1 - timedelta(days=19))
        check(len(e.active("STALE")) == 1, "setup: active bearish")

        e.signal("FRESH", D2, "bullish", "strong", 1.0)
        r = ev(e)
        s = e.active("STALE")
        check(r["skipped"] == 1 and any(x["reason"] == "stale_signal" and x["ticker"] == "STALE" for x in r["errors"])
              and len(s) == 1 and s[0].alert_type == "bearish" and r["status"] == "success",
              "stale signal skipped; active alert preserved, no neutral generated")
        r1 = ev(e, "STALE")
        check(r1["skipped"] == 1 and len(e.active("STALE")) == 1, "ticker-specific stale also skipped")

        # missing: delete signals for STALE entirely
        e.db.execute(AnalysisResult.__table__.delete().where(AnalysisResult.ticker == "STALE"))
        e.db.commit()
        ev(e)
        check(len(e.active("STALE")) == 1 and e.active("STALE")[0].alert_type == "bearish",
              "missing signal leaves alert untouched")

        # market stalled: nothing changes
        n = len(e.alerts())
        r = e.svc.evaluate(today=D2 + timedelta(days=30))
        check(r["skipped"] == r["evaluated"] == 1 and len(e.alerts()) == n
              and all(x["reason"] == "market_data_stalled" for x in r["errors"]), "stalled market skips everything")

        # freshness reference from daily_features
        e.db.add(DailyMarketFeatures(ticker="FRESH", trading_date=D2))
        e.db.commit()
        check(ev(e)["unchanged"] == 1, "reference from daily_features works")

        # older signal than active alert
        e.signal("OLD", D3, "bullish", "strong", 1.0)
        ev(e)
        e.signal("OLD", D1, "bearish", "strong", 1.0)  # older row exists, but latest remains D3
        r = ev(e, "OLD")
        check(r["unchanged"] == 1 and e.active("OLD")[0].alert_type == "bullish", "older row ignored (latest wins)")
        e.db.execute(AnalysisResult.__table__.delete().where(
            AnalysisResult.ticker == "OLD", AnalysisResult.trading_date == D3))
        e.db.commit()
        r = ev(e, "OLD")  # latest signal is now D1 < alert's D3
        a = e.active("OLD")
        check(r["skipped"] == 1 and any(x["reason"] == "outdated_signal" for x in r["errors"])
              and a[0].alert_type == "bullish" and a[0].trading_date == D3,
              "older signal never overwrites/resolves newer alert state")

        # invalid classification: skipped, no neutral invented, alert preserved
        e.signal("INV", D1, "bullish", "strong", 1.0)
        ev(e, "INV")
        e.signal("INV", D2, "buy!", "strong", 1.0)
        r = ev(e, "INV")
        check(r["skipped"] == 1 and r["status"] == "partial_success"
              and r["errors"][0]["reason"] == "invalid_signal"
              and e.active("INV")[0].alert_type == "bullish" and len(e.alerts("INV")) == 1,
              "invalid classification skipped, not defaulted to neutral")

        r = ev(e, "NOSUCH")
        check(r["skipped"] == 1 and r["errors"][0]["reason"] == "no_signal" and len(e.alerts("NOSUCH")) == 0,
              "unknown ticker handled")
    finally:
        e.close()

    e = Env()
    try:
        r = ev(e)
        check(r["status"] == "success" and r["evaluated"] == 0, "empty analysis_results handled")
    finally:
        e.close()


def test_failures_and_atomicity() -> None:
    section("Failure isolation and atomic transitions")
    e = Env()
    try:
        for t in ("AAA", "BAD", "ZZZ"):
            e.signal(t, D2, "bullish", "strong", 1.0)

        def boom(mapper, connection, target):
            if target.ticker == "BAD":
                raise OperationalError("INSERT", {}, Exception("simulated db failure"))

        event.listen(Alert, "before_insert", boom)
        try:
            r = ev(e)
        finally:
            event.remove(Alert, "before_insert", boom)
        check(r["failed"] == 1 and r["created"] == 2 and r["status"] == "partial_success"
              and r["errors"][0]["ticker"] == "BAD" and r["errors"][0]["reason"] == "evaluation_error",
              f"partial failure: others created, BAD failed: {r}")
        check(len(e.active("AAA")) == 1 and len(e.active("ZZZ")) == 1 and len(e.alerts("BAD")) == 0,
              "failed ticker rolled back, others intact")
        r = ev(e)
        check(r["created"] == 1 and r["unchanged"] == 2, "recovery run creates only the missing alert")

        # failure mid-transition: new insert fails after old was resolved -> old stays active
        e.signal("AAA", D3, "bearish", "strong", 1.0)

        def boom2(mapper, connection, target):
            if target.ticker == "AAA":
                raise OperationalError("INSERT", {}, Exception("simulated db failure"))

        event.listen(Alert, "before_insert", boom2)
        try:
            r = ev(e, "AAA")
        finally:
            event.remove(Alert, "before_insert", boom2)
        al = e.alerts("AAA")
        check(r["failed"] == 1 and r["status"] == "failed" and len(al) == 1 and al[0].status == "active"
              and al[0].alert_type == "bullish" and al[0].resolved_at is None and al[0].active_ticker == "AAA",
              "failed transition fully rolled back (old alert still active, nothing half-done)")
        r = ev(e, "AAA")
        check(r["transitioned"] == 1 and len(e.active("AAA")) == 1, "transition succeeds on retry")
    finally:
        e.close()


def test_races() -> None:
    section("Concurrency: unique backstop + retry")
    # DB-level invariant
    e = Env()
    try:
        e.signal("RACE", D2, "bullish", "strong", 1.0)
        ev(e)
        dup = Alert(ticker="RACE", active_ticker="RACE", alert_type="bullish", severity="high", message="m",
                    trading_date=D2, confidence_score=1.0, status="active",
                    first_detected_at=e.alerts()[0].first_detected_at,
                    last_detected_at=e.alerts()[0].first_detected_at)
        e.db.add(dup)
        try:
            e.db.commit()
            check(False, "DB rejects second active alert for a ticker")
        except IntegrityError:
            e.db.rollback()
            check(True, "DB rejects second active alert for a ticker")
        bad = Alert(ticker="RACE", active_ticker=None, alert_type="x", severity="low", message="m",
                    trading_date=D2, confidence_score=1.0, status="active",
                    first_detected_at=dup.first_detected_at, last_detected_at=dup.first_detected_at)
        e.db.add(bad)
        try:
            e.db.commit()
            check(False, "CHECK rejects active alert without active_ticker")
        except (IntegrityError, OperationalError):
            e.db.rollback()
            check(True, "CHECK rejects active alert without active_ticker")
    finally:
        e.close()

    # simulated lost race: stale read says "no active alert"
    e = Env()
    try:
        e.signal("LOST", D2, "bullish", "strong", 1.0)
        ev(e)
        real = e.svc._load_active_locked
        calls = {"n": 0}

        def stale_first(ticker):
            calls["n"] += 1
            return None if calls["n"] == 1 else real(ticker)

        e.signal("LOST", D3, "bullish", "strong", 1.0)
        e.svc._load_active_locked = stale_first
        r = ev(e, "LOST")
        e.svc._load_active_locked = real
        check(r["failed"] == 0 and r["updated"] == 1 and len(e.active("LOST")) == 1 and len(e.alerts("LOST")) == 1
              and e.active("LOST")[0].trading_date == D3, f"lost race retried and converged: {r}")
    finally:
        e.close()

    # real threads, separate sessions; MySQL FOR UPDATE serializes conflicting rows
    ok_runs = 0
    for run in range(5):
        e = Env()
        try:
            for t in ("T1", "T2", "T3"):
                e.signal(t, D2, "bullish", "strong", 1.0)
            results, errs = [], []

            def worker():
                db = e.Session()
                svc = AlertEvaluationService(db)
                try:
                    results.append(svc.evaluate(today=TODAY))
                except Exception as exc:  # noqa: BLE001
                    errs.append(exc)
                finally:
                    db.close()

            ths = [threading.Thread(target=worker) for _ in range(2)]
            [t.start() for t in ths]
            [t.join(30) for t in ths]
            per = [len(e.active(t)) for t in ("T1", "T2", "T3")]
            if per == [1, 1, 1] and len(e.alerts()) == 3 and not errs:
                ok_runs += 1
            else:
                print("  run", run, per, len(e.alerts()), errs, results)
        finally:
            e.close()
    check(ok_runs == 5, f"concurrent evaluations: exactly one active alert per ticker ({ok_runs}/5 runs)")


def test_api() -> None:
    section("API: evaluate, retrieval filters, pagination, read-only")
    e = Env()
    try:
        app = FastAPI()
        app.include_router(alerts_api.router)

        def override():
            db = e.Session()
            try:
                yield db
            finally:
                db.close()

        app.dependency_overrides[get_db] = override
        c = TestClient(app)

        # evaluate uses real 'today' -> signals dated relative to real today
        real_today = date.today()
        e.signal("KPLC", real_today, "bullish", "strong", 1.0)
        e.signal("KCB", real_today, "bearish", "moderate", 0.6667)
        e.signal("EQTY", real_today, "neutral", None, 0.0)

        before = len(e.alerts())
        check(c.get("/alerts").json() == [] and len(e.alerts()) == before, "GET on empty is read-only")

        r = c.post("/alerts/evaluate")
        check(r.status_code == 200 and r.json()["created"] == 3 and r.json()["evaluated"] == 3, f"POST evaluate: {r.json()}")
        check(c.post("/alerts/evaluate").json()["unchanged"] == 3, "POST idempotent")
        check(c.post("/alerts/evaluate?ticker=KPLC").json()["evaluated"] == 1, "POST ?ticker=KPLC")
        check(c.post("/alerts/evaluate?ticker=NOPE").status_code == 404, "unknown ticker -> 404")
        check(c.post("/alerts/evaluate?ticker=bad%20tick").status_code == 422, "invalid ticker -> 422")

        n = len(e.alerts())
        check(len(c.get("/alerts").json()) == 3 and len(e.alerts()) == n, "GET /alerts")
        check([a["ticker"] for a in c.get("/alerts?ticker=kplc").json()] == ["KPLC"], "ticker filter (case-insens.)")
        check(len(c.get("/alerts?status=active").json()) == 3 and c.get("/alerts?status=resolved").json() == [],
              "status filter")
        check([a["ticker"] for a in c.get("/alerts?alert_type=bearish").json()] == ["KCB"], "alert_type filter")
        check(c.get("/alerts?ticker=KPLC&alert_type=bearish").json() == [], "filters combine with AND")
        check(c.get("/alerts?status=bogus").status_code == 422 and c.get("/alerts?alert_type=buy").status_code == 422
              and c.get("/alerts?limit=0").status_code == 422 and c.get("/alerts?limit=500").status_code == 422
              and c.get("/alerts?offset=-1").status_code == 422, "invalid filters/pagination -> 422")

        # history + pagination: flip KPLC a few times
        for i, (s, st) in enumerate([("bearish", "strong"), ("neutral", None), ("bullish", "moderate")], 1):
            e.signal("KPLC", real_today + timedelta(days=i), s, st, 0.5)
            e.svc.evaluate(today=real_today + timedelta(days=i))
        hist = c.get("/alerts?ticker=KPLC&limit=100").json()
        check(len(hist) == 4 and [a["status"] for a in hist].count("active") == 1, "history retained, one active")
        ids = [a["id"] for a in hist]
        check(ids == sorted(ids, reverse=True), "newest-first deterministic order")
        p1 = c.get("/alerts?ticker=KPLC&limit=2&offset=0").json()
        p2 = c.get("/alerts?ticker=KPLC&limit=2&offset=2").json()
        check([a["id"] for a in p1 + p2] == ids and not set(a["id"] for a in p1) & set(a["id"] for a in p2),
              "pagination pages are disjoint and complete")
        a = hist[0]
        check({"id", "ticker", "alert_type", "severity", "message", "status", "trading_date", "signal_strength",
               "confidence_score", "first_detected_at", "last_detected_at", "resolved_at", "evidence",
               "timestamp"} <= set(a) and isinstance(a["evidence"], dict), "response schema complete")
    finally:
        e.close()


def main() -> int:
    print("=" * 70 + "\nAlert Engine test (dedicated MySQL test database)\n" + "=" * 70)
    for fn in (test_rules, test_five_classes_and_basic, test_updates, test_transitions,
               test_missing_stale_outdated_invalid, test_failures_and_atomicity, test_races, test_api):
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            global _FAIL
            _FAIL += 1
            import traceback
            traceback.print_exc()
            print(f"[FAIL] {fn.__name__} raised {type(exc).__name__}: {exc}")
    print("\n" + "=" * 70 + f"\nPassed: {_PASS}   Failed: {_FAIL}\n" + "=" * 70)
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())