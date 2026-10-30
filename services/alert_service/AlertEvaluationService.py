"""
services/alert_service/AlertEvaluationService.py

Service Layer — turns the latest persisted combined signal per ticker into
alert lifecycle state in the existing `alerts` table.

Pipeline position:

    analysis_results (SignalService)  ->  AlertEvaluationService  ->  alerts

Per-ticker outcomes (exactly one each; they sum to `evaluated`):
    created      no active alert existed; one was created
    updated      same classification; strength/confidence/date/evidence refreshed
    transitioned classification changed; old resolved + new created (ONE commit)
    unchanged    identical state already stored (no write at all)
    skipped      invalid / stale / outdated / no signal (existing alerts untouched)
    failed       unexpected error; that ticker rolled back, run continues

`resolved` counts alerts resolved (== transitioned). Alerts created in total
= created + transitioned.

Safety:
  - Missing, stale or older data NEVER resolves or overwrites an alert.
  - Atomic transitions: resolve (flush) then insert, single commit.
  - Dedup: SELECT ... FOR UPDATE on the active row + UNIQUE(active_ticker)
    as the backstop; an IntegrityError rolls back and retries once, which then
    observes the winner's row.
  - Signals are read with one query (no N+1); active alerts are prefetched in
    one query. A row lock is only taken when a write is actually needed.

Does NOT recompute any signal, and does not create its own session.
"""

from __future__ import annotations

import logging
import math
from datetime import date, datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import and_, func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from intelligence.alert_rules import (
    ALERT_TYPES,
    AlertDecision,
    InvalidSignalError,
    check_freshness,
    evaluate_alert_rule,
)
from models.alert_data import ALERT_STATUSES, STATUS_ACTIVE, STATUS_RESOLVED, Alert
from models.analytics_data import AnalysisResult
from models.daily_market_features import DailyMarketFeatures

logger = logging.getLogger(__name__)

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")

ACTION_CREATE = "create"
ACTION_UPDATE = "update"
ACTION_TRANSITION = "transition"
ACTION_UNCHANGED = "unchanged"
ACTION_OUTDATED = "outdated"

REASON_NO_SIGNAL = "no_signal"
REASON_INVALID = "invalid_signal"
REASON_OUTDATED = "outdated_signal"
REASON_ERROR = "evaluation_error"
# Reasons that make the run "partial_success" (stale/outdated/no_signal are expected skips).
_ERROR_REASONS = {REASON_INVALID, REASON_ERROR}

_CONF_TOL = 1e-9


def _as_date(value: Any) -> date:
    return value.date() if isinstance(value, datetime) else value


def plan_action(decision: AlertDecision, active: Any) -> str:
    """
    Pure lifecycle decision. `active` is None or any object exposing
    trading_date / alert_type / severity / signal_strength / confidence_score /
    message / evidence (ORM row or snapshot).
    """
    if active is None:
        return ACTION_CREATE

    active_date = _as_date(active.trading_date)
    if decision.trading_date < active_date:
        return ACTION_OUTDATED
    if decision.alert_type != active.alert_type:
        return ACTION_TRANSITION

    unchanged = (
        decision.trading_date == active_date
        and decision.severity == active.severity
        and decision.signal_strength == active.signal_strength
        and math.isclose(decision.confidence_score, active.confidence_score, abs_tol=_CONF_TOL)
        and decision.message == active.message
        and decision.evidence == active.evidence
    )
    return ACTION_UNCHANGED if unchanged else ACTION_UPDATE


class _Snapshot:
    """Detached copy of an active alert (survives session rollbacks)."""

    __slots__ = (
        "trading_date", "alert_type", "severity", "signal_strength",
        "confidence_score", "message", "evidence",
    )

    def __init__(self, alert: Alert) -> None:
        for name in self.__slots__:
            setattr(self, name, getattr(alert, name))


class AlertEvaluationService:
    """
    Usage:
        service = AlertEvaluationService(db)
        result  = service.evaluate()                 # NSE-wide
        result  = service.evaluate(ticker="KPLC")    # one ticker
        alerts  = service.get_alerts(status="active")
    """

    def __init__(self, db: Session) -> None:
        self.db = db

    # ==================================================================
    # PUBLIC API
    # ==================================================================

    def evaluate(
        self, ticker: Optional[str] = None, today: Optional[date] = None
    ) -> dict[str, Any]:
        """
        Evaluate the latest AnalysisResult per ticker (or just `ticker`).

        Args:
            ticker: optional ticker filter (case-insensitive).
            today:  override "today" (Nairobi) — for tests only.
        """
        symbol = (ticker or "").strip().upper() or None
        today = today or datetime.now(NAIROBI_TZ).date()
        counts = self._new_counts()
        errors: list[dict[str, Any]] = []

        try:
            signals = self._load_latest_signals(symbol)
            reference = self._reference_session()
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.error("Alert evaluation could not read signals: %s", exc)
            return self._result("failed", counts, errors + [
                self._issue(symbol, None, REASON_ERROR, f"Database error reading signals: {exc}")
            ])

        if not signals:
            if symbol:
                counts["evaluated"] = counts["skipped"] = 1
                errors.append(self._issue(symbol, None, REASON_NO_SIGNAL,
                                          "No combined signal found for this ticker."))
            return self._result("success", counts, errors)

        counts["evaluated"] = len(signals)

        # --- pure phase: rules + freshness (no DB writes) -----------------
        decisions: list[AlertDecision] = []
        for signal in signals:
            try:
                decision = evaluate_alert_rule(signal)
            except InvalidSignalError as exc:
                counts["skipped"] += 1
                errors.append(self._issue(signal.ticker, signal.trading_date, REASON_INVALID, str(exc)))
                logger.warning("Skipping %s: invalid signal: %s", signal.ticker, exc)
                continue

            stale = check_freshness(decision.trading_date, reference, today)
            if stale:
                counts["skipped"] += 1
                errors.append(self._issue(decision.ticker, decision.trading_date, stale,
                                          "Signal not fresh; existing alert left untouched."))
                continue
            decisions.append(decision)

        # --- prefetch active alerts (one query) ---------------------------
        try:
            snapshots = self._load_active_snapshots([d.ticker for d in decisions])
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.error("Alert evaluation could not read active alerts: %s", exc)
            counts["failed"] += len(decisions)
            errors.append(self._issue(symbol, None, REASON_ERROR, f"Database error reading alerts: {exc}"))
            return self._result("failed", counts, errors)

        # --- write phase: one isolated transaction per ticker -------------
        for decision in decisions:
            try:
                action = self._process(decision, snapshots.get(decision.ticker))
            except Exception as exc:  # noqa: BLE001 — isolate per-ticker failures
                self.db.rollback()
                counts["failed"] += 1
                errors.append(self._issue(decision.ticker, decision.trading_date, REASON_ERROR, str(exc)))
                logger.exception("Alert evaluation failed for %s", decision.ticker)
                continue

            if action == ACTION_CREATE:
                counts["created"] += 1
            elif action == ACTION_UPDATE:
                counts["updated"] += 1
            elif action == ACTION_TRANSITION:
                counts["transitioned"] += 1
                counts["resolved"] += 1
            elif action == ACTION_UNCHANGED:
                counts["unchanged"] += 1
            else:  # outdated
                counts["skipped"] += 1
                errors.append(self._issue(decision.ticker, decision.trading_date, REASON_OUTDATED,
                                          "Active alert already reflects a newer trading date."))

        if counts["evaluated"] and counts["failed"] == counts["evaluated"]:
            status = "failed"
        elif any(e["reason"] in _ERROR_REASONS for e in errors):
            status = "partial_success"
        else:
            status = "success"

        logger.info("Alert evaluation complete: status=%s %s", status, counts)
        return self._result(status, counts, errors)

    def get_alerts(
        self,
        *,
        ticker: Optional[str] = None,
        status: Optional[str] = None,
        alert_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Alert]:
        """Read-only. Newest first (first_detected_at DESC, id DESC)."""
        if limit < 1 or limit > 200:
            raise ValueError(f"limit must be between 1 and 200, got {limit}.")
        if offset < 0:
            raise ValueError(f"offset must be >= 0, got {offset}.")
        if status is not None and status not in ALERT_STATUSES:
            raise ValueError(f"status must be one of {ALERT_STATUSES}.")
        if alert_type is not None and alert_type not in ALERT_TYPES:
            raise ValueError(f"alert_type must be one of {ALERT_TYPES}.")

        stmt = select(Alert)
        symbol = (ticker or "").strip().upper()
        if symbol:
            stmt = stmt.where(Alert.ticker == symbol)
        if status:
            stmt = stmt.where(Alert.status == status)
        if alert_type:
            stmt = stmt.where(Alert.alert_type == alert_type)

        stmt = stmt.order_by(Alert.first_detected_at.desc(), Alert.id.desc()).offset(offset).limit(limit)
        return list(self.db.execute(stmt).scalars().all())

    # ==================================================================
    # READS
    # ==================================================================

    def _load_latest_signals(self, ticker: Optional[str]) -> list[AnalysisResult]:
        """Latest row per ticker in ONE query. (ticker, trading_date) is unique => no ties."""
        latest = select(
            AnalysisResult.ticker.label("ticker"),
            func.max(AnalysisResult.trading_date).label("max_date"),
        ).group_by(AnalysisResult.ticker)
        if ticker:
            latest = latest.where(AnalysisResult.ticker == ticker)
        latest = latest.subquery()

        stmt = (
            select(AnalysisResult)
            .join(latest, and_(
                AnalysisResult.ticker == latest.c.ticker,
                AnalysisResult.trading_date == latest.c.max_date,
            ))
            .order_by(AnalysisResult.ticker.asc(), AnalysisResult.id.asc())
        )
        return list(self.db.execute(stmt).scalars().all())

    def _reference_session(self) -> Optional[date]:
        """Market's latest observed session: max(daily_features), else max(analysis_results)."""
        ref = self.db.execute(select(func.max(DailyMarketFeatures.trading_date))).scalar_one_or_none()
        if ref is None:
            ref = self.db.execute(select(func.max(AnalysisResult.trading_date))).scalar_one_or_none()
        return _as_date(ref) if ref is not None else None

    def _load_active_snapshots(self, tickers: list[str]) -> dict[str, _Snapshot]:
        if not tickers:
            return {}
        rows = self.db.execute(
            select(Alert).where(Alert.status == STATUS_ACTIVE, Alert.ticker.in_(set(tickers)))
        ).scalars().all()
        return {row.ticker: _Snapshot(row) for row in rows}

    def _load_active_locked(self, ticker: str) -> Optional[Alert]:
        """Current-read of the ticker's active alert with a row lock (MySQL FOR UPDATE)."""
        return self.db.execute(
            select(Alert)
            .where(Alert.ticker == ticker, Alert.status == STATUS_ACTIVE)
            .with_for_update()
        ).scalars().first()

    # ==================================================================
    # WRITE PATH
    # ==================================================================

    def _process(self, decision: AlertDecision, prefetched: Optional[_Snapshot]) -> str:
        """
        Cheap pre-check on the snapshot (no lock). Only when a write looks
        necessary: lock, re-plan on fresh data, apply, commit. One retry on
        IntegrityError (lost a race on UNIQUE(active_ticker)).
        """
        action = plan_action(decision, prefetched)
        if action in (ACTION_UNCHANGED, ACTION_OUTDATED):
            return action

        for attempt in (1, 2):
            try:
                active = self._load_active_locked(decision.ticker)
                action = plan_action(decision, active)
                if action in (ACTION_UNCHANGED, ACTION_OUTDATED):
                    self.db.rollback()  # release the lock; nothing to write
                    return action

                self._apply(action, decision, active)
                self.db.commit()
                logger.info("Alert %s: %s %s (%s)", action, decision.ticker,
                            decision.alert_type, decision.severity)
                return action
            except IntegrityError:
                self.db.rollback()
                if attempt == 2:
                    raise
                logger.warning("Alert race on %s; retrying once.", decision.ticker)
        raise RuntimeError("unreachable")  # pragma: no cover

    def _apply(self, action: str, decision: AlertDecision, active: Optional[Alert]) -> None:
        now = datetime.now(NAIROBI_TZ)

        if action == ACTION_CREATE:
            self.db.add(self._new_alert(decision, now))

        elif action == ACTION_UPDATE:
            assert active is not None
            active.trading_date = decision.trading_date
            active.severity = decision.severity
            active.signal_strength = decision.signal_strength
            active.confidence_score = decision.confidence_score
            active.message = decision.message
            active.evidence = decision.evidence
            active.last_detected_at = now

        elif action == ACTION_TRANSITION:
            assert active is not None
            active.status = STATUS_RESOLVED
            active.resolved_at = now
            active.active_ticker = None  # frees the unique slot BEFORE the insert
            self.db.flush()
            self.db.add(self._new_alert(decision, now))

        self.db.flush()

    @staticmethod
    def _new_alert(decision: AlertDecision, now: datetime) -> Alert:
        return Alert(
            ticker=decision.ticker,
            active_ticker=decision.ticker,
            alert_type=decision.alert_type,
            severity=decision.severity,
            message=decision.message,
            trading_date=decision.trading_date,
            signal_strength=decision.signal_strength,
            confidence_score=decision.confidence_score,
            status=STATUS_ACTIVE,
            first_detected_at=now,
            last_detected_at=now,
            resolved_at=None,
            evidence=decision.evidence,
            timestamp=now,
        )

    # ==================================================================
    # RESPONSE BUILDERS
    # ==================================================================

    @staticmethod
    def _new_counts() -> dict[str, int]:
        return {
            "evaluated": 0, "created": 0, "updated": 0, "transitioned": 0,
            "resolved": 0, "unchanged": 0, "skipped": 0, "failed": 0,
        }

    @staticmethod
    def _issue(ticker: Optional[str], trading_date: Any, reason: str, detail: str) -> dict[str, Any]:
        return {
            "ticker": ticker,
            "trading_date": trading_date.isoformat() if isinstance(trading_date, date) else None,
            "reason": reason,
            "detail": detail,
        }

    @staticmethod
    def _result(status: str, counts: dict[str, int], errors: list[dict[str, Any]]) -> dict[str, Any]:
        return {"status": status, **counts, "errors": errors}