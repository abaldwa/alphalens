"""
datastore/api/routers/alerts.py

Phase: 1.7 (DataStore API Full + Daily Pipeline + Dashboard)
Specs: SPEC-DS-002, SPEC-ALERT-001, SPEC-MODEL-006, SPEC-PIPE-005
Owner: Platform / DataStore
Consumers: dashboard/screens/daily_dashboard.py

GET /api/v1/alerts/today — synthesizes today's alerts from already-written
data rather than a separate alerts table: P&D blocks/flags and high exit
urgency from ml_signals (Store 4, DuckDB), drift halt/warning from
pipeline_drift_log (transactional, SQLite, written by
systems/ml_signal_engine/inference/daily_inference.py's PSI check).
"All today's alerts" (architecture doc) is a read-time join across these,
not a separate write path — there's no dedicated alerts table to keep in
sync, so an alert can never silently fall out of date with its source row.
"""

import logging
from datetime import datetime

from fastapi import APIRouter

from config.settings import EXIT_URGENT_THRESHOLD, PIPELINE_LOG_DB_PATH, SIGNALS_DUCKDB_PATH
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection, get_sqlite_connection
from datastore.api.schemas import AlertRow, AlertsResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/alerts", tags=["Alerts"])


@router.get("/today", response_model=AlertsResponse)
async def get_alerts_today() -> AlertsResponse:
    """All alerts for today's IST date: P&D blocks/flags, urgent exits, drift halts/warnings."""
    today_date = now_ist().date()
    today = datetime.combine(today_date, datetime.min.time())
    alerts = []

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        pnd_rows = conn.execute(
            "SELECT ticker, pnd_score, pnd_phase, pnd_block FROM ml_signals "
            "WHERE date = ? AND model_name = 'pnd_detector' AND pnd_score IS NOT NULL AND pnd_score > 40",
            [today_date],
        ).fetchall()
        for ticker, score, phase, blocked in pnd_rows:
            alerts.append(
                AlertRow(
                    date=today, ticker=ticker,
                    alert_type="pnd_block" if blocked else "pnd_flag",
                    severity="high" if blocked else "medium",
                    message=f"{ticker}: P&D score {score:.0f} ({phase}){' — BLOCKED' if blocked else ' — flagged'}",
                )
            )

        exit_rows = conn.execute(
            "SELECT ticker, exit_urgency, exit_type FROM ml_signals "
            "WHERE date = ? AND model_name = 'exit_signal' AND exit_urgency IS NOT NULL AND exit_urgency > ?",
            [today_date, EXIT_URGENT_THRESHOLD],
        ).fetchall()
        for ticker, urgency, exit_type in exit_rows:
            alerts.append(
                AlertRow(
                    date=today, ticker=ticker, alert_type="exit_urgent", severity="high",
                    message=f"{ticker}: exit urgency {urgency:.0f} ({exit_type}) — immediate exit recommended",
                )
            )

    try:
        with get_sqlite_connection(PIPELINE_LOG_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT worst_feature, worst_psi, worst_status FROM pipeline_drift_log WHERE date = ?",
                [today_date.isoformat()],
            )
            drift_row = cursor.fetchone()
            if drift_row is not None and drift_row[2] != "ok":
                feature, psi, status = drift_row
                alerts.append(
                    AlertRow(
                        date=today, ticker=None,
                        alert_type="drift_halt" if status == "halt" else "drift_warning",
                        severity="high" if status == "halt" else "medium",
                        message=f"Feature drift [{status}]: {feature} PSI={psi:.3f}",
                    )
                )
    except Exception as exc:
        logger.warning(f"alerts.today: could not read pipeline_drift_log ({exc})")

    return AlertsResponse(date=today, alerts=alerts, count=len(alerts))
