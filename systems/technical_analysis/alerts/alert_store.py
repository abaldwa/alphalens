"""
systems/technical_analysis/alerts/alert_store.py

Phase: 3.x (Technical Analysis Alerts)
Specs: SPEC-TA-009
Owner: Technical Analysis / Alerts
Consumers: datastore/api/routers/ta_alerts.py, ingestion/scheduler/daily_pipeline.py

User-defined alerts: a user picks a (ticker, template_name) pair to watch.
This module owns the `ta_alerts` (definitions) and `ta_alert_triggers`
(append-only history of days the condition was fully true) tables in
SIGNALS_DUCKDB_PATH, and the state-change logic that turns
`systems/technical_analysis/alerts/daily_alert_checker.py`'s daily
`ta_signals` full-match snapshot into "this alert just turned on today"
events — as opposed to `ta_signals`/`DailyAlertChecker`, which re-persists
every full match every day with no notion of a user's watchlist or of
whether a match is new vs. still-ongoing.

Reuses systems/technical_analysis/screener/engine.py's condition-evaluation
engine indirectly, via ta_signals (already the daily output of that engine
for all 42 templates) — no duplicate evaluation logic here.
"""

import logging
from dataclasses import dataclass
from datetime import date as date_type
from typing import List, Optional

import pandas as pd

from config.settings import SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from systems.technical_analysis.screener.templates import TEMPLATE_MAP

logger = logging.getLogger(__name__)

_CREATE_TA_ALERTS_SQL = """
CREATE TABLE IF NOT EXISTS ta_alerts (
    alert_id INTEGER PRIMARY KEY,
    ticker VARCHAR NOT NULL,
    template_name VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    last_triggered_date DATE
)
"""

_CREATE_TA_ALERT_TRIGGERS_SQL = """
CREATE TABLE IF NOT EXISTS ta_alert_triggers (
    alert_id INTEGER NOT NULL,
    date DATE NOT NULL,
    PRIMARY KEY (alert_id, date)
)
"""

# DuckDB has no AUTOINCREMENT — sequence-backed alert_id, same idiom as
# other DuckDB tables in this codebase that need a surrogate key.
_CREATE_TA_ALERT_ID_SEQ_SQL = "CREATE SEQUENCE IF NOT EXISTS ta_alert_id_seq START 1"


@dataclass
class AlertDefinition:
    """One row from ta_alerts, optionally enriched with trigger state.

    Parameters
    ----------
    alert_id : int
    ticker : str
    template_name : str
    category : str
        Looked up from TEMPLATE_MAP, not stored redundantly in ta_alerts.
    active : bool
    last_triggered_date : str or None
        ISO date string of the most recent day this alert's condition
        was fully true, or None if it has never triggered.
    triggered_today : bool
        True if last_triggered_date equals the most recent ta_signals date.
    """

    alert_id: int
    ticker: str
    template_name: str
    category: str
    active: bool
    last_triggered_date: Optional[str]
    triggered_today: bool


def _ensure_tables(conn) -> None:
    """Create ta_alerts/ta_alert_triggers (+ id sequence) if missing.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection

    Spec References
    ----------------
    SPEC-TA-009: ta_alerts/ta_alert_triggers schema
    """
    conn.execute(_CREATE_TA_ALERT_ID_SEQ_SQL)
    conn.execute(_CREATE_TA_ALERTS_SQL)
    conn.execute(_CREATE_TA_ALERT_TRIGGERS_SQL)


def create_alert(ticker: str, template_name: str) -> int:
    """Create a new user-defined alert watching (ticker, template_name).

    Parameters
    ----------
    ticker : str
        NSE ticker symbol; normalised to upper case.
    template_name : str
        Must be a key in systems.technical_analysis.screener.templates.TEMPLATE_MAP.

    Returns
    -------
    int
        The new alert's alert_id.

    Spec References
    ----------------
    SPEC-TA-009: POST /api/v1/ta/user-alerts

    Raises
    ------
    ValueError
        If template_name is not a known template.
    """
    if template_name not in TEMPLATE_MAP:
        raise ValueError(f"Unknown template_name '{template_name}'")

    ticker_upper = ticker.upper()
    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False) as conn:
        _ensure_tables(conn)
        row = conn.execute(
            """
            INSERT INTO ta_alerts (alert_id, ticker, template_name, created_at, active)
            VALUES (nextval('ta_alert_id_seq'), ?, ?, CURRENT_TIMESTAMP, TRUE)
            RETURNING alert_id
            """,
            [ticker_upper, template_name],
        ).fetchone()
    logger.info("create_alert: %s / %s -> alert_id=%d", ticker_upper, template_name, row[0])
    return int(row[0])


def delete_alert(alert_id: int) -> bool:
    """Deactivate (soft-delete) an alert.

    Parameters
    ----------
    alert_id : int

    Returns
    -------
    bool
        True if a row was updated, False if alert_id doesn't exist.

    Spec References
    ----------------
    SPEC-TA-009: DELETE /api/v1/ta/user-alerts/{alert_id}
    """
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False) as conn:
        _ensure_tables(conn)
        existing = conn.execute(
            "SELECT 1 FROM ta_alerts WHERE alert_id = ? AND active = TRUE", [alert_id]
        ).fetchone()
        if existing is None:
            return False
        conn.execute(
            "UPDATE ta_alerts SET active = FALSE WHERE alert_id = ?",
            [alert_id],
        )
    return True


def list_alerts(active_only: bool = True) -> List[AlertDefinition]:
    """List user-defined alerts, enriched with trigger state.

    Parameters
    ----------
    active_only : bool, optional
        If True (default), only return non-deleted alerts.

    Returns
    -------
    list of AlertDefinition
        triggered_today reflects whether last_triggered_date equals the
        most recent date present in ta_signals (i.e. the latest date the
        pipeline evaluated), not necessarily today's calendar date.

    Spec References
    ----------------
    SPEC-TA-009: GET /api/v1/ta/user-alerts
    """
    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, read_only=True, persist=False) as conn:
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name IN ('ta_alerts', 'ta_signals')"
            ).fetchall()
        ]
        if "ta_alerts" not in tables:
            return []

        latest_date = None
        if "ta_signals" in tables:
            row = conn.execute("SELECT MAX(date) FROM ta_signals").fetchone()
            latest_date = str(row[0]) if row and row[0] is not None else None

        where = "WHERE active = TRUE" if active_only else ""
        df = conn.execute(
            f"""
            SELECT alert_id, ticker, template_name, active, last_triggered_date
            FROM ta_alerts
            {where}
            ORDER BY alert_id DESC
            """
        ).fetchdf()

    result = []
    for _, r in df.iterrows():
        last_triggered = None if pd.isna(r["last_triggered_date"]) else str(r["last_triggered_date"])[:10]
        result.append(
            AlertDefinition(
                alert_id=int(r["alert_id"]),
                ticker=str(r["ticker"]),
                template_name=str(r["template_name"]),
                category=TEMPLATE_MAP[r["template_name"]].category if r["template_name"] in TEMPLATE_MAP else "custom",
                active=bool(r["active"]),
                last_triggered_date=last_triggered,
                triggered_today=(latest_date is not None and last_triggered == latest_date),
            )
        )
    return result


def check_alerts(run_date: date_type) -> List[int]:
    """Check every active alert against that day's ta_signals full matches.

    For each active alert whose (ticker, template_name) has a full match
    in ta_signals on run_date: record a ta_alert_triggers row for
    (alert_id, run_date) if not already present, and update
    ta_alerts.last_triggered_date. An alert is "newly triggered" this run
    if it did not already have a trigger row for run_date before this call
    (idempotent re-runs of the same date report no new triggers).

    Parameters
    ----------
    run_date : date

    Returns
    -------
    list of int
        alert_ids that were newly triggered by this call.

    Spec References
    ----------------
    SPEC-TA-009: daily alert-trigger evaluation, called from
    ingestion/scheduler/daily_pipeline.py's check_ta_alerts step.

    PIT Assumptions
    ----------------
    None — reads ta_signals for run_date only, which is itself computed
    from that date's feature Parquet (no look-ahead).
    """
    date_str = run_date.isoformat()
    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False) as conn:
        _ensure_tables(conn)
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'ta_signals'"
            ).fetchall()
        ]
        if "ta_signals" not in tables:
            logger.info("check_alerts: ta_signals table not found yet — nothing to check")
            return []

        active = conn.execute(
            "SELECT alert_id, ticker, template_name FROM ta_alerts WHERE active = TRUE"
        ).fetchall()
        if not active:
            return []

        newly_triggered: List[int] = []
        for alert_id, ticker, template_name in active:
            matched = conn.execute(
                """
                SELECT 1 FROM ta_signals
                WHERE date = ? AND ticker = ? AND template_name = ? AND score >= 0.9999999
                """,
                [date_str, ticker, template_name],
            ).fetchone()
            if matched is None:
                continue

            already = conn.execute(
                "SELECT 1 FROM ta_alert_triggers WHERE alert_id = ? AND date = ?",
                [alert_id, date_str],
            ).fetchone()
            if already is None:
                conn.execute(
                    "INSERT INTO ta_alert_triggers (alert_id, date) VALUES (?, ?)",
                    [alert_id, date_str],
                )
                newly_triggered.append(alert_id)

            conn.execute(
                "UPDATE ta_alerts SET last_triggered_date = ? WHERE alert_id = ?",
                [date_str, alert_id],
            )

    logger.info(
        "check_alerts: %s — %d active alert(s), %d newly triggered",
        date_str, len(active), len(newly_triggered),
    )
    return newly_triggered
