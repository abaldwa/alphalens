"""
datastore/api/routers/system.py

Phase: 1.7 (DataStore API Full + Daily Pipeline + Dashboard)
Specs: SPEC-DS-002, SPEC-PIPE-001, SPEC-SCHED-005, SPEC-PIPE-005, SPEC-SYS-002, SPEC-SCHED-013
Owner: Platform / DataStore
Consumers: dashboard/screens/daily_dashboard.py, ingestion/scheduler

GET /health — pipeline status, last run, stock count, drift status,
scheduler heartbeats.
[AS BUILT] Supersedes the bare `/health` route that lived directly in
datastore/api/main.py since Phase 0.1 (SPEC-PIPE-001, SPEC-SCHED-005) —
same path and same pipeline_runs-reading behavior, now under routers/
per this prompt's "implement all Phase 1 API endpoints" reorganization,
extended with stock_count (distinct tickers in the latest ohlcv_adjusted
date), drift (pipeline_drift_log, written by daily_inference.py's PSI
check), and scheduler heartbeats (SPEC-SCHED-013).

[AS BUILT, SPEC-SCHED-013] `scheduler` was added after a real, multi-day-
running scheduler process's recurring jobs silently stopped firing
entirely, with nothing anywhere recording that fact — discovering it
required reading the scheduler process's own log file by hand via /proc.
This field makes "has each recurring job actually fired recently"
directly observable through the API instead.
"""

import logging

from fastapi import APIRouter

from config.settings import DUCKDB_PATH, PIPELINE_LOG_DB_PATH
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection, get_sqlite_connection
from datastore.api.schemas import DriftStatus, SystemHealthResponse
from datastore.api.utils.scheduler_status import get_scheduler_heartbeats

logger = logging.getLogger(__name__)

router = APIRouter(tags=["System"])

API_VERSION = "1.0"


def _last_pipeline_run() -> dict:
    try:
        with get_sqlite_connection(PIPELINE_LOG_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT run_id, date, status, stocks_processed, "
                "started_at, completed_at, error_message "
                "FROM pipeline_runs ORDER BY run_id DESC LIMIT 1"
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return {
                "run_id": row[0], "date": row[1], "status": row[2], "stocks_processed": row[3],
                "started_at": row[4], "completed_at": row[5], "error_message": row[6],
            }
    except Exception as exc:
        logger.warning(f"health: could not fetch last pipeline run: {exc}")
        return None


def _stock_count() -> int:
    try:
        # persist=False — DUCKDB_PATH is also written by the ingestion
        # scheduler; see datastore/api/db.py's module docstring (SPEC-SCHED-013).
        with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
            row = conn.execute(
                "SELECT COUNT(DISTINCT ticker) FROM ohlcv_adjusted "
                "WHERE date = (SELECT MAX(date) FROM ohlcv_adjusted)"
            ).fetchone()
            return int(row[0]) if row and row[0] is not None else 0
    except Exception as exc:
        logger.warning(f"health: could not compute stock_count: {exc}")
        return 0


def _drift_status() -> DriftStatus:
    try:
        with get_sqlite_connection(PIPELINE_LOG_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT date, worst_feature, worst_psi, worst_status "
                "FROM pipeline_drift_log ORDER BY date DESC LIMIT 1"
            )
            row = cursor.fetchone()
            if row is None:
                return DriftStatus()
            return DriftStatus(date=row[0], worst_feature=row[1], worst_psi=row[2], worst_status=row[3])
    except Exception as exc:
        logger.warning(f"health: could not fetch drift status: {exc}")
        return DriftStatus()


@router.get("/health", response_model=SystemHealthResponse)
async def health_check() -> SystemHealthResponse:
    """SPEC-PIPE-001 liveness + SPEC-SCHED-005 pipeline status + stock count + drift status + scheduler heartbeats."""
    return SystemHealthResponse(
        status="healthy",
        timestamp=now_ist(),
        version=API_VERSION,
        last_pipeline_run=_last_pipeline_run(),
        stock_count=_stock_count(),
        drift=_drift_status(),
        scheduler=get_scheduler_heartbeats(),
    )
