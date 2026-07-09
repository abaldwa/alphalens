"""
datastore/api/routers/pipeline.py

Phase: 3.x (Backlog item #6 refactor)
Specs: SPEC-PIPE-001, SPEC-SYS-002
Owner: Platform / DataStore
Consumers: dashboard, ingestion/scheduler

GET /api/v1/pipeline/status/{date} — per-date daily pipeline execution
status.

[AS BUILT, item #6] Moved out of datastore/api/main.py (previously the
last inline route left over from before P1.7's router-file reorganization
— see main.py's module docstring) into its own router file, same path,
same tags, same behavior, wired into main.py the same way as every other
router. Pure refactor — still reads PIPELINE_LOG_DB_PATH's pipeline_runs/
pipeline_checkpoints tables the same way routers/system.py's /health does
for the latest run; this endpoint adds per-date and per-step detail.
"""

import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, Path

from config.settings import PIPELINE_LOG_DB_PATH
from datastore.api import schemas
from datastore.api.db import get_sqlite_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/pipeline", tags=["Pipeline"])


@router.get("/status/{date}", response_model=schemas.PipelineStatus)
async def get_pipeline_status(
    date: datetime = Path(..., description="Pipeline date (YYYY-MM-DD)"),
) -> schemas.PipelineStatus:
    """
    Query daily pipeline execution status.

    SPEC-PIPE-001, SPEC-SYS-002: Returns overall pipeline health (ingestion,
    feature engineering, inference, output generation).

    Args:
        date: Date of pipeline run

    Returns:
        PipelineStatus with run summary

    Raises:
        HTTPException 404: If no pipeline run on this date
    """
    # ingestion/scheduler/checkpoint.py writes one pipeline_runs row per day
    # and one pipeline_checkpoints row per pipeline step that day, both into
    # PIPELINE_LOG_DB_PATH — same store routers/system.py's /health reads
    # for the latest run; this endpoint adds per-date and per-step detail.
    date_str = date.strftime("%Y-%m-%d")
    with get_sqlite_connection(PIPELINE_LOG_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status, stocks_processed, started_at, completed_at, error_message "
            "FROM pipeline_runs WHERE date = ? ORDER BY run_id DESC LIMIT 1",
            (date_str,),
        )
        run_row = cursor.fetchone()
        if run_row is None:
            raise HTTPException(status_code=404, detail=f"No pipeline run found for {date_str}")
        status, stocks_processed, started_at, completed_at, error_message = run_row

        cursor.execute(
            "SELECT step_name FROM pipeline_checkpoints WHERE date = ? ORDER BY step_index DESC LIMIT 1",
            (date_str,),
        )
        step_row = cursor.fetchone()
        stage = step_row[0] if step_row else "unknown"

        cursor.execute(
            "SELECT COUNT(*), SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) "
            "FROM pipeline_checkpoints WHERE date = ?",
            (date_str,),
        )
        total_steps, completed_steps, failed_steps = cursor.fetchone()
        completed_steps = completed_steps or 0
        failed_steps = failed_steps or 0
        completeness_pct = (completed_steps / total_steps * 100.0) if total_steps else 0.0

    duration_seconds = None
    if started_at and completed_at:
        duration_seconds = (
            datetime.fromisoformat(completed_at) - datetime.fromisoformat(started_at)
        ).total_seconds()

    return schemas.PipelineStatus(
        date=date,
        status=status,
        stage=stage,
        records_processed=stocks_processed or 0,
        records_skipped=0,
        records_failed=failed_steps,
        data_completeness_pct=completeness_pct,
        started_at=started_at,
        completed_at=completed_at,
        duration_seconds=duration_seconds,
        error_summary=error_message,
        notes=None,
    )
