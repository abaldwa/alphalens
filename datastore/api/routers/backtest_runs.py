"""
datastore/api/routers/backtest_runs.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 3
(BacktestUmbrellaPlan.md at the repo root)
Owner: Platform / DataStore
Consumers: Phase 4's unified Backtest frontend page

Read/list API over the new backtest_runs DuckDB table (Store 6,
config.settings.BACKTEST_DUCKDB_PATH, datastore/schema/create_backtest.py)
— the unified run-record store every channel's Backtest/Walk-Forward run
writes into via backtest/core/run_store.py::save_run_result().

Deliberately a NEW router (prefix /api/v1/backtest, same base as the
existing backtest_reports.py router but disjoint sub-paths: /runs vs
/reports) rather than a modification of backtest_reports.py — that
existing router is a live-used read-only passthrough for the legacy
backtest/reports/*.json files backing the current /ml-backtest page, and
per the "wrap, don't refactor" principle applied throughout this
initiative, it is left untouched. The two routers coexist under the same
prefix; Phase 4's frontend cutover (not this phase) decides whether/how
to eventually retire the legacy one.

No write endpoints here: runs are written by backtest/core/run_store.py,
called from wherever a BacktestOrchestrator/WalkForwardRunner run is
kicked off (a script today; Phase 5/6's background job runner later) —
not from an API request. This keeps "who can trigger a potentially
expensive multi-year backtest" a deliberate, out-of-band decision rather
than an open HTTP endpoint.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backtest.core.run_store import get_run, get_run_lineage, list_runs
from backtest.core.feature_log import query_feature_log
from config.settings import BACKTEST_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/backtest", tags=["Backtest"])


class BacktestRunSummary(BaseModel):
    run_id: str
    parent_run_id: Optional[str] = None
    channel: str
    strategy_id: str
    horizon_bucket: str
    mode: str
    start_date: str
    end_date: str
    capital_mode: str
    initial_capital: float
    created_at: str
    metrics: Optional[dict] = None
    data_gaps: List[dict] = []
    integrity_passed: Optional[bool] = None
    live_eligible: bool = False


class BacktestRunListResponse(BaseModel):
    runs: List[BacktestRunSummary]


class BacktestRunLineageResponse(BaseModel):
    run_id: str
    lineage: List[BacktestRunSummary]


class FeatureLogRow(BaseModel):
    ticker: str
    as_of_date: str
    horizon_bucket: str
    feature_vector: dict
    signal_output: Optional[str] = None
    decision_taken: str


class FeatureLogResponse(BaseModel):
    run_id: str
    rows: List[FeatureLogRow]


def _summary(row: dict) -> BacktestRunSummary:
    return BacktestRunSummary(**{k: row[k] for k in BacktestRunSummary.model_fields if k in row})


@router.get("/runs", response_model=BacktestRunListResponse)
async def list_backtest_runs(
    channel: Optional[str] = Query(None, description="Filter: technical | fundamental | ml | momentum"),
    mode: Optional[str] = Query(None, description="Filter: backtest | walk_forward | paper"),
    strategy_id: Optional[str] = Query(None),
    limit: int = Query(100, le=500),
) -> BacktestRunListResponse:
    """List runs across all four channels, most recent first — the unified
    view Phase 4's frontend results table renders."""
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        rows = list_runs(conn, channel=channel, mode=mode, strategy_id=strategy_id, limit=limit)
    return BacktestRunListResponse(runs=[_summary(r) for r in rows])


@router.get("/runs/{run_id}", response_model=BacktestRunSummary)
async def get_backtest_run(run_id: str) -> BacktestRunSummary:
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        row = get_run(conn, run_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return _summary(row)


@router.get("/runs/{run_id}/lineage", response_model=BacktestRunLineageResponse)
async def get_backtest_run_lineage(run_id: str) -> BacktestRunLineageResponse:
    """Parent_run_id chain, oldest first — the feedback-loop 'compare to
    parent run' view (BacktestUmbrellaPlan.md's Feature-Vector Logging &
    Feedback Loop section)."""
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        chain = get_run_lineage(conn, run_id)
    if not chain:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return BacktestRunLineageResponse(run_id=run_id, lineage=[_summary(r) for r in chain])


@router.get("/runs/{run_id}/feature_log", response_model=FeatureLogResponse)
async def get_backtest_run_feature_log(run_id: str) -> FeatureLogResponse:
    """Every logged decision for a run — the feature-reengineering
    feedback loop's read side (backtest/core/feature_log.py::query_feature_log)."""
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        run_exists = get_run(conn, run_id) is not None
        if not run_exists:
            raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
        rows = query_feature_log(conn, run_id)
    for r in rows:
        r["as_of_date"] = str(r["as_of_date"])
    return FeatureLogResponse(run_id=run_id, rows=[FeatureLogRow(**r) for r in rows])
