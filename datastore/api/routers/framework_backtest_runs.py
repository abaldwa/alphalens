"""
datastore/api/routers/framework_backtest_runs.py

Read-only API over framework_backtest_runs (momentum_framework/results/
db_writer.py's table, same DB as backtest_runs -- config.settings.
BACKTEST_DUCKDB_PATH) -- the new native-engine campaign results table
(momentum_framework/scripts/run_full_campaign.py writes here), distinct
from the legacy backtest_runs table backtest_runs.py already serves.

DECOUPLING (explicit user instruction, 2026-09-04, ahead of a frontend
rewrite): config_json/metrics_json are raw JSON blobs on the DB row --
this router parses them server-side into a flat, typed response so the
frontend never has to know the blob's internal shape. If that shape
changes (a new config field, a renamed metric), only this router's
_row_to_summary() needs updating, not every page that reads this table.

New router (own prefix), not an addition to backtest_runs.py -- same
"wrap, don't refactor" rationale that file's own docstring already
states for its own existence relative to backtest_reports.py.
"""

import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from config.settings import BACKTEST_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection

router = APIRouter(prefix="/api/v1/framework-backtest", tags=["Framework Backtest"])


class FrameworkRunSummary(BaseModel):
    run_id: str
    strategy_id: str
    strategy_code: str
    band_id: int
    top_n: Optional[int] = None
    lookback_months: Optional[int] = None
    rebalance_cadence_days: Optional[int] = None
    position_sizing: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    cagr: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    trade_count: int
    run_executed_at: Optional[str] = None


class FrameworkRunListResponse(BaseModel):
    runs: List[FrameworkRunSummary]
    total: int


def _row_to_summary(row: Dict[str, Any]) -> FrameworkRunSummary:
    config = json.loads(row["config_json"]) if row.get("config_json") else {}
    metrics = json.loads(row["metrics_json"]) if row.get("metrics_json") else {}
    return FrameworkRunSummary(
        run_id=row["run_id"],
        strategy_id=row["strategy_id"],
        strategy_code=row["strategy_code"],
        band_id=row["band_id"],
        top_n=config.get("top_n"),
        lookback_months=config.get("lookback_months"),
        rebalance_cadence_days=config.get("rebalance_cadence_days"),
        position_sizing=config.get("position_sizing"),
        start_date=str(row["start_date"]) if row.get("start_date") else None,
        end_date=str(row["end_date"]) if row.get("end_date") else None,
        cagr=metrics.get("cagr"),
        sharpe_ratio=metrics.get("sharpe_ratio"),
        max_drawdown=metrics.get("max_drawdown"),
        trade_count=row["trade_count"],
        run_executed_at=str(row["run_executed_at"]) if row.get("run_executed_at") else None,
    )


@router.get("/runs", response_model=FrameworkRunListResponse)
async def list_framework_runs(
    strategy_code: Optional[str] = Query(None, description="Filter to one strategy code, e.g. R01"),
    band_id: Optional[int] = Query(None, description="Filter to one band_id"),
    limit: int = Query(500, le=5000),
    offset: int = Query(0, ge=0),
) -> FrameworkRunListResponse:
    where = []
    params: List[Any] = []
    if strategy_code:
        where.append("strategy_code = ?")
        params.append(strategy_code)
    if band_id is not None:
        where.append("band_id = ?")
        params.append(band_id)
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""

    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM framework_backtest_runs {where_clause}", params
        ).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT run_id, strategy_id, strategy_code, band_id, start_date, end_date,
                   config_json, metrics_json, trade_count, run_executed_at
            FROM framework_backtest_runs
            {where_clause}
            ORDER BY run_executed_at DESC
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchdf()

    runs = [_row_to_summary(row.to_dict()) for _, row in rows.iterrows()]
    return FrameworkRunListResponse(runs=runs, total=total)
