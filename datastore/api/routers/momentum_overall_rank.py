"""
datastore/api/routers/momentum_overall_rank.py

Read-only API over the overall (all-800-stock) momentum rank — NOT a new
computation. momentum_framework/common/momentum_rank_cache.py's own
docstring establishes that momentum_return is computed ONCE per (date,
lookback) over band_id=13 (M13, the full ADTV-liquid universe superset),
then SLICED into the other bands (M02/M04/M07/M09/M10/M12) — so
band_id=13's rows in momentum_rank_snapshots ALREADY ARE the overall
momentum rank across all ~800 stocks. This router just exposes that slice
through a typed endpoint instead of every caller needing to know the
cache DB's path/schema/band-13-means-overall convention directly.

Cache DB: momentum_framework/cache/universe_cache.duckdb — a DIFFERENT
file from config.settings.BACKTEST_DUCKDB_PATH/normalised DB, built by
momentum_framework/scripts/build_universe_cache.py (see that script for
how to refresh/extend the cached date range).

New router (own prefix), same "wrap, don't refactor" rationale as every
other router added this session.
"""

from pathlib import Path
from typing import List, Optional

import duckdb
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter(prefix="/api/v1/momentum-rank", tags=["Momentum Rank"])

CACHE_DB_PATH = Path(__file__).resolve().parents[3] / "momentum_framework" / "cache" / "universe_cache.duckdb"
OVERALL_BAND_ID = 13  # M13 — the full ADTV-liquid universe; see module docstring
VALID_LOOKBACKS = {1, 3, 6, 9, 12}


class OverallRankRow(BaseModel):
    ticker: str
    momentum_return: float
    rank: int


class OverallRankResponse(BaseModel):
    as_of_date: str
    lookback_months: int
    total_ranked: int
    rows: List[OverallRankRow]


@router.get("/overall", response_model=OverallRankResponse)
async def get_overall_momentum_rank(
    as_of_date: str = Query(..., description="YYYY-MM-DD, must be a real rebalance/cached date"),
    lookback_months: int = Query(6, description="1, 3, 6, 9, or 12"),
    top_n: Optional[int] = Query(None, description="Limit to the top N ranked tickers; omit for all ~800"),
) -> OverallRankResponse:
    if lookback_months not in VALID_LOOKBACKS:
        raise HTTPException(status_code=400, detail=f"lookback_months must be one of {sorted(VALID_LOOKBACKS)}")
    if not CACHE_DB_PATH.exists():
        raise HTTPException(status_code=503, detail=f"Momentum rank cache not found at {CACHE_DB_PATH} — run build_universe_cache.py first")

    limit_clause = "LIMIT ?" if top_n else ""
    params = [OVERALL_BAND_ID, lookback_months, as_of_date] + ([top_n] if top_n else [])

    conn = duckdb.connect(str(CACHE_DB_PATH), read_only=True)
    try:
        count_row = conn.execute(
            "SELECT COUNT(*) FROM momentum_rank_snapshots WHERE band_id = ? AND lookback_months = ? AND as_of_date = ?",
            [OVERALL_BAND_ID, lookback_months, as_of_date],
        ).fetchone()
        total = count_row[0] if count_row else 0
        if total == 0:
            raise HTTPException(
                status_code=404,
                detail=f"No cached overall momentum rank for {as_of_date} (lookback={lookback_months}mo) — "
                       f"date may be outside the cached range or not a rebalance grid point",
            )
        rows = conn.execute(
            f"""
            SELECT ticker, momentum_return, rank
            FROM momentum_rank_snapshots
            WHERE band_id = ? AND lookback_months = ? AND as_of_date = ?
            ORDER BY rank
            {limit_clause}
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    return OverallRankResponse(
        as_of_date=as_of_date,
        lookback_months=lookback_months,
        total_ranked=total,
        rows=[OverallRankRow(ticker=t, momentum_return=r, rank=k) for t, r, k in rows],
    )
