"""
datastore/api/routers/ohlcv.py

Phase: 1.7 (DataStore API Full + Daily Pipeline + Dashboard)
Specs: SPEC-DS-001, SPEC-DS-002, SPEC-DS-003
Owner: Platform / DataStore
Consumers: dashboard, systems/ml_signal_engine, backtest

GET /api/v1/ohlcv/{ticker}?from=&to=&adjusted=true — SPEC-DS-002's
literal Phase 1 OHLCV contract. Query param names are `from`/`to` per
the build prompt; aliased to `from_date`/`to_date` Python identifiers
since `from` is a reserved keyword.

[AS BUILT] `adjusted` is accepted for contract completeness but the
underlying ohlcv_adjusted table (ingestion/scheduler/daily_pipeline.py's
step_download_bhavcopy + ingestion/adjust/price_adjuster.py) only ever
stores the corporate-action-adjusted series — there is no separate raw/
unadjusted table to switch to yet, so `adjusted=false` is accepted but
behaves identically to `adjusted=true` (same as main.py's pre-existing
`as_of` parameter on this endpoint, which is accepted for PIT-contract
symmetry but doesn't filter rows since price data is PITRule.NONE).
"""

import logging
from datetime import date as date_type
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.schemas import OHLCVResponse, OHLCVRow, OHLCVUniverseResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ohlcv", tags=["OHLCV"])


# [AS BUILT] Registered before /{ticker} — a dynamic path would otherwise
# swallow "_meta" as a ticker value (same FastAPI route-ordering pitfall
# documented in datastore/api/routers/signals.py's top_buys/{ticker} fix).
@router.get("/_meta/tickers", response_model=OHLCVUniverseResponse)
async def get_ohlcv_tickers(
    min_rows: int = Query(0, ge=0, description="Only tickers with >= this many rows"),
) -> OHLCVUniverseResponse:
    """
    Distinct tickers present in ohlcv_adjusted, with row counts —
    "every ticker this DataStore has ever observed data for", broader than
    config.universe.get_tickers()'s current-investable-universe filter
    (tier/ADTV/market-cap thresholds). Lets a SPEC-DS-002-compliant
    consumer (e.g. backtest/run_phase1_backtest.py) distinguish "the
    current universe" from "everything historically seen" without a
    direct DuckDB query of its own — needed for
    BacktestIntegrityChecker's check_04_survivorship.
    """
    with get_duckdb_connection(DUCKDB_PATH, read_only=True) as conn:
        rows = conn.execute(
            "SELECT ticker, COUNT(*) AS n_rows FROM ohlcv_adjusted GROUP BY ticker HAVING COUNT(*) >= ? "
            "ORDER BY ticker",
            [min_rows],
        ).fetchall()
    return OHLCVUniverseResponse(tickers=[r[0] for r in rows], row_counts={r[0]: r[1] for r in rows})


@router.get("/{ticker}", response_model=OHLCVResponse)
async def get_ohlcv(
    ticker: str,
    from_date: date_type = Query(..., alias="from", description="Inclusive start date (YYYY-MM-DD)"),
    to_date: date_type = Query(..., alias="to", description="Inclusive end date (YYYY-MM-DD)"),
    adjusted: bool = Query(True, description="Accepted for contract completeness — see module docstring"),
) -> OHLCVResponse:
    """
    Query OHLCV for a ticker over [from, to].

    Raises
    ------
    HTTPException 400
        If ticker is empty or from > to.
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    if from_date > to_date:
        raise HTTPException(status_code=400, detail="from must be <= to")

    with get_duckdb_connection(DUCKDB_PATH, read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT date, ticker, open, high, low, close, volume, delivery_pct, adj_factor
            FROM ohlcv_adjusted
            WHERE ticker = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
            """,
            [ticker, from_date, to_date],
        ).fetchall()

    data = [
        OHLCVRow(
            date=row[0], ticker=row[1], open=row[2], high=row[3], low=row[4],
            close=row[5], volume=row[6], adjusted_close=row[5], delivery_pct=row[7], adj_factor=row[8],
        )
        for row in rows
    ]

    return OHLCVResponse(
        ticker=ticker, start_date=from_date, end_date=to_date, data=data, record_count=len(data)
    )


@router.get("/{ticker}/latest", response_model=Optional[OHLCVRow])
async def get_ohlcv_latest(ticker: str) -> Optional[OHLCVRow]:
    """Most recent OHLCV row for a ticker (architecture doc's `/ohlcv/{ticker}/latest`)."""
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")

    with get_duckdb_connection(DUCKDB_PATH, read_only=True) as conn:
        row = conn.execute(
            """
            SELECT date, ticker, open, high, low, close, volume, delivery_pct, adj_factor
            FROM ohlcv_adjusted WHERE ticker = ? ORDER BY date DESC LIMIT 1
            """,
            [ticker],
        ).fetchone()

    if row is None:
        return None
    return OHLCVRow(
        date=row[0], ticker=row[1], open=row[2], high=row[3], low=row[4],
        close=row[5], volume=row[6], adjusted_close=row[5], delivery_pct=row[7], adj_factor=row[8],
    )
