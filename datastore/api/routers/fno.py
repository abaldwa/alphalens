"""
datastore/api/routers/fno.py

Phase: 2.3 (F&O Features + Signal63D + Full Phase 2 Feature Matrix)
Specs: SPEC-DS-001, SPEC-DS-002, SPEC-PIPE-001
Owner: Platform / DataStore
Consumers: features/fno_features.py

GET /api/v1/fno/{ticker}?from=&to= — every F&O contract row (futures and
options, all expiries/strikes) for `ticker` over [from, to]. Read-only:
ingestion/scheduler/daily_pipeline.py's step_download_fno already writes
fno_data directly (SPEC-PIPE-001 ingestion-writes-DataStore-directly
precedent), same as corporate_actions.

No separate "is F&O eligible" endpoint: a ticker with zero rows in the
requested window IS the eligibility signal (features/fno_features.py
checks for an empty response, same as it would check any other real
absence-of-data case) — avoids maintaining a second, potentially stale
eligibility list alongside the one source of truth this table already is.

[AS BUILT] persist=False on every connection — same SPEC-SCHED-013
rationale as every other router here (datastore/api/db.py's module
docstring): this API process and the ingestion scheduler share DUCKDB_PATH.
"""

import logging
from datetime import date as date_type

from fastapi import APIRouter, HTTPException, Query

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.schemas import FNOResponse, FNORow

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/fno", tags=["F&O"])


@router.get("/{ticker}", response_model=FNOResponse)
async def get_fno_chain(
    ticker: str,
    from_date: date_type = Query(..., alias="from", description="Inclusive start date (YYYY-MM-DD)"),
    to_date: date_type = Query(..., alias="to", description="Inclusive end date (YYYY-MM-DD)"),
) -> FNOResponse:
    """
    Query every F&O contract row for a ticker over [from, to].

    Raises
    ------
    HTTPException 400
        If ticker is empty or from > to.
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    if from_date > to_date:
        raise HTTPException(status_code=400, detail="from must be <= to")

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            """
            SELECT trade_date, ticker, instrument, expiry, strike, option_type,
                   oi, oi_change, volume, settle_price, close_price, underlying_price
            FROM fno_data
            WHERE ticker = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date ASC, expiry ASC, strike ASC
            """,
            [ticker, from_date, to_date],
        ).fetchall()

    data = [
        FNORow(
            trade_date=row[0], ticker=row[1], instrument=row[2], expiry=row[3],
            strike=row[4], option_type=row[5], oi=row[6], oi_change=row[7],
            volume=row[8], settle_price=row[9], close_price=row[10], underlying_price=row[11],
        )
        for row in rows
    ]

    return FNOResponse(
        ticker=ticker, start_date=from_date, end_date=to_date, data=data, record_count=len(data)
    )
