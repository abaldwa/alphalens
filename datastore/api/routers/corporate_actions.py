"""
datastore/api/routers/corporate_actions.py

Phase: 2.2 (AMFI MF Holdings + Corporate Action Features)
Specs: SPEC-DS-001, SPEC-DS-002, SPEC-PIPE-002
Owner: Platform / DataStore
Consumers: features/corporate_action_features.py

GET /api/v1/corporate_actions/{ticker}?from=&to= — read-only. No write
endpoint here: ingestion/scrapers/bhavcopy.py and ingestion/adjust/
price_adjuster.py already write `corporate_actions` directly against
DUCKDB_PATH (established P0.4 precedent), this router only fills the
missing READ side so features/corporate_action_features.py can comply
with SPEC-DS-002 (features read via the API, never a direct DuckDB query)
without duplicating ingestion's existing write path.

[AS BUILT, SPEC-SCHED-013] persist=False — see ohlcv.py's module
docstring for the cross-process DUCKDB_PATH lock-conflict this avoids.
"""

import logging
from datetime import date as date_type
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.schemas import CorporateActionResponse, CorporateActionRow

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/corporate_actions", tags=["Corporate Actions"])

_COLUMNS = ["ticker", "ex_date", "action_type", "ratio", "announcement_date", "record_date"]
_SELECT_COLS = ", ".join(_COLUMNS)


@router.get("/{ticker}", response_model=CorporateActionResponse)
async def get_corporate_actions(
    ticker: str,
    from_date: Optional[date_type] = Query(None, alias="from", description="Inclusive ex_date range start"),
    to_date: Optional[date_type] = Query(None, alias="to", description="Inclusive ex_date range end"),
) -> CorporateActionResponse:
    """
    Query corporate actions for a ticker over an optional [from, to] ex_date window.

    No PIT filtering here — corporate actions are PITRule.NONE for the
    purpose of this endpoint (the same ex_date/ratio/announcement_date
    fields ingestion already writes); a caller needing PIT-correct
    "what was known as of X" semantics filters by announcement_date
    client-side (mirrors ohlcv.py's `as_of`-accepted-but-unused precedent).
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    if from_date and to_date and from_date > to_date:
        raise HTTPException(status_code=400, detail="from must be <= to")

    conditions = ["ticker = ?"]
    params = [ticker]
    if from_date:
        conditions.append("ex_date >= ?")
        params.append(from_date)
    if to_date:
        conditions.append("ex_date <= ?")
        params.append(to_date)

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"SELECT {_SELECT_COLS} FROM corporate_actions WHERE {' AND '.join(conditions)} ORDER BY ex_date ASC",
            params,
        ).fetchall()

    data = [CorporateActionRow(**dict(zip(_COLUMNS, row))) for row in rows]
    return CorporateActionResponse(ticker=ticker, data=data, record_count=len(data))
