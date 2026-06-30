"""
datastore/api/routers/governance.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-DS-001, SPEC-DS-002, SPEC-DS-003, SPEC-PIPE-003 (CRITICAL)
Owner: Platform / DataStore
Consumers: features/governance.py, dashboard, systems/ml_signal_engine

GET /api/v1/governance/{ticker}?start_date=&end_date=&as_of= — against the
`shareholding` DuckDB table (Store 2, datastore/schema/create_normalised.py).

[AS BUILT, P2.6] No standalone `governance` table exists. `shareholding`
IS this project's governance store — 12_platform_architecture.md (line
320) labels it literally: "/governance/  # Shareholding patterns (PIT via
filing_date)". This router exposes the SAME table datastore/api/routers/
shareholding.py already serves, under the build prompt's explicitly
requested /api/v1/governance/{ticker} path — same query + PIT logic
(enforce_pit_shareholding), now including the P2.6 superstar_flag/
superstar_change columns trendlyne.py writes. Kept as a thin, separate
router (rather than just an alias on shareholding.py) so the path name
the build prompt asked for actually exists and is independently
discoverable in the OpenAPI schema.

SPEC-PIPE-003 (CRITICAL): PIT key is filing_date, NEVER quarter_end_date.

[AS BUILT, SPEC-SCHED-013] persist=False on every connection — see
ohlcv.py's module docstring for the full incident this avoids.
"""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.pit import enforce_pit_shareholding
from datastore.api.schemas import GovernanceResponse, GovernanceRow

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/governance", tags=["Governance"])

_COLUMNS = [
    "ticker", "quarter_end_date", "filing_date",
    "promoter_pct", "promoter_pledge", "fii_pct", "dii_pct", "mf_pct", "retail_pct",
    "superstar_flag", "superstar_change",
]
_SELECT_COLS = ", ".join(_COLUMNS)


@router.get("/{ticker}", response_model=GovernanceResponse)
async def get_governance(
    ticker: str,
    start_date: Optional[datetime] = Query(None, description="quarter_end_date range start (inclusive)"),
    end_date: Optional[datetime] = Query(None, description="quarter_end_date range end (inclusive)"),
    as_of: Optional[datetime] = Query(
        None, description="PIT reference (default: end_date or now); only rows with filing_date <= as_of returned"
    ),
) -> GovernanceResponse:
    """
    Query governance (shareholding + superstar tracking) for a ticker,
    PIT-filtered by filing_date.

    SPEC-PIPE-003 (CRITICAL): quarter_end_date bounds which quarters to
    consider; filing_date is the actual PIT gate (never quarter_end_date).
    start_date/end_date default to a wide-open range (all history up to
    as_of) when omitted, since governance.py callers typically want
    "the latest known state as of as_of", not a specific window.
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    pit_reference = as_of or end_date or datetime.utcnow()
    range_start = start_date or datetime(1990, 1, 1)
    range_end = end_date or pit_reference
    if range_start > range_end:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"""
            SELECT {_SELECT_COLS} FROM shareholding
            WHERE ticker = ? AND quarter_end_date >= ? AND quarter_end_date <= ?
            """,
            [ticker, range_start.date(), range_end.date()],
        ).fetchall()

    df = pd.DataFrame(rows, columns=_COLUMNS)
    if not df.empty:
        df["filing_date"] = pd.to_datetime(df["filing_date"])
        df = enforce_pit_shareholding(df, as_of=pit_reference, filing_date_col="filing_date")

    data = [GovernanceRow(**row) for row in df.to_dict(orient="records")]
    return GovernanceResponse(ticker=ticker, as_of=pit_reference, data=data, record_count=len(data))
