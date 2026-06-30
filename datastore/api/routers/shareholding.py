"""
datastore/api/routers/shareholding.py

Phase: 2.1 (Fundamental Data Ingestion + PIT Validation)
Specs: SPEC-DS-001, SPEC-DS-002, SPEC-DS-003, SPEC-PIPE-003 (CRITICAL)
Owner: Platform / DataStore
Consumers: ingestion/scrapers/screener.py, features/governance.py

GET /api/v1/shareholding/{ticker}?start_date=&end_date=&as_of= and
POST /api/v1/shareholding/write — against the `shareholding` DuckDB table
(Store 2, datastore/schema/create_normalised.py). One row per
(ticker, quarter_end_date).

SPEC-PIPE-003 (CRITICAL): PIT key is filing_date, NEVER quarter_end_date —
BSE shareholding filings are due ~21 calendar days after quarter-end
(config.settings.SHAREHOLDING_FILING_DELAY_DAYS). The GET here enforces
this via datastore/api/pit.py's enforce_pit_shareholding.

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
from datastore.api.schemas import ShareholdingResponse, ShareholdingRow, ShareholdingWrite, ShareholdingWriteResult

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/shareholding", tags=["Shareholding"])

_COLUMNS = [
    "ticker", "quarter_end_date", "filing_date",
    "promoter_pct", "promoter_pledge", "fii_pct", "dii_pct", "mf_pct", "retail_pct",
    "superstar_flag", "superstar_change",
]
_SELECT_COLS = ", ".join(_COLUMNS)


@router.get("/{ticker}", response_model=ShareholdingResponse)
async def get_shareholding(
    ticker: str,
    start_date: datetime = Query(..., description="quarter_end_date range start (inclusive)"),
    end_date: datetime = Query(..., description="quarter_end_date range end (inclusive)"),
    as_of: Optional[datetime] = Query(
        None, description="PIT reference (default: end_date); only rows with filing_date <= as_of are returned"
    ),
) -> ShareholdingResponse:
    """
    Query shareholding pattern for a ticker, PIT-filtered by filing_date.

    SPEC-PIPE-003 (CRITICAL): quarter_end_date bounds which quarters to
    consider; filing_date is the actual PIT gate (never quarter_end_date).
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    pit_reference = as_of or end_date

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"""
            SELECT {_SELECT_COLS} FROM shareholding
            WHERE ticker = ? AND quarter_end_date >= ? AND quarter_end_date <= ?
            """,
            [ticker, start_date.date(), end_date.date()],
        ).fetchall()

    df = pd.DataFrame(rows, columns=_COLUMNS)
    if not df.empty:
        df["filing_date"] = pd.to_datetime(df["filing_date"])
        df = enforce_pit_shareholding(df, as_of=pit_reference, filing_date_col="filing_date")

    data = [ShareholdingRow(**row) for row in df.to_dict(orient="records")]
    return ShareholdingResponse(ticker=ticker, as_of=pit_reference, data=data, record_count=len(data))


@router.post("/write", response_model=ShareholdingWriteResult)
async def write_shareholding(record: ShareholdingWrite) -> ShareholdingWriteResult:
    """
    Upsert one quarterly shareholding row — SPEC-DS-004:
    same (ticker, quarter_end_date) replaces, never duplicates.

    Raises
    ------
    HTTPException 400
        If filing_date <= quarter_end_date (SPEC-PIPE-003: a build
        failure — a filing cannot predate the quarter it discloses).
    """
    if record.filing_date.date() <= record.quarter_end_date.date():
        raise HTTPException(
            status_code=400,
            detail="SPEC-PIPE-003 violation: filing_date must be after quarter_end_date",
        )

    values = [getattr(record, col) if col not in ("quarter_end_date", "filing_date")
              else getattr(record, col).date() for col in _COLUMNS]
    placeholders = ", ".join("?" for _ in _COLUMNS)
    update_cols = [c for c in _COLUMNS if c not in ("ticker", "quarter_end_date")]
    # [AS BUILT, P2.6] COALESCE, not a blind overwrite — same reasoning as
    # fundamentals.py's write_fundamentals: shareholding now has two independent
    # writers per (ticker, quarter_end_date) row (screener.py's promoter/FII/DII/
    # etc. fields, and trendlyne.py's superstar_flag/superstar_change only,
    # everything else NULL in its own ShareholdingWrite payload). A NULL in the
    # incoming payload must not clobber an existing stored value from the other
    # writer; a real value still always overwrites.
    update_clause = ", ".join(f"{c} = COALESCE(excluded.{c}, shareholding.{c})" for c in update_cols)

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        conn.execute(
            f"""
            INSERT INTO shareholding ({_SELECT_COLS}) VALUES ({placeholders})
            ON CONFLICT (ticker, quarter_end_date) DO UPDATE SET {update_clause}
            """,
            values,
        )

    logger.info(f"shareholding.write: {record.ticker} {record.quarter_end_date.date()}")
    return ShareholdingWriteResult(
        ticker=record.ticker, quarter_end_date=record.quarter_end_date, written=True
    )
