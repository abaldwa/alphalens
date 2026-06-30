"""
datastore/api/routers/fundamentals.py

Phase: 2.1 (Fundamental Data Ingestion + PIT Validation)
Specs: SPEC-DS-001, SPEC-DS-002, SPEC-DS-003, SPEC-PIPE-003 (CRITICAL)
Owner: Platform / DataStore
Consumers: ingestion/scrapers/screener.py, features/fundamental.py

GET /api/v1/fundamentals/{ticker}?start_date=&end_date=&as_of= and
POST /api/v1/fundamentals/write — against the `fundamentals` DuckDB table
(Store 2, datastore/schema/create_normalised.py). One row per
(ticker, fiscal_year, quarter).

[AS BUILT] Replaces main.py's P0.1 stub `GET /api/v1/fundamentals/{ticker}`
(which always returned an empty list — never wired to a real query) — same
"move inline stub into a real router" pattern as every other P1.7 router.

SPEC-PIPE-003 (CRITICAL): the GET here enforces PIT correctness via
datastore/api/pit.py's enforce_pit_fundamentals — only rows with
announcement_date <= as_of are returned, sorted ascending by
announcement_date. quarter_end_date is never used as a filter or sort key.

[AS BUILT, SPEC-SCHED-013] persist=False on every connection — DUCKDB_PATH
is also written by the ingestion scheduler from a separate long-lived
process; see ohlcv.py's module docstring for the full incident this avoids.
"""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.pit import enforce_pit_fundamentals
from datastore.api.schemas import FundamentalsResponse, FundamentalsRow, FundamentalsWrite, FundamentalsWriteResult

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/fundamentals", tags=["Fundamentals"])

_COLUMNS = [
    "ticker", "fiscal_year", "quarter", "quarter_end_date", "announcement_date",
    "revenue", "ebitda", "pat", "eps", "operating_margin", "ebitda_margin", "net_margin",
    "roe", "roce", "debt_to_equity", "interest_coverage", "fcf", "asset_turnover",
    "inventory_days", "receivable_days", "payable_days", "book_value_per_share", "shares_outstanding",
    "gross_profit", "capex", "current_assets", "current_liabilities", "total_debt", "cash_and_equivalents",
    "depreciation",
    "sector_specific_metric_1", "sector_specific_metric_2", "sector_specific_metric_3",
    "sector_specific_metric_4", "sector_specific_metric_5", "sector_specific_metric_6",
]
_SELECT_COLS = ", ".join(_COLUMNS)


# [AS BUILT, P2.6] MUST be registered before /{ticker}: FastAPI/Starlette
# matches routes by registration order, and the dynamic /{ticker} pattern
# would otherwise swallow "RELIANCE/history" as ticker="RELIANCE",
# {missing path segment} — same route-ordering discipline as signals.py's
# documented /ml/top_buys/{date}-before-/ml/{ticker}/{date} fix.
@router.get("/{ticker}/history", response_model=FundamentalsResponse)
async def get_fundamentals_history_by_quarters(
    ticker: str,
    quarters: int = Query(8, ge=1, le=80, description="Number of most recent quarters to return"),
    as_of: Optional[datetime] = Query(None, description="PIT reference (default: now)"),
) -> FundamentalsResponse:
    """
    Most recent `quarters` quarterly fundamentals rows for a ticker,
    PIT-filtered by announcement_date <= as_of, descending by
    quarter_end_date trimmed to `quarters` rows then re-sorted ascending
    by announcement_date (same ordering convention as GET /{ticker}).

    [AS BUILT, P2.6] Distinct from datastore/client.py's existing
    get_fundamentals_history(ticker, as_of, lookback_years) — that method
    calls GET /{ticker} with a YEAR-based lookback window; this build
    prompt's literal `?quarters=8` is a COUNT-based window instead (some
    tickers report irregularly / have gaps, where N years doesn't reliably
    mean N*4 quarters). Both are kept — neither supersedes the other.
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    pit_reference = as_of or datetime.utcnow()

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"""
            SELECT {_SELECT_COLS} FROM fundamentals
            WHERE ticker = ? AND announcement_date <= ?
            ORDER BY quarter_end_date DESC
            LIMIT ?
            """,
            [ticker, pit_reference.date(), quarters],
        ).fetchall()

    df = pd.DataFrame(rows, columns=_COLUMNS)
    if not df.empty:
        df["announcement_date"] = pd.to_datetime(df["announcement_date"])
        df = df.sort_values("announcement_date")
        df = df.astype(object).where(df.notna(), None)

    data = [FundamentalsRow(**row) for row in df.to_dict(orient="records")]
    return FundamentalsResponse(ticker=ticker, as_of=pit_reference, data=data, record_count=len(data))


@router.get("/{ticker}", response_model=FundamentalsResponse)
async def get_fundamentals(
    ticker: str,
    start_date: datetime = Query(..., description="quarter_end_date range start (inclusive)"),
    end_date: datetime = Query(..., description="quarter_end_date range end (inclusive)"),
    as_of: Optional[datetime] = Query(
        None, description="PIT reference (default: end_date); only rows with announcement_date <= as_of are returned"
    ),
) -> FundamentalsResponse:
    """
    Query fundamentals for a ticker, PIT-filtered by announcement_date.

    SPEC-PIPE-003 (CRITICAL): start_date/end_date bound the
    quarter_end_date fetch window (which quarters to consider at all);
    as_of is the actual PIT gate — a quarter whose announcement_date is
    after as_of is excluded even if its quarter_end_date falls inside the
    window, since that result was not yet public knowledge as of as_of.
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    pit_reference = as_of or end_date

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"""
            SELECT {_SELECT_COLS} FROM fundamentals
            WHERE ticker = ? AND quarter_end_date >= ? AND quarter_end_date <= ?
            """,
            [ticker, start_date.date(), end_date.date()],
        ).fetchall()

    df = pd.DataFrame(rows, columns=_COLUMNS)
    if not df.empty:
        df["announcement_date"] = pd.to_datetime(df["announcement_date"])
        df = enforce_pit_fundamentals(df, as_of=pit_reference, announcement_date_col="announcement_date")
    # NaN → None: cast to object dtype first so pandas doesn't coerce None back to NaN
    # in float64 columns. Pydantic v2 rejects float('nan') for finite-number fields.
    if not df.empty:
        df = df.astype(object).where(df.notna(), None)

    data = [FundamentalsRow(**row) for row in df.to_dict(orient="records")]
    return FundamentalsResponse(ticker=ticker, as_of=pit_reference, data=data, record_count=len(data))


@router.post("/write", response_model=FundamentalsWriteResult)
async def write_fundamentals(record: FundamentalsWrite) -> FundamentalsWriteResult:
    """
    Upsert one quarterly fundamentals row — SPEC-DS-004:
    same (ticker, fiscal_year, quarter) replaces, never duplicates.

    Raises
    ------
    HTTPException 400
        If announcement_date <= quarter_end_date (SPEC-PIPE-003: a build
        failure — results cannot be announced before the quarter they
        cover has even ended).
    """
    if record.announcement_date.date() <= record.quarter_end_date.date():
        raise HTTPException(
            status_code=400,
            detail="SPEC-PIPE-003 violation: announcement_date must be after quarter_end_date",
        )

    values = [getattr(record, col) if col not in ("quarter_end_date", "announcement_date")
              else getattr(record, col).date() for col in _COLUMNS]
    placeholders = ", ".join("?" for _ in _COLUMNS)
    update_cols = [c for c in _COLUMNS if c not in ("ticker", "fiscal_year", "quarter")]
    # [AS BUILT, P2.6] COALESCE, not a blind overwrite: as of P2.6, fundamentals
    # has TWO independent writers for the same (ticker, fiscal_year, quarter) row
    # — screener.py (revenue/ebitda/... + depreciation) and tijori.py
    # (sector_specific_metric_1-6 only, every other field NULL in its own
    # FundamentalsWrite payload). A plain `col = excluded.col` upsert would let
    # whichever scraper runs second silently null out the other's columns on
    # every write. COALESCE(excluded.col, fundamentals.col) makes each write
    # additive: a NULL in the incoming payload leaves the existing stored value
    # untouched; a real (non-NULL) value still always wins and overwrites
    # (e.g. screener.py re-filing a restated quarter's revenue).
    update_clause = ", ".join(f"{c} = COALESCE(excluded.{c}, fundamentals.{c})" for c in update_cols)

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        conn.execute(
            f"""
            INSERT INTO fundamentals ({_SELECT_COLS}) VALUES ({placeholders})
            ON CONFLICT (ticker, fiscal_year, quarter) DO UPDATE SET {update_clause}
            """,
            values,
        )

    logger.info(f"fundamentals.write: {record.ticker} FY{record.fiscal_year}Q{record.quarter}")
    return FundamentalsWriteResult(
        ticker=record.ticker, fiscal_year=record.fiscal_year, quarter=record.quarter, written=True
    )
