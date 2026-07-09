"""
datastore/api/routers/multibagger.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-DS-002, SPEC-DS-004, SPEC-MODEL-001, SPEC-UI-003
Owner: Platform / DataStore
Consumers: dashboard, datastore/api/routers/watchlist.py, systems/ml_signal_engine, backtest

GET /api/v1/signals/ml/multibagger/{ticker}?as_of= and
POST /api/v1/signals/ml/multibagger/write — against the ml_multibagger
DuckDB table (Store 4, datastore/schema/create_signals.py). One row per
(date, ticker), written weekly by systems/ml_signal_engine/inference/
score_multibagger.py (M-08).

[AS BUILT, P2.6] Same "no further PIT filtering on read, most-recent-row
semantics" reasoning as datastore/api/routers/forensic.py's module
docstring — see that file.
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from config.settings import SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.schemas import MultibaggerRow, MultibaggerWrite, MultibaggerWriteResult

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/signals/ml/multibagger", tags=["Multibagger"])

_COLUMNS = [
    "date", "ticker", "mb_probability", "mb_tier", "mb_archetype",
    "survival_6m", "survival_12m", "survival_18m", "survival_24m", "survival_36m",
    "shap_top5_json", "analogues_json",
]
_SELECT_COLS = ", ".join(_COLUMNS)


@router.get("/{ticker}", response_model=Optional[MultibaggerRow])
async def get_multibagger_score(
    ticker: str,
    as_of: Optional[datetime] = Query(None, description="Most recent row at or before this date (default: now)"),
) -> Optional[MultibaggerRow]:
    """Most recent multibagger scoring row for a ticker at or before `as_of`, or None if none exists."""
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    pit_reference = as_of or datetime.utcnow()

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        row = conn.execute(
            f"""
            SELECT {_SELECT_COLS} FROM ml_multibagger
            WHERE ticker = ? AND date <= ?
            ORDER BY date DESC LIMIT 1
            """,
            [ticker, pit_reference.date()],
        ).fetchone()

    return MultibaggerRow(**dict(zip(_COLUMNS, row))) if row else None


@router.post("/write", response_model=MultibaggerWriteResult)
async def write_multibagger_score(record: MultibaggerWrite) -> MultibaggerWriteResult:
    """Upsert one multibagger scoring row — SPEC-DS-004: same date+ticker replaces, never duplicates."""
    values = [getattr(record, col) if col != "date" else record.date.date() for col in _COLUMNS]
    placeholders = ", ".join("?" for _ in _COLUMNS)
    update_cols = [c for c in _COLUMNS if c not in ("date", "ticker")]
    update_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=False) as conn:
        conn.execute(
            f"""
            INSERT INTO ml_multibagger ({_SELECT_COLS}) VALUES ({placeholders})
            ON CONFLICT (date, ticker) DO UPDATE SET {update_clause}
            """,
            values,
        )

    logger.info(f"multibagger.write: {record.ticker} {record.date.date()}")
    return MultibaggerWriteResult(date=record.date, ticker=record.ticker, written=True)
