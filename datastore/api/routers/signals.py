"""
datastore/api/routers/signals.py

Phase: 1.7 (DataStore API Full + Daily Pipeline + Dashboard)
Specs: SPEC-DS-002, SPEC-DS-003, SPEC-DS-004, SPEC-MODEL-006
Owner: Platform / DataStore
Consumers: dashboard, systems/ml_signal_engine/inference/daily_inference.py, backtest

GET /api/v1/signals/ml/{ticker}/{date}, GET /api/v1/signals/ml/top_buys/{date},
POST /api/v1/signals/ml/write — against the ml_signals DuckDB table (Store 4,
datastore/schema/create_signals.py). One row per (date, ticker, model_name) —
each model writes its own row (SPEC-DS-004 upsert unit: "system_name" ==
model_name here).

top_buys excludes any ticker P&D-blocked that date (SPEC-MODEL-006: "P&D
pre-filter takes priority" — a blocked stock must never reach a buy-signal
list, enforced again here at the read layer as defense in depth alongside
daily_inference.py's write-time enforcement).

[AS BUILT] All ml_signals access here (reads and the write) uses a plain
get_duckdb_connection(SIGNALS_DUCKDB_PATH) — no read_only=True — so every
route shares one connection-pool entry per file (datastore/api/db.py
caches by path + read_only flag; DuckDB itself rejects opening a second,
differently-configured connection to the same file from within one
process). This API server is the *only* writer of signals.duckdb
(SPEC-DS-002: writes flow exclusively through this API, no other process
touches the file), so there is no cross-process single-writer-lock
concern here the way there is for ohlcv.py's DUCKDB_PATH, which the
scheduler also writes to from a separate process — that one keeps
read_only=True for its GETs.
"""

import logging
from datetime import date as date_type
from typing import List

from fastapi import APIRouter, HTTPException, Path, Query

from config.settings import SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.schemas import MLSignalRow, MLSignalWrite, MLSignalWriteResult

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/signals", tags=["Signals"])

_ML_SIGNAL_COLUMNS = [
    "date", "ticker", "model_name", "model_version", "signal_direction",
    "buy_prob", "hold_prob", "sell_prob", "q10_return", "q50_return", "q90_return",
    "meta_label", "meta_prob", "conformal_lower", "conformal_upper",
    "pnd_score", "pnd_phase", "pnd_block", "hmm_regime", "hmm_regime_prob", "hmm_stability",
    "exit_urgency", "exit_type", "exit_survival_5d", "exit_survival_21d", "exit_survival_63d",
    "shap_top5_json",
]
_SELECT_COLS = ", ".join(_ML_SIGNAL_COLUMNS)


def _row_to_signal(row) -> MLSignalRow:
    return MLSignalRow(**dict(zip(_ML_SIGNAL_COLUMNS, row)))


# [AS BUILT] /ml/top_buys/{date} MUST be registered before /ml/{ticker}/{date}:
# FastAPI matches routes in registration order, and the dynamic
# /ml/{ticker}/{date} pattern would otherwise swallow "top_buys" as a
# ticker value, making the top_buys handler unreachable (caught via a
# live smoke test — GET .../top_buys/2024-06-01 returned [] even with
# matching rows present, because it was silently being routed to
# get_ml_signals(ticker="top_buys", date=...) instead).
@router.get("/ml/top_buys/{date}", response_model=List[MLSignalRow])
async def get_top_buys(
    date: date_type = Path(..., description="Signal date (YYYY-MM-DD)"),
    n: int = Query(5, ge=1, le=100, description="Number of top buy signals to return"),
    model_name: str = Query("signal_5d", description="Which signal model's buy_prob to rank by"),
) -> List[MLSignalRow]:
    """
    Top-N buy signals for a date, ranked by buy_prob, excluding any ticker
    P&D-blocked that date (SPEC-MODEL-006).
    """
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH) as conn:
        rows = conn.execute(
            f"""
            SELECT {_SELECT_COLS} FROM ml_signals
            WHERE date = ? AND model_name = ? AND buy_prob IS NOT NULL
              AND ticker NOT IN (
                  SELECT ticker FROM ml_signals
                  WHERE date = ? AND model_name = 'pnd_detector' AND pnd_block = TRUE
              )
            ORDER BY buy_prob DESC
            LIMIT ?
            """,
            [date, model_name, date, n],
        ).fetchall()

    return [_row_to_signal(r) for r in rows]


@router.get("/ml/{ticker}/{date}", response_model=List[MLSignalRow])
async def get_ml_signals(
    ticker: str, date: date_type = Path(..., description="Signal date (YYYY-MM-DD)")
) -> List[MLSignalRow]:
    """All models' signal rows for one ticker on one date."""
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH) as conn:
        rows = conn.execute(
            f"SELECT {_SELECT_COLS} FROM ml_signals WHERE ticker = ? AND date = ? ORDER BY model_name",
            [ticker, date],
        ).fetchall()

    return [_row_to_signal(r) for r in rows]


@router.post("/ml/write", response_model=MLSignalWriteResult)
async def write_ml_signal(signal: MLSignalWrite) -> MLSignalWriteResult:
    """
    Upsert one model's full output row for one (date, ticker) —
    SPEC-DS-004: "same date+ticker+system replaces, never duplicates".
    """
    values = [getattr(signal, col) if col not in ("date",) else signal.date.date() for col in _ML_SIGNAL_COLUMNS]
    placeholders = ", ".join("?" for _ in _ML_SIGNAL_COLUMNS)
    update_cols = [c for c in _ML_SIGNAL_COLUMNS if c not in ("date", "ticker", "model_name")]
    update_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH) as conn:
        conn.execute(
            f"""
            INSERT INTO ml_signals ({_SELECT_COLS}) VALUES ({placeholders})
            ON CONFLICT (date, ticker, model_name) DO UPDATE SET {update_clause}
            """,
            values,
        )

    logger.info(f"signals.write: {signal.model_name} {signal.ticker} {signal.date.date()}")
    return MLSignalWriteResult(
        date=signal.date, ticker=signal.ticker, model_name=signal.model_name, written=True
    )
