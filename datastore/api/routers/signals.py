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

[AS BUILT, AF-1] Was previously a plain get_duckdb_connection(SIGNALS_
DUCKDB_PATH) — persist=True cached across requests — on the reasoning
that this API is the sole writer of signals.duckdb so there was no
cross-process lock conflict. That turned out not to hold: the ingestion
scheduler's check_ta_alerts step (a separate OS process) also needs to
touch signals.duckdb, and a long-lived cached connection here starved it
of the single-writer lock (BuildLog.md "Fix check_ta_alerts cross-process
DuckDB lock race", commit 8147579). Every call site here now passes
persist=False explicitly (lock releases after each request) and an
explicit read_only= per endpoint (True for the GETs, False for the
write).
"""

import logging
from datetime import date as date_type
from typing import List

from fastapi import APIRouter, HTTPException, Path, Query

from config.settings import SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.schemas import (
    MLSignalRow,
    MLSignalWrite,
    MLSignalWriteResult,
    SignalUniverseRow,
)
from ingestion.scheduler.checkpoint import CheckpointManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/signals", tags=["Signals"])

# A43: is_backfill is a Python-side join, not a SQL one — ml_signals (DuckDB)
# and pipeline_checkpoints (SQLite) are different databases. A fresh
# CheckpointManager is cheap (SQLite, opened per call via get_sqlite_connection)
# so it's safe to construct once per module import and reuse across requests.
_checkpoint_manager = CheckpointManager()


def _attach_is_backfill(rows: List[MLSignalRow]) -> List[MLSignalRow]:
    """Populate MLSignalRow.is_backfill from pipeline_checkpoints' write_signals
    step for each row's date, caching one lookup per distinct date so a
    multi-row response (e.g. top_buys, history) doesn't re-query per row."""
    cache: dict = {}
    for row in rows:
        row_date = row.date.date() if hasattr(row.date, "date") else row.date
        if row_date not in cache:
            cache[row_date] = _checkpoint_manager.get_step_is_backfill(row_date, "write_signals")
        row.is_backfill = cache[row_date]
    return rows

_ML_SIGNAL_COLUMNS = [
    "date", "ticker", "model_name", "model_version", "signal_direction",
    "buy_prob", "hold_prob", "sell_prob", "q10_return", "q50_return", "q90_return",
    "meta_label", "meta_prob", "conformal_lower", "conformal_upper",
    "pnd_score", "pnd_phase", "pnd_block", "hmm_regime", "hmm_regime_prob", "hmm_stability",
    "exit_urgency", "exit_type", "exit_survival_5d", "exit_survival_21d", "exit_survival_63d",
    "shap_top5_json", "in_training_universe",
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
    carry_forward: bool = Query(
        False,
        description=(
            "If true and no signals were written for `date`, fall back to the most "
            "recent earlier date that has them (rows still carry their real, earlier "
            "`date` field — nothing is relabeled as `date`). For live 'what to do "
            "today' views only. Backdated Entry / historical audit callers must leave "
            "this False: SPEC-MODEL-006's Gate 7 forward-time day count requires an "
            "honest 'no signal that day' rather than a silently backfilled one."
        ),
    ),
) -> List[MLSignalRow]:
    """
    Top-N buy signals for a date, ranked by buy_prob, excluding any ticker
    P&D-blocked that date (SPEC-MODEL-006).
    """
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        query_date = date
        if carry_forward:
            resolved = conn.execute(
                "SELECT MAX(date) FROM ml_signals WHERE date <= ? AND model_name = ? AND buy_prob IS NOT NULL",
                [date, model_name],
            ).fetchone()[0]
            if resolved is None:
                return []
            query_date = resolved

        # FutureDevelopment.md #15 ("Daily Insights row fusion"): each model
        # writes its own row keyed by (date, ticker, model_name), so the
        # bare `s.*` row for model_name=signal_5d has NULL meta_label/
        # conformal_lower/pnd_score/hmm_regime even when meta_labeler/
        # pnd_detector/hmm_market scored the same ticker/date — hub.js's
        # Daily Insights table reads those columns straight off this row.
        # Fix: read-time LEFT JOIN across the other three models' rows for
        # the same (ticker, date), COALESCE-ing the signal_5d row's own
        # (always-NULL for these columns) value with the fused one so the
        # response shape (MLSignalRow) is unchanged — only the columns that
        # are always NULL on a signal_5d row (meta_label/meta_prob,
        # conformal_lower/upper are signal_5d's own, pnd_*, hmm_*) get
        # fused in. hmm_market rows key on ticker='MARKET' (market-wide,
        # not per-ticker), so that join is on date only.
        rows = conn.execute(
            """
            SELECT
                s.date, s.ticker, s.model_name, s.model_version, s.signal_direction,
                s.buy_prob, s.hold_prob, s.sell_prob, s.q10_return, s.q50_return, s.q90_return,
                COALESCE(s.meta_label, meta.meta_label) AS meta_label,
                COALESCE(s.meta_prob, meta.meta_prob) AS meta_prob,
                s.conformal_lower, s.conformal_upper,
                COALESCE(s.pnd_score, pnd.pnd_score) AS pnd_score,
                COALESCE(s.pnd_phase, pnd.pnd_phase) AS pnd_phase,
                COALESCE(s.pnd_block, pnd.pnd_block) AS pnd_block,
                COALESCE(s.hmm_regime, hmm.hmm_regime) AS hmm_regime,
                COALESCE(s.hmm_regime_prob, hmm.hmm_regime_prob) AS hmm_regime_prob,
                COALESCE(s.hmm_stability, hmm.hmm_stability) AS hmm_stability,
                s.exit_urgency, s.exit_type, s.exit_survival_5d, s.exit_survival_21d, s.exit_survival_63d,
                s.shap_top5_json
            FROM ml_signals s
            LEFT JOIN ml_signals meta
                ON meta.date = s.date AND meta.ticker = s.ticker AND meta.model_name = 'meta_labeler'
            LEFT JOIN ml_signals pnd
                ON pnd.date = s.date AND pnd.ticker = s.ticker AND pnd.model_name = 'pnd_detector'
            LEFT JOIN ml_signals hmm
                ON hmm.date = s.date AND hmm.model_name = 'hmm_market'
            WHERE s.date = ? AND s.model_name = ? AND s.buy_prob IS NOT NULL
              AND s.ticker NOT IN (
                  SELECT ticker FROM ml_signals
                  WHERE date = ? AND model_name = 'pnd_detector' AND pnd_block = TRUE
              )
            ORDER BY s.buy_prob DESC
            LIMIT ?
            """,
            [query_date, model_name, query_date, n],
        ).fetchall()

    return _attach_is_backfill([_row_to_signal(r) for r in rows])


@router.get("/ml/history/{ticker}", response_model=List[MLSignalRow])
async def get_signal_history(
    ticker: str,
    model_name: str = Query("signal_5d", description="Which model's rows to return"),
    n: int = Query(10, ge=1, le=100, description="Number of most recent dated rows"),
) -> List[MLSignalRow]:
    """
    Last N dated rows for one ticker/model — #17's rolling scorecard of
    signal_5d's own recent calls (recommended date/price/expected-return vs
    current price/return is computed client-side off these rows plus
    /api/v1/ohlcv/{ticker} close prices). is_backfill (A43) is attached via
    a Python-side join against pipeline_checkpoints after the DuckDB read.
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT {_SELECT_COLS} FROM ml_signals
            WHERE ticker = ? AND model_name = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            [ticker, model_name, n],
        ).fetchall()
    return _attach_is_backfill([_row_to_signal(r) for r in rows])


# [AS BUILT, #21] Registered before /ml/{ticker}/{date} for the same reason
# /ml/top_buys/{date} is: FastAPI route-matching order would otherwise let
# the dynamic {ticker} pattern swallow "universe" as a ticker value.
@router.get("/ml/universe/{date}", response_model=List[SignalUniverseRow])
async def get_signal_universe(
    date: date_type = Path(..., description="Signal date (YYYY-MM-DD)"),
    carry_forward: bool = Query(
        True,
        description="If true and signal_5d has no rows for `date`, fall back to the most recent earlier date that does.",
    ),
) -> List[SignalUniverseRow]:
    """
    Every ticker scored by signal_5d on the resolved date, joined against
    meta_labeler/pnd_detector (same date, ml_signals) and ml_forensic/
    ml_multibagger (their own most recent row at-or-before that date, since
    those models don't necessarily score daily) — backs #21's sortable
    full-universe Signal Deep Dive table.
    """
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        query_date = date
        if carry_forward:
            resolved = conn.execute(
                "SELECT MAX(date) FROM ml_signals WHERE date <= ? AND model_name = 'signal_5d' AND buy_prob IS NOT NULL",
                [date],
            ).fetchone()[0]
            if resolved is None:
                return []
            query_date = resolved

        rows = conn.execute(
            """
            SELECT s.ticker, s.date, s.buy_prob, s.q50_return, m.meta_prob, p.pnd_score,
                   f.forensic_flag_label, mb.mb_probability
            FROM (SELECT * FROM ml_signals WHERE date = ? AND model_name = 'signal_5d' AND buy_prob IS NOT NULL) s
            LEFT JOIN (SELECT ticker, meta_prob FROM ml_signals WHERE date = ? AND model_name = 'meta_labeler') m
                ON s.ticker = m.ticker
            LEFT JOIN (SELECT ticker, pnd_score FROM ml_signals WHERE date = ? AND model_name = 'pnd_detector') p
                ON s.ticker = p.ticker
            LEFT JOIN (
                SELECT ticker, forensic_flag_label FROM ml_forensic
                WHERE date = (SELECT MAX(date) FROM ml_forensic WHERE date <= ?)
            ) f ON s.ticker = f.ticker
            LEFT JOIN (
                SELECT ticker, mb_probability FROM ml_multibagger
                WHERE date = (SELECT MAX(date) FROM ml_multibagger WHERE date <= ?)
            ) mb ON s.ticker = mb.ticker
            ORDER BY s.buy_prob DESC
            """,
            [query_date, query_date, query_date, query_date, query_date],
        ).fetchall()

    return [
        SignalUniverseRow(
            ticker=r[0], date=r[1], buy_prob=r[2], q50_return=r[3],
            meta_label_prob=r[4], pnd_score=r[5], forensic_flag=r[6], mb_probability=r[7],
        )
        for r in rows
    ]


@router.get("/ml/{ticker}/{date}", response_model=List[MLSignalRow])
async def get_ml_signals(
    ticker: str,
    date: date_type = Path(..., description="Signal date (YYYY-MM-DD)"),
    carry_forward: bool = Query(
        False,
        description=(
            "If true and this ticker has no rows on `date`, fall back to its most "
            "recent earlier date with rows (each row keeps its real, earlier `date` "
            "value). Intended for live lookup views, not PIT-sensitive historical audits."
        ),
    ),
) -> List[MLSignalRow]:
    """All models' signal rows for one ticker on one date."""
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        if carry_forward:
            # Resolve the fallback date inside the same SQL statement as the
            # row fetch. A prior two-step version fetched MAX(date) into
            # Python and re-bound it into a second `date = ?` equality
            # query; that silently matched zero rows because duckdb hands
            # TIMESTAMP columns back as `datetime`, not `date`, so the
            # re-bound Python value's type no longer matched the stored
            # column type on equality. A subquery avoids ever re-binding a
            # value duckdb handed back to us.
            rows = conn.execute(
                f"""
                SELECT {_SELECT_COLS} FROM ml_signals
                WHERE ticker = ? AND date = (
                    SELECT MAX(date) FROM ml_signals WHERE ticker = ? AND date <= ?
                )
                ORDER BY model_name
                """,
                [ticker, ticker, date],
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT {_SELECT_COLS} FROM ml_signals WHERE ticker = ? AND date = ? ORDER BY model_name",
                [ticker, date],
            ).fetchall()

    return _attach_is_backfill([_row_to_signal(r) for r in rows])


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

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=False) as conn:
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
