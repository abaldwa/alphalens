"""
datastore/api/routers/forensic.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-DS-002, SPEC-DS-004, SPEC-MODEL-009, SPEC-MODEL-010
Owner: Platform / DataStore
Consumers: dashboard, systems/ml_signal_engine, backtest

GET /api/v1/signals/ml/forensic/{ticker}?as_of= and
POST /api/v1/signals/ml/forensic/write — against the ml_forensic DuckDB
table (Store 4, datastore/schema/create_signals.py). One row per
(date, ticker), written by systems/ml_signal_engine/inference/
score_forensic.py (M-09/M-10).

[AS BUILT, P2.6] No further PIT filtering on read: unlike fundamentals/
shareholding (raw disclosures with their own separate filing/announcement
date), ml_forensic rows are already "as of" their own `date` column — the
scoring script that wrote a row already used PIT-filtered inputs to
compute it (same reasoning datastore/api/routers/signals.py's ml_signals
GET endpoints already document/apply: no enforce_pit_* call on this kind
of model-output table). GET returns the most recent row at-or-before
`as_of` (default: now) — a "current forensic risk score" read, not a
specific-date lookup the way signals.py's /ml/{ticker}/{date} is (no date
path param in the build prompt's literal endpoint signature).

[AS BUILT, SPEC-SCHED-013] no persist=False here: this API server is the
sole writer of signals.duckdb (same reasoning as signals.py's module
docstring).
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from config.settings import SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.schemas import ForensicRow, ForensicSummaryResponse, ForensicWrite, ForensicWriteResult

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/signals/ml/forensic", tags=["Forensic"])

_COLUMNS = [
    "date", "ticker", "beneish_m", "altman_z", "piotroski_f", "ohlson_o", "dechow_f",
    "sloan_accrual", "benford_mad", "forensic_composite", "forensic_flag",
    "forensic_flag_label", "forensic_ml_prob", "shap_top5_json", "pattern_match",
]
_SELECT_COLS = ", ".join(_COLUMNS)


# [AS BUILT, P2.6-dashboard] /summary MUST be registered before /{ticker}: same
# route-ordering discipline as signals.py's /ml/top_buys/{date}-before-/ml/{ticker}/{date}.
@router.get("/summary", response_model=ForensicSummaryResponse)
async def get_forensic_summary() -> ForensicSummaryResponse:
    """
    Universe-wide forensic flag counts for the most recent scored date.
    red_count = red + black labels (critical); amber_count = orange + yellow (elevated).
    Returns available=False when ml_forensic has never been written.
    """
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH) as conn:
        latest = conn.execute("SELECT MAX(date) FROM ml_forensic").fetchone()
        latest_date = latest[0] if latest else None
        if latest_date is None:
            return ForensicSummaryResponse()

        rows = conn.execute(
            "SELECT forensic_flag_label FROM ml_forensic WHERE date = ?",
            [latest_date],
        ).fetchall()

    labels = [r[0] for r in rows if r[0] is not None]
    red = sum(1 for lbl in labels if lbl in ("red", "black"))
    amber = sum(1 for lbl in labels if lbl in ("orange", "yellow"))
    green = sum(1 for lbl in labels if lbl == "green")
    return ForensicSummaryResponse(
        as_of_date=latest_date,
        red_count=red,
        amber_count=amber,
        green_count=green,
        total_scored=len(labels),
        available=True,
    )


@router.get("/{ticker}", response_model=Optional[ForensicRow])
async def get_forensic_score(
    ticker: str,
    as_of: Optional[datetime] = Query(None, description="Most recent row at or before this date (default: now)"),
) -> Optional[ForensicRow]:
    """Most recent forensic scoring row for a ticker at or before `as_of`, or None if none exists."""
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    pit_reference = as_of or datetime.utcnow()

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH) as conn:
        row = conn.execute(
            f"""
            SELECT {_SELECT_COLS} FROM ml_forensic
            WHERE ticker = ? AND date <= ?
            ORDER BY date DESC LIMIT 1
            """,
            [ticker, pit_reference.date()],
        ).fetchone()

    return ForensicRow(**dict(zip(_COLUMNS, row))) if row else None


@router.post("/write", response_model=ForensicWriteResult)
async def write_forensic_score(record: ForensicWrite) -> ForensicWriteResult:
    """Upsert one forensic scoring row — SPEC-DS-004: same date+ticker replaces, never duplicates."""
    values = [getattr(record, col) if col != "date" else record.date.date() for col in _COLUMNS]
    placeholders = ", ".join("?" for _ in _COLUMNS)
    update_cols = [c for c in _COLUMNS if c not in ("date", "ticker")]
    update_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH) as conn:
        conn.execute(
            f"""
            INSERT INTO ml_forensic ({_SELECT_COLS}) VALUES ({placeholders})
            ON CONFLICT (date, ticker) DO UPDATE SET {update_clause}
            """,
            values,
        )

    logger.info(f"forensic.write: {record.ticker} {record.date.date()}")
    return ForensicWriteResult(date=record.date, ticker=record.ticker, written=True)
