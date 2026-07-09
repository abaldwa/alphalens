"""
datastore/api/routers/regime.py

Phase: 1.7 (DataStore API Full + Daily Pipeline + Dashboard)
Specs: SPEC-DS-002, SPEC-DS-003
Owner: Platform / DataStore
Consumers: dashboard/screens/daily_dashboard.py, systems/ml_signal_engine

GET /api/v1/macro/regime — current market-wide HMM state (M-01), written
by daily_inference.py as a 'hmm_market' row in ml_signals (ticker='MARKET',
a sentinel since the HMM regime detector is market-wide, not per-ticker —
see systems/ml_signal_engine/models/hmm/regime_detector.py, P1.2).
"""

import logging
from datetime import date as date_type
from typing import Optional

from fastapi import APIRouter, Query

from config.settings import SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.schemas import RegimeHistoryResponse, RegimeHistoryRow, RegimeResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/macro", tags=["Macro"])

HMM_MARKET_MODEL_NAME = "hmm_market"
HMM_MARKET_TICKER = "MARKET"


# [AS BUILT, P3.x] /regime/history (SPEC-UI-002 Signal Detail screen).
@router.get("/regime/history", response_model=RegimeHistoryResponse)
async def get_regime_history(
    days: int = Query(30, ge=1, le=365, description="Number of most recent days to return"),
) -> RegimeHistoryResponse:
    """Last `days` market-wide HMM regime rows, ascending by date (SPEC-UI-002)."""
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT date, hmm_regime, hmm_regime_prob, hmm_stability FROM ml_signals
            WHERE ticker = ? AND model_name = ? ORDER BY date DESC LIMIT ?
            """,
            [HMM_MARKET_TICKER, HMM_MARKET_MODEL_NAME, days],
        ).fetchall()

    history = [
        RegimeHistoryRow(date=r[0], hmm_regime=r[1], hmm_regime_prob=r[2], hmm_stability=r[3])
        for r in reversed(rows)
    ]
    return RegimeHistoryResponse(days=history)


@router.get("/regime", response_model=RegimeResponse)
async def get_regime(
    as_of: Optional[date_type] = Query(None, description="PIT reference date (default: latest available)"),
) -> RegimeResponse:
    """Latest market-wide HMM regime state at or before `as_of`."""
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        if as_of is None:
            row = conn.execute(
                """
                SELECT date, hmm_regime, hmm_regime_prob, hmm_stability FROM ml_signals
                WHERE ticker = ? AND model_name = ? ORDER BY date DESC LIMIT 1
                """,
                [HMM_MARKET_TICKER, HMM_MARKET_MODEL_NAME],
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT date, hmm_regime, hmm_regime_prob, hmm_stability FROM ml_signals
                WHERE ticker = ? AND model_name = ? AND date <= ? ORDER BY date DESC LIMIT 1
                """,
                [HMM_MARKET_TICKER, HMM_MARKET_MODEL_NAME, as_of],
            ).fetchone()

    if row is None:
        return RegimeResponse(available=False)

    return RegimeResponse(
        date=row[0], hmm_regime=row[1], hmm_regime_prob=row[2], hmm_stability=row[3], available=True
    )
