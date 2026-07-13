"""
datastore/api/routers/sector_rotation.py

Phase: FutureDevelopment #25 (ML12 steps 4-6 — daily sector rotation report)
Owner: Platform / DataStore
Consumers: dashboard sector rotation screen

Routes:
  GET /api/v1/sector_rotation/report  — full ranked sector report + top
                                         stocks per in-favor sector

Thin controller over features/sector_rotation.py's real
compute_sector_rotation_report() — reads index_ohlcv (DUCKDB_PATH) and
ml_signals/ml_multibagger (SIGNALS_DUCKDB_PATH), both via
persist=False connections (SPEC-SCHED-013 — shared with the ingestion
scheduler / other long-lived processes).
"""

from __future__ import annotations

import logging
from datetime import date as date_type
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from config.settings import DUCKDB_PATH, SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from features.sector_rotation import DEFAULT_TOP_N_STOCKS, compute_sector_rotation_report

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/sector_rotation", tags=["Sector Rotation"])


class SectorRotationRow(BaseModel):
    sector: str
    index_name: str
    rank: int
    trailing_21d_return: Optional[float]
    nifty500_trailing_21d_return: Optional[float]
    relative_strength: Optional[float]
    top_stocks: List[Dict[str, Any]]


class SectorRotationReport(BaseModel):
    as_of_date: Optional[str]
    sectors: List[SectorRotationRow]


@router.get("/report", response_model=SectorRotationReport)
async def get_sector_rotation_report(
    as_of_date: Optional[date_type] = Query(None, description="Restrict to index_ohlcv rows on/before this date; defaults to the latest date available"),
    top_n_stocks: int = Query(DEFAULT_TOP_N_STOCKS, ge=1, le=25, description="Top stocks per sector by buy_prob/mb_probability"),
) -> SectorRotationReport:
    """
    Ranked sectors by trailing-21-trading-day relative strength vs Nifty
    500 (config/sector_index_map.py's mapped sectors only — see that
    module's docstring for which sectors have no matching NSE index and
    are excluded), each with real top-N stocks by current ml_signals
    buy_prob / ml_multibagger probability.
    """
    date_str = as_of_date.isoformat() if as_of_date is not None else None
    try:
        with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=True) as normalised_conn:
            with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as signals_conn:
                report = compute_sector_rotation_report(
                    normalised_conn, signals_conn, as_of_date=date_str, top_n_stocks=top_n_stocks
                )
    except Exception as exc:  # pragma: no cover - defensive, mirrors other routers
        logger.exception("sector_rotation report failed")
        raise HTTPException(status_code=500, detail=str(exc))

    return SectorRotationReport(**report)
