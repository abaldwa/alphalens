"""
datastore/api/routers/sector_accumulation.py

Phase: FeatureBacklog.md ML29 — sector accumulation detection
Owner: Platform / DataStore
Consumers: dashboard Sector Rotation screen ("Sector Accumulation" section)

Routes:
  GET /api/v1/sector_accumulation/daily        — accumulation_score per
                                                  sector over a date range
  GET /api/v1/sector_accumulation/drilldown    — per-stock breakdown for
                                                  one (sector, date) cell

Thin controller over features/sector_accumulation.py's real
compute_sector_accumulation()/sector_accumulation_drilldown() — reads
ohlcv_adjusted + fundamentals (both DUCKDB_PATH), via a persist=False
connection (SPEC-SCHED-013 — shared with the ingestion scheduler / other
long-lived processes).
"""

from __future__ import annotations

import logging
from datetime import date as date_type
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from features.sector_accumulation import (
    DEFAULT_LOOKBACK_DAYS,
    compute_sector_accumulation,
    sector_accumulation_drilldown,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/sector_accumulation", tags=["Sector Accumulation"])


class SectorAccumulationRow(BaseModel):
    date: str
    sector: str
    accumulation_score: float
    delivery_volume: float
    sector_shares_outstanding: float
    n_stocks_included: int


class SectorAccumulationDrilldownRow(BaseModel):
    ticker: str
    volume: float
    delivery_pct: float
    delivery_volume: float
    shares_outstanding: float
    contribution_pct: float


@router.get("/daily", response_model=List[SectorAccumulationRow])
async def get_sector_accumulation_daily(
    start_date: Optional[date_type] = Query(None, description="Defaults to end_date minus lookback_days"),
    end_date: Optional[date_type] = Query(None, description="Defaults to the latest date with real ohlcv_adjusted data"),
    lookback_days: int = Query(DEFAULT_LOOKBACK_DAYS, ge=1, le=730, description="Used only when start_date is omitted"),
) -> List[SectorAccumulationRow]:
    """
    Daily accumulation_score per sector: (sum of each constituent stock's
    delivery% x volume) / sector's total outstanding shares (a simple sum
    of each constituent stock's own shares_outstanding, PIT-correct as of
    each date) — to surface sectors under constant/steady accumulation.
    """
    start_str = start_date.isoformat() if start_date is not None else None
    end_str = end_date.isoformat() if end_date is not None else None
    try:
        with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=True) as conn:
            df = compute_sector_accumulation(conn, start_date=start_str, end_date=end_str, lookback_days=lookback_days)
    except Exception as exc:  # pragma: no cover - defensive, mirrors other routers
        logger.exception("sector_accumulation daily failed")
        raise HTTPException(status_code=500, detail=str(exc))

    # NaN -> None: DuckDB NULLs surface as float('nan'), which Pydantic v2
    # rejects even for Optional[float] fields (same fix as fundamentals.py).
    df = df.astype(object).where(df.notna(), None)
    return [SectorAccumulationRow(**row) for row in df.to_dict(orient="records")]


@router.get("/drilldown", response_model=List[SectorAccumulationDrilldownRow])
async def get_sector_accumulation_drilldown(
    sector: str = Query(..., description="Sector name, as in config.universe.load_universe()'s sector column"),
    date: date_type = Query(..., description="Date to drill into (one cell from the /daily table)"),
) -> List[SectorAccumulationDrilldownRow]:
    """Per-stock delivery-volume/shares_outstanding breakdown backing one
    (sector, date) accumulation_score cell, for a dashboard drill-down click."""
    try:
        with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=True) as conn:
            df = sector_accumulation_drilldown(conn, sector=sector, date_str=date.isoformat())
    except Exception as exc:  # pragma: no cover - defensive, mirrors other routers
        logger.exception("sector_accumulation drilldown failed")
        raise HTTPException(status_code=500, detail=str(exc))

    df = df.astype(object).where(df.notna(), None)
    return [SectorAccumulationDrilldownRow(**row) for row in df.to_dict(orient="records")]
