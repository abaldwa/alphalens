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

GET /api/v1/macro/market_regimes — a SEPARATE, deliberately distinct
taxonomy: rule-based Bull/Bear/Sideways DATE-RANGE segments (not daily
point labels) computed by systems/regime/market_regime.py's 20%-threshold
classifier from an index's close price, persisted via systems/regime/
regime_store.py. Built for the Backtest module's "which strategy works in
which market phase" per-regime breakdown — the HMM regime above answers
"what does today look like," this answers "what were the confirmed
Bull/Bear/Sideways stretches over history." Do not conflate the two.

GET /api/v1/macro/regime/per-ticker — per-ticker HMM regime snapshot for
a single date, read from the daily feature parquet (not ml_signals).
"""

import logging
from datetime import date as date_type
from typing import List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from config.settings import DUCKDB_PATH, SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.schemas import RegimeHistoryResponse, RegimeHistoryRow, RegimeResponse
from systems.regime.market_regime import METHOD_NAME
from systems.regime.regime_store import list_regime_segments

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/macro", tags=["Macro"])

HMM_MARKET_MODEL_NAME = "hmm_market"
HMM_MARKET_TICKER = "MARKET"

DEFAULT_REGIME_INDEX = "Nifty 500"


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


class MarketRegimeSegmentResponse(BaseModel):
    index_name: str
    regime: str
    start_date: date_type
    end_date: date_type
    confirmed_date: date_type
    method: str
    move_pct: Optional[float] = None


class MarketRegimeSegmentListResponse(BaseModel):
    index_name: str
    segments: List[MarketRegimeSegmentResponse]


@router.get("/market_regimes", response_model=MarketRegimeSegmentListResponse)
async def get_market_regimes(
    index_name: str = Query(DEFAULT_REGIME_INDEX, description="Index name in index_ohlcv, e.g. 'Nifty 500'"),
    as_of: Optional[date_type] = Query(
        None, description="PIT-safe: only segments confirmed at or before this date"
    ),
    start_date: Optional[date_type] = Query(None, description="Restrict to segments overlapping this date range"),
    end_date: Optional[date_type] = Query(None, description="Restrict to segments overlapping this date range"),
    method: str = Query(
        METHOD_NAME,
        description=(
            "Classification method, e.g. '20pct_threshold_v1' (default — matches original "
            "single-threshold behavior), '15pct_threshold_v1', '10pct_threshold_v1', "
            "'5pct_threshold_v1'. Backfilled by scripts/backfill_market_regimes.py."
        ),
    ),
) -> MarketRegimeSegmentListResponse:
    """Rule-based Bull/Bear/Sideways date-range segments for `index_name`
    under one classification `method` (backfilled by
    scripts/backfill_market_regimes.py) — the Backtest module's per-regime
    performance breakdown, and its Market Regime Timeline comparison across
    thresholds, both read this. Defaults to the original 20% threshold
    method for backward compatibility with any caller not passing `method`
    explicitly; the Backtest page calls this 4x (once per threshold) to
    render its stacked timeline comparison."""
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=True) as conn:
        rows = list_regime_segments(
            conn, index_name, as_of=as_of, start_date=start_date, end_date=end_date, method=method
        )
    return MarketRegimeSegmentListResponse(
        index_name=index_name, segments=[MarketRegimeSegmentResponse(**r) for r in rows]
    )


# ---------------------------------------------------------------------------
# Per-ticker HMM regime (read from daily feature parquet)
# ---------------------------------------------------------------------------

HMM_REGIME_COLS = [
    "hmm_regime", "hmm_regime_prob_bullish", "hmm_regime_prob_bearish",
    "hmm_regime_duration", "hmm_regime_transition", "hmm_regime_stability",
]
_REGIME_NAMES = {0.0: "bearish", 1.0: "sideways", 2.0: "bullish"}


class PerTickerRegimeRow(BaseModel):
    ticker: str
    hmm_regime: Optional[float] = None
    hmm_regime_name: Optional[str] = None
    hmm_regime_prob_bullish: Optional[float] = None
    hmm_regime_prob_bearish: Optional[float] = None
    hmm_regime_duration: Optional[float] = None
    hmm_regime_transition: Optional[bool] = None
    hmm_regime_stability: Optional[float] = None


class PerTickerRegimeResponse(BaseModel):
    date: str
    tickers: List[PerTickerRegimeRow]


@router.get("/regime/per-ticker", response_model=PerTickerRegimeResponse)
async def get_per_ticker_regime(
    as_of: Optional[date_type] = Query(None, description="Date (YYYY-MM-DD). Default: latest available."),
) -> PerTickerRegimeResponse:
    """Per-ticker HMM regime for a single date, read from the daily feature
    parquet. Rows with all-NaN regime columns (undecodable tickers) are
    excluded. The hmm_regime_name is a human-readable label derived from
    the numeric rank (0=bearish, 1=sideways, 2=volatile, 3=bullish)."""
    import pandas as pd
    from config.settings import FEATURES_DAILY_DIR

    if as_of is None:
        parquets = sorted(FEATURES_DAILY_DIR.glob("*.parquet"))
        if not parquets:
            return PerTickerRegimeResponse(date="", tickers=[])
        parquet_path = parquets[-1]
    else:
        parquet_path = FEATURES_DAILY_DIR / f"{as_of.isoformat()}.parquet"

    if not parquet_path.exists():
        return PerTickerRegimeResponse(date=str(as_of or ""), tickers=[])

    date_str = parquet_path.stem
    df = pd.read_parquet(parquet_path, columns=["ticker"] + HMM_REGIME_COLS)
    df = df.dropna(subset=["hmm_regime"], how="all")
    df = df.sort_values("hmm_regime_stability", ascending=False, na_position="last")

    tickers = []
    for _, row in df.iterrows():
        regime_val = row["hmm_regime"]
        tickers.append(PerTickerRegimeRow(
            ticker=row["ticker"],
            hmm_regime=regime_val if pd.notna(regime_val) else None,
            hmm_regime_name=_REGIME_NAMES.get(regime_val) if pd.notna(regime_val) else None,
            hmm_regime_prob_bullish=row["hmm_regime_prob_bullish"] if pd.notna(row["hmm_regime_prob_bullish"]) else None,
            hmm_regime_prob_bearish=row["hmm_regime_prob_bearish"] if pd.notna(row["hmm_regime_prob_bearish"]) else None,
            hmm_regime_duration=row["hmm_regime_duration"] if pd.notna(row["hmm_regime_duration"]) else None,
            hmm_regime_transition=bool(row["hmm_regime_transition"]) if pd.notna(row["hmm_regime_transition"]) else None,
            hmm_regime_stability=row["hmm_regime_stability"] if pd.notna(row["hmm_regime_stability"]) else None,
        ))

    return PerTickerRegimeResponse(date=date_str, tickers=tickers)
