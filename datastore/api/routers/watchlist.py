"""
datastore/api/routers/watchlist.py

Phase: 1.7 (DataStore API Full + Daily Pipeline + Dashboard); made real P2.6
Specs: SPEC-DS-002, SPEC-UI-003
Owner: Platform / DataStore
Consumers: dashboard/screens/daily_dashboard.py

GET /api/v1/watchlist/current — top-20 ranked by mb_probability, from the
most recent date present in ml_multibagger (Store 4,
datastore/schema/create_signals.py), written weekly by
systems/ml_signal_engine/inference/score_multibagger.py (M-08).

[AS BUILT, P2.6] Was a Phase 1 stub (always returned an empty
WatchlistResponse with implemented=False) because M-08 didn't exist yet.
Now reads the real table — SPEC-UI-003's "Top 20 ranked by multibagger
probability... survival curves, archetypes". implemented=True is returned
whenever ml_multibagger has at least one row for the latest date,
implemented=False (honest empty response, same as the old stub) if the
table is still empty (e.g. score_multibagger.py has never been run) —
never a fabricated placeholder list.
"""

import logging
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, Query

from config.settings import DUCKDB_PATH, SIGNALS_DUCKDB_PATH
from config.training_universe import filter_recommendable
from config.universe import load_universe_raw
from datastore.api.db import get_duckdb_connection
from datastore.api.schemas import DailyWatchlistResponse, DailyWatchlistRow, WatchlistResponse
from datastore.api.utils.feature_store import read_feature_row

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/watchlist", tags=["Watchlist"])

_COLUMNS = [
    "date", "ticker", "mb_probability", "mb_tier", "mb_archetype",
    "survival_6m", "survival_12m", "survival_18m", "survival_24m", "survival_36m",
]
_SELECT_COLS = ", ".join(_COLUMNS)
_TOP_N = 20

# (model_name in ml_signals, display horizon label, horizon in trading days)
_HORIZON_MODELS = [
    ("signal_5d", "5d", 5),
    ("signal_21d", "21d", 21),
    ("signal_63d", "63d", 63),
]
_ATR_MULTIPLIER = 1.5  # realistic-target fallback: horizon-scaled ATR band, used only when
# the quantile model has no q50_return for a ticker/date (never a fixed 15%)


def _build_price_map(price_df: pd.DataFrame) -> dict:
    """
    REV26 (2026-07-21 review): a plain `dict(zip(...))` here would let a
    NaN `close` (fetchdf() surfaces a NULL float as float('nan'), not None)
    reach DailyWatchlistRow's float fields — `if price is not None` below
    doesn't catch NaN. ohlcv_adjusted.close is schema-NOT-NULL today so this
    isn't reachable via that table alone, but this cast is the same cheap,
    always-correct belt-and-suspenders pattern already applied in
    fundamentals.py/sector_accumulation.py wherever a DataFrame's float
    column feeds a Pydantic response model, in case that ever changes.
    """
    return {t: (c if pd.notna(c) else None) for t, c in zip(price_df["ticker"], price_df["close"])}


@router.get("/current", response_model=WatchlistResponse)
async def get_watchlist_current() -> WatchlistResponse:
    """Top 20 tickers by mb_probability, from the most recent ml_multibagger date.

    ML24/ML27 (2026-07-11): MultiBagger is the one screen that does not
    exclude sub-recommendation-floor (ADTV < 20cr/day) tickers — it splits
    them into `low_liquidity_tickers` instead of the main `tickers` list,
    each independently top-20-by-mb_probability (config/training_universe.py).
    """
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        latest = conn.execute("SELECT MAX(date) FROM ml_multibagger").fetchone()
        latest_date = latest[0] if latest else None
        if latest_date is None:
            return WatchlistResponse()

        rows = conn.execute(
            f"""
            SELECT {_SELECT_COLS} FROM ml_multibagger
            WHERE date = ? AND mb_probability IS NOT NULL
            ORDER BY mb_probability DESC
            """,
            [latest_date],
        ).fetchall()

    all_df = pd.DataFrame([dict(zip(_COLUMNS, r)) for r in rows])
    if all_df.empty:
        return WatchlistResponse(implemented=True, notes=f"No multibagger scores for {latest_date}.")

    recommendable_df = filter_recommendable(all_df).head(_TOP_N)
    low_liq_df = all_df[~all_df["ticker"].isin(recommendable_df["ticker"])].head(_TOP_N)

    tickers = recommendable_df.to_dict("records")
    low_liquidity_tickers = low_liq_df.to_dict("records")
    return WatchlistResponse(
        tickers=tickers,
        low_liquidity_tickers=low_liquidity_tickers,
        implemented=True,
        notes=(
            f"Top {len(tickers)} multibagger watchlist for {latest_date} (SPEC-UI-003); "
            f"{len(low_liquidity_tickers)} additional sub-Rs20cr-ADTV picks shown separately (ML27)."
        ),
    )


@router.get("/pillar_summary")
async def get_ml_pillar_summary() -> dict:
    """Home page pillar-outcome card: buy-signal count + avg forward
    q50_return for whichever of the 5d/21d/63d horizon models has the most
    recent signal date. No win-rate/success-rate table exists for ML
    signals anywhere in this codebase (unlike Technical's
    strategy_confidence_summary) — top_strategy/success_rate stay null
    rather than fabricating a number; per project memory, ml_signals has
    historically been close to a single-date snapshot, so a small/zero
    recommendation_count here reflects real data availability, not a bug."""
    best: Optional[tuple] = None  # (date, model_name, horizon_label)
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        for model_name, horizon_label, _horizon_days in _HORIZON_MODELS:
            latest = conn.execute(
                "SELECT MAX(date) FROM ml_signals WHERE model_name = ? AND buy_prob IS NOT NULL",
                [model_name],
            ).fetchone()
            query_date = latest[0] if latest else None
            if query_date is not None and (best is None or query_date > best[0]):
                best = (query_date, model_name, horizon_label)

        if best is None:
            return {"as_of_date": None, "available": False, "recommendation_count": 0,
                    "avg_expected_return_pct": None, "top_strategy": None, "top_strategy_success_rate_pct": None}

        query_date, model_name, horizon_label = best
        row = conn.execute(
            """
            SELECT COUNT(*), AVG(q50_return)
            FROM ml_signals
            WHERE date = ? AND model_name = ? AND buy_prob IS NOT NULL
              AND ticker NOT IN (
                  SELECT ticker FROM ml_signals
                  WHERE date = ? AND model_name = 'pnd_detector' AND pnd_block = TRUE
              )
            """,
            [query_date, model_name, query_date],
        ).fetchone()

    count, avg_return = row if row else (0, None)
    return {
        "as_of_date": str(query_date),
        "available": True,
        "recommendation_count": int(count or 0),
        "avg_expected_return_pct": float(avg_return * 100) if avg_return is not None else None,
        "top_strategy": f"ML {horizon_label} signal",
        "top_strategy_success_rate_pct": None,
    }


@router.get("/daily", response_model=DailyWatchlistResponse)
async def get_daily_watchlist(
    date: Optional[str] = Query(None, description="YYYY-MM-DD; defaults to each horizon's latest signal date"),
    n_per_horizon: int = Query(5, ge=1, le=50, description="Top-N buy signals per horizon"),
) -> DailyWatchlistResponse:
    """Daily WatchList: top buy signals for the 5d/21d/63d models plus the
    MultiBagger list, with a realistic (not fixed-%) target price per row.

    Target is q50_return (median forward-return quantile) from the model
    itself when available — q10/q90 give the downside/upside band. If a
    ticker's quantile output is null, falls back to an ATR-derived,
    horizon-scaled band (never a fixed 15%).
    """
    universe = load_universe_raw()
    name_map = dict(zip(universe["ticker"], universe["company_name"].fillna("")))
    sector_map = dict(zip(universe["ticker"], universe["sector"].fillna("")))

    rows: List[DailyWatchlistRow] = []
    resolved_date: Optional[str] = None

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        for model_name, horizon_label, horizon_days in _HORIZON_MODELS:
            if date:
                query_date: Optional[str] = date
            else:
                latest = conn.execute(
                    "SELECT MAX(date) FROM ml_signals WHERE model_name = ? AND buy_prob IS NOT NULL",
                    [model_name],
                ).fetchone()
                query_date = latest[0] if latest else None
            if query_date is None:
                continue
            if resolved_date is None:
                resolved_date = str(query_date)

            # ML24 (2026-07-11): over-fetch by a buffer, then apply the ADTV
            # recommendation floor in Python and truncate back to
            # n_per_horizon — a straight SQL LIMIT here would let sub-floor
            # tickers occupy slots that should go to recommendable ones.
            sig_rows_raw = conn.execute(
                """
                SELECT ticker, buy_prob, signal_direction, q10_return, q50_return, q90_return
                FROM ml_signals
                WHERE date = ? AND model_name = ? AND buy_prob IS NOT NULL
                  AND ticker NOT IN (
                      SELECT ticker FROM ml_signals
                      WHERE date = ? AND model_name = 'pnd_detector' AND pnd_block = TRUE
                  )
                ORDER BY buy_prob DESC
                LIMIT ?
                """,
                [query_date, model_name, query_date, n_per_horizon * 5],
            ).fetchall()
            if not sig_rows_raw:
                continue

            raw_df = pd.DataFrame(sig_rows_raw, columns=["ticker", "buy_prob", "signal_direction", "q10_return", "q50_return", "q90_return"])
            filtered_df = filter_recommendable(raw_df)
            sig_rows = list(filtered_df.head(n_per_horizon).itertuples(index=False, name=None))
            if not sig_rows:
                continue

            tickers = [r[0] for r in sig_rows]
            placeholders = ", ".join("?" * len(tickers))
            with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as pconn:
                price_df = pconn.execute(
                    f"""
                    SELECT ticker, close FROM ohlcv_adjusted
                    WHERE ticker IN ({placeholders})
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) = 1
                    """,
                    tickers,
                ).fetchdf()
            price_map = _build_price_map(price_df)

            for ticker, buy_prob, direction, q10, q50, q90 in sig_rows:
                price = price_map.get(ticker)
                target_price = target_low = target_high = expected_return_pct = None
                basis = "quantile"

                if price is not None and q50 is not None:
                    target_price = round(price * (1 + q50), 2)
                    target_low = round(price * (1 + q10), 2) if q10 is not None else None
                    target_high = round(price * (1 + q90), 2) if q90 is not None else None
                    expected_return_pct = round(q50 * 100, 2)
                elif price is not None:
                    frow = read_feature_row(ticker, str(query_date))
                    atr_pct = None
                    if frow is not None and "atr_14_pct" in frow and pd.notna(frow["atr_14_pct"]):
                        atr_pct = float(frow["atr_14_pct"])
                    if atr_pct is not None:
                        # Volatility-scaled band (sqrt-of-time), not a fixed percentage.
                        band = (atr_pct / 100) * _ATR_MULTIPLIER * (horizon_days ** 0.5)
                        target_price = round(price * (1 + band), 2)
                        target_low = round(price * (1 - band / 2), 2)
                        target_high = round(price * (1 + band * 1.5), 2)
                        expected_return_pct = round(band * 100, 2)
                        basis = "atr"
                    else:
                        basis = "unavailable"
                else:
                    basis = "unavailable"

                rows.append(DailyWatchlistRow(
                    ticker=ticker,
                    company_name=name_map.get(ticker) or None,
                    sector=sector_map.get(ticker) or None,
                    horizon=horizon_label,
                    horizon_days=horizon_days,
                    current_price=round(float(price), 2) if price is not None else None,
                    buy_prob=buy_prob,
                    signal_direction=direction,
                    target_price=target_price,
                    target_low=target_low,
                    target_high=target_high,
                    expected_return_pct=expected_return_pct,
                    target_basis=basis,
                ))

    multibagger_resp = await get_watchlist_current()

    return DailyWatchlistResponse(
        date=resolved_date,
        rows=rows,
        multibagger=multibagger_resp.tickers,
        low_liquidity_multibagger=multibagger_resp.low_liquidity_tickers,
        count=len(rows),
    )
