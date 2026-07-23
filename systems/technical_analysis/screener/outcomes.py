"""
systems/technical_analysis/screener/outcomes.py

Phase: post-Phase-3 (Technical Analysis Screener confidence rework)
Owner: Technical Analysis / Screener
Consumers: scripts/compute_strategy_confidence.py,
           datastore/api/routers/technical.py

Thin TA-screener-specific adapter over the general
backtest/strategy_confidence.py evaluator (which replaced this module's
previous, rejected touch-based resistance/support win-loss computation —
see that module's docstring for the full rationale). All logic beyond
"build the (date, ticker, strategy_id, direction) signal list and hand it
to the shared evaluator" lives in strategy_confidence.py so future
strategy families (momentum, ML signal models) reuse it without their own
bespoke outcome-computation module.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

import pandas as pd

from backtest import strategy_confidence as sc
from config.settings import DUCKDB_PATH, SIGNALS_DUCKDB_PATH
from config.training_universe import RECOMMENDATION_ADTV_FLOOR_CR, filter_recommendable
from config.universe import load_universe_raw
from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

# All 42 TA screener templates are BUY-only setups (confirmed against
# systems/technical_analysis/screener/templates.py — no short/sell
# template exists), so every signal is evaluated long.
TA_DIRECTION = "long"


def build_signal_events(
    signals_db_path=SIGNALS_DUCKDB_PATH,
    *,
    since: Optional[str] = None,
    adtv_floor_cr: float = RECOMMENDATION_ADTV_FLOOR_CR,
) -> List[sc.SignalEvent]:
    """Reads ta_signals and applies the same liquidity floor the live
    watchlist uses (config.training_universe.filter_recommendable) before
    evaluation — the rejected prior implementation scored every ticker
    ta_signals ever produced, including illiquid names a real trade could
    never fill at the recommended size/price."""
    with get_duckdb_connection(signals_db_path, persist=False, read_only=True) as conn:
        tables = [r[0] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'ta_signals'"
        ).fetchall()]
        if not tables:
            return []

        query = "SELECT date, ticker, template_name FROM ta_signals"
        params: list = []
        if since:
            query += " WHERE date >= ?"
            params.append(since)
        df = conn.execute(query, params).fetchdf()

    if df.empty:
        return []

    universe = load_universe_raw()
    df = filter_recommendable(df, universe=universe, floor_cr=adtv_floor_cr)
    if df.empty:
        return []

    return [
        sc.SignalEvent(date=pd.Timestamp(d), ticker=str(t), strategy_id=str(s), direction=TA_DIRECTION)
        for d, t, s in zip(df["date"], df["ticker"], df["template_name"])
    ]


def load_market_regime(signals_db_path=SIGNALS_DUCKDB_PATH) -> pd.DataFrame:
    """Real market-wide HMM regime history (ml_signals, ticker='MARKET',
    model_name='hmm_market' — see datastore/api/routers/regime.py). Empty
    DataFrame (not an error) if no regime rows exist yet; the evaluator
    treats every date before the first regime row, or when this is empty,
    as strategy_confidence.REGIME_UNKNOWN rather than guessing."""
    with get_duckdb_connection(signals_db_path, persist=False, read_only=True) as conn:
        tables = [r[0] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'ml_signals'"
        ).fetchall()]
        if not tables:
            return pd.DataFrame(columns=["date", "hmm_regime"])
        df = conn.execute(
            "SELECT date, hmm_regime FROM ml_signals WHERE ticker = 'MARKET' AND model_name = 'hmm_market' ORDER BY date"
        ).fetchdf()
    return df


def compute_and_store_ta_confidence(
    *,
    since: Optional[str] = None,
    horizon_days: int = 5,
    win_threshold_pct: float = 0.0,
    chunk_size_dates: int = 20,
) -> Dict[str, sc.ConfidenceResult]:
    """Builds TA screener signal events, evaluates them against the shared
    strategy_confidence framework, and persists both per-signal detail and
    the per-strategy/per-regime confidence summary. `n_strategies_compared`
    is fixed at 42 (the real number of registered templates — see
    systems/technical_analysis/screener/templates.py::TEMPLATES) so the
    Deflated Sharpe correction reflects the actual number of strategies
    being compared side by side on the win-rates screen, not just however
    many happen to have fired signals in this particular run."""
    from systems.technical_analysis.screener.templates import TEMPLATES

    signals = build_signal_events(since=since)
    if not signals:
        return {}

    tickers = sorted({s.ticker for s in signals})
    since_bound = min(s.date for s in signals) - pd.Timedelta(days=400)
    placeholders = ",".join("?" * len(tickers))
    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        ohlcv = conn.execute(
            f"""
            SELECT ticker, date, open, high, low, close FROM ohlcv_adjusted
            WHERE ticker IN ({placeholders}) AND date >= ?
            ORDER BY ticker, date
            """,
            tickers + [since_bound.date()],
        ).fetchdf()

    if ohlcv.empty:
        return {}

    regime_df = load_market_regime()

    def _log_chunk_progress(i: int, n_chunks: int, n_rows: int) -> None:
        logger.info("TA strategy confidence: chunk %d/%d persisted (%d detail rows written)", i, n_chunks, n_rows)

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=False) as conn:
        sc.create_outcomes_table(conn)
        results = sc.evaluate_signals_chunked(
            signals, ohlcv, conn, regime_df=regime_df, horizon_days=horizon_days,
            win_threshold_pct=win_threshold_pct, n_strategies_compared=len(TEMPLATES),
            chunk_size_dates=chunk_size_dates, on_chunk_persisted=_log_chunk_progress,
        )
        n_summary = sc.persist_summary(conn, results)
    logger.info("TA strategy confidence: %d strategies summarized, %d summary rows written", len(results), n_summary)

    return results
