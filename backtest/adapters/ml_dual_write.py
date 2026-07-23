"""
backtest/adapters/ml_dual_write.py

Phase: Unified Backtest & Paper Trading Umbrella, open item #4
("Dual-write run_phase{1,2,3}_backtest.py into the unified backtest_runs
schema" — BacktestUmbrellaPlan.md, Sequencing Dependencies / open items)
Owner: Platform / Backtest
Consumers: backtest/run_phase1_backtest.py, run_phase2_backtest.py,
run_phase3_backtest.py

Additive-only helper: takes a real, already-produced BacktestEngine
BacktestResults object (never re-runs anything, never fabricates a
result) and writes it into the unified `backtest_runs` DuckDB table via
ml_adapter.py's existing result-schema translator + run_store.py, so
these three CLI scripts' real historical runs show up in the same
unified list as Technical/Fundamental/Momentum runs (Phase 3/4 UI).

Per "wrap, don't refactor": none of backtest/engine.py, ml_adapter.py,
or run_store.py are modified by this module — it only composes them.
Failures here (e.g. DB lock, schema not yet migrated) are logged and
swallowed, never raised, so a report script's primary job (writing its
JSON report / printing the gate result) can never be broken by this
being best-effort, additive bookkeeping.
"""

import logging
from datetime import date as date_type
from typing import Any, Optional

import pandas as pd

from backtest.adapters.ml_adapter import wrap_ml_backtest_result
from backtest.core.horizon import HorizonBucket
from backtest.core.run_context import BacktestRun
from backtest.core.run_store import save_run_result
from backtest.engine import BacktestResults
from config.settings import BACKTEST_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

_HORIZON_DAYS_TO_BUCKET = {
    5: HorizonBucket.D5,
    21: HorizonBucket.D21,
    63: HorizonBucket.D63,
}


def _as_date(value: Any) -> date_type:
    if isinstance(value, date_type):
        return value
    return pd.Timestamp(value).date()


def dual_write_ml_run(
    engine_results: BacktestResults,
    strategy_id: str,
    horizon_days: int,
    ohlcv: pd.DataFrame,
    initial_capital: float,
    random_seed: int,
    universe_spec: str = "config.universe.get_tickers()",
) -> Optional[str]:
    """
    Best-effort: translate + persist one real BacktestEngine run into the
    unified backtest_runs table. Returns the run_id on success, None on
    any failure (logged, never raised — see module docstring).
    """
    bucket = _HORIZON_DAYS_TO_BUCKET.get(horizon_days)
    if bucket is None:
        logger.warning(f"ml_dual_write: no HorizonBucket mapping for horizon_days={horizon_days}, skipping")
        return None

    try:
        start_date = _as_date(ohlcv["date"].min())
        end_date = _as_date(ohlcv["date"].max())
        run = BacktestRun(
            channel="ml",
            strategy_id=strategy_id,
            horizon_bucket=bucket,
            mode="backtest",
            universe_spec=universe_spec,
            start_date=start_date,
            end_date=end_date,
            capital_mode="lump",
            initial_capital=initial_capital,
            random_seed=random_seed,
            config={"model_name": engine_results.model_name, "horizon_days": horizon_days},
        )
        result = wrap_ml_backtest_result(run, engine_results)
        with get_duckdb_connection(BACKTEST_DUCKDB_PATH) as conn:
            save_run_result(conn, result)
        logger.info(f"ml_dual_write: saved run_id={run.run_id} ({strategy_id})")
        return run.run_id
    except Exception:
        logger.exception(f"ml_dual_write: failed to dual-write run for strategy_id={strategy_id!r} (non-fatal)")
        return None
