"""
backtest/adapters/ml_adapter.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 1
Owner: Platform / Backtest
Consumers: Phase 3's unified backtest_runs API/UI (to show ML alongside
Technical/Fundamental/Momentum in the same results view)

Per the confirmed 2026-07-20 decision ("backtest/engine.py: wrap, don't
refactor" — BacktestUmbrellaPlan.md), backtest/engine.py::BacktestEngine
is NOT MODIFIED and does not implement backtest.core.engine.StrategyAdapter.

This is deliberate, not a shortcut: BacktestEngine.run_full_backtest()
already IS a complete, self-contained walk-forward backtest — it owns its
own fold-splitting, its own P&D -> Signal -> MetaLabel -> Exit model
training loop, and its own PortfolioSimulator. Decomposing that into
core/engine.py's per-rebalance-date generate_signals() protocol would
mean re-implementing BacktestEngine's internals against a different
control flow — exactly the "refactor a live module" risk the confirmed
decision explicitly avoided.

Instead, this module is a RESULT-SCHEMA TRANSLATOR: it calls
BacktestEngine.run_full_backtest() unchanged, then maps its
BacktestResults/FoldResult output onto the shared BacktestRunResult /
core.metrics.BacktestMetrics shape, so Phase 3's unified API/UI can list
an ML run in the same table as a Technical/Fundamental/Momentum run.

Known field gaps in the translation (documented, not silently backfilled
— No-Mock-Data Policy): BacktestEngine's own pipeline doesn't compute
XIRR, Sortino, Calmar, turnover_ratio, cash_position_series, or FY-netted
tax — those fields are left None/empty in the translated output rather
than fabricated. cagr_trading_day_legacy is populated directly from
FoldResult.cagr (already trading-day-annualized, matching
core.metrics.trading_day_cagr's methodology); the primary calendar/365.25
cagr field is left None since BacktestEngine doesn't report enough date
precision per-fold to recompute it faithfully without guessing.
"""

from typing import Any, Dict

from backtest.core.run_context import BacktestRun, BacktestRunResult
from backtest.engine import BacktestResults


def channel() -> str:
    return "ml"


def wrap_ml_backtest_result(run: BacktestRun, engine_results: BacktestResults) -> BacktestRunResult:
    """
    Translate an existing, unmodified BacktestEngine.run_full_backtest()
    result into the shared BacktestRunResult schema.

    Parameters
    ----------
    run : BacktestRun
        Must have run.channel == "ml".
    engine_results : BacktestResults
        The return value of BacktestEngine.run_full_backtest(), called
        by the caller BEFORE invoking this function — this module never
        constructs or drives a BacktestEngine itself, only translates
        its already-produced output.
    """
    if run.channel != "ml":
        raise ValueError(f"ml_adapter is only valid for channel='ml' runs, got {run.channel!r}")

    aggregate: Dict[str, Any] = engine_results.aggregate or {}
    metrics = {
        "cagr": None,  # see module docstring: not faithfully recomputable from FoldResult alone
        "cagr_trading_day_legacy": aggregate.get("cagr"),
        "xirr": None,
        "final_capital": aggregate.get("final_equity"),
        "total_contributed": run.initial_capital,  # BacktestEngine has no SIP concept — lump-sum only
        "max_drawdown": aggregate.get("max_drawdown"),
        "win_rate": aggregate.get("win_rate"),
        "profit_factor": aggregate.get("profit_factor"),
        "sortino": None,
        "calmar": None,
        "n_distinct_tickers_traded": None,  # BacktestResults doesn't track this per-run; see Phase 2 note below
        "turnover_ratio": None,
        "n_trades": aggregate.get("n_trades"),
        "benchmark_cagr": aggregate.get("benchmark_cagr"),
        "excess_return": aggregate.get("excess_return"),
        "benchmark_status": "ok" if aggregate.get("benchmark_cagr") is not None else "insufficient_benchmark_history",
        "cash_position_series": [],
    }

    return BacktestRunResult(
        run=run,
        metrics=metrics,
        data_gaps=[],  # BacktestEngine raises rather than silently gapping (see its _build_dataset() docstring)
        integrity_passed=engine_results.integrity_passed,
        integrity_detail=engine_results.integrity_detail,
    )
