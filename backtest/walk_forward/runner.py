"""
backtest/walk_forward/runner.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 2.5
Owner: Platform / Backtest
Consumers: Phase 6's fine-tuning loop (reads refit_log to decide whether
a retrain helped), Phase 3's unified API (mode="walk_forward" runs)

Design note — why there's no separate day_driver.py module: the original
plan sketched `backtest/walk_forward/day_driver.py` as a loop shared with
Phase 5's `paper_trading/live_runner.py`, distinct from
`backtest/core/engine.py`. Building it, that turned out to be
`core/engine.py`'s `BacktestOrchestrator` itself — "advance one period,
retrain-if-due, generate signals, size, execute, log" is exactly
`BacktestOrchestrator.run()`'s loop (refit hook added this phase), and
the walk-forward-vs-paper-trading distinction is entirely about WHERE
`OrchestratorConfig.price_lookup`/`universe_provider` read from (a
historical DB slice here; the live daily pipeline in Phase 5) and
whether trades are auto-executed or queued for human approval (Phase 5
only) — neither needs a second copy of the loop. So `WalkForwardRunner`
here is a thin config/period-log wrapper around the SAME
`BacktestOrchestrator`, and Phase 5's `live_runner.py` will be the same
again with a live `DataSource` and an approval gate layered on top,
rather than three independent implementations of one loop.

Point-in-time safety (the mandatory lookahead-leakage test,
BacktestUmbrellaPlan.md Phase 2.5): every Phase 2 adapter already reads
data through a strictly `as_of_date`-scoped path (momentum's
trailing_momentum_from_panel filters `available_dates <= as_of_ts`;
technical/fundamental read exactly one day's feature Parquet file per
call) — so leakage safety is a structural property of the adapters
themselves, not something this runner has to additionally enforce. The
leakage test (test_walk_forward_runner.py) verifies this empirically
rather than trusting it by construction.
"""

import logging
from typing import Optional

from backtest.core.engine import BacktestOrchestrator, OrchestratorConfig, StrategyAdapter
from backtest.core.horizon import sizing_for
from backtest.core.run_context import BacktestRun, BacktestRunResult

logger = logging.getLogger(__name__)


class WalkForwardRunner:
    """
    Runs a BacktestRun with mode="walk_forward": historical replay from
    run.start_date to run.end_date, calling adapter.refit(as_of_date) (if
    the adapter implements it) at the horizon bucket's default cadence
    unless config.refit_cadence_days overrides it.
    """

    def __init__(self, feature_log_writer=None) -> None:
        self._orchestrator = BacktestOrchestrator(feature_log_writer=feature_log_writer)

    def run(
        self, run: BacktestRun, adapter: StrategyAdapter, config: OrchestratorConfig,
        refit_cadence_days: Optional[int] = None,
    ) -> BacktestRunResult:
        if run.mode != "walk_forward":
            raise ValueError(f"WalkForwardRunner requires run.mode='walk_forward', got {run.mode!r}")

        # Per-horizon-bucket retrain cadence, confirmed 2026-07-20 (matches the
        # user's "learn based on signals generated that month" example for a
        # 21-day/monthly-cadence strategy) — reuses the same table as position
        # sizing/rebalancing rather than taking an independent cadence input,
        # unless the caller explicitly overrides it.
        cadence = refit_cadence_days or sizing_for(run.horizon_bucket).default_rebalance_cadence_days
        effective_config = OrchestratorConfig(
            trading_days=config.trading_days, universe_provider=config.universe_provider,
            price_lookup=config.price_lookup, sector_lookup=config.sector_lookup,
            is_delisted=config.is_delisted, rebalance_cadence_days=config.rebalance_cadence_days,
            refit_cadence_days=cadence,
        )
        return self._orchestrator.run(run, adapter, effective_config)
