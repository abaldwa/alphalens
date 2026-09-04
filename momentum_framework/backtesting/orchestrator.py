"""
BacktestOrchestrator - native simulation engine, with legacy-report
normalization retained for the migration window.

NATIVE EXECUTION PORTED 2026-09-04 (run_native()) — trading-calendar-
driven loop: resolves each rebalance date's band-scoped universe
(StrategyAdapter.resolve_universe()), calls strategy.rebalance(),
executes the returned Signals into a Portfolio (backtesting/portfolio.py),
marks to market and calls strategy.update_portfolio_equity() every
trading day (not just rebalance days — R08/R09's exposure multiplier
needs a real daily equity history), and computes metrics via
metrics.standard.MetricsCalculator on the resulting equity curve.

This is deliberately simpler than backtest/core/engine.py (no tax lots,
no slippage, no FY settlement — see backtesting/portfolio.py's
docstring) — it exists to make trade-by-trade PARITY CHECKING against
the legacy engine possible at all (nothing to diff against without a
real simulation), not to replace the legacy engine's sophistication yet.

run() (legacy report normalization) is UNCHANGED and still used when a
caller already has a legacy engine report.json to normalize — the two
paths coexist; run_native() does not replace run(), it adds the
previously-missing "actually simulate it" capability.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import logging

import pandas as pd

from momentum_framework.backtesting.adapter import StrategyAdapter
from momentum_framework.backtesting.portfolio import Portfolio
from momentum_framework.backtesting.result import BacktestResult
from momentum_framework.metrics.nomenclature import build_strategy_id
from momentum_framework.metrics.standard import MetricsCalculator

logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """Everything the orchestrator needs beyond what the strategy already knows."""
    start_date: str
    end_date: str = "2026-06-30"
    initial_capital: float = 1_000_000.0
    max_tickers: int = 800
    min_history_days: int = 60
    capital_mode: str = "lump"
    exit_variant: str = "unconstrained"  # "baseline" is a RETIRED legacy exit policy — see docs/CODE_TRACEABILITY.md
    ohlcv_snapshot_dir: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


class BacktestOrchestrator:
    """
    Runs a single StrategyAdapter over a date range and returns a
    standardized BacktestResult.

    Delegates the actual simulation to backtest/run_orchestrator_backtest.py
    (the existing, validated engine) during the migration window — see
    module docstring.
    """

    def __init__(self, strategy: StrategyAdapter, config: BacktestConfig):
        self.strategy = strategy
        self.config = config

    def build_legacy_job_spec(self) -> Dict[str, Any]:
        """
        Translate this orchestrator's strategy+config into the job dict
        shape backtest/run_strategy_queue.py already knows how to execute.
        Keeping this translation in one place is what makes the "rerun
        under the new framework, compare against the old numbers" migration
        step tractable — every strategy file only has to get describe()
        right, not reimplement queue-job serialization.
        """
        params = self.strategy.describe()
        return {
            "kind": "orchestrator",
            "channel": "momentum",
            "start_date": self.config.start_date,
            "end_date": self.config.end_date,
            "rank_band_id": params["band_id"],
            "top_n": params["top_n"],
            "lookback_months": params["lookback_months"],
            "rebalance_cadence_days": params["rebalance_cadence_days"],
            "rank_method": params["rank_method"],
            "strategy_family": params["strategy_code"],
            "initial_capital": self.config.initial_capital,
            "max_tickers": self.config.max_tickers,
            "min_history_days": self.config.min_history_days,
            "capital_mode": self.config.capital_mode,
            "exit_variant": self.config.exit_variant,
            "ohlcv_snapshot_dir": self.config.ohlcv_snapshot_dir,
            **{k: v for k, v in params.items()
               if k not in {"strategy_code", "rank_method", "band_id", "top_n",
                            "lookback_months", "rebalance_cadence_days"}},
            **self.config.extra,
        }

    def run(self, report_dict: Dict[str, Any]) -> BacktestResult:
        """
        Normalize an ALREADY-RUN legacy engine report.json into a
        standardized BacktestResult (e.g. one produced by
        backtest/run_strategy_queue.py) — used for the parity-verification
        step in docs/MIGRATION.md, where the same config is run through
        BOTH the legacy engine and run_native() and the two results are
        diffed. For actually EXECUTING a backtest with this framework, use
        run_native() instead.
        """
        return self._normalize_report(report_dict)

    def run_native(self, conn: Any) -> BacktestResult:
        """
        Actually simulate the backtest: trading-calendar-driven loop,
        band-scoped ranking, Portfolio execution, daily mark-to-market
        feeding update_portfolio_equity(), metrics via MetricsCalculator.
        See this module's docstring for what's deliberately simplified
        relative to backtest/core/engine.py.
        """
        calendar = self._trading_calendar(conn)
        if not calendar:
            raise ValueError(
                f"No trading days found in ohlcv_adjusted for "
                f"[{self.config.start_date}, {self.config.end_date}] — check the date range"
            )

        # Match legacy's REAL behavior (explicit user decision 2026-09-04,
        # see common/signals.py::MomentumSignal.floor_date docstring): no
        # signal may see OHLCV data before this backtest's own start_date,
        # even though ohlcv_adjusted has data back to 2005 — legacy never
        # reaches back either, so every strategy sits idle at the start of
        # any window until its longest lookback warms up INSIDE it, exactly
        # like the published legacy baseline does.
        if hasattr(self.strategy, "signal"):
            self.strategy.signal.floor_date = self.config.start_date

        rebalance_dates = set(calendar[::self.strategy.rebalance_cadence_days])
        rebalance_dates.add(calendar[0])  # always establish an initial basket

        portfolio = Portfolio(self.config.initial_capital)
        equity_curve: Dict[str, float] = {}

        for as_of_date in calendar:
            if as_of_date in rebalance_dates:
                universe = self.strategy.resolve_universe(as_of_date, conn)
                if universe:
                    signals = self.strategy.rebalance(as_of_date, universe, conn)
                    if signals:
                        # Union of signal tickers AND currently-held tickers —
                        # a position being SOLD (held, absent from this
                        # rebalance's target) still needs today's real price,
                        # not Portfolio._sell()'s stale-entry-price fallback.
                        needed = {s.ticker for s in signals} | set(portfolio.positions.keys())
                        prices = self._closes_on(conn, list(needed), as_of_date)
                        portfolio.rebalance_to_target(signals, prices, as_of_date)

            held_prices = self._closes_on(conn, list(portfolio.positions.keys()), as_of_date)
            equity = portfolio.market_value(held_prices)
            equity_curve[as_of_date] = equity
            self.strategy.update_portfolio_equity(as_of_date, equity)

        equity_series = pd.Series(equity_curve)
        equity_series.index = pd.to_datetime(equity_series.index)
        equity_series = equity_series.sort_index()

        metrics = MetricsCalculator().compute(
            equity_series, trade_count=len(portfolio.trade_log),
        ).to_dict()

        params = self.strategy.describe()
        # Filter to only parameters that build_strategy_id() accepts
        identity_fields = {
            "filter_preset", "crash_regime_enabled", "vol_scaling_mode",
            "weight_method", "skip_months", "vol_target_enabled",
            "vol_target_pct", "liquidity_quintile",
        }
        identity_params = {k: v for k, v in params.items() if k in identity_fields}
        strategy_id = build_strategy_id(
            strategy_code=params["strategy_code"],
            rank_method=params["rank_method"],
            band_id=params["band_id"],
            top_n=params["top_n"],
            lookback_months=params["lookback_months"],
            rebalance_cadence_days=params["rebalance_cadence_days"],
            **identity_params,
        )

        from datetime import datetime, timezone

        from momentum_framework.common.git_provenance import get_source_commit
        provenance = get_source_commit()
        run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

        return BacktestResult(
            run_id=f"native_{strategy_id}_{provenance.commit_short}_{run_timestamp}",
            strategy_id=strategy_id,
            config=self.strategy.describe(),
            metrics=metrics,
            trades=list(portfolio.trade_log),  # copy — see BacktestResult.trades docstring
            trade_count=len(portfolio.trade_log),
            integrity_passed=True,
            integrity_detail={
                "engine": "native", "trading_days": len(calendar),
                "source_commit_dirty": provenance.is_dirty,
            },
            source_commit=provenance.commit_hash,
        )

    def _trading_calendar(self, conn: Any) -> List[str]:
        rows = conn.execute(
            "SELECT DISTINCT date FROM ohlcv_adjusted WHERE date >= ? AND date <= ? ORDER BY date",
            [self.config.start_date, self.config.end_date],
        ).fetchall()
        return [str(r[0]) for r in rows]

    def _closes_on(self, conn: Any, tickers: List[str], as_of_date: str) -> Dict[str, float]:
        if not tickers:
            return {}
        placeholders = ",".join("?" for _ in tickers)
        rows = conn.execute(
            f"SELECT ticker, close FROM ohlcv_adjusted WHERE ticker IN ({placeholders}) AND date = ?",
            list(tickers) + [as_of_date],
        ).fetchall()
        return {ticker: close for ticker, close in rows}

    def _normalize_report(self, report: Dict[str, Any]) -> BacktestResult:
        run = report.get("run", {})
        config = run.get("config", {})
        metrics = report.get("metrics", {})

        params = self.strategy.describe()
        # Filter to only parameters that build_strategy_id() accepts
        identity_fields = {
            "filter_preset", "crash_regime_enabled", "vol_scaling_mode",
            "weight_method", "skip_months", "vol_target_enabled",
            "vol_target_pct", "liquidity_quintile",
        }
        identity_params = {k: v for k, v in params.items() if k in identity_fields}
        strategy_id = build_strategy_id(
            strategy_code=params["strategy_code"],
            rank_method=params["rank_method"],
            band_id=params["band_id"],
            top_n=params["top_n"],
            lookback_months=params["lookback_months"],
            rebalance_cadence_days=params["rebalance_cadence_days"],
            **identity_params,
        )

        return BacktestResult(
            run_id=run.get("run_id", "unknown"),
            strategy_id=strategy_id,
            config=config,
            metrics=metrics,
            trade_log_path=report.get("trade_log_path"),
            integrity_passed=report.get("integrity_passed", False),
            integrity_detail=report.get("integrity_detail", {}),
            data_gaps=report.get("data_gaps", []),
        )
