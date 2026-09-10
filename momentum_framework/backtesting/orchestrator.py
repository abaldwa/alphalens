"""
BacktestOrchestrator - native simulation engine, with legacy-report
normalization retained for the migration window.

NATIVE EXECUTION PORTED 2026-09-04 (run_native()) — trading-calendar-
driven loop: resolves each rebalance date's band-scoped universe
(StrategyAdapter.resolve_universe()), calls strategy.rebalance() with
real Portfolio ground truth (held positions, realized equity curve so
far — see StrategyAdapter.rebalance()'s PURE-FUNCTION CONTRACT docstring,
explicit user instruction 2026-09-04: strategies must not own mutable
state duplicating what the orchestrator already tracks), executes the
returned Signals into a Portfolio (backtesting/portfolio.py), marks to
market every trading day (not just rebalance days — R08/R09's exposure
multiplier needs a real daily equity history), and computes metrics via
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
        # ticker -> {date_str: close}, populated lazily by _closes_on() —
        # see that method's docstring for why (perf fix 2026-09-10: a
        # per-trading-day point query for mark-to-market, run unconditionally
        # for EVERY day not just rebalance days, was ~60% of total job time
        # on a full 2009-2026 run: 4333 days x ~6ms/query = ~27s/job versus
        # <1s for one bulk fetch per ticker across its whole history).
        self._price_cache: Dict[str, Dict[str, float]] = {}

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
            # Enables the momentum_rank_snapshots cache fast-path in
            # TrailingMomentumSignal.compute() (common/signals.py) — see
            # that module's MomentumSignal.band_id docstring. Safe
            # regardless of this strategy's start_date or any further
            # universe narrowing it does (R07's circuit-lock filter, R12's
            # liquidity_quintile) — see common/momentum_rank_cache.py's
            # is_floor_eligible() docstring for why one unbounded cache
            # serves every floor_date correctly.
            self.strategy.signal.band_id = self.strategy.band_id

        rebalance_dates = set(calendar[::self.strategy.rebalance_cadence_days])
        rebalance_dates.add(calendar[0])  # always establish an initial basket

        portfolio = Portfolio(self.config.initial_capital)
        # Parallel lists, not a {date_str: float} dict rebuilt into a Series
        # every rebalance call (performance fix, 2026-09-04 — the original
        # per-call `pd.Series(dict) + pd.to_datetime(strings)` reconversion
        # nearly doubled a full 2009-2026 run's wall time, since it re-
        # parsed the ENTIRE accumulated history from scratch on every one
        # of ~200 rebalance calls). Timestamps are pre-converted once, on
        # the day they're recorded, so building `equity_so_far` at
        # rebalance time is a cheap Series-from-already-typed-lists
        # construction, not a string-reparsing pass over growing history.
        equity_dates: List[pd.Timestamp] = []
        equity_values: List[float] = []

        for as_of_date in calendar:
            if as_of_date in rebalance_dates:
                universe = self.strategy.resolve_universe(as_of_date, conn)
                if universe:
                    # FRAMEWORK-LEVEL circuit-lock exclusion (Category B/data-
                    # quality item #2, 2026-09-06, explicit user instruction:
                    # "exclude ADTV/circuit locked trades out of our universe,
                    # you will never be able to buy them"). Previously this was
                    # opt-in and R07-only (common/liquidity.py::filter_tradeable,
                    # applied only to R07's new_entrants) — every other strategy
                    # (R01/R03/R09/R11/R12/R13/R14-R17) could select a circuit-
                    # locked ticker as a fresh buy signal, which Portfolio would
                    # then execute as a phantom fill at a price that was never
                    # actually tradeable that day. Filtering HERE, once, before
                    # any strategy sees the universe, fixes it for all of them
                    # uniformly with no per-strategy code required. ADTV floor
                    # is deliberately NOT applied here — R12's liquidity-quintile
                    # design needs the full liquidity spectrum, including
                    # illiquid names, as a first-class research variable; an
                    # ADTV floor stays a strategy-level opt-in (min_adtv_cr).
                    #
                    # This prevents NEW buys of a locked name. A currently-
                    # HELD ticker that becomes locked on a rebalance date is
                    # handled separately, in Portfolio.rebalance_to_target()
                    # itself (skips the sell, keeps holding until it
                    # unlocks — fixed 2026-09-06, see that method's comment).
                    # 2026-09-06: routed through get_circuit_locked_tickers_auto()
                    # (common/liquidity.py) — prefers the pre-materialized
                    # circuit_lock_snapshots cache (~instant) over a live
                    # per-rebalance query (~7ms), same reasoning as the
                    # existing momentum-rank cache. See that function's
                    # docstring and scripts/build_liquidity_cache.py.
                    from momentum_framework.common.liquidity import get_circuit_locked_tickers_auto
                    locked = get_circuit_locked_tickers_auto(conn, universe, as_of_date)
                    if locked:
                        universe = [t for t in universe if t not in locked]
                if universe and self.strategy.extra_params.get("exclude_extraordinary_returns", False):
                    # Sensitivity-analysis toggle (2026-09-06, see
                    # config/extraordinary_return_tickers.py) — strips the
                    # top-N P&L-contributing tickers from the tradeable
                    # universe, for an explicit with/without comparison
                    # rather than a permanent data-quality exclusion (see
                    # config/backtest_exclusions.py's, which this is
                    # deliberately kept separate from). N is configurable
                    # (extraordinary_returns_top_n, default 15) — user
                    # decision 2026-09-06, was a fixed top-30 list before.
                    from config.extraordinary_return_tickers import extraordinary_return_tickers
                    top_n = self.strategy.extra_params.get("extraordinary_returns_top_n", 15)
                    outliers = extraordinary_return_tickers(top_n=top_n)
                    universe = [t for t in universe if t not in outliers]
                if universe:
                    # PURE-FUNCTION CONTRACT (see StrategyAdapter.rebalance()'s
                    # docstring) — `held` is REAL post-execution ground truth
                    # from Portfolio (not a strategy's own memory of what it
                    # last requested, which can silently diverge if a buy
                    # failed — see that docstring for the R07 incident this
                    # replaced), and `equity_curve` is the Portfolio's own
                    # realized daily series so far, replacing the old
                    # update_portfolio_equity() mutation hook.
                    held = frozenset(portfolio.positions.keys())
                    equity_so_far = pd.Series(equity_values, index=equity_dates, dtype=float)
                    signals = self.strategy.rebalance(as_of_date, universe, conn, held, equity_so_far)
                    # Generic position-sizing pass (2026-09-04): "equal" is a
                    # no-op; "inverse_volatility" resizes buy signals via
                    # StrategyBase.size_signals() so ANY strategy's own
                    # ranking signal can also run weighted, not only
                    # R14-R17 (which size themselves inline and are skipped
                    # here via has_own_weighting — see StrategyBase docstring).
                    if signals and hasattr(self.strategy, "size_signals"):
                        signals = self.strategy.size_signals(signals, as_of_date, conn)
                    if signals:
                        # Union of signal tickers AND currently-held tickers —
                        # a position being SOLD (held, absent from this
                        # rebalance's target) still needs today's real price,
                        # not Portfolio._sell()'s stale-entry-price fallback.
                        needed = {s.ticker for s in signals} | set(portfolio.positions.keys())
                        prices = self._closes_on(conn, list(needed), as_of_date)
                        portfolio.rebalance_to_target(signals, prices, as_of_date, conn=conn)

            held_prices = self._closes_on(conn, list(portfolio.positions.keys()), as_of_date)
            equity = portfolio.market_value(held_prices)
            equity_dates.append(pd.Timestamp(as_of_date))
            equity_values.append(equity)

        equity_series = pd.Series(equity_values, index=equity_dates, dtype=float).sort_index()

        metrics = MetricsCalculator().compute(
            equity_series, trade_count=len(portfolio.trade_log),
        ).to_dict()

        # Post-tax overlay (2026-09-06, see common/tax_overlay.py's module
        # docstring) — ADDITIONAL fields merged into `metrics`, never
        # replacing the pre-tax CAGR/Sharpe/MaxDD/equity-curve numbers
        # above. Uses the SAME years window MetricsCalculator computed its
        # pre-tax CAGR over, so post_tax_cagr is directly comparable.
        from momentum_framework.common.tax_overlay import compute_post_tax_metrics
        years = (equity_series.index[-1] - equity_series.index[0]).days / 365.25
        metrics.update(compute_post_tax_metrics(
            trade_log=portfolio.trade_log,
            initial_capital=self.config.initial_capital,
            pre_tax_ending_value=float(equity_series.iloc[-1]),
            years=years,
        ))

        params = self.strategy.describe()
        # Filter to only parameters that build_strategy_id() accepts
        identity_fields = {
            "filter_preset", "crash_regime_enabled", "vol_scaling_mode",
            "weight_method", "skip_months", "vol_target_enabled",
            "vol_target_pct", "liquidity_quintile", "exclude_extraordinary_returns",
            "extraordinary_returns_top_n",
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
            equity_curve=equity_series.rename("portfolio_value").rename_axis("date").reset_index(),
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
        """Looks up each ticker's close on as_of_date from an in-memory,
        per-orchestrator cache — see __init__'s _price_cache comment. Any
        ticker not yet cached gets its ENTIRE [start_date, end_date] close
        series pulled in one bulk query (not just as_of_date), so a ticker
        held across many rebalances costs one DB round-trip total instead
        of one per day it's held."""
        if not tickers:
            return {}
        self._ensure_prices_cached(conn, tickers)
        result = {}
        for ticker in tickers:
            close = self._price_cache.get(ticker, {}).get(as_of_date)
            if close is not None:
                result[ticker] = close
        return result

    def _ensure_prices_cached(self, conn: Any, tickers: List[str]) -> None:
        missing = [t for t in tickers if t not in self._price_cache]
        if not missing:
            return
        placeholders = ",".join("?" for _ in missing)
        rows = conn.execute(
            f"SELECT ticker, date, close FROM ohlcv_adjusted "
            f"WHERE ticker IN ({placeholders}) AND date >= ? AND date <= ?",
            list(missing) + [self.config.start_date, self.config.end_date],
        ).fetchall()
        for ticker in missing:
            self._price_cache[ticker] = {}
        for ticker, date, close in rows:
            self._price_cache[ticker][str(date)] = close

    def _normalize_report(self, report: Dict[str, Any]) -> BacktestResult:
        run = report.get("run", {})
        config = run.get("config", {})
        metrics = report.get("metrics", {})

        params = self.strategy.describe()
        # Filter to only parameters that build_strategy_id() accepts
        identity_fields = {
            "filter_preset", "crash_regime_enabled", "vol_scaling_mode",
            "weight_method", "skip_months", "vol_target_enabled",
            "vol_target_pct", "liquidity_quintile", "exclude_extraordinary_returns",
            "extraordinary_returns_top_n",
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
