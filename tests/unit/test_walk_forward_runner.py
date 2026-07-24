"""tests/unit/test_walk_forward_runner.py — backtest/walk_forward/runner.py.

Includes the mandatory lookahead-leakage test (BacktestUmbrellaPlan.md
Phase 2.5): the single highest-value test in the whole plan, since a
leakage bug here would silently overstate every downstream Walk-Forward
and Phase-6 result.
"""

from datetime import date

import pandas as pd
import pytest

from backtest.adapters.momentum_adapter import MomentumAdapter
from backtest.core.engine import OrchestratorConfig
from backtest.core.horizon import HorizonBucket
from backtest.core.run_context import BacktestRun
from backtest.walk_forward.runner import WalkForwardRunner


def _run(**overrides):
    defaults = dict(
        channel="momentum", strategy_id="mom_walk_forward", horizon_bucket=HorizonBucket.D21,
        mode="walk_forward", universe_spec="test", start_date=date(2020, 1, 1), end_date=date(2020, 12, 1),
        capital_mode="lump", initial_capital=1_000_000.0,
    )
    defaults.update(overrides)
    return BacktestRun(**defaults)


class TestModeGuard:
    def test_rejects_non_walk_forward_run(self):
        adapter = MomentumAdapter(price_panel=pd.DataFrame(), top_n=2)
        run = _run(mode="backtest")
        config = OrchestratorConfig(
            trading_days=pd.date_range("2020-01-01", periods=10), universe_provider=lambda d: [],
            price_lookup=lambda t, d: None,
        )
        with pytest.raises(ValueError, match="mode='walk_forward'"):
            WalkForwardRunner().run(run, adapter, config)


class _RefittingAdapter:
    """A fake adapter with a refit() method, to prove the orchestrator's
    refit hook fires at the expected cadence."""

    channel = "technical"

    def __init__(self):
        self.refit_calls = []
        self._version = 0

    def refit(self, as_of_date):
        self._version += 1
        self.refit_calls.append(as_of_date)
        return f"v{self._version}"

    def generate_signals(self, universe, as_of_date, horizon_bucket):
        return []

    def feature_vector(self, ticker, as_of_date):
        return {}


class TestRefitHook:
    def test_refit_called_at_configured_cadence(self):
        adapter = _RefittingAdapter()
        trading_days = pd.date_range("2020-01-01", periods=105, freq="B")
        # rebalance_cadence_days matched to refit_cadence_days so every rebalance
        # date is also a refit-eligible date (the two cadences are independent in
        # general, but for this test we want every refit boundary actually visited).
        run = _run(channel="technical", mode="walk_forward", horizon_bucket=HorizonBucket.D21)
        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: [], price_lookup=lambda t, d: None,
            rebalance_cadence_days=21,
        )
        result = WalkForwardRunner().run(run, adapter, config, refit_cadence_days=21)
        # 105 days / 21-day cadence -> 5 rebalance dates, all refit-eligible
        assert len(adapter.refit_calls) == 5
        assert len(result.refit_log) == 5
        assert result.refit_log[0]["model_version"] == "v1"

    def test_adapter_without_refit_produces_empty_refit_log(self):
        adapter = MomentumAdapter(price_panel=pd.DataFrame(), top_n=2)
        trading_days = pd.date_range("2020-01-01", periods=50, freq="B")
        run = _run()
        config = OrchestratorConfig(
            trading_days=trading_days, universe_provider=lambda d: [], price_lookup=lambda t, d: None,
        )
        result = WalkForwardRunner().run(run, adapter, config, refit_cadence_days=5)
        assert result.refit_log == []


class TestLookaheadLeakage:
    """The mandatory leakage test: for a sample of periods in a real
    Walk-Forward run, the model/rule active at period T must have used
    only data with timestamps <= T. Verified by fuzzing the "future"
    portion of the price panel and confirming period-T output is
    unchanged."""

    def _panel(self, n_days, seed_future_from=None, future_value=99999.0):
        dates = pd.bdate_range("2020-01-01", periods=n_days)
        prices = {
            "A": [100.0 + i * 0.1 for i in range(n_days)],
            "B": [100.0 + i * 0.3 for i in range(n_days)],
        }
        panel = pd.DataFrame(prices, index=dates)
        if seed_future_from is not None:
            panel.loc[panel.index >= seed_future_from, :] = future_value
        return panel

    def test_signal_at_period_t_unaffected_by_fuzzing_data_after_t(self):
        n_days = 200
        as_of_index = 150  # a rebalance date well before the end of the panel

        clean_panel = self._panel(n_days)
        as_of_date = clean_panel.index[as_of_index].date()

        adapter_clean = MomentumAdapter(price_panel=clean_panel, top_n=1, lookback_months=6)
        signals_clean = adapter_clean.generate_signals(["A", "B"], as_of_date, HorizonBucket.D21)

        # Fuzz every price AFTER as_of_date to an absurd, obviously-different value.
        fuzzed_panel = self._panel(n_days, seed_future_from=clean_panel.index[as_of_index + 1])
        adapter_fuzzed = MomentumAdapter(price_panel=fuzzed_panel, top_n=1, lookback_months=6)
        signals_fuzzed = adapter_fuzzed.generate_signals(["A", "B"], as_of_date, HorizonBucket.D21)

        clean_actions = {(s.ticker, s.action) for s in signals_clean}
        fuzzed_actions = {(s.ticker, s.action) for s in signals_fuzzed}
        assert clean_actions == fuzzed_actions, (
            "MomentumAdapter's signal at as_of_date changed when future prices were "
            "fuzzed — this indicates a lookahead-leakage bug."
        )

    def test_full_walk_forward_run_unaffected_by_fuzzing_the_tail(self):
        """End-to-end version: run the same Walk-Forward config up to (but not
        past) a boundary date, once against a clean panel and once against a
        panel whose data AFTER that boundary has been fuzzed, and assert the
        adapter's holdings at the boundary are identical either way — proving
        the run through that point never depended on the (differing) future."""
        n_days = 200
        fuzz_boundary_idx = int(n_days * 0.8)

        clean_panel = self._panel(n_days)
        fuzzed_panel = self._panel(n_days, seed_future_from=clean_panel.index[fuzz_boundary_idx])
        # Truncate both panels to STRICTLY BEFORE the boundary — a real
        # Walk-Forward run at that point in history could not have seen
        # anything at-or-past it regardless. _panel's seed_future_from
        # fuzzes index >= fuzz_boundary_idx (inclusive), so the truncation
        # must stop at fuzz_boundary_idx (exclusive) for the two truncated
        # panels to actually be identical — an off-by-one here (previously
        # `: fuzz_boundary_idx + 1`, which included the first-fuzzed row in
        # the supposedly "clean" truncated_fuzzed panel) was silently masked
        # before the orchestrator's daily exit-policy pass started reading
        # every trading day's price rather than only rebalance dates', so it
        # never actually observed that boundary row before now.
        truncated_clean = clean_panel.iloc[:fuzz_boundary_idx]
        truncated_fuzzed = fuzzed_panel.iloc[:fuzz_boundary_idx]  # identical to truncated_clean by construction

        def run_up_to_boundary(panel):
            adapter = MomentumAdapter(price_panel=panel, top_n=1, lookback_months=3)
            run = _run(end_date=panel.index[-1].date())
            price_map = {(t, d.date()): panel.loc[d, t] for t in panel.columns for d in panel.index}
            config = OrchestratorConfig(
                trading_days=panel.index, universe_provider=lambda d: list(panel.columns),
                price_lookup=lambda t, d: price_map.get((t, d)),
            )
            result = WalkForwardRunner().run(run, adapter, config)
            return adapter, result

        adapter_a, result_a = run_up_to_boundary(truncated_clean)
        adapter_b, result_b = run_up_to_boundary(truncated_fuzzed)

        assert adapter_a._currently_held == adapter_b._currently_held
        assert result_a.metrics["n_distinct_tickers_traded"] == result_b.metrics["n_distinct_tickers_traded"]
        assert result_a.metrics["final_capital"] == pytest.approx(result_b.metrics["final_capital"])
