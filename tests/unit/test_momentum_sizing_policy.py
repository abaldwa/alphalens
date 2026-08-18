"""tests/unit/test_momentum_sizing_policy.py

The three sizing decisions taken 2026-08-18, pinned so they cannot revert
silently:

  1. Momentum is FULLY INVESTED (investable_pct = 1.0) — its equal-weight
     slot is 1/top_n and no ceiling clips it.
  2. Momentum takes NO sector diversification — its risk control is the
     universe itself (top 800 by ADTV), not a per-sector cap.
  3. top_n is HONOURED — the book is divided into the strategy's own number
     of slots, not a hardcoded 10.

Each was wrong before: the orchestrator never passed n_target_positions, so
every strategy sized for 10 slots regardless of top_n, and momentum ran under
the D21 bucket's 3% position / 20% sector caps — roughly 45% invested.
"""

import pytest

from backtest.core.engine import OrchestratorConfig
from backtest.core.horizon import HorizonBucket
from backtest.core.portfolio import StrategyPortfolio
from backtest.run_orchestrator_backtest import _sizing_overrides_for


class TestSizingOverrides:
    def test_momentum_removes_the_sector_cap(self):
        overrides = _sizing_overrides_for("momentum", top_n=15)
        assert overrides["max_sector_pct"] == 1.0

    def test_momentum_position_ceiling_is_exactly_the_equal_weight_slot(self):
        """Raised to the slot size, not to some round number: the ceiling must
        never quietly become the rule that sizes a position."""
        for top_n in (5, 10, 15, 20):
            assert _sizing_overrides_for("momentum", top_n)["max_position_pct"] == pytest.approx(1.0 / top_n)

    @pytest.mark.parametrize("channel", ["technical", "fundamental", "ml"])
    def test_every_other_channel_keeps_its_bucket_defaults(self, channel):
        """Deliberately not a global change — these limits exist for good
        reasons on the channels that were designed around them."""
        assert _sizing_overrides_for(channel, top_n=15) is None


class TestFullyInvested:
    def _portfolio(self, top_n=15, capital=1_000_000.0):
        return StrategyPortfolio(
            initial_capital=capital, horizon_bucket=HorizonBucket.D21,
            n_target_positions=top_n,
            sizing_overrides=_sizing_overrides_for("momentum", top_n),
        )

    def test_a_slot_is_one_nth_of_the_book(self):
        portfolio = self._portfolio(top_n=15)
        qty = portfolio.position_size(price=100.0, portfolio_value=1_000_000.0)
        # 1/15 of 1,000,000 = 66,666 -> 666 shares at 100. Not 3% (300 shares).
        assert qty == 666

    def test_the_whole_book_is_deployable(self):
        """top_n slots of 1/top_n each == 100% of capital. Under the old 3%
        ceiling, 15 slots reached 45% and the rest sat in cash."""
        portfolio = self._portfolio(top_n=15)
        slot = portfolio.position_size(price=100.0, portfolio_value=1_000_000.0) * 100.0
        assert slot * 15 == pytest.approx(1_000_000.0, rel=0.01)

    def test_a_sector_never_blocks_a_buy(self):
        portfolio = self._portfolio(top_n=15)
        prices = {}
        for i in range(10):
            ticker = f"T{i}"
            prices[ticker] = 100.0
            assert portfolio.can_buy(ticker, "Financial Services", 100.0, prices), (
                f"buy {i + 1} into one sector was rejected — the sector cap is still active"
            )
            portfolio.buy(ticker, "Financial Services", 100.0, "2026-08-14", prices)


class TestTopNIsHonoured:
    def test_the_config_defaults_to_asking_the_adapter(self):
        assert OrchestratorConfig.n_target_positions is None or True  # documented default
        config = OrchestratorConfig(
            trading_days=[], universe_provider=lambda d: [], price_lookup=lambda t, d: None,
        )
        assert config.n_target_positions is None
        assert config.sizing_overrides is None

    def test_slot_count_changes_the_slot_size(self):
        """The bug this closes: 10 slots were assumed for every strategy."""
        ten = StrategyPortfolio(
            initial_capital=1_000_000.0, horizon_bucket=HorizonBucket.D21, n_target_positions=10,
            sizing_overrides=_sizing_overrides_for("momentum", 10),
        )
        twenty = StrategyPortfolio(
            initial_capital=1_000_000.0, horizon_bucket=HorizonBucket.D21, n_target_positions=20,
            sizing_overrides=_sizing_overrides_for("momentum", 20),
        )
        assert ten.position_size(100.0, 1_000_000.0) == 2 * twenty.position_size(100.0, 1_000_000.0)

    def test_the_orchestrator_takes_the_slot_count_from_the_adapter(self, monkeypatch):
        """The wiring, not just the portfolio: the orchestrator must pass the
        adapter's top_n through. It passed nothing at all before, so every
        strategy silently sized for 10."""
        import backtest.core.engine as engine_mod

        captured = {}
        real = engine_mod.StrategyPortfolio

        def _spy(*args, **kwargs):
            captured.update(kwargs)
            return real(*args, **kwargs)

        monkeypatch.setattr(engine_mod, "StrategyPortfolio", _spy)

        class _Adapter:
            channel = "momentum"
            top_n = 15

            def generate_signals(self, universe, as_of_date, horizon_bucket):
                return []

            def feature_vector(self, ticker, as_of_date):
                return {}

        import pandas as pd

        from backtest.core.run_context import BacktestRun
        from datetime import date

        days = pd.DatetimeIndex(pd.bdate_range("2026-08-03", "2026-08-14"))
        config = OrchestratorConfig(
            trading_days=days, universe_provider=lambda d: [], price_lookup=lambda t, d: None,
            persist_signals=False, enforce_readiness=False, collect_timings=False,
        )
        run = BacktestRun(
            run_id="sizing-wiring", channel="momentum", strategy_id="probe",
            horizon_bucket=HorizonBucket.D21, mode="backtest", universe_spec="x",
            start_date=date(2026, 8, 3), end_date=date(2026, 8, 14),
            capital_mode="lump", initial_capital=1_000_000.0,
        )
        engine_mod.BacktestOrchestrator().run(run, _Adapter(), config)
        assert captured.get("n_target_positions") == 15
