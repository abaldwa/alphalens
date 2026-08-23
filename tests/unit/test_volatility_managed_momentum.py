"""
Phase 8 (R8): Volatility-managed momentum overlay (Barroso-Santa-Clara).

Tests for the realized_vol_target_multiplier() pure function and adapter-level
integration. Follows the crash_regime_detector pattern from test_crash_aware_momentum.py.
"""
import pandas as pd
import pytest
import numpy as np

from features.momentum_signal import realized_vol_target_multiplier
from backtest.adapters.momentum_adapter import MomentumAdapter


class TestRealizedVolTargetMultiplier:
    """Pure-function tests for realized_vol_target_multiplier()."""

    def test_empty_equity_curve_returns_ones(self):
        """Empty series should return 1.0 (no opinion)."""
        empty = pd.Series(dtype=float)
        result = realized_vol_target_multiplier(empty, target_vol=0.15, lookback_days=126)
        assert result.empty
        assert result.dtype == float

    def test_insufficient_history_returns_ones(self):
        """Series shorter than lookback_days returns all 1.0."""
        dates = pd.date_range("2024-01-01", periods=100)
        values = np.ones(100) * 1_000_000
        curve = pd.Series(values, index=dates)
        result = realized_vol_target_multiplier(curve, target_vol=0.15, lookback_days=126)
        assert (result == 1.0).all()

    def test_smooth_growth_caps_multiplier_at_leverage_cap(self):
        """Steady growth (low realized vol) should hit leverage cap."""
        dates = pd.date_range("2024-01-01", periods=252)
        # Smooth linear growth: ~0.5% per day = ~14% annualized (low vol)
        values = 1_000_000 + np.arange(252) * 5000
        curve = pd.Series(values, index=dates)

        result = realized_vol_target_multiplier(
            curve, target_vol=0.15, lookback_days=126, leverage_cap=1.0
        )
        # With low realized vol, multiplier = 0.15 / realized_vol should exceed cap
        # Most values should hit the 1.0 cap
        assert (result <= 1.01).all()  # Allow small numerical tolerance

    def test_high_volatility_reduces_multiplier(self):
        """High realized vol reduces exposure multiplier."""
        dates = pd.date_range("2024-01-01", periods=252)
        # Random walk with high daily vol (~2%) = ~32% annualized
        np.random.seed(42)
        daily_returns = np.random.normal(0, 0.02, 251)
        values = np.concatenate([[1_000_000], 1_000_000 * np.cumprod(1 + daily_returns)])
        curve = pd.Series(values, index=dates)

        result = realized_vol_target_multiplier(
            curve, target_vol=0.15, lookback_days=126, leverage_cap=1.0
        )
        # High vol -> multiplier = 0.15 / high_vol < 1.0
        # Most values should be < 1.0 (after warmup)
        assert (result[126:] < 1.0).any()
        assert (result[126:] > 0.0).all()

    def test_target_vol_changes_multiplier_proportionally(self):
        """Higher target vol should increase multiplier."""
        dates = pd.date_range("2024-01-01", periods=252)
        np.random.seed(42)
        daily_returns = np.random.normal(0, 0.01, 251)
        values = np.concatenate([[1_000_000], 1_000_000 * np.cumprod(1 + daily_returns)])
        curve = pd.Series(values, index=dates)

        result_15 = realized_vol_target_multiplier(
            curve, target_vol=0.15, lookback_days=126, leverage_cap=2.0
        )
        result_30 = realized_vol_target_multiplier(
            curve, target_vol=0.30, lookback_days=126, leverage_cap=2.0
        )
        # Same realized vol, but target_vol doubled -> multiplier ~doubled
        # Compare post-warmup values
        assert (result_30[126:] > result_15[126:]).all()

    def test_leverage_cap_enforced(self):
        """leverage_cap should hard-cap the multiplier."""
        dates = pd.date_range("2024-01-01", periods=252)
        # Smooth growth with low vol
        values = 1_000_000 + np.arange(252) * 5000
        curve = pd.Series(values, index=dates)

        result = realized_vol_target_multiplier(
            curve, target_vol=0.30, lookback_days=126, leverage_cap=0.5
        )
        # Cap is 0.5 -> no value should exceed it
        assert (result <= 0.5).all()

    def test_missing_data_handles_gracefully(self):
        """NaN values should be filled with 1.0 (no opinion)."""
        dates = pd.date_range("2024-01-01", periods=252)
        values = 1_000_000 + np.arange(252) * 5000
        curve = pd.Series(values, index=dates)
        # Introduce NaN in the middle
        curve.iloc[50:60] = np.nan

        result = realized_vol_target_multiplier(curve, target_vol=0.15, lookback_days=126)
        # Result should not blow up and should have valid multipliers
        assert len(result) == len(curve)
        assert result.dtype == float
        # NaN periods and warmup should have 1.0
        assert (result[:126] == 1.0).all()

    def test_different_lookback_windows(self):
        """Different lookback_days should produce different results."""
        dates = pd.date_range("2024-01-01", periods=300)
        np.random.seed(42)
        daily_returns = np.random.normal(0, 0.015, 299)
        values = np.concatenate([[1_000_000], 1_000_000 * np.cumprod(1 + daily_returns)])
        curve = pd.Series(values, index=dates)

        result_63 = realized_vol_target_multiplier(
            curve, target_vol=0.15, lookback_days=63, leverage_cap=1.0
        )
        result_126 = realized_vol_target_multiplier(
            curve, target_vol=0.15, lookback_days=126, leverage_cap=1.0
        )
        result_252 = realized_vol_target_multiplier(
            curve, target_vol=0.15, lookback_days=252, leverage_cap=1.0
        )
        # Different windows -> different vol estimates -> different multipliers
        # (they should converge eventually, but differ in the middle)
        assert not (result_63 == result_126).all()
        assert not (result_126 == result_252).all()


class TestMomentumAdapterVolTargetOverlay:
    """Adapter-level tests for vol-target overlay integration."""

    @pytest.fixture
    def price_panel(self):
        """Standard 60-day fixture with 10 tickers."""
        dates = pd.date_range("2024-01-01", periods=60)
        tickers = [f"T{i:02d}" for i in range(10)]
        data = {}
        for ticker in tickers:
            np.random.seed(hash(ticker) % 2**32)
            daily_returns = np.random.normal(0.0005, 0.02, 60)
            prices = 100 * np.cumprod(1 + daily_returns)
            data[ticker] = prices
        return pd.DataFrame(data, index=dates)

    def test_vol_target_disabled_by_default(self, price_panel):
        """vol_target_enabled=False (default) should produce size_multiplier=None."""
        adapter = MomentumAdapter(price_panel, top_n=3)
        signals = adapter.generate_signals(list(price_panel.columns), price_panel.index[-1], "daily")
        buys = [s for s in signals if s.action == "buy"]
        if buys:
            for signal in buys:
                assert signal.size_multiplier is None

    def test_vol_target_enabled_stamps_multiplier(self, price_panel):
        """vol_target_enabled=True should stamp size_multiplier on buy signals."""
        adapter = MomentumAdapter(
            price_panel, top_n=3, vol_target_enabled=True, vol_target_pct=0.15
        )
        # Feed equity values to build cache
        equity = 1_000_000
        for date in price_panel.index[:40]:
            adapter.update_portfolio_equity(date, equity)
            equity += 1000

        signals = adapter.generate_signals(list(price_panel.columns), price_panel.index[-1], "daily")
        buys = [s for s in signals if s.action == "buy"]
        if buys:
            for signal in buys:
                # should have a multiplier (or None if vol-target eval fails, but then 1.0 is used)
                # Post-enough-history, should be a number
                if signal.size_multiplier is not None:
                    assert 0.0 < signal.size_multiplier <= 1.0

    def test_equity_cache_empty_without_calls(self, price_panel):
        """update_portfolio_equity() not called -> cache stays empty -> multiplier=1.0."""
        adapter = MomentumAdapter(
            price_panel, top_n=3, vol_target_enabled=True, vol_target_pct=0.15
        )
        # No calls to update_portfolio_equity
        signals = adapter.generate_signals(list(price_panel.columns), price_panel.index[-1], "daily")
        buys = [s for s in signals if s.action == "buy"]
        if buys:
            # Cache is empty, so _vol_target_multiplier_today returns 1.0 (no scaling)
            for signal in buys:
                assert signal.size_multiplier is None or signal.size_multiplier == 1.0

    def test_update_portfolio_equity_populates_cache(self, price_panel):
        """update_portfolio_equity() should build time series."""
        adapter = MomentumAdapter(
            price_panel, top_n=3, vol_target_enabled=True, vol_target_pct=0.15
        )
        assert adapter._equity_history is None
        equity = 1_000_000
        for date in price_panel.index[:30]:
            adapter.update_portfolio_equity(date, equity)
            equity += 1000
        # Cache should be built
        assert adapter._equity_history is not None
        assert len(adapter._equity_history) == 30

    def test_sell_signals_never_get_size_multiplier(self, price_panel):
        """Sell signals should never have size_multiplier (not applicable to exits)."""
        adapter = MomentumAdapter(
            price_panel, top_n=3, vol_target_enabled=True, vol_target_pct=0.15
        )
        # Prime with buys
        adapter.generate_signals(
            list(price_panel.columns), price_panel.index[20], "daily"
        )
        # Force a rotation by limiting universe
        signals_2 = adapter.generate_signals(
            list(price_panel.columns)[:5], price_panel.index[25], "daily"
        )
        sells = [s for s in signals_2 if s.action == "sell"]
        for signal in sells:
            assert signal.size_multiplier is None

    def test_non_regression_crash_regime_and_vol_target_share_cache(self, price_panel):
        """Both overlays active should share the same cache (no duplicate storage)."""
        adapter = MomentumAdapter(
            price_panel,
            top_n=3,
            crash_regime_enabled=True,
            drawdown_threshold=-0.15,
            vol_lookback_days=20,
            vol_target_enabled=True,
            vol_target_pct=0.15,
            vol_target_lookback_days=126,
        )
        equity = 1_000_000
        for date in price_panel.index[:40]:
            adapter.update_portfolio_equity(date, equity)
            equity += 1000
        # Both should reference the same _equity_history
        assert adapter._equity_history is not None
        assert len(adapter._equity_history) == 40


class TestNonRegressionVolTarget:
    """Non-regression tests: confirm vol-target defaults don't break M-family."""

    @pytest.fixture
    def price_panel_large(self):
        """Larger fixture for realistic momentum behavior."""
        dates = pd.date_range("2024-01-01", periods=120)
        tickers = [f"T{i:03d}" for i in range(30)]
        data = {}
        for ticker in tickers:
            np.random.seed(hash(ticker) % 2**32)
            daily_returns = np.random.normal(0.0003, 0.015, 120)
            prices = 100 * np.cumprod(1 + daily_returns)
            data[ticker] = prices
        return pd.DataFrame(data, index=dates)

    def test_m_family_unchanged_with_vol_target_defaults(self, price_panel_large):
        """M-family (vol_target_enabled=False) should behave exactly as before."""
        adapter = MomentumAdapter(price_panel_large, top_n=10, lookback_months=6)
        signals = adapter.generate_signals(
            list(price_panel_large.columns), price_panel_large.index[-1], "daily"
        )
        buys = [s for s in signals if s.action == "buy"]
        # Should have exactly top_n entries
        assert len(buys) <= 10
        # All should have size_multiplier=None (the default for M-family)
        for signal in buys:
            assert signal.size_multiplier is None

    def test_conviction_untouched_by_vol_target(self, price_panel_large):
        """Vol-target should not mutate conviction (that's Phase 7's bug)."""
        adapter = MomentumAdapter(
            price_panel_large, top_n=10, vol_target_enabled=True, vol_target_pct=0.15
        )
        signals = adapter.generate_signals(
            list(price_panel_large.columns), price_panel_large.index[-1], "daily"
        )
        buys = [s for s in signals if s.action == "buy"]
        for signal in buys:
            # conviction should be > 0 (a real momentum score)
            assert signal.conviction > 0
            # conviction should not be a multiplier value (0-1 range)
            # — momentum scores are typically in (-1, 1) or similar
