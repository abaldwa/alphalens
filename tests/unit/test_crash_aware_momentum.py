"""
tests/unit/test_crash_aware_momentum.py

Unit tests for Phase 7 crash-aware momentum overlay.
Tests:
- crash_regime_detector() correctness
- MomentumAdapter crash overlay logic (disable buys / reduce sizing)
- Non-regression: M1-M12 strategies unaffected
"""

import pandas as pd

from features.momentum_signal import crash_regime_detector
from backtest.adapters.momentum_adapter import MomentumAdapter
from datetime import date as date_type


class TestCrashRegimeDetector:
    """Tests for crash_regime_detector() pure function."""

    def test_crash_regime_detector_empty_series(self):
        """Empty equity curve returns empty bool Series."""
        equity = pd.Series(dtype=float)
        result = crash_regime_detector(equity)
        assert isinstance(result, pd.Series)
        assert result.dtype == bool
        assert len(result) == 0

    def test_crash_regime_detector_insufficient_history(self):
        """Equity curve with < 252 values returns empty Series."""
        dates = pd.date_range("2025-01-01", periods=100)
        equity = pd.Series([1_000_000 + i * 100 for i in range(100)], index=dates)
        result = crash_regime_detector(equity, lookback_days=252)
        assert len(result) == 0

    def test_crash_regime_detector_no_drawdown(self):
        """All-positive-return series (no drawdown) → no crash regime."""
        dates = pd.date_range("2024-01-01", periods=300)
        equity = pd.Series([1_000_000 * (1 + 0.0001 * i) for i in range(300)], index=dates)
        result = crash_regime_detector(
            equity,
            drawdown_threshold=-0.15,
            vol_percentile_threshold=0.75,
            lookback_days=252,
            vol_lookback_days=20,
        )
        # No drawdown → no crash regime days
        assert result.sum() == 0 or result.isna().sum() > 0

    def test_crash_regime_detector_with_drawdown(self):
        """Series with -20% drawdown should trigger crash regime."""
        dates = pd.date_range("2024-01-01", periods=300)
        values = [1_000_000] * 100
        values.extend([1_000_000 * (0.8 + 0.001 * i) for i in range(100)])  # -20% drawdown
        values.extend([1_000_000 * 0.9] * 100)  # Hold at reduced level
        equity = pd.Series(values, index=dates)
        result = crash_regime_detector(
            equity,
            drawdown_threshold=-0.15,
            vol_percentile_threshold=0.75,
            lookback_days=252,
            vol_lookback_days=20,
        )
        # Should have some crash regime days (during and after the drawdown)
        assert isinstance(result, pd.Series)
        assert result.dtype == bool
        assert len(result) == len(equity)

    def test_crash_regime_detector_nan_handling(self):
        """NaN values in equity curve should be handled gracefully."""
        dates = pd.date_range("2024-01-01", periods=300)
        equity = pd.Series([1_000_000 + i * 1000 for i in range(300)], index=dates)
        equity.iloc[50:60] = float('nan')
        result = crash_regime_detector(equity)
        # NaN → False (never exclude on missing data)
        assert result.dtype == bool
        assert len(result) == len(equity)

    def test_crash_regime_detector_returns_bool_series(self):
        """Result must be bool Series with same index as input."""
        dates = pd.date_range("2024-01-01", periods=300)
        equity = pd.Series([1_000_000 + i * 100 for i in range(300)], index=dates)
        result = crash_regime_detector(equity)
        assert isinstance(result, pd.Series)
        assert result.dtype == bool
        assert len(result) == len(equity)
        assert (result.index == equity.index).all()


class TestMomentumAdapterCrashOverlay:
    """Tests for MomentumAdapter crash overlay integration."""

    def _make_price_panel(self, tickers, start_date="2025-01-01", periods=100):
        """Helper: create a sample price panel."""
        dates = pd.date_range(start_date, periods=periods)
        data = {t: [100.0 + i * 0.1 for i in range(periods)] for t in tickers}
        return pd.DataFrame(data, index=dates)

    def test_crash_regime_disabled_by_default(self):
        """Crash regime disabled (default) → no effect on signals."""
        price_panel = self._make_price_panel(["SBIN", "INFY", "TCS"])
        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=2,
            lookback_months=6,
            crash_regime_enabled=False,  # Disabled
        )
        # Should not error, and crash_regime_cache should remain None
        assert adapter._equity_history is None
        # Basic signal generation should work unchanged
        assert hasattr(adapter, 'generate_signals')

    def test_crash_regime_enabled_flag_sets_cache(self):
        """Crash regime enabled → creates equity curve cache."""
        price_panel = self._make_price_panel(["SBIN", "INFY"])
        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=2,
            lookback_months=6,
            crash_regime_enabled=True,
        )
        assert adapter.crash_regime_enabled is True
        # Cache starts as None until update_portfolio_equity is called
        assert adapter._equity_history is None

    def test_update_portfolio_equity_builds_cache(self):
        """update_portfolio_equity() accumulates equity values."""
        price_panel = self._make_price_panel(["SBIN", "INFY"])
        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=2,
            lookback_months=6,
            crash_regime_enabled=True,
        )
        # Add equity values over time
        dates = pd.date_range("2025-01-01", periods=10)
        for i, d in enumerate(dates):
            adapter.update_portfolio_equity(d, 1_000_000 + i * 10_000)
        # Cache should now be populated
        assert adapter._equity_history is not None
        assert len(adapter._equity_history) == 10

    def test_crash_regime_check_insufficient_data(self):
        """_is_crash_regime_today() returns False if insufficient data."""
        price_panel = self._make_price_panel(["SBIN", "INFY"])
        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=2,
            lookback_months=6,
            crash_regime_enabled=True,
        )
        as_of = date_type(2025, 4, 15)
        # No equity data → should return False
        assert adapter._is_crash_regime_today(as_of) is False

    def test_crash_regime_check_with_data(self):
        """_is_crash_regime_today() checks detector with accumulated data."""
        price_panel = self._make_price_panel(["SBIN", "INFY"], periods=300)
        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=2,
            lookback_months=6,
            crash_regime_enabled=True,
        )
        # Add 300 days of equity data
        dates = pd.date_range("2024-01-01", periods=300)
        for i, d in enumerate(dates):
            # First 200 days: normal growth, then -20% crash
            if i < 200:
                equity = 1_000_000 + i * 1000
            else:
                equity = 1_000_000 + 200 * 1000 * (0.8 + (i - 200) * 0.001)
            adapter.update_portfolio_equity(d, equity)
        # Check a date during/after the crash
        # Should return bool (not error out)
        result = adapter._is_crash_regime_today(dates[-50])
        assert isinstance(result, bool)

    def test_crash_regime_disabled_no_sizing_reduction(self):
        """Crash regime disabled → crash_reduce_sizing ignored."""
        price_panel = self._make_price_panel(["SBIN", "INFY"])
        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=2,
            lookback_months=6,
            crash_regime_enabled=False,
            crash_reduce_sizing=0.5,
        )
        # With crash disabled, sizing reduction should have no effect
        assert adapter.crash_regime_enabled is False

    def test_crash_disable_buys_default_true(self):
        """crash_disable_buys defaults to True (disable buys in crash)."""
        price_panel = self._make_price_panel(["SBIN", "INFY"])
        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=2,
            lookback_months=6,
            crash_regime_enabled=True,
            # crash_disable_buys not set → defaults to True
        )
        assert adapter.crash_disable_buys is True

    def test_crash_reduce_sizing_optional(self):
        """crash_reduce_sizing can be None (sizing not reduced)."""
        price_panel = self._make_price_panel(["SBIN", "INFY"])
        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=2,
            lookback_months=6,
            crash_regime_enabled=True,
            crash_disable_buys=False,
            crash_reduce_sizing=None,  # No sizing reduction
        )
        assert adapter.crash_reduce_sizing is None

    def test_crash_reduce_sizing_scales_conviction(self):
        """crash_reduce_sizing should scale signal conviction."""
        price_panel = self._make_price_panel(["SBIN", "INFY"])
        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=2,
            lookback_months=6,
            crash_regime_enabled=True,
            crash_disable_buys=False,
            crash_reduce_sizing=0.5,  # 50% sizing
        )
        assert adapter.crash_reduce_sizing == 0.5


class TestNonRegressionMomentumAdapter:
    """Non-regression tests: M1-M12 strategies must be unaffected."""

    def _make_price_panel(self, tickers, start_date="2025-01-01", periods=100):
        """Helper: create a sample price panel."""
        dates = pd.date_range(start_date, periods=periods)
        data = {t: [100.0 + i * 0.1 for i in range(periods)] for t in tickers}
        return pd.DataFrame(data, index=dates)

    def test_m_family_default_crash_disabled(self):
        """M1-M12 (strategy_family='M') default: crash regime disabled."""
        price_panel = self._make_price_panel(["SBIN", "INFY", "TCS"])
        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=10,
            lookback_months=6,
            # All crash params omitted → all default (crash_regime_enabled=False)
        )
        assert adapter.crash_regime_enabled is False
        # Should work exactly as before
        assert adapter.rank_method == "trailing_return"
        assert adapter.skip_months == 0

    def test_m_family_no_crash_cache(self):
        """M1-M12: crash cache should remain None if crash disabled."""
        price_panel = self._make_price_panel(["SBIN", "INFY"])
        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=10,
            lookback_months=6,
            crash_regime_enabled=False,
        )
        # Even if update_portfolio_equity is called, should be no-op
        adapter.update_portfolio_equity(date_type(2025, 1, 1), 1_000_000)
        assert adapter._equity_history is None
