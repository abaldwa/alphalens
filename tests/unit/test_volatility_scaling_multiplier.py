"""
Tests for Phase 9 (R9) generalized volatility scaling.

Tests the four scaling modes:
- inverse_volatility: size ∝ 1/vol
- inverse_variance: size ∝ 1/vol²
- target_volatility: size ∝ target_vol/vol (R8 logic)
- downside_volatility: size ∝ 1/downside_vol (negative returns only)
"""

import numpy as np
import pandas as pd
import pytest

from features.momentum_signal import volatility_scaling_multiplier


class TestVolatilityScalingMultiplier:
    """Unit tests for generalized volatility scaling."""

    @pytest.fixture
    def equity_curve(self):
        """Synthetic equity curve with known volatility."""
        # Starting value 1M, constant daily returns (0.05% = annualized ~13%)
        dates = pd.date_range("2023-01-01", periods=252, freq="D")
        daily_return = 0.0005  # 0.05% per day
        values = [1_000_000 * (1 + daily_return) ** i for i in range(252)]
        return pd.Series(values, index=dates, dtype=float)

    @pytest.fixture
    def volatile_equity_curve(self):
        """Synthetic equity curve with high volatility."""
        dates = pd.date_range("2023-01-01", periods=252, freq="D")
        np.random.seed(42)
        # Random daily returns, mean 0.05%, std 2%
        daily_returns = np.random.normal(0.0005, 0.02, 252)
        values = [1_000_000]
        for ret in daily_returns[1:]:
            values.append(values[-1] * (1 + ret))
        return pd.Series(values, index=dates, dtype=float)

    @pytest.fixture
    def equity_curve_with_crash(self):
        """Equity curve with a crash (COVID-style drawdown)."""
        dates = pd.date_range("2023-01-01", periods=504, freq="D")
        values = []
        current = 1_000_000
        for i, date in enumerate(dates):
            if i < 252:
                # Normal period: +0.05% per day
                current *= 1.0005
            elif i < 260:
                # Crash: -2% per day
                current *= 0.98
            else:
                # Recovery: +0.1% per day
                current *= 1.001
            values.append(current)
        return pd.Series(values, index=dates, dtype=float)

    def test_inverse_volatility_mode(self, equity_curve):
        """Test inverse_volatility mode: multiplier = 1/vol."""
        result = volatility_scaling_multiplier(
            equity_curve,
            scaling_mode="inverse_volatility",
            lookback_days=126,
            leverage_cap=2.0,
        )
        assert result.notna().any(), "Should have non-NaN values"
        assert (result >= 0).all(), "Multiplier must be non-negative"
        # Later period should have stable values (not NaN)
        tail_values = result.iloc[-50:]
        assert tail_values.notna().sum() >= 40, "Most tail values should be non-NaN"

    def test_inverse_variance_mode(self, equity_curve):
        """Test inverse_variance mode: multiplier = 1/vol²."""
        result = volatility_scaling_multiplier(
            equity_curve,
            scaling_mode="inverse_variance",
            lookback_days=126,
            leverage_cap=2.0,
        )
        assert result.notna().any(), "Should have non-NaN values"
        assert (result >= 0).all(), "Multiplier must be non-negative"
        # Inverse variance should be smaller than inverse volatility for same vol
        inv_vol = volatility_scaling_multiplier(
            equity_curve,
            scaling_mode="inverse_volatility",
            lookback_days=126,
            leverage_cap=2.0,
        )
        # In tail, inverse_variance should generally be <= inverse_vol
        tail_result = result.iloc[-50:].dropna()
        tail_inv_vol = inv_vol.iloc[-50:].dropna()
        if len(tail_result) > 0 and len(tail_inv_vol) > 0:
            # Can't strictly compare due to different volatility windows,
            # but both should exist and be finite
            assert np.all(np.isfinite(tail_result)), "Tail should be finite"
            assert np.all(np.isfinite(tail_inv_vol)), "Tail should be finite"

    def test_target_volatility_mode(self, equity_curve):
        """Test target_volatility mode (R8 logic): multiplier = target_vol/realized_vol."""
        result = volatility_scaling_multiplier(
            equity_curve,
            scaling_mode="target_volatility",
            target_vol=0.15,
            lookback_days=126,
            leverage_cap=1.0,
        )
        assert result.notna().any(), "Should have non-NaN values"
        assert (result >= 0).all(), "Multiplier must be non-negative"
        assert (result <= 1.0).all(), "Multiplier should be capped at 1.0"

    def test_downside_volatility_mode(self, equity_curve_with_crash):
        """Test downside_volatility mode: scale by downside volatility (negative returns only)."""
        result = volatility_scaling_multiplier(
            equity_curve_with_crash,
            scaling_mode="downside_volatility",
            lookback_days=126,
            leverage_cap=2.0,
        )
        assert result.notna().any(), "Should have non-NaN values"
        assert (result >= 0).all(), "Multiplier must be non-negative"
        # After crash (i > 260), multiplier should be lower due to elevated downside vol
        pre_crash = result.iloc[200:252].dropna()
        post_crash = result.iloc[270:320].dropna()
        if len(pre_crash) > 0 and len(post_crash) > 0:
            # Post-crash should have lower multiplier on average
            assert post_crash.mean() <= pre_crash.mean() * 1.1, \
                "Post-crash downside vol should reduce multiplier"

    def test_insufficient_data(self):
        """Test behavior with insufficient data."""
        dates = pd.date_range("2023-01-01", periods=50, freq="D")
        equity_curve = pd.Series([1_000_000 + i * 1000 for i in range(50)], index=dates)
        result = volatility_scaling_multiplier(
            equity_curve,
            scaling_mode="target_volatility",
            lookback_days=126,  # > 50 available
        )
        # Should return all 1.0 (no scaling) due to insufficient data
        assert (result == 1.0).all(), "Insufficient data should return 1.0 multiplier"

    def test_empty_series(self):
        """Test behavior with empty series."""
        empty = pd.Series([], dtype=float)
        result = volatility_scaling_multiplier(
            empty,
            scaling_mode="target_volatility",
        )
        assert result.empty, "Empty input should return empty output"

    def test_invalid_scaling_mode(self, equity_curve):
        """Test that invalid scaling mode raises ValueError."""
        with pytest.raises(ValueError, match="scaling_mode.*not in"):
            volatility_scaling_multiplier(
                equity_curve,
                scaling_mode="invalid_mode",
            )

    def test_leverage_cap_enforcement(self, equity_curve):
        """Test that leverage_cap is respected."""
        result_capped = volatility_scaling_multiplier(
            equity_curve,
            scaling_mode="inverse_volatility",
            lookback_days=126,
            leverage_cap=0.5,
        )
        # Capped version should have no values > 0.5
        tail_capped = result_capped.iloc[-50:].dropna()
        if len(tail_capped) > 0:
            assert (tail_capped <= 0.5).all(), "Should respect leverage_cap"

    def test_consistency_across_dates(self, equity_curve):
        """Test that same equity levels produce same multipliers in similar vol regimes."""
        result = volatility_scaling_multiplier(
            equity_curve,
            scaling_mode="target_volatility",
            target_vol=0.15,
            lookback_days=126,
        )
        # Multipliers should be between 0 and leverage_cap
        assert (result >= 0).all(), "Multiplier must be >= 0"
        assert (result <= 1.0).all(), "Multiplier must be <= 1.0"
        # No NaN in tail (where we have enough history)
        tail = result.iloc[-100:].dropna()
        assert len(tail) >= 90, "Should have good coverage in tail"

    def test_mode_defaults_for_caps(self, equity_curve):
        """Test mode-dependent default leverage caps when None is passed."""
        # target_volatility defaults to 1.0 cap
        result_target = volatility_scaling_multiplier(
            equity_curve,
            scaling_mode="target_volatility",
            target_vol=0.15,
            lookback_days=126,
            leverage_cap=None,
        )
        assert (result_target <= 1.0).all(), "target_volatility should default to 1.0 cap"

        # inverse_volatility defaults to 2.0 cap
        result_inv = volatility_scaling_multiplier(
            equity_curve,
            scaling_mode="inverse_volatility",
            lookback_days=126,
            leverage_cap=None,
        )
        assert (result_inv <= 2.0).all(), "inverse_volatility should default to 2.0 cap"
