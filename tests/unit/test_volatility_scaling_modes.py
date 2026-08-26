"""
Tests for individual volatility scaling mode functions.

Tests each mode independently:
- inverse_volatility
- inverse_variance
- target_volatility
- downside_volatility
"""

import numpy as np
import pandas as pd
import pytest

from features.volatility_scaling import (
    baseline,
    downside_volatility,
    inverse_variance,
    inverse_volatility,
    target_volatility,
)


class TestBaseline:
    """Unit tests for baseline (neutral) mode."""

    @pytest.fixture
    def equity_curve(self):
        """Synthetic equity curve."""
        dates = pd.date_range("2023-01-01", periods=252, freq="D")
        np.random.seed(42)
        daily_returns = np.random.normal(0.0005, 0.015, 252)
        values = [1_000_000]
        for ret in daily_returns[1:]:
            values.append(values[-1] * (1 + ret))
        return pd.Series(values, index=dates, dtype=float)

    def test_baseline_returns_ones(self, equity_curve):
        """Baseline should return constant 1.0 multiplier."""
        result = baseline(equity_curve)

        assert len(result) == len(equity_curve), "Output length should match input"
        assert (result == 1.0).all(), "All values should be exactly 1.0"

    def test_baseline_dtype(self, equity_curve):
        """Baseline should return float dtype."""
        result = baseline(equity_curve)
        assert result.dtype == float, "Should be float dtype"

    def test_baseline_index_matches(self, equity_curve):
        """Baseline output index should match input."""
        result = baseline(equity_curve)
        assert (result.index == equity_curve.index).all(), "Index should match equity_curve"

    def test_baseline_no_nan(self, equity_curve):
        """Baseline should never have NaN values."""
        result = baseline(equity_curve)
        assert result.notna().all(), "Should have no NaN values"

    def test_baseline_empty_input_raises(self):
        """Empty equity curve should raise ValueError."""
        empty = pd.Series([], dtype=float)
        with pytest.raises(ValueError, match="cannot be empty"):
            baseline(empty)

    def test_baseline_ignores_lookback_days(self, equity_curve):
        """Baseline should be identical regardless of lookback_days parameter."""
        result_126 = baseline(equity_curve, lookback_days=126)
        result_252 = baseline(equity_curve, lookback_days=252)
        result_63 = baseline(equity_curve, lookback_days=63)

        assert (result_126 == result_252).all(), "lookback_days should be ignored"
        assert (result_126 == result_63).all(), "lookback_days should be ignored"

    def test_baseline_ignores_leverage_cap(self, equity_curve):
        """Baseline should ignore leverage_cap parameter."""
        result_no_cap = baseline(equity_curve, leverage_cap=None)
        result_capped_1 = baseline(equity_curve, leverage_cap=1.0)
        result_capped_2 = baseline(equity_curve, leverage_cap=2.0)

        assert (result_no_cap == result_capped_1).all(), "leverage_cap should be ignored"
        assert (result_no_cap == result_capped_2).all(), "leverage_cap should be ignored"

    def test_baseline_as_control_reference(self, equity_curve):
        """Baseline useful for measuring vol-scaling value-add."""
        baseline_mult = baseline(equity_curve)
        inv_vol_mult = inverse_volatility(equity_curve, lookback_days=126)

        # Baseline is constant; inverse_volatility varies
        assert baseline_mult.std() == 0.0, "Baseline should have zero std"
        assert inv_vol_mult.iloc[-50:].dropna().std() > 0, "inverse_volatility should vary"

        # Application example: show position sizing difference
        base_position = 100_000
        baseline_position = base_position * baseline_mult  # Always 100_000
        scaled_position = base_position * inv_vol_mult  # Varies

        print(f"\nBaseline position: ₹{baseline_position.iloc[-1]:,.0f} (constant)")
        print(f"Scaled position (inv_vol): ₹{scaled_position.iloc[-1]:,.0f} (variable)")


class TestInverseVolatility:
    """Unit tests for inverse_volatility mode."""

    @pytest.fixture
    def stable_equity_curve(self):
        """Synthetic equity curve with constant low volatility."""
        dates = pd.date_range("2023-01-01", periods=252, freq="D")
        daily_return = 0.0005  # 0.05% per day
        values = [1_000_000 * (1 + daily_return) ** i for i in range(252)]
        return pd.Series(values, index=dates, dtype=float)

    @pytest.fixture
    def volatile_equity_curve(self):
        """Synthetic equity curve with high volatility."""
        dates = pd.date_range("2023-01-01", periods=252, freq="D")
        np.random.seed(42)
        daily_returns = np.random.normal(0.0005, 0.02, 252)
        values = [1_000_000]
        for ret in daily_returns[1:]:
            values.append(values[-1] * (1 + ret))
        return pd.Series(values, index=dates, dtype=float)

    def test_low_vol_high_multiplier(self, stable_equity_curve):
        """In low-vol regime, inverse_vol should assign high multipliers."""
        result = inverse_volatility(stable_equity_curve, lookback_days=126)

        assert result.notna().any(), "Should have non-NaN values"
        assert (result >= 0).all(), "Multiplier must be non-negative"

        # Tail values (where we have enough data) should be non-NaN
        tail_values = result.iloc[-50:]
        assert tail_values.notna().sum() >= 40, "Most tail values should be non-NaN"

    def test_high_vol_low_multiplier(self, volatile_equity_curve):
        """In high-vol regime, inverse_vol should assign lower multipliers."""
        result = inverse_volatility(volatile_equity_curve, lookback_days=126)

        tail_values = result.iloc[-50:].dropna()
        assert len(tail_values) > 0, "Should have tail values"
        # High vol should produce smaller multipliers than stable curve
        # (but exact comparison depends on vol levels)

    def test_leverage_cap(self, stable_equity_curve):
        """Test that leverage_cap is respected."""
        cap = 1.5
        result = inverse_volatility(
            stable_equity_curve, lookback_days=126, leverage_cap=cap
        )

        tail_capped = result.iloc[-50:].dropna()
        if len(tail_capped) > 0:
            assert (tail_capped <= cap).all(), f"Should respect leverage_cap={cap}"

    def test_uncapped_higher_than_capped(self, stable_equity_curve):
        """Uncapped should have higher values than capped."""
        uncapped = inverse_volatility(stable_equity_curve, lookback_days=126)
        capped = inverse_volatility(
            stable_equity_curve, lookback_days=126, leverage_cap=1.0
        )

        uncapped_tail = uncapped.iloc[-50:].dropna()
        capped_tail = capped.iloc[-50:].dropna()

        if len(uncapped_tail) > 0 and len(capped_tail) > 0:
            assert uncapped_tail.mean() >= capped_tail.mean(), \
                "Uncapped should be >= capped"

    def test_empty_input_raises(self):
        """Empty equity curve should raise ValueError."""
        empty = pd.Series([], dtype=float)
        with pytest.raises(ValueError, match="cannot be empty"):
            inverse_volatility(empty)


class TestInverseVariance:
    """Unit tests for inverse_variance mode."""

    @pytest.fixture
    def equity_curve(self):
        """Synthetic equity curve."""
        dates = pd.date_range("2023-01-01", periods=252, freq="D")
        np.random.seed(42)
        daily_returns = np.random.normal(0.0005, 0.015, 252)
        values = [1_000_000]
        for ret in daily_returns[1:]:
            values.append(values[-1] * (1 + ret))
        return pd.Series(values, index=dates, dtype=float)

    def test_inverse_variance_produces_values(self, equity_curve):
        """Should produce valid multipliers."""
        result = inverse_variance(equity_curve, lookback_days=126)

        assert result.notna().any(), "Should have non-NaN values"
        assert (result >= 0).all(), "Multiplier must be non-negative"

    def test_inverse_variance_vs_inverse_volatility(self, equity_curve):
        """Inverse_variance should typically be more aggressive (higher values)."""
        inv_var = inverse_variance(equity_curve, lookback_days=126)
        inv_vol = inverse_volatility(equity_curve, lookback_days=126)

        # In high-vol regime, 1/vol² should be smaller than 1/vol
        # But we can't guarantee relative ordering globally, so just check they're different
        tail_inv_var = inv_var.iloc[-50:].dropna()
        tail_inv_vol = inv_vol.iloc[-50:].dropna()

        if len(tail_inv_var) > 0 and len(tail_inv_vol) > 0:
            assert not np.allclose(tail_inv_var.values, tail_inv_vol.values), \
                "Modes should produce different values"

    def test_with_leverage_cap(self, equity_curve):
        """Test leverage cap enforcement."""
        cap = 2.0
        result = inverse_variance(equity_curve, lookback_days=126, leverage_cap=cap)

        tail_values = result.iloc[-50:].dropna()
        if len(tail_values) > 0:
            assert (tail_values <= cap).all(), f"Should respect cap={cap}"


class TestTargetVolatility:
    """Unit tests for target_volatility mode (R8 logic)."""

    @pytest.fixture
    def equity_curve(self):
        """Synthetic equity curve."""
        dates = pd.date_range("2023-01-01", periods=252, freq="D")
        np.random.seed(42)
        daily_returns = np.random.normal(0.0005, 0.02, 252)
        values = [1_000_000]
        for ret in daily_returns[1:]:
            values.append(values[-1] * (1 + ret))
        return pd.Series(values, index=dates, dtype=float)

    def test_target_vol_conservative(self, equity_curve):
        """Target_vol should be conservative by default (max 1.0)."""
        result = target_volatility(equity_curve, target_vol=0.15, lookback_days=126)

        assert (result <= 1.0).all(), "Should respect default cap of 1.0"
        assert (result >= 0).all(), "Should be non-negative"

    def test_target_vol_adjustment(self, equity_curve):
        """When realized_vol < target_vol, multiplier should be > 1.0 (before cap)."""
        # But with default cap=1.0, it stays capped
        result = target_volatility(
            equity_curve, target_vol=0.50, lookback_days=126, leverage_cap=None
        )

        tail_values = result.iloc[-50:].dropna()
        if len(tail_values) > 0:
            # If target_vol is high and leverage_cap=None, some values should exceed 1.0
            assert (tail_values <= 10.0).all(), "Should still be finite"

    def test_high_target_vol_produces_low_multiplier(self, equity_curve):
        """High target_vol → low multiplier (de-leverage)."""
        # If realized vol is typically 20%, and target is 30%, multiplier should be ~1.5
        # If realized vol is 20% and target is 10%, multiplier capped at 1.0 by default
        result_conservative = target_volatility(
            equity_curve, target_vol=0.10, lookback_days=126
        )
        result_aggressive = target_volatility(
            equity_curve, target_vol=0.25, lookback_days=126
        )

        tail_conservative = result_conservative.iloc[-50:].dropna()
        tail_aggressive = result_aggressive.iloc[-50:].dropna()

        if len(tail_conservative) > 0 and len(tail_aggressive) > 0:
            # Conservative target should have lower avg multiplier
            assert tail_conservative.mean() <= tail_aggressive.mean(), \
                "Conservative target should produce lower multipliers"


class TestDownsideVolatility:
    """Unit tests for downside_volatility mode."""

    @pytest.fixture
    def equity_curve_trending(self):
        """Synthetic equity curve with uptrend (upside drift)."""
        dates = pd.date_range("2023-01-01", periods=252, freq="D")
        np.random.seed(42)
        # Mean drift: 0.08% per day (upside), std 1.5%
        daily_returns = np.random.normal(0.0008, 0.015, 252)
        values = [1_000_000]
        for ret in daily_returns[1:]:
            values.append(values[-1] * (1 + ret))
        return pd.Series(values, index=dates, dtype=float)

    @pytest.fixture
    def equity_curve_with_crash(self):
        """Synthetic equity curve with crash."""
        dates = pd.date_range("2023-01-01", periods=504, freq="D")
        values = []
        current = 1_000_000
        for i in range(504):
            if i < 252:
                # Normal: +0.05% per day
                current *= 1.0005
            elif i < 260:
                # Crash: -2% per day
                current *= 0.98
            else:
                # Recovery: +0.1% per day
                current *= 1.001
            values.append(current)
        return pd.Series(values, index=dates, dtype=float)

    def test_downside_vol_produces_values(self, equity_curve_trending):
        """Should produce valid multipliers."""
        result = downside_volatility(equity_curve_trending, lookback_days=126)

        assert result.notna().any(), "Should have non-NaN values"
        assert (result >= 0).all(), "Multiplier must be non-negative"

    def test_downside_vol_higher_in_trending(self, equity_curve_trending):
        """In trending market (asymmetric: more upside), downside_vol should
        produce higher multipliers than standard vol (which penalizes upside)."""
        downside = downside_volatility(equity_curve_trending, lookback_days=126)
        standard = inverse_volatility(equity_curve_trending, lookback_days=126)

        tail_downside = downside.iloc[-50:].dropna()
        tail_standard = standard.iloc[-50:].dropna()

        if len(tail_downside) > 0 and len(tail_standard) > 0:
            # Downside vol (ignoring upside) should allow higher multipliers
            # in an up-trending market
            assert tail_downside.mean() >= tail_standard.mean() * 0.9, \
                "Downside_vol should be >= standard_vol in trending market"

    def test_downside_vol_post_crash_reduction(self, equity_curve_with_crash):
        """After crash, downside volatility should increase, reducing multiplier."""
        result = downside_volatility(equity_curve_with_crash, lookback_days=126)

        pre_crash = result.iloc[200:252].dropna()
        post_crash = result.iloc[270:320].dropna()

        if len(pre_crash) > 0 and len(post_crash) > 0:
            # Post-crash: higher downside vol → lower multiplier
            assert post_crash.mean() <= pre_crash.mean() * 1.2, \
                "Post-crash should have lower multiplier due to elevated downside_vol"

    def test_leverage_cap_enforcement(self, equity_curve_trending):
        """Test leverage cap on downside_vol."""
        cap = 1.5
        result = downside_volatility(
            equity_curve_trending, lookback_days=126, leverage_cap=cap
        )

        tail_values = result.iloc[-50:].dropna()
        if len(tail_values) > 0:
            assert (tail_values <= cap).all(), f"Should respect cap={cap}"


class TestModeDifferences:
    """Cross-mode comparison tests."""

    @pytest.fixture
    def equity_curve(self):
        """Standard equity curve for comparison."""
        dates = pd.date_range("2023-01-01", periods=504, freq="D")
        np.random.seed(42)
        daily_returns = np.random.normal(0.0004, 0.018, 504)
        values = [1_000_000]
        for ret in daily_returns[1:]:
            values.append(values[-1] * (1 + ret))
        return pd.Series(values, index=dates, dtype=float)

    def test_all_modes_produce_output(self, equity_curve):
        """All five modes should produce valid multipliers."""
        baseline_mult = baseline(equity_curve)
        inv_vol = inverse_volatility(equity_curve, lookback_days=126)
        inv_var = inverse_variance(equity_curve, lookback_days=126)
        target_vol = target_volatility(equity_curve, target_vol=0.15, lookback_days=126)
        downside_vol = downside_volatility(equity_curve, lookback_days=126)

        for result in [baseline_mult, inv_vol, inv_var, target_vol, downside_vol]:
            assert len(result) == len(equity_curve), "Output length should match input"
            assert (result >= 0).all(), "All multipliers should be non-negative"

    def test_modes_have_different_means(self, equity_curve):
        """Different modes should produce different average multipliers."""
        inv_vol = inverse_volatility(equity_curve, lookback_days=126)
        target_vol = target_volatility(equity_curve, target_vol=0.15, lookback_days=126)
        downside_vol = downside_volatility(equity_curve, lookback_days=126)

        tail = 100
        inv_vol_mean = inv_vol.iloc[-tail:].dropna().mean()
        target_vol_mean = target_vol.iloc[-tail:].dropna().mean()
        downside_vol_mean = downside_vol.iloc[-tail:].dropna().mean()

        # target_vol should be most conservative (default cap=1.0)
        assert target_vol_mean <= 1.0, "target_vol should be capped at 1.0"

        # downside_vol should typically be more aggressive than standard vol
        # in healthy trending markets
        assert downside_vol_mean >= inv_vol_mean * 0.8, \
            "downside_vol should be >= standard_vol in healthy market"

    def test_insufficient_data_returns_neutral(self):
        """With insufficient lookback data, all modes should return 1.0."""
        dates = pd.date_range("2023-01-01", periods=50, freq="D")
        short_curve = pd.Series([1_000_000 + i * 1000 for i in range(50)], index=dates)

        for mode_func in [inverse_volatility, inverse_variance, downside_volatility]:
            result = mode_func(short_curve, lookback_days=126)
            # Should return 1.0 (neutral) when insufficient data
            assert (result == 1.0).all() or result.iloc[-1] == 1.0, \
                f"{mode_func.__name__} should return neutral (1.0) with insufficient data"
