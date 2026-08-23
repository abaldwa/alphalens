"""
tests/unit/test_pct_of_52wk_high.py

Unit tests for features/momentum_signal.py::pct_of_52wk_high()
R5 (spec 7.5): 52-week-high momentum signal.
"""

import pandas as pd

from features.momentum_signal import pct_of_52wk_high


class TestPctOf52WkHigh:
    """Pure-function tests for 52-week-high ranking signal."""

    def test_empty_inputs(self):
        """Empty price panel or ticker list returns empty Series."""
        empty_df = pd.DataFrame()
        result = pct_of_52wk_high(empty_df, ["SBIN", "INFY"], "2025-01-01")
        assert result.empty

        result = pct_of_52wk_high(pd.DataFrame(), [], "2025-01-01")
        assert result.empty

    def test_insufficient_history(self):
        """Fewer than lookback_days+1 dates returns empty Series."""
        dates = pd.date_range("2025-01-01", periods=100, freq="D")
        prices = pd.DataFrame({
            "SBIN": [100.0] * 100,
            "INFY": [200.0] * 100,
        }, index=dates)

        result = pct_of_52wk_high(prices, ["SBIN"], "2025-01-01", lookback_days=252)
        assert result.empty

    def test_at_52wk_high(self):
        """Stock at its 252-day peak scores 1.0."""
        dates = pd.date_range("2023-01-01", periods=300, freq="D")
        prices = pd.DataFrame({
            "SBIN": [100.0] * 300,  # flat at 100
        }, index=dates)

        result = pct_of_52wk_high(prices, ["SBIN"], "2024-01-01", lookback_days=252)
        assert len(result) == 1
        assert "SBIN" in result.index
        # At-or-above 52wk high
        assert abs(result["SBIN"] - 1.0) < 0.001

    def test_at_52wk_low(self):
        """Stock at its 252-day low scores near bottom of range."""
        dates = pd.date_range("2023-01-01", periods=300, freq="D")
        # Trend from 200 down to 100 at end
        prices_vals = [200.0 - (100.0 * i / 299) for i in range(300)]
        prices = pd.DataFrame({
            "TICKER": prices_vals,
        }, index=dates)

        # Last date: price is ~100, 52wk high is ~200 (from full window)
        result = pct_of_52wk_high(prices, ["TICKER"], dates[-1].strftime("%Y-%m-%d"), lookback_days=252)
        assert len(result) == 1
        assert "TICKER" in result.index
        # Score should be low (current / high where high is from window)
        assert result["TICKER"] < 0.6

    def test_above_52wk_high(self):
        """Stock can score >= 1.0 when current equals or exceeds peak in window."""
        dates = pd.date_range("2023-01-01", periods=300, freq="D")
        # First 250 days at 100, last 50 days at 150
        prices_vals = [100.0 if i < 250 else 150.0 for i in range(300)]
        prices = pd.DataFrame({
            "TICKER": prices_vals,
        }, index=dates)

        # Last date: price is 150, 52wk high is 150 (current price is at/above high)
        result = pct_of_52wk_high(prices, ["TICKER"], dates[-1].strftime("%Y-%m-%d"), lookback_days=252)
        assert len(result) == 1
        assert result["TICKER"] >= 1.0  # At or above the 52wk high

    def test_missing_ticker(self):
        """Ticker not in price_panel is excluded."""
        dates = pd.date_range("2023-01-01", periods=300, freq="D")
        prices = pd.DataFrame({
            "SBIN": [100.0] * 300,
        }, index=dates)

        result = pct_of_52wk_high(prices, ["SBIN", "NONEXISTENT"], dates[-1].strftime("%Y-%m-%d"), lookback_days=252)
        assert len(result) == 1
        assert "SBIN" in result.index
        assert "NONEXISTENT" not in result.index

    def test_multiple_tickers(self):
        """Multiple tickers ranked independently."""
        dates = pd.date_range("2023-01-01", periods=300, freq="D")
        prices = pd.DataFrame({
            "SBIN": [100.0] * 300,
            "INFY": [200.0] * 250 + [150.0] * 50,  # Down 50% recently
            "TCS": [150.0] * 100 + [200.0] * 200,  # Up trend
        }, index=dates)

        result = pct_of_52wk_high(
            prices, ["SBIN", "INFY", "TCS"],
            dates[-1].strftime("%Y-%m-%d"), lookback_days=252
        )
        assert len(result) == 3
        # SBIN at high (1.0), INFY down (0.75), TCS near high (1.0)
        assert abs(result["SBIN"] - 1.0) < 0.01
        assert abs(result["INFY"] - 0.75) < 0.01
        assert result["TCS"] >= 1.0

    def test_future_date_ignored(self):
        """Date beyond price panel returns empty."""
        dates = pd.date_range("2023-01-01", periods=100, freq="D")
        prices = pd.DataFrame({
            "SBIN": [100.0] * 100,
        }, index=dates)

        result = pct_of_52wk_high(prices, ["SBIN"], "2030-01-01", lookback_days=252)
        assert result.empty

    def test_exact_lookback_window_boundary(self):
        """Signal at exactly lookback_days+1 observations."""
        dates = pd.date_range("2023-01-01", periods=253, freq="D")  # 252-day lookback + 1
        prices = pd.DataFrame({
            "SBIN": list(range(1, 254)),  # trend up from 1 to 253
        }, index=dates)

        result = pct_of_52wk_high(prices, ["SBIN"], dates[-1].strftime("%Y-%m-%d"), lookback_days=252)
        assert len(result) == 1
        # Current: 253, High: 253, Score: 253/253 = 1.0
        assert abs(result["SBIN"] - 1.0) < 0.001

    def test_nan_prices_in_window_handled(self):
        """NaN prices in window don't break calculation; .max() skips NaN values."""
        dates = pd.date_range("2023-01-01", periods=300, freq="D")
        prices = pd.DataFrame({
            "CLEAN": [100.0] * 300,
            "DIRTY": [100.0] * 100 + [float("nan")] * 100 + [100.0] * 100,
        }, index=dates)

        result = pct_of_52wk_high(
            prices, ["CLEAN", "DIRTY"],
            dates[-1].strftime("%Y-%m-%d"), lookback_days=252
        )
        # Both should be present: .max() skips NaN values, so DIRTY's high is still 100.0
        assert "CLEAN" in result.index
        assert "DIRTY" in result.index
        # Both flat at 100 score 1.0
        assert abs(result["CLEAN"] - 1.0) < 0.001
        assert abs(result["DIRTY"] - 1.0) < 0.001

    def test_zero_high_excluded(self):
        """Division by zero when high=0 is handled (ticker excluded)."""
        dates = pd.date_range("2023-01-01", periods=300, freq="D")
        prices = pd.DataFrame({
            "NORMAL": [100.0] * 300,
            "ZERO_HIGH": [0.0] * 300,  # Pathological: price always 0
        }, index=dates)

        result = pct_of_52wk_high(
            prices, ["NORMAL", "ZERO_HIGH"],
            dates[-1].strftime("%Y-%m-%d"), lookback_days=252
        )
        assert "NORMAL" in result.index
        assert "ZERO_HIGH" not in result.index
