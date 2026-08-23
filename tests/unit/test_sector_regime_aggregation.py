"""
tests/unit/test_sector_regime_aggregation.py

Test sector and regime aggregation functions from backtest/core/metrics.py.

Verifies:
1. aggregate_by_sector() groups holdings by sector and computes aggregate returns
2. aggregate_by_regime() groups daily returns by regime label
3. Edge cases: empty data, missing sector/regime labels, None returns
"""

import pytest
import pandas as pd

from backtest.core.metrics import aggregate_by_sector, aggregate_by_regime


class TestAggregateBySector:
    """Test sector-based aggregation of holdings."""

    @pytest.fixture
    def sample_holdings(self):
        """Create sample holdings with ticker and return info."""
        return [
            {"ticker": "INFY", "return_pct": 5.0, "entry_date": "2025-01-01"},
            {"ticker": "TCS", "return_pct": 3.0, "entry_date": "2025-01-02"},
            {"ticker": "HDFC", "return_pct": -2.0, "entry_date": "2025-01-03"},
            {"ticker": "ICICI", "return_pct": 4.0, "entry_date": "2025-01-04"},
            {"ticker": "ITC", "return_pct": 1.0, "entry_date": "2025-01-05"},
        ]

    @pytest.fixture
    def sector_lookup(self):
        """Create sector mapping."""
        return {
            "INFY": "IT",
            "TCS": "IT",
            "HDFC": "Finance",
            "ICICI": "Finance",
            "ITC": "Consumer",
        }

    def test_aggregates_by_sector(self, sample_holdings, sector_lookup):
        """Holdings should be grouped by sector."""
        result = aggregate_by_sector(sample_holdings, sector_lookup)

        assert "IT" in result
        assert "Finance" in result
        assert "Consumer" in result

    def test_computes_average_return_per_sector(self, sample_holdings, sector_lookup):
        """Avg return per sector should be correct."""
        result = aggregate_by_sector(sample_holdings, sector_lookup)

        # IT: (5.0 + 3.0) / 2 = 4.0
        assert result["IT"]["avg_return_pct"] == pytest.approx(4.0)
        # Finance: (-2.0 + 4.0) / 2 = 1.0
        assert result["Finance"]["avg_return_pct"] == pytest.approx(1.0)
        # Consumer: 1.0
        assert result["Consumer"]["avg_return_pct"] == pytest.approx(1.0)

    def test_counts_trades_per_sector(self, sample_holdings, sector_lookup):
        """Trade count per sector should be correct."""
        result = aggregate_by_sector(sample_holdings, sector_lookup)

        assert result["IT"]["n_trades"] == 2
        assert result["Finance"]["n_trades"] == 2
        assert result["Consumer"]["n_trades"] == 1

    def test_handles_missing_sector_lookup(self, sample_holdings):
        """Without sector_lookup, all holdings should be grouped as 'Unknown'."""
        result = aggregate_by_sector(sample_holdings, sector_lookup=None)

        # All returns are present: 5 + 3 - 2 + 4 + 1 = 11, avg = 11/5 = 2.2
        assert "Unknown" in result
        assert result["Unknown"]["n_trades"] == 5
        assert result["Unknown"]["avg_return_pct"] == pytest.approx(2.2)

    def test_handles_empty_holdings(self):
        """Empty holdings list should return empty dict."""
        result = aggregate_by_sector([], sector_lookup={})

        assert result == {}

    def test_handles_holdings_with_missing_return(self, sector_lookup):
        """Holdings without return_pct should be skipped in avg calculation."""
        holdings = [
            {"ticker": "INFY", "return_pct": 5.0},
            {"ticker": "TCS", "return_pct": None},  # Missing return
            {"ticker": "HDFC", "return_pct": -2.0},
        ]
        result = aggregate_by_sector(holdings, sector_lookup)

        # IT: only INFY has return, so avg = 5.0
        assert result["IT"]["avg_return_pct"] == pytest.approx(5.0)
        assert result["IT"]["n_trades"] == 2  # Both tickers counted

    def test_computes_max_min_returns(self, sample_holdings, sector_lookup):
        """Max and min returns per sector should be computed."""
        result = aggregate_by_sector(sample_holdings, sector_lookup)

        # IT: [5.0, 3.0]
        assert result["IT"]["max_return_pct"] == pytest.approx(5.0)
        assert result["IT"]["min_return_pct"] == pytest.approx(3.0)

        # Finance: [-2.0, 4.0]
        assert result["Finance"]["max_return_pct"] == pytest.approx(4.0)
        assert result["Finance"]["min_return_pct"] == pytest.approx(-2.0)


class TestAggregateByRegime:
    """Test regime-based aggregation of daily returns."""

    @pytest.fixture
    def sample_returns(self):
        """Create sample daily returns."""
        return pd.Series(
            [0.01, 0.02, -0.01, 0.03, -0.02, 0.01, -0.03, 0.02],
            index=pd.date_range("2025-01-01", periods=8, freq="D"),
            name="daily_return"
        )

    @pytest.fixture
    def regime_labels(self):
        """Create regime labels aligned with returns."""
        return pd.Series(
            ["bull", "bull", "bear", "bear", "bull", "bull", "crash", "crash"],
            index=pd.date_range("2025-01-01", periods=8, freq="D"),
            name="regime"
        )

    def test_aggregates_by_regime(self, sample_returns, regime_labels):
        """Returns should be grouped by regime."""
        result = aggregate_by_regime(sample_returns, regime_labels)

        assert "bull" in result
        assert "bear" in result
        assert "crash" in result

    def test_computes_avg_daily_return_per_regime(self, sample_returns, regime_labels):
        """Avg daily return per regime should be correct."""
        result = aggregate_by_regime(sample_returns, regime_labels)

        # bull: indices 0,1,4,5 = [0.01, 0.02, -0.02, 0.01] avg = 0.005
        bull_ret = (0.01 + 0.02 - 0.02 + 0.01) / 4
        assert result["bull"]["avg_daily_return"] == pytest.approx(bull_ret)

        # bear: indices 2,3 = [-0.01, 0.03] avg = 0.01
        bear_ret = (-0.01 + 0.03) / 2
        assert result["bear"]["avg_daily_return"] == pytest.approx(bear_ret)

        # crash: indices 6,7 = [-0.03, 0.02] avg = -0.005
        crash_ret = (-0.03 + 0.02) / 2
        assert result["crash"]["avg_daily_return"] == pytest.approx(crash_ret)

    def test_computes_volatility_per_regime(self, sample_returns, regime_labels):
        """Volatility per regime should be computed."""
        result = aggregate_by_regime(sample_returns, regime_labels)

        # Bull volatility
        assert "volatility" in result["bull"]
        assert result["bull"]["volatility"] >= 0

        # Crash volatility (should be higher due to extreme moves)
        assert result["crash"]["volatility"] >= 0

    def test_counts_days_per_regime(self, sample_returns, regime_labels):
        """Number of days per regime should be correct."""
        result = aggregate_by_regime(sample_returns, regime_labels)

        assert result["bull"]["n_days"] == 4  # indices 0,1,4,5
        assert result["bear"]["n_days"] == 2  # indices 2,3
        assert result["crash"]["n_days"] == 2  # indices 6,7

    def test_computes_cumulative_return_per_regime(self, sample_returns, regime_labels):
        """Cumulative return per regime should be computed."""
        result = aggregate_by_regime(sample_returns, regime_labels)

        # bull: [0.01, 0.02, -0.02, 0.01] -> (1+0.01)*(1+0.02)*(1-0.02)*(1+0.01) - 1
        bull_cum = (1 + 0.01) * (1 + 0.02) * (1 - 0.02) * (1 + 0.01) - 1
        assert result["bull"]["cumulative_return"] == pytest.approx(bull_cum)

    def test_handles_missing_regime_labels(self, sample_returns):
        """Without regime labels, all returns should be grouped as 'Unknown'."""
        result = aggregate_by_regime(sample_returns, regime_labels=None)

        assert "Unknown" in result
        assert result["Unknown"]["n_days"] == len(sample_returns)

    def test_handles_empty_returns(self):
        """Empty returns series should return empty dict."""
        returns = pd.Series([], dtype=float)
        result = aggregate_by_regime(returns, regime_labels=None)

        assert result == {}

    def test_handles_empty_regime_labels(self, sample_returns):
        """Empty regime labels should fall back to 'Unknown'."""
        regime_labels = pd.Series([], dtype=str)
        result = aggregate_by_regime(sample_returns, regime_labels)

        # With empty regime labels, should fall back to "Unknown"
        assert "Unknown" in result
        assert result["Unknown"]["n_days"] == len(sample_returns)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
