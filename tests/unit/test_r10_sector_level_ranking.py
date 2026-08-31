"""
tests/unit/test_r10_sector_level_ranking.py

B-028: Test R10 sector-level ranking integration.

Verifies that R10 (Nigam-Pandey momentum) correctly implements sector-level
aggregation:
1. Ranks sectors by average momentum of constituents
2. Selects top N sectors
3. Within top sectors, selects top K individual stocks
4. Distribution shows balanced sector exposure (not concentrated in single sector)
"""

import pandas as pd
import pytest
from backtest.adapters.momentum_adapter import MomentumAdapter
from backtest.core.horizon import HorizonBucket
from features.momentum_strategy import rank_sectors, rank_constituents_within_sectors


class TestR10SectorRankingLogic:
    """Test the underlying sector ranking functions used by R10."""

    def test_rank_sectors_by_average_momentum(self):
        """Sectors ranked by average momentum of their constituents."""
        momentum = pd.Series({
            # IT sector: average = (0.20 + 0.12) / 2 = 0.16
            "TCS": 0.20,
            "INFY": 0.12,
            # Finance sector: average = (0.10 + 0.08) / 2 = 0.09
            "HDFC": 0.10,
            "ICICI": 0.08,
        })
        sector_lookup = {
            "TCS": "IT",
            "INFY": "IT",
            "HDFC": "Finance",
            "ICICI": "Finance",
        }

        sector_scores = rank_sectors(momentum, sector_lookup)

        # IT should rank first (0.16 > 0.09)
        assert sector_scores.index[0] == "IT"
        assert sector_scores.index[1] == "Finance"
        assert abs(sector_scores["IT"] - 0.16) < 1e-6
        assert abs(sector_scores["Finance"] - 0.09) < 1e-6

    def test_filter_constituents_within_top_sectors(self):
        """Only constituents of top sectors are selected."""
        momentum = pd.Series({
            "TCS": 0.20,
            "INFY": 0.12,
            "HDFC": 0.10,
            "ICICI": 0.08,
            "RELIANCE": 0.05,  # Energy sector
        })
        sector_lookup = {
            "TCS": "IT",
            "INFY": "IT",
            "HDFC": "Finance",
            "ICICI": "Finance",
            "RELIANCE": "Energy",
        }

        # Select only top 2 sectors (IT and Finance, excluding Energy)
        filtered = rank_constituents_within_sectors(
            momentum, sector_lookup, ["IT", "Finance"]
        )

        assert len(filtered) == 4
        assert set(filtered.index) == {"TCS", "INFY", "HDFC", "ICICI"}
        assert "RELIANCE" not in filtered.index

    def test_sector_selection_creates_balanced_distribution(self):
        """Top sectors selection prevents concentration in single sector."""
        momentum = pd.Series({
            # IT: 5 stocks with high momentum (avg 0.18)
            "TCS": 0.20,
            "INFY": 0.18,
            "WIPRO": 0.16,
            "HCLTECH": 0.18,
            "TECHM": 0.17,
            # Finance: 3 stocks with medium momentum (avg 0.10)
            "HDFC": 0.12,
            "ICICI": 0.10,
            "SBIN": 0.08,
            # Auto: 2 stocks with low momentum (avg 0.04)
            "MARUTI": 0.05,
            "BAJAJAUT": 0.03,
        })
        sector_lookup = {
            "TCS": "IT", "INFY": "IT", "WIPRO": "IT", "HCLTECH": "IT", "TECHM": "IT",
            "HDFC": "Finance", "ICICI": "Finance", "SBIN": "Finance",
            "MARUTI": "Auto", "BAJAJAUT": "Auto",
        }

        # Select top 2 sectors
        sector_scores = rank_sectors(momentum, sector_lookup)
        top_sectors = sector_scores.head(2).index.tolist()
        assert "IT" in top_sectors
        assert "Finance" in top_sectors
        assert "Auto" not in top_sectors

        # Filter to top sectors
        filtered = rank_constituents_within_sectors(
            momentum, sector_lookup, top_sectors
        )

        # Should have 8 stocks total (5 IT + 3 Finance)
        assert len(filtered) == 8
        # Verify sector distribution
        it_stocks = {s for s, m in zip(filtered.index, filtered.values)
                     if sector_lookup.get(s) == "IT"}
        finance_stocks = {s for s, m in zip(filtered.index, filtered.values)
                          if sector_lookup.get(s) == "Finance"}
        assert len(it_stocks) == 5
        assert len(finance_stocks) == 3


class TestR10MomentumAdapterIntegration:
    """Test R10 sector ranking with MomentumAdapter."""

    def test_momentum_adapter_with_sector_ranking(self):
        """MomentumAdapter with industry_momentum rank_method applies sector ranking."""
        # Create a simple price panel
        dates = pd.date_range("2025-01-01", periods=30, freq="D")
        price_panel = pd.DataFrame({
            "TCS": [100 + i for i in range(30)],  # Uptrend
            "INFY": [100 + i*0.8 for i in range(30)],  # Moderate uptrend
            "HDFC": [100 + i*0.5 for i in range(30)],  # Mild uptrend
            "ICICI": [100 - i*0.3 for i in range(30)],  # Downtrend
            "RELIANCE": [100 - i*0.5 for i in range(30)],  # Stronger downtrend
        }, index=dates)

        sector_lookup = {
            "TCS": "IT",
            "INFY": "IT",
            "HDFC": "Finance",
            "ICICI": "Finance",
            "RELIANCE": "Energy",
        }

        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=3,
            lookback_months=1,
            sector_lookup=sector_lookup,
            rank_method="industry_momentum",
            top_sectors=2,  # Select top 2 sectors
        )

        # Generate signals on last date
        test_date = dates[-1]
        universe = ["TCS", "INFY", "HDFC", "ICICI", "RELIANCE"]
        horizon = HorizonBucket(rebalance_date=test_date, holding_days=5)

        signals = adapter.generate_signals(universe, test_date, horizon)

        # Should generate buy signals for top 3 stocks from top 2 sectors
        buy_signals = [s for s in signals if s.action == "buy"]
        assert len(buy_signals) == 3, f"Expected 3 buy signals, got {len(buy_signals)}"

        # Bought stocks should be from IT and Finance (top 2 sectors)
        bought_tickers = {s.ticker for s in buy_signals}
        for ticker in bought_tickers:
            sector = sector_lookup.get(ticker)
            # Should not have RELIANCE (Energy is not in top 2 sectors)
            assert ticker != "RELIANCE", "RELIANCE should not be selected (Energy not in top 2)"
            assert sector in ["IT", "Finance"], f"{ticker} should be from top sectors"

    def test_sector_ranking_prevents_single_sector_concentration(self):
        """Sector ranking prevents portfolio from being too concentrated."""
        # Create a scenario where one sector dominates
        dates = pd.date_range("2025-01-01", periods=50, freq="D")
        price_panel = pd.DataFrame({
            # IT sector (5 stocks, all with strong uptrend)
            "TCS": [100 + i*2 for i in range(50)],
            "INFY": [100 + i*1.9 for i in range(50)],
            "WIPRO": [100 + i*1.8 for i in range(50)],
            "HCLTECH": [100 + i*1.7 for i in range(50)],
            "TECHM": [100 + i*1.6 for i in range(50)],
            # Finance sector (3 stocks, weak uptrend)
            "HDFC": [100 + i*0.3 for i in range(50)],
            "ICICI": [100 + i*0.2 for i in range(50)],
            "SBIN": [100 + i*0.1 for i in range(50)],
        }, index=dates)

        sector_lookup = {
            "TCS": "IT", "INFY": "IT", "WIPRO": "IT", "HCLTECH": "IT", "TECHM": "IT",
            "HDFC": "Finance", "ICICI": "Finance", "SBIN": "Finance",
        }

        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=5,  # Hold 5 stocks
            lookback_months=2,
            sector_lookup=sector_lookup,
            rank_method="industry_momentum",
            top_sectors=2,  # Select top 2 sectors
        )

        test_date = dates[-1]
        universe = list(sector_lookup.keys())
        horizon = HorizonBucket(rebalance_date=test_date, holding_days=5)

        signals = adapter.generate_signals(universe, test_date, horizon)

        buy_signals = [s for s in signals if s.action == "buy"]
        assert len(buy_signals) == 5, f"Expected 5 buy signals, got {len(buy_signals)}"

        bought_tickers = {s.ticker for s in buy_signals}
        sector_counts = {}
        for ticker in bought_tickers:
            sector = sector_lookup[ticker]
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

        # Should have a distribution, not all from IT
        # With top_sectors=2, should have both IT and Finance represented
        assert "IT" in sector_counts, "IT should be represented"
        assert "Finance" in sector_counts, "Finance should be represented"
        # IT might have more, but Finance should have at least 1
        assert sector_counts["Finance"] >= 1, "Finance should have at least 1 stock"


class TestR10SectorRankingEdgeCases:
    """Test edge cases in sector-level ranking."""

    def test_empty_sector_lookup_fallback(self):
        """Without sector_lookup, should fall back to individual ranking."""
        dates = pd.date_range("2025-01-01", periods=30, freq="D")
        price_panel = pd.DataFrame({
            "A": [100 + i for i in range(30)],
            "B": [100 + i*0.5 for i in range(30)],
            "C": [100 - i*0.5 for i in range(30)],
        }, index=dates)

        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=2,
            lookback_months=1,
            sector_lookup=None,  # No sector lookup
            rank_method="industry_momentum",
            top_sectors=2,
        )

        test_date = dates[-1]
        universe = ["A", "B", "C"]
        horizon = HorizonBucket(rebalance_date=test_date, holding_days=5)

        signals = adapter.generate_signals(universe, test_date, horizon)

        buy_signals = [s for s in signals if s.action == "buy"]
        # Should still select top 2, even without sector info
        assert len(buy_signals) == 2

    def test_single_stock_per_sector(self):
        """Sector ranking works with single stock per sector."""
        dates = pd.date_range("2025-01-01", periods=30, freq="D")
        price_panel = pd.DataFrame({
            "TCS": [100 + i for i in range(30)],
            "HDFC": [100 + i*0.5 for i in range(30)],
            "RELIANCE": [100 + i*0.3 for i in range(30)],
        }, index=dates)

        sector_lookup = {
            "TCS": "IT",
            "HDFC": "Finance",
            "RELIANCE": "Energy",
        }

        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=2,
            lookback_months=1,
            sector_lookup=sector_lookup,
            rank_method="industry_momentum",
            top_sectors=2,
        )

        test_date = dates[-1]
        universe = ["TCS", "HDFC", "RELIANCE"]
        horizon = HorizonBucket(rebalance_date=test_date, holding_days=5)

        signals = adapter.generate_signals(universe, test_date, horizon)

        buy_signals = [s for s in signals if s.action == "buy"]
        # Should select 2 stocks from top 2 sectors
        assert len(buy_signals) == 2
        bought_tickers = {s.ticker for s in buy_signals}
        # Should be TCS and HDFC (top 2 by momentum within their respective sectors)
        assert "TCS" in bought_tickers
        assert "HDFC" in bought_tickers


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
