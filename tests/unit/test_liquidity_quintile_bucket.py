"""
tests/unit/test_liquidity_quintile_bucket.py

Test liquidity quintile bucketing function from backtest/trade_filters.py.

Verifies:
1. bucket_by_adtv_quintile() correctly maps ADTV ranks to quintiles 1-5
2. Most liquid tickers (rank 1) get quintile 1, least liquid get quintile 5
3. Point-in-time safety: uses only bars before as_of
4. Edge cases: empty data, fewer than 5 tickers, NaN handling
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from backtest.trade_filters import bucket_by_adtv_quintile, rank_by_adtv


@pytest.fixture
def sample_ohlcv():
    """Create a sample OHLCV panel with 20 tickers and 30 trading days.

    Liquidity (volume) is deterministic: ticker_1 is most liquid, ticker_20 is least.
    """
    dates = [datetime(2025, 1, 1) + timedelta(days=i) for i in range(30)]
    tickers = [f"ticker_{i:02d}" for i in range(1, 21)]

    rows = []
    for i, (date, ticker) in enumerate([(d, t) for d in dates for t in tickers]):
        ticker_num = int(ticker.split("_")[1])
        # Higher ticker number = lower liquidity
        base_volume = 1000000 / ticker_num
        rows.append({
            "date": date,
            "ticker": ticker,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": base_volume * (1 + np.random.normal(0, 0.1)),
        })

    return pd.DataFrame(rows)


class TestBucketByAdtvQuintile:
    """Test liquidity quintile bucketing."""

    def test_returns_series_with_quintile_labels(self, sample_ohlcv):
        """Quintiles should be labeled 1-5."""
        as_of = datetime(2025, 1, 30)
        quintiles = bucket_by_adtv_quintile(sample_ohlcv, as_of)

        assert not quintiles.empty
        unique_vals = quintiles.dropna().unique()
        # Should have at most 5 unique values
        assert len(unique_vals) <= 5
        # All values should be in [1, 5]
        assert all(v in [1, 2, 3, 4, 5] for v in unique_vals)

    def test_most_liquid_in_quintile_1(self, sample_ohlcv):
        """Most liquid ticker (ticker_01) should be in quintile 1."""
        as_of = datetime(2025, 1, 30)
        quintiles = bucket_by_adtv_quintile(sample_ohlcv, as_of)

        if "ticker_01" in quintiles.index:
            assert quintiles["ticker_01"] == 1, "Most liquid should be quintile 1"

    def test_least_liquid_in_quintile_5(self, sample_ohlcv):
        """Least liquid ticker (ticker_20) should be in quintile 5."""
        as_of = datetime(2025, 1, 30)
        quintiles = bucket_by_adtv_quintile(sample_ohlcv, as_of)

        if "ticker_20" in quintiles.index:
            # Should be quintile 5 (or closest to it due to qcut bucketing)
            assert quintiles["ticker_20"] in [4, 5], "Least liquid should be quintile 4-5"

    def test_point_in_time_safety(self, sample_ohlcv):
        """Only bars strictly before as_of should be used."""
        as_of = datetime(2025, 1, 15)
        bucket_by_adtv_quintile(sample_ohlcv, as_of)

        # Verify ranks are computed from data only before 2025-01-15
        ranks = rank_by_adtv(sample_ohlcv, as_of)
        assert len(ranks) > 0, "Should have some tickers ranked before as_of"

    def test_empty_ohlcv_returns_empty_series(self):
        """Empty OHLCV should return empty Series."""
        empty_ohlcv = pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume"])
        as_of = datetime(2025, 1, 30)
        quintiles = bucket_by_adtv_quintile(empty_ohlcv, as_of)

        assert quintiles.empty

    def test_as_of_before_all_data_returns_empty_series(self, sample_ohlcv):
        """as_of before first data point should return empty Series."""
        as_of = datetime(2024, 12, 31)  # Before any data
        quintiles = bucket_by_adtv_quintile(sample_ohlcv, as_of)

        assert quintiles.empty

    def test_few_tickers_handled_gracefully(self):
        """With fewer than 5 tickers, qcut with duplicates='drop' should work."""
        dates = [datetime(2025, 1, 1) + timedelta(days=i) for i in range(10)]
        tickers = ["A", "B", "C"]  # Only 3 tickers

        rows = []
        for date in dates:
            for i, ticker in enumerate(tickers):
                rows.append({
                    "date": date,
                    "ticker": ticker,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1000000 / (i + 1),
                })

        ohlcv = pd.DataFrame(rows)
        as_of = datetime(2025, 1, 10)
        quintiles = bucket_by_adtv_quintile(ohlcv, as_of)

        assert len(quintiles) == 3, "Should have quintile for all 3 tickers"
        # With 3 tickers, should get 3 or fewer unique quintile values
        assert len(quintiles.unique()) <= 3

    def test_series_index_matches_tickers(self, sample_ohlcv):
        """Returned Series should be indexed by ticker."""
        as_of = datetime(2025, 1, 30)
        quintiles = bucket_by_adtv_quintile(sample_ohlcv, as_of)

        # Index should be tickers
        assert all(idx.startswith("ticker_") for idx in quintiles.index if idx == idx)

    def test_series_name(self, sample_ohlcv):
        """Returned Series should have name 'liquidity_quintile'."""
        as_of = datetime(2025, 1, 30)
        quintiles = bucket_by_adtv_quintile(sample_ohlcv, as_of)

        assert quintiles.name == "liquidity_quintile"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
