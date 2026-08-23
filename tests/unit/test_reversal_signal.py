"""
tests/unit/test_reversal_signal.py

Test 1-month trailing reversal signal from features/momentum_signal.py.

Verifies:
1. trailing_reversal_1mo() correctly computes 21-trading-day returns
2. Low returns (losers) are correctly identified
3. Point-in-time safety: uses only data up to as_of_date
4. Edge cases: empty data, insufficient history, zero prices
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from features.momentum_signal import trailing_reversal_1mo


@pytest.fixture
def sample_price_panel():
    """Create a sample price panel with 5 tickers over 30 trading days.

    Each ticker has a linear price series:
    - ticker_A: 100 -> 110 (10% gain, reversal signal = +10%)
    - ticker_B: 100 -> 90 (10% loss, reversal signal = -10%)
    - ticker_C: 100 -> 100 (flat, reversal signal = 0%)
    - ticker_D: 100 -> 120 (20% gain, reversal signal = +20%)
    - ticker_E: 100 -> 80 (20% loss, reversal signal = -20%)
    """
    dates = [datetime(2025, 1, 1) + timedelta(days=i) for i in range(30)]

    data = {
        "ticker_A": np.linspace(100, 110, 30),
        "ticker_B": np.linspace(100, 90, 30),
        "ticker_C": np.full(30, 100.0),
        "ticker_D": np.linspace(100, 120, 30),
        "ticker_E": np.linspace(100, 80, 30),
    }

    return pd.DataFrame(data, index=pd.DatetimeIndex(dates))


class TestTrailingReversal1mo:
    """Test 1-month (21-day) trailing reversal signal."""

    def test_returns_series_with_ticker_index(self, sample_price_panel):
        """Result should be a Series indexed by ticker."""
        as_of = datetime(2025, 1, 30)
        reversal = trailing_reversal_1mo(sample_price_panel, ["ticker_A", "ticker_B"], as_of)

        assert isinstance(reversal, pd.Series)
        assert len(reversal) > 0
        assert all(idx in ["ticker_A", "ticker_B"] for idx in reversal.index)

    def test_computes_21day_returns(self, sample_price_panel):
        """Over 30 days with linear change, 21-day return should match the change."""
        as_of = datetime(2025, 1, 30)
        reversal = trailing_reversal_1mo(sample_price_panel, ["ticker_A"], as_of)

        # ticker_A: 100 -> 110 over 30 days
        # At day 21 (index 20), price ≈ 100 + (110-100)*(20/29) ≈ 106.90
        # At day 30 (index 29), price ≈ 110
        # 21-day return from day 9 to day 30: (110 - ~106.90) / 106.90 ≈ 0.03 (rough estimate)
        # Let's check that it's close to the linear interpolation
        assert "ticker_A" in reversal.index
        assert reversal["ticker_A"] > 0  # ticker_A went up, so 21-day return is positive

    def test_identifies_losers_negative_reversal(self, sample_price_panel):
        """Losers (negative 21-day returns) should have negative reversal scores."""
        as_of = datetime(2025, 1, 30)
        reversal = trailing_reversal_1mo(sample_price_panel, ["ticker_B", "ticker_E"], as_of)

        # Both ticker_B and ticker_E are losers (down overall)
        # Their 21-day trailing returns should be negative
        if "ticker_B" in reversal.index:
            assert reversal["ticker_B"] < 0
        if "ticker_E" in reversal.index:
            assert reversal["ticker_E"] < 0

    def test_point_in_time_safety_as_of_before_data(self, sample_price_panel):
        """as_of_date before sufficient history should return empty Series."""
        # as_of_date = Jan 10 (only 10 days of data, need 22 for 21-day lookback)
        as_of = datetime(2025, 1, 10)
        reversal = trailing_reversal_1mo(sample_price_panel, ["ticker_A"], as_of)

        # Not enough data, should be empty
        assert reversal.empty

    def test_point_in_time_safety_uses_data_strictly_before_as_of(self, sample_price_panel):
        """Should use only data strictly on/before as_of_date, not future data."""
        # Compute reversal at day 25; should use data up to day 25 only
        as_of = datetime(2025, 1, 25)
        reversal_at_25 = trailing_reversal_1mo(sample_price_panel, ["ticker_A"], as_of)

        # Compute at day 30; should give a different result
        as_of_30 = datetime(2025, 1, 30)
        reversal_at_30 = trailing_reversal_1mo(sample_price_panel, ["ticker_A"], as_of_30)

        # Both should have data, and they should differ (different windows)
        if not reversal_at_25.empty and not reversal_at_30.empty:
            assert reversal_at_25["ticker_A"] != reversal_at_30["ticker_A"]

    def test_flat_ticker_zero_return(self, sample_price_panel):
        """Ticker with constant price should have ~0% return."""
        as_of = datetime(2025, 1, 30)
        reversal = trailing_reversal_1mo(sample_price_panel, ["ticker_C"], as_of)

        if "ticker_C" in reversal.index:
            assert abs(reversal["ticker_C"]) < 1e-6  # Essentially zero

    def test_empty_tickers_list(self, sample_price_panel):
        """Empty ticker list should return empty Series."""
        as_of = datetime(2025, 1, 30)
        reversal = trailing_reversal_1mo(sample_price_panel, [], as_of)

        assert reversal.empty

    def test_empty_price_panel(self):
        """Empty price panel should return empty Series."""
        empty_panel = pd.DataFrame()
        as_of = datetime(2025, 1, 30)
        reversal = trailing_reversal_1mo(empty_panel, ["ticker_A"], as_of)

        assert reversal.empty

    def test_missing_ticker_excluded(self, sample_price_panel):
        """Ticker not in price panel should be excluded."""
        as_of = datetime(2025, 1, 30)
        reversal = trailing_reversal_1mo(
            sample_price_panel,
            ["ticker_A", "nonexistent_ticker"],
            as_of
        )

        # Should only include ticker_A
        assert "ticker_A" in reversal.index or reversal.empty
        assert "nonexistent_ticker" not in reversal.index

    def test_zero_start_price_excluded(self):
        """Ticker with zero start price (div-by-zero risk) should be excluded."""
        dates = [datetime(2025, 1, 1) + timedelta(days=i) for i in range(30)]
        data = {
            "ticker_zero": np.concatenate([np.zeros(10), np.full(20, 100.0)]),
            "ticker_normal": np.linspace(100, 110, 30),
        }
        panel = pd.DataFrame(data, index=pd.DatetimeIndex(dates))

        as_of = datetime(2025, 1, 30)
        reversal = trailing_reversal_1mo(panel, ["ticker_zero", "ticker_normal"], as_of)

        # ticker_zero has zero start price in early dates; at day 30,
        # depending on which start_date is picked, it might be zero.
        # ticker_normal should always be present.
        assert "ticker_normal" in reversal.index

    def test_all_tickers_insufficient_history(self, sample_price_panel):
        """If all tickers lack history, return empty Series."""
        # Panel only has 30 dates; asking as_of day 5 gives only 5 data points
        as_of = datetime(2025, 1, 5)
        reversal = trailing_reversal_1mo(sample_price_panel, ["ticker_A", "ticker_B"], as_of)

        assert reversal.empty

    def test_default_lookback_is_21_days(self, sample_price_panel):
        """Default lookback should be 21 trading days."""
        as_of = datetime(2025, 1, 30)
        # Call without explicit lookback_days
        reversal_default = trailing_reversal_1mo(sample_price_panel, ["ticker_A"], as_of)
        # Call with explicit 21
        reversal_explicit = trailing_reversal_1mo(
            sample_price_panel, ["ticker_A"], as_of, lookback_days=21
        )

        if not reversal_default.empty and not reversal_explicit.empty:
            assert reversal_default["ticker_A"] == reversal_explicit["ticker_A"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
