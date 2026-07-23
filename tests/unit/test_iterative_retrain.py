"""
tests/unit/test_iterative_retrain.py

Unit tests for backtest/iterative_retrain.py's holdout fiscal-year
selection — the "leave out one full fiscal year, skipping any year whose
trades might not be fully resolved yet" mechanism.
"""

import pandas as pd
import pytest

from backtest.iterative_retrain import select_holdout_fiscal_year


class TestSelectHoldoutFiscalYear:
    def test_long_horizon_skips_the_most_recent_complete_fy(self):
        # User's own worked example: run in July 2026 with a ~1-year-horizon
        # strategy (252 trading days) should leave out FY2024-25
        # (2024-04-01 to 2025-03-31), skipping FY2025-26 as unresolved.
        result = select_holdout_fiscal_year(pd.Timestamp("2026-07-21"), resolution_buffer_days=252)

        assert result.holdout_start == pd.Timestamp("2024-04-01")
        assert result.holdout_end == pd.Timestamp("2025-03-31")
        assert result.skipped_fiscal_years == [2025]

    def test_short_horizon_uses_the_most_recent_complete_fy(self):
        # A 5-day-horizon strategy's trades from FY2025-26 (ended 2026-03-31)
        # are long since resolved by July 2026 — no buffer year needed.
        result = select_holdout_fiscal_year(pd.Timestamp("2026-07-21"), resolution_buffer_days=5)

        assert result.holdout_start == pd.Timestamp("2025-04-01")
        assert result.holdout_end == pd.Timestamp("2026-03-31")
        assert result.skipped_fiscal_years == []

    def test_holdout_end_is_always_before_as_of_date(self):
        result = select_holdout_fiscal_year(pd.Timestamp("2026-07-21"), resolution_buffer_days=252)
        assert result.holdout_end < pd.Timestamp("2026-07-21")

    def test_raises_when_horizon_exceeds_lookback_window(self):
        with pytest.raises(ValueError):
            select_holdout_fiscal_year(
                pd.Timestamp("2026-07-21"), resolution_buffer_days=100_000, max_lookback_years=3,
            )

    def test_explain_mentions_skipped_years(self):
        result = select_holdout_fiscal_year(pd.Timestamp("2026-07-21"), resolution_buffer_days=252)
        assert "FY2025-26" in result.explain()
        assert "FY2024-25" in result.explain()
