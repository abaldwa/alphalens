"""
tests/unit/test_r11_reversal_selection.py

Unit tests for R11 reversal selection (B-029 fix).
Verifies that R11 correctly selects LOWEST pct_of_52wk_high scores
(stocks far from 52-week highs, oversold) for mean-reversion strategy.
"""

import pandas as pd
from backtest.reversal_selector import (
    get_sort_order_for_rank_method,
    select_by_rank_method,
)


class TestR11ReversalSelection:
    """Tests for R11 52-week-high reversal strategy selection."""

    def test_pct_of_52wk_high_is_reversal(self):
        """Verify that pct_of_52wk_high is recognized as a reversal method."""
        ascending = get_sort_order_for_rank_method("pct_of_52wk_high")
        assert ascending is True, "pct_of_52wk_high should be reversal (ascending=True)"

    def test_r11_selects_lowest_scores(self):
        """R11 should select LOWEST pct_of_52wk_high scores (most oversold stocks)."""
        # pct_of_52wk_high scores: higher = near 52wk high, lower = far from high (oversold)
        scores = pd.Series({
            "STOCK_A": 0.95,  # Near 52wk high (not oversold)
            "STOCK_B": 0.50,  # Mid-range
            "STOCK_C": 0.10,  # Far from 52wk high (very oversold) <- should pick
            "STOCK_D": 0.20,  # Somewhat oversold <- should pick
            "STOCK_E": 0.85,  # Near 52wk high (not oversold)
        })

        selected = select_by_rank_method(scores, top_n=2, rank_method="pct_of_52wk_high")

        # R11 reversal should select the TWO LOWEST scores: C and D
        assert selected == {"STOCK_C", "STOCK_D"}, (
            f"R11 should select lowest scores (most oversold). "
            f"Expected {{'STOCK_C', 'STOCK_D'}}, got {selected}"
        )

    def test_r11_reversal_vs_momentum_direction(self):
        """Compare R11 reversal selection vs momentum (trend-following) direction."""
        scores = pd.Series({
            "SBIN": 0.92,   # Near 52wk high
            "INFY": 0.55,   # Mid-range
            "TCS": 0.15,    # Far from 52wk high (oversold)
        })

        # R11 (reversal): should select TCS (lowest)
        r11_selected = select_by_rank_method(scores, top_n=1, rank_method="pct_of_52wk_high")
        assert "TCS" in r11_selected, "R11 reversal should select TCS (lowest score)"
        assert "SBIN" not in r11_selected, "R11 should not select SBIN (highest score)"

        # Trend-following (if it used pct_of_52wk_high): would select SBIN (highest)
        # We verify the logic here by checking ascending vs descending
        ascending_reversal = get_sort_order_for_rank_method("pct_of_52wk_high")
        assert ascending_reversal is True

    def test_r11_empty_scores(self):
        """Empty scores series returns empty set."""
        empty_scores = pd.Series(dtype="float64")
        selected = select_by_rank_method(empty_scores, top_n=5, rank_method="pct_of_52wk_high")
        assert len(selected) == 0

    def test_r11_fewer_tickers_than_top_n(self):
        """Fewer tickers than top_n returns all tickers (sorted by rank)."""
        scores = pd.Series({
            "STOCK_A": 0.80,
            "STOCK_B": 0.30,
        })

        selected = select_by_rank_method(scores, top_n=5, rank_method="pct_of_52wk_high")

        # Both tickers should be selected, ordered by lowest first
        assert len(selected) == 2
        assert selected == {"STOCK_A", "STOCK_B"}

    def test_r11_all_tied_scores(self):
        """Ties broken arbitrarily but consistently."""
        scores = pd.Series({
            "STOCK_A": 0.50,
            "STOCK_B": 0.50,
            "STOCK_C": 0.50,
            "STOCK_D": 0.60,
        })

        selected = select_by_rank_method(scores, top_n=2, rank_method="pct_of_52wk_high")

        # Should select 2 stocks with lowest score (0.50), order may vary
        assert len(selected) == 2
        # All selected should have score 0.50 or lower
        for ticker in selected:
            assert scores[ticker] <= 0.50

    def test_r11_with_nan_scores(self):
        """NaN scores are excluded from selection."""
        scores = pd.Series({
            "STOCK_A": 0.80,
            "STOCK_B": float("nan"),
            "STOCK_C": 0.20,
        })

        selected = select_by_rank_method(scores, top_n=2, rank_method="pct_of_52wk_high")

        # Should select C (lowest) and A (next lowest), excluding NaN
        # pandas sort_values() naturally excludes NaN
        assert "STOCK_B" not in selected
        assert "STOCK_C" in selected
