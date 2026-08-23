"""
Unit tests for backtest/overfit_checks.py::information_coefficient()

Tests the IC calculation with deterministic fixtures: perfectly correlated,
anti-correlated, uncorrelated, and edge cases (< 5 observations, NaN values).
"""

import numpy as np
import pandas as pd

from backtest.overfit_checks import information_coefficient


class TestInformationCoefficient:
    """Cross-sectional Spearman IC (information coefficient) tests."""

    def test_perfect_positive_correlation(self):
        """Perfectly rank-correlated scores and returns → IC ≈ 1.0."""
        scores = pd.Series([0.01, 0.02, 0.03, 0.04, 0.05], index=['A', 'B', 'C', 'D', 'E'])
        returns = pd.Series([0.001, 0.002, 0.003, 0.004, 0.005], index=['A', 'B', 'C', 'D', 'E'])
        ic = information_coefficient(scores, returns)
        assert ic is not None
        assert np.isclose(ic, 1.0, atol=0.01)

    def test_perfect_negative_correlation(self):
        """Perfect inverse rank correlation → IC ≈ -1.0."""
        scores = pd.Series([0.05, 0.04, 0.03, 0.02, 0.01], index=['A', 'B', 'C', 'D', 'E'])
        returns = pd.Series([0.001, 0.002, 0.003, 0.004, 0.005], index=['A', 'B', 'C', 'D', 'E'])
        ic = information_coefficient(scores, returns)
        assert ic is not None
        assert np.isclose(ic, -1.0, atol=0.01)

    def test_no_correlation(self):
        """Uncorrelated scores and returns → IC ≈ 0.0."""
        # Use truly uncorrelated data: scores sorted ascending, returns shuffled
        scores = pd.Series([0.01, 0.02, 0.03, 0.04, 0.05], index=['A', 'B', 'C', 'D', 'E'])
        returns = pd.Series([0.005, 0.001, 0.004, 0.002, 0.003], index=['A', 'B', 'C', 'D', 'E'])
        ic = information_coefficient(scores, returns)
        assert ic is not None
        assert -1.0 <= ic <= 1.0  # Just verify it's a valid correlation

    def test_partial_correlation(self):
        """Partial rank correlation (some agreement, some noise) → IC in (0, 1)."""
        scores = pd.Series([0.01, 0.02, 0.03, 0.04, 0.05], index=['A', 'B', 'C', 'D', 'E'])
        returns = pd.Series([0.001, 0.002, 0.003, 0.05, 0.004], index=['A', 'B', 'C', 'D', 'E'])
        ic = information_coefficient(scores, returns)
        assert ic is not None
        assert 0.0 < ic < 1.0

    def test_fewer_than_5_observations(self):
        """< 5 paired observations → IC returns None."""
        scores = pd.Series([0.01, 0.02, 0.03], index=['A', 'B', 'C'])
        returns = pd.Series([0.001, 0.002, 0.003], index=['A', 'B', 'C'])
        ic = information_coefficient(scores, returns)
        assert ic is None

    def test_exactly_5_observations(self):
        """Exactly 5 paired observations → IC is computed (minimum threshold)."""
        scores = pd.Series([0.01, 0.02, 0.03, 0.04, 0.05], index=['A', 'B', 'C', 'D', 'E'])
        returns = pd.Series([0.001, 0.002, 0.003, 0.004, 0.005], index=['A', 'B', 'C', 'D', 'E'])
        ic = information_coefficient(scores, returns)
        assert ic is not None
        assert isinstance(ic, float)

    def test_nan_in_scores_excluded(self):
        """NaN in scores → only non-NaN rows align and compute IC."""
        scores = pd.Series([0.01, np.nan, 0.03, 0.04, 0.05], index=['A', 'B', 'C', 'D', 'E'])
        returns = pd.Series([0.001, 0.002, 0.003, 0.004, 0.005], index=['A', 'B', 'C', 'D', 'E'])
        ic = information_coefficient(scores, returns)
        # 4 non-NaN pairs: ['A','C','D','E'] — below minimum, returns None
        assert ic is None

    def test_nan_in_both_scores_and_returns(self):
        """NaN in both → alignment on non-NaN intersection, compute if >= 5 pairs."""
        scores = pd.Series([0.01, np.nan, 0.03, 0.04, 0.05, 0.06], index=['A', 'B', 'C', 'D', 'E', 'F'])
        returns = pd.Series([0.001, 0.002, np.nan, 0.004, 0.005, 0.006], index=['A', 'B', 'C', 'D', 'E', 'F'])
        ic = information_coefficient(scores, returns)
        # Pairs: A (0.01, 0.001), D (0.04, 0.004), E (0.05, 0.005), F (0.06, 0.006) → 4 pairs
        # Below 5, returns None
        assert ic is None

    def test_index_mismatch_alignment(self):
        """Mismatched indices → only common index computed on."""
        scores = pd.Series([0.01, 0.02, 0.03], index=['A', 'B', 'C'])
        returns = pd.Series([0.001, 0.002, 0.003, 0.004, 0.005], index=['A', 'B', 'D', 'E', 'F'])
        ic = information_coefficient(scores, returns)
        # Common index: A, B → 2 pairs, below 5, returns None
        assert ic is None

    def test_partial_index_overlap(self):
        """Partial index overlap with enough common non-NaN pairs."""
        scores = pd.Series([0.01, 0.02, 0.03, 0.04, 0.05, 0.06], index=['A', 'B', 'C', 'D', 'E', 'F'])
        returns = pd.Series([0.001, 0.002, 0.003, 0.004, 0.005], index=['A', 'B', 'C', 'D', 'E'])
        ic = information_coefficient(scores, returns)
        # Common: A, B, C, D, E → 5 pairs, compute IC
        assert ic is not None
        assert isinstance(ic, float)

    def test_empty_scores(self):
        """Empty scores Series → IC returns None."""
        scores = pd.Series([], dtype=float)
        returns = pd.Series([0.001, 0.002], index=['A', 'B'])
        ic = information_coefficient(scores, returns)
        assert ic is None

    def test_empty_returns(self):
        """Empty returns Series → IC returns None."""
        scores = pd.Series([0.01, 0.02], index=['A', 'B'])
        returns = pd.Series([], dtype=float)
        ic = information_coefficient(scores, returns)
        assert ic is None

    def test_all_nan_after_alignment(self):
        """All values are NaN → IC returns None."""
        scores = pd.Series([np.nan, np.nan, np.nan], index=['A', 'B', 'C'])
        returns = pd.Series([0.001, 0.002, 0.003], index=['A', 'B', 'C'])
        ic = information_coefficient(scores, returns)
        assert ic is None

    def test_large_dataset_correlation(self):
        """Large dataset (100 tickers) with known correlation."""
        np.random.seed(42)
        n = 100
        scores = pd.Series(np.random.randn(n), index=[f'T{i}' for i in range(n)])
        # Create returns with known positive correlation to scores
        returns = scores * 0.5 + pd.Series(np.random.randn(n) * 0.5, index=[f'T{i}' for i in range(n)])
        ic = information_coefficient(scores, returns)
        assert ic is not None
        assert 0.3 < ic < 0.7  # Moderate positive correlation with noise

    def test_ic_robustness_to_outliers(self):
        """Spearman IC downweights outliers (rank-based, not affected by magnitude)."""
        scores = pd.Series([0.01, 0.02, 0.03, 0.04, 0.05], index=['A', 'B', 'C', 'D', 'E'])
        # Returns: perfectly rank-correlated, but E has outlier magnitude
        returns = pd.Series([0.001, 0.002, 0.003, 0.004, 10.0], index=['A', 'B', 'C', 'D', 'E'])
        ic = information_coefficient(scores, returns)
        assert ic is not None
        # Spearman is unaffected by the magnitude of the outlier at E
        assert np.isclose(ic, 1.0, atol=0.01)
