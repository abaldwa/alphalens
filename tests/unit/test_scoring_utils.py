"""
tests/unit/test_scoring_utils.py

Pure-logic tests for systems/fundamental_analysis/scoring_utils.py's
MIN_COVERAGE gate (2026-07-25 model-review fix): weighted_zscore_composite
and combine_subscores must refuse to return a score when too little of
the intended weight is backed by real data, rather than silently
renormalizing a sparse input into a full-precision, full-confidence-looking
number.
"""

import numpy as np
import pytest

from systems.fundamental_analysis.scoring_utils import MIN_COVERAGE, combine_subscores, weighted_zscore_composite


class TestWeightedZscoreCompositeCoverage:
    WEIGHTS = {"a": 0.25, "b": 0.25, "c": 0.25, "d": 0.25}

    def test_full_coverage_scores_normally(self):
        score = weighted_zscore_composite({"a": 1.0, "b": 1.0, "c": 1.0, "d": 1.0}, self.WEIGHTS)
        assert score == pytest.approx(60.0)

    def test_below_min_coverage_returns_none(self):
        # 1 of 4 equal weights present -> 25% coverage, below 50%.
        assert weighted_zscore_composite({"a": 1.0}, self.WEIGHTS) is None

    def test_exactly_at_min_coverage_scores(self):
        # 2 of 4 equal weights present -> exactly 50% coverage.
        score = weighted_zscore_composite({"a": 1.0, "b": 1.0}, self.WEIGHTS)
        assert score is not None
        assert score == pytest.approx(60.0)

    def test_all_missing_returns_none(self):
        assert weighted_zscore_composite({}, self.WEIGHTS) is None

    def test_nan_inputs_count_as_missing_for_coverage(self):
        assert weighted_zscore_composite({"a": np.nan, "b": np.nan, "c": 1.0}, self.WEIGHTS) is None

    def test_unequal_weights_coverage_by_absolute_weight_not_count(self):
        # 1 of 2 factors present, but that 1 factor carries 80% of total
        # absolute weight -> coverage is 0.8, above MIN_COVERAGE, despite
        # being only half the factor *count*.
        weights = {"dominant": 0.8, "minor": 0.2}
        score = weighted_zscore_composite({"dominant": 1.0}, weights)
        assert score is not None


class TestCombineSubscoresCoverage:
    WEIGHTS = {"leg1": 0.5, "leg2": 0.5}

    def test_full_coverage_scores_normally(self):
        score = combine_subscores({"leg1": 80.0, "leg2": 60.0}, self.WEIGHTS)
        assert score == pytest.approx(70.0)

    def test_below_min_coverage_returns_none(self):
        weights = {"leg1": 0.2, "leg2": 0.2, "leg3": 0.2, "leg4": 0.2, "leg5": 0.2}
        assert combine_subscores({"leg1": 80.0}, weights) is None

    def test_all_none_returns_none(self):
        assert combine_subscores({"leg1": None, "leg2": None}, self.WEIGHTS) is None

    def test_min_coverage_constant_is_half(self):
        assert MIN_COVERAGE == pytest.approx(0.5)
