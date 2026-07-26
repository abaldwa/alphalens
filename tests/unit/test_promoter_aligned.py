"""
tests/unit/test_promoter_aligned.py

Regression test for the 2026-07-25 model-review fix to
systems/fundamental_analysis/management/promoter_aligned.py's pledge-
severity calculation: pledge severity must stay distinguishable across
its full range, not collapse to an identical floored value once
promoter_pct is already low.
"""

from systems.fundamental_analysis.management.promoter_aligned import _promoter_alignment_score


class TestPromoterAlignmentScore:
    def test_higher_pledge_always_scores_lower_than_lower_pledge(self):
        low_pledge = _promoter_alignment_score({"promoter_pct": 20.0, "promoter_pledge": 30.0, "dilution_3y": 0.0})
        high_pledge = _promoter_alignment_score({"promoter_pct": 20.0, "promoter_pledge": 95.0, "dilution_3y": 0.0})
        assert low_pledge is not None and high_pledge is not None
        assert low_pledge > high_pledge

    def test_pledge_severity_distinguishable_even_when_promoter_pct_is_low(self):
        # Regression: the prior clip(promoter_pct - pledge, 0, 100) version
        # floored both of these to 0 identically, losing the distinction
        # between "somewhat pledged" and "almost entirely pledged."
        mid_pledge = _promoter_alignment_score({"promoter_pct": 15.0, "promoter_pledge": 40.0, "dilution_3y": 0.0})
        near_total_pledge = _promoter_alignment_score({"promoter_pct": 15.0, "promoter_pledge": 98.0, "dilution_3y": 0.0})
        assert mid_pledge != near_total_pledge

    def test_zero_pledge_leaves_promoter_pct_leg_unscaled(self):
        score_no_pledge = _promoter_alignment_score({"promoter_pct": 60.0, "promoter_pledge": 0.0, "dilution_3y": 0.0})
        score_no_pledge_field = _promoter_alignment_score({"promoter_pct": 60.0, "dilution_3y": 0.0})
        assert score_no_pledge == score_no_pledge_field

    def test_full_pledge_drives_raw_leg_to_zero(self):
        score = _promoter_alignment_score({"promoter_pct": 60.0, "promoter_pledge": 100.0, "dilution_3y": 0.0})
        # raw leg -> 0, dilution leg -> neutral (50 via weighted_zscore_composite at z=0);
        # combine_subscores({"raw": 0.0, "dilution": 50.0}, {"raw": 0.7, "dilution": 0.3}) = 15.0
        assert score == 15.0

    def test_missing_promoter_pct_drops_below_min_coverage(self):
        # raw leg (weight 0.7) unavailable, only dilution (weight 0.3) present
        # -> 30% coverage, below MIN_COVERAGE (50%) -> None, not a
        # partial-confidence score.
        assert _promoter_alignment_score({"dilution_3y": 0.0}) is None
