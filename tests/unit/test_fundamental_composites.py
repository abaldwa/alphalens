"""
tests/unit/test_fundamental_composites.py

A65: pure-logic tests for `features/fundamental_composites.py` (SPEC-FA-008),
previously untested (40.98% coverage, no test file). Real dicts/DataFrames,
no DB/network.
"""


import numpy as np
import pandas as pd
import pytest

from features.fundamental_composites import (
    growth_score,
    management_quality_score,
    matches_screener_preset,
    quality_score,
    select_peers,
)


class TestQualityScore:
    def test_all_positive_zscores_gives_above_50(self):
        score = quality_score({"roe": 1.0, "roce": 1.0, "net_margin": 1.0, "debt_to_equity": -1.0})
        assert score > 50

    def test_all_nan_returns_none(self):
        assert quality_score({"roe": np.nan, "roce": np.nan}) is None

    def test_missing_keys_returns_none(self):
        assert quality_score({}) is None

    def test_partial_inputs_renormalized(self):
        # Only roe present, weight 0.30 -> full weight of the only present input
        score = quality_score({"roe": 2.0})
        assert score == pytest.approx(50 + 10 * 2.0)

    def test_clipped_to_0_100(self):
        score = quality_score({"roe": 100.0, "roce": 100.0, "net_margin": 100.0, "debt_to_equity": -100.0})
        assert score == 100.0
        score_low = quality_score({"roe": -100.0, "roce": -100.0, "net_margin": -100.0, "debt_to_equity": 100.0})
        assert score_low == 0.0


class TestGrowthScore:
    def test_computes_weighted_composite(self):
        score = growth_score({"revenue_growth_yoy": 1.0, "eps_growth_yoy": 1.0, "revenue_cagr_3yr": 1.0})
        assert score == pytest.approx(60.0)

    def test_all_missing_returns_none(self):
        assert growth_score({}) is None


class TestManagementQualityScore:
    def test_missing_promoter_pledge_returns_none(self):
        assert management_quality_score({}) is None

    def test_nan_promoter_pledge_returns_none(self):
        assert management_quality_score({"promoter_pledge": float("nan")}) is None

    def test_zero_pledge_no_flags_is_neutral_50(self):
        assert management_quality_score({"promoter_pledge": 0.0}) == 50.0

    def test_pledge_reduces_score(self):
        assert management_quality_score({"promoter_pledge": 20.0}) == 40.0

    def test_spiral_flag_subtracts_20(self):
        score = management_quality_score({"promoter_pledge": 0.0, "promoter_pledge_spiral_flag": True})
        assert score == 30.0

    def test_institutional_conviction_flag_adds_15(self):
        score = management_quality_score({"promoter_pledge": 0.0, "institutional_conviction_flag": True})
        assert score == 65.0

    def test_clipped_to_0_100(self):
        score = management_quality_score({"promoter_pledge": 1000.0})
        assert score == 0.0


class TestSelectPeers:
    def _panel(self):
        return pd.DataFrame({"ticker": ["A", "B", "C", "D", "E"]})

    def test_no_sector_returns_empty(self):
        result = select_peers("A", self._panel(), sector_map={}, mcap_map={})
        assert result == []

    def test_no_candidates_in_sector_returns_empty(self):
        sector_map = {"A": "IT", "B": "Banking"}
        result = select_peers("A", self._panel(), sector_map=sector_map, mcap_map={})
        assert result == []

    def test_ranks_by_closeness_in_log_market_cap(self):
        sector_map = {t: "IT" for t in ["A", "B", "C", "D", "E"]}
        mcap_map = {"A": 100.0, "B": 105.0, "C": 50000.0, "D": 90.0, "E": 200.0}
        result = select_peers("A", self._panel(), sector_map=sector_map, mcap_map=mcap_map, k=2)
        assert result == ["B", "D"]

    def test_falls_back_to_sector_alphabetical_when_no_mcap(self):
        sector_map = {t: "IT" for t in ["A", "B", "C", "D", "E"]}
        result = select_peers("A", self._panel(), sector_map=sector_map, mcap_map={}, k=3)
        assert result == ["B", "C", "D"]

    def test_falls_back_when_own_mcap_missing(self):
        sector_map = {t: "IT" for t in ["A", "B", "C"]}
        mcap_map = {"B": 100.0, "C": 200.0}
        result = select_peers("A", self._panel(), sector_map=sector_map, mcap_map=mcap_map, k=2)
        assert result == ["B", "C"]

    def test_falls_back_when_no_candidate_has_mcap(self):
        sector_map = {t: "IT" for t in ["A", "B", "C"]}
        mcap_map = {"A": 100.0}
        result = select_peers("A", self._panel(), sector_map=sector_map, mcap_map=mcap_map, k=2)
        assert result == ["B", "C"]

    def test_excludes_self(self):
        sector_map = {t: "IT" for t in ["A", "B"]}
        result = select_peers("A", self._panel(), sector_map=sector_map, mcap_map={})
        assert "A" not in result


class TestMatchesScreenerPreset:
    def test_unknown_preset_raises(self):
        with pytest.raises(ValueError):
            matches_screener_preset({}, "not_a_real_preset")

    def test_quality_compounder_passes_when_all_thresholds_cleared(self):
        ratios = {"roe": 1.5, "roce": 1.2, "debt_to_equity": -1.0}
        assert matches_screener_preset(ratios, "quality_compounder") is True

    def test_quality_compounder_fails_below_threshold(self):
        ratios = {"roe": 0.5, "roce": 1.2, "debt_to_equity": -1.0}
        assert matches_screener_preset(ratios, "quality_compounder") is False

    def test_missing_input_fails_conservatively(self):
        ratios = {"roe": 1.5, "roce": 1.2}  # missing debt_to_equity
        assert matches_screener_preset(ratios, "quality_compounder") is False

    def test_nan_input_fails(self):
        ratios = {"roe": float("nan"), "roce": 1.2, "debt_to_equity": -1.0}
        assert matches_screener_preset(ratios, "quality_compounder") is False

    def test_negative_threshold_sign_adjustment_garp(self):
        # pe_ratio threshold is -0.5, meaning pe_ratio must be <= -0.5 (cheap)
        ratios = {"revenue_growth_yoy": 1.0, "pe_ratio": -1.0}
        assert matches_screener_preset(ratios, "garp") is True
        ratios_fail = {"revenue_growth_yoy": 1.0, "pe_ratio": 0.5}
        assert matches_screener_preset(ratios_fail, "garp") is False

    def test_turnaround_preset(self):
        ratios = {"revenue_growth_yoy": 1.5, "eps_growth_yoy": 1.5}
        assert matches_screener_preset(ratios, "turnaround") is True
