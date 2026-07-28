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
    SCORE_FUNCTIONS,
    SCREENER_PRESETS,
    STRATEGY_CATALOG,
    fcf_low_debt_score,
    garp_score,
    growth_score,
    magic_formula_score,
    management_quality_score,
    matches_screener_preset,
    quality_score,
    quality_value_composite,
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

    def test_below_min_coverage_returns_none_not_a_false_confidence_score(self):
        # Only roe present (weight 0.30 of a total 1.0) -> 30% coverage,
        # below MIN_COVERAGE (50%). [2026-07-25 model-review fix] Previously
        # this silently renormalized to a full-precision score off 1 of 4
        # factors — now refuses to score rather than imply full confidence.
        assert quality_score({"roe": 2.0}) is None

    def test_at_or_above_min_coverage_renormalizes_and_scores(self):
        # roe (0.30) + roce (0.30) = 60% coverage, above MIN_COVERAGE.
        score = quality_score({"roe": 2.0, "roce": 2.0})
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


class TestMagicFormulaScore:
    def test_all_positive_zscores_gives_above_50(self):
        score = magic_formula_score({"ev_ebit_yield": 1.0, "magic_formula_roc": 1.0})
        assert score > 50

    def test_all_missing_returns_none(self):
        assert magic_formula_score({}) is None


class TestQualityValueComposite:
    def test_all_positive_zscores_gives_above_50(self):
        score = quality_value_composite({"ev_ebit_yield": 1.0, "book_to_market": 1.0, "roce": 1.0, "roe": 1.0})
        assert score > 50

    def test_all_missing_returns_none(self):
        assert quality_value_composite({}) is None


class TestFcfLowDebtScore:
    def test_cheap_and_safe_gives_above_50(self):
        score = fcf_low_debt_score({"fcf_ev_yield": 1.0, "net_debt_to_ebitda": -1.0, "interest_coverage": 1.0})
        assert score > 50

    def test_all_missing_returns_none(self):
        assert fcf_low_debt_score({}) is None


class TestFcfLowDebtRealColumnPipeline:
    """[BUG FIX, 2026-07-28 second model-review, item 8] The prior fix
    round claimed fcf_low_debt "verified 3 real matches" against
    GESHIP/INDIAMART/INDUSTOWER after renaming its leverage column from
    debt_to_ebitda (never a real column) to net_debt_to_ebitda — but
    nothing was committed backing that claim, so it wasn't independently
    reproducible. This exercises the SAME end-to-end pipeline the real
    screener uses (raw ratios -> features.fundamental._sector_relative_
    zscore -> matches_screener_preset), with synthetic-but-controlled
    values, rather than pre-supplying already-z-scored inputs like
    TestScreenerPresets above does — so a future regression that
    reintroduces the wrong column name would fail this test, not just a
    one-off manual check against today's live universe."""

    def test_low_leverage_high_fcf_yield_ticker_matches_after_real_zscoring(self):
        from features.fundamental import _sector_relative_zscore

        # Three same-sector peers: GOODCO has clearly above-peer FCF yield,
        # clearly below-peer leverage, and above-peer interest coverage —
        # exactly the shape a real "cheap, low-debt cash generator" should
        # have. BADCO is the mirror-image (expensive, levered, weak
        # coverage). AVGCO anchors the sector mean/std with a middling profile.
        raw = pd.DataFrame([
            {"ticker": "GOODCO", "sector": "IT", "fcf_ev_yield": 0.15, "net_debt_to_ebitda": 0.2, "interest_coverage": 12.0},
            {"ticker": "BADCO", "sector": "IT", "fcf_ev_yield": 0.01, "net_debt_to_ebitda": 4.5, "interest_coverage": 1.5},
            {"ticker": "AVGCO", "sector": "IT", "fcf_ev_yield": 0.06, "net_debt_to_ebitda": 2.0, "interest_coverage": 5.0},
        ])
        zscored = _sector_relative_zscore(raw, ["fcf_ev_yield", "net_debt_to_ebitda", "interest_coverage"])

        good_row = zscored[zscored["ticker"] == "GOODCO"].iloc[0]
        bad_row = zscored[zscored["ticker"] == "BADCO"].iloc[0]

        good_ratios = {
            "fcf_ev_yield": good_row["fcf_ev_yield"],
            "net_debt_to_ebitda": good_row["net_debt_to_ebitda"],
            "interest_coverage": good_row["interest_coverage"],
        }
        bad_ratios = {
            "fcf_ev_yield": bad_row["fcf_ev_yield"],
            "net_debt_to_ebitda": bad_row["net_debt_to_ebitda"],
            "interest_coverage": bad_row["interest_coverage"],
        }

        assert matches_screener_preset(good_ratios, "fcf_low_debt") is True
        assert matches_screener_preset(bad_ratios, "fcf_low_debt") is False

        # The composite score function (SCORE_FUNCTIONS' "fcf_low_debt" kind)
        # must also read the real post-rename column name correctly.
        assert fcf_low_debt_score(good_ratios) > 50
        assert fcf_low_debt_score(bad_ratios) < 50

        # Regression guard for the exact bug this fix addressed: the old,
        # never-real "debt_to_ebitda" name must not silently satisfy
        # anything — a ratios dict using it instead of net_debt_to_ebitda
        # is missing input, not a passing/failing leverage screen.
        stale_key_ratios = {
            "fcf_ev_yield": good_row["fcf_ev_yield"],
            "debt_to_ebitda": good_row["net_debt_to_ebitda"],
            "interest_coverage": good_row["interest_coverage"],
        }
        assert matches_screener_preset(stale_key_ratios, "fcf_low_debt") is False


class TestGarpScore:
    def test_growth_and_cheap_valuation_gives_above_50(self):
        # [BUG FIX, 4th fundamental-strategies review, item 5 follow-up] the
        # original inputs here ("revenue_growth_yoy") didn't match GARP's
        # actual growth leg feature ("revenue_cagr_3yr" — see garp.py's
        # GROWTH_WEIGHTS), so this test previously only produced a non-None
        # score by relying on the exact MIN_COVERAGE boundary bug this
        # review fixed (1-of-2 factors present in both the growth leg and
        # the overall leg combination, each exactly at the old-permissive
        # 50% threshold). Using the real feature name gives the growth leg
        # full (100%) coverage, so this test now exercises real signal
        # rather than the coverage-floor edge case.
        score = garp_score({"revenue_cagr_3yr": 1.0, "eps_growth_yoy": 1.0, "pe_ratio": -1.0})
        assert score > 50

    def test_all_missing_returns_none(self):
        assert garp_score({}) is None


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
        # pe_ratio threshold is -0.5, meaning pe_ratio must be <= -0.5 (cheap).
        # garp was replaced in place with revenue+eps growth + PE discipline.
        ratios = {"revenue_growth_yoy": 1.0, "eps_growth_yoy": 1.0, "pe_ratio": -1.0}
        assert matches_screener_preset(ratios, "garp") is True
        ratios_fail = {"revenue_growth_yoy": 1.0, "eps_growth_yoy": 1.0, "pe_ratio": 0.5}
        assert matches_screener_preset(ratios_fail, "garp") is False

    def test_magic_formula_preset(self):
        ratios = {"ev_ebit_yield": 1.0, "magic_formula_roc": 1.0}
        assert matches_screener_preset(ratios, "magic_formula") is True
        assert matches_screener_preset({"ev_ebit_yield": 0.1, "magic_formula_roc": 1.0}, "magic_formula") is False

    def test_magic_formula_excludes_financial_services_sector(self):
        # [2026-07-25 model-review fix] EV/EBIT-yield and NWC-based ROC are
        # structurally meaningless for banks/NBFCs/insurers — sector z-scoring
        # doesn't fix that, so Financial Services is excluded outright,
        # matching Greenblatt's own Magic Formula exclusion rule.
        ratios = {"ev_ebit_yield": 5.0, "magic_formula_roc": 5.0}  # would easily pass on ratios alone
        assert matches_screener_preset(ratios, "magic_formula", sector="Financial Services") is False
        assert matches_screener_preset(ratios, "magic_formula", sector="Information Technology") is True
        assert matches_screener_preset(ratios, "magic_formula") is True  # no sector supplied -> filter doesn't apply

    def test_sector_exclusion_does_not_apply_to_other_presets(self):
        ratios = {"revenue_growth_yoy": 1.5, "eps_growth_yoy": 1.5}
        assert matches_screener_preset(ratios, "turnaround", sector="Financial Services") is True

    def test_quality_value_preset(self):
        ratios = {"ev_ebit_yield": 1.0, "book_to_market": 1.0, "roce": 1.0, "roe": 1.0}
        assert matches_screener_preset(ratios, "quality_value") is True
        assert matches_screener_preset({**ratios, "roe": 0.1}, "quality_value") is False

    def test_fcf_low_debt_preset(self):
        ratios = {"fcf_ev_yield": 1.0, "net_debt_to_ebitda": -1.0, "interest_coverage": 1.0}
        assert matches_screener_preset(ratios, "fcf_low_debt") is True
        assert matches_screener_preset({**ratios, "net_debt_to_ebitda": 1.0}, "fcf_low_debt") is False

    def test_turnaround_preset(self):
        ratios = {"revenue_growth_yoy": 1.5, "eps_growth_yoy": 1.5}
        assert matches_screener_preset(ratios, "turnaround") is True

    def test_deep_value_solvency_preset(self):
        ratios = {
            "book_to_market": 1.0, "ev_ebit_yield": 1.0,
            "debt_to_equity": -1.0, "interest_coverage": 1.0, "current_ratio": 1.0,
        }
        assert matches_screener_preset(ratios, "deep_value_solvency") is True
        assert matches_screener_preset({**ratios, "debt_to_equity": 1.0}, "deep_value_solvency") is False

    def test_cash_flow_backed_earnings_preset(self):
        ratios = {"cfo_to_pat": 1.0, "fcf_ev_yield": 1.0, "receivable_days_change": -1.0}
        assert matches_screener_preset(ratios, "cash_flow_backed_earnings") is True
        assert matches_screener_preset({**ratios, "receivable_days_change": 1.0}, "cash_flow_backed_earnings") is False

    def test_turnaround_recovery_preset(self):
        ratios = {
            "delta_roa_1y": 1.0, "delta_current_ratio_1y": 1.0,
            "delta_long_term_debt_to_assets_1y": -1.0, "margin_expansion": 1.0,
        }
        assert matches_screener_preset(ratios, "turnaround_recovery") is True
        assert matches_screener_preset({**ratios, "margin_expansion": -1.0}, "turnaround_recovery") is False


# Representative all-positive z-score inputs covering every field referenced
# by any SCORE_FUNCTIONS composite — sign of each weight in the underlying
# weight dicts determines whether "positive" is favorable, so this input
# should push every score above the neutral midpoint (50) when computable.
_ALL_POSITIVE_RATIOS = {
    name: 1.0 for name in [
        "roe", "roce", "net_margin", "debt_to_equity", "revenue_growth_yoy", "eps_growth_yoy",
        "revenue_cagr_3yr", "ev_ebit_yield", "fcf_ev_yield", "magic_formula_roc", "book_to_market",
        "cfo_to_pat", "pe_ratio", "interest_coverage", "reinvestment_rate",
        "avg_roce_5y", "margin_stability_5y", "dilution_3y", "capital_allocation_efficiency",
        "sales_cagr_5y", "gross_margin", "asset_turnover", "delta_roce_3y", "eps_acceleration",
        "margin_expansion", "ev_to_ebitda", "market_cap", "company_age_years", "capex_intensity",
        "delta_operating_cash_flow_1y", "net_debt_to_ebitda", "receivable_days_change",
        "inventory_days_change", "avg_ebitda_margin_5y", "promoter_pct", "promoter_pledge",
        "institutional_ownership_pct",
    ]
    # earnings_volatility_5y is a "lower=better" raw magnitude (weighted
    # negatively wherever used) — give it a value whose z-score sign matches
    # "high volatility," so composites are pushed the intended direction.
} | {
    "earnings_volatility_5y": -1.0,
    # promoter_pct/promoter_pledge feed management_quality_score/
    # promoter_aligned_score's raw-percentage scale (0-100), not a z-score —
    # 1.0 would read as "1% promoter holding" (bad), so use realistic values.
    "promoter_pct": 70.0,
    "promoter_pledge": 0.0,
    # institutional_ownership_pct feeds under_followed's raw-percentage
    # under-followed proxy, where LOW ownership is favorable — keep it low.
    "institutional_ownership_pct": 1.0,
}


class TestAllNewCompositeScores:
    """Sweep of every SCORE_FUNCTIONS entry with representative inputs —
    each should compute without error, return a float in [0, 100], and land
    above the neutral 50 midpoint for uniformly favorable inputs."""

    @pytest.mark.parametrize("name", sorted(SCORE_FUNCTIONS.keys()))
    def test_positive_inputs_score_above_50(self, name):
        score = SCORE_FUNCTIONS[name](_ALL_POSITIVE_RATIOS)
        assert score is not None
        assert 50 < score <= 100

    @pytest.mark.parametrize("name", sorted(SCORE_FUNCTIONS.keys()))
    def test_all_missing_inputs_returns_none(self, name):
        assert SCORE_FUNCTIONS[name]({}) is None


class TestStrategyCatalog:
    def test_covers_all_26_strategies(self):
        assert len(STRATEGY_CATALOG) == 26

    def test_every_preset_kind_entry_is_a_real_preset(self):
        for key, meta in STRATEGY_CATALOG.items():
            if meta["kind"] == "preset":
                assert key in SCREENER_PRESETS, f"{key} claims kind=preset but isn't in SCREENER_PRESETS"

    def test_every_composite_score_kind_entry_has_a_function(self):
        for key, meta in STRATEGY_CATALOG.items():
            if meta["kind"] == "composite_score":
                assert key in SCORE_FUNCTIONS, f"{key} claims kind=composite_score but isn't in SCORE_FUNCTIONS"

    def test_every_entry_has_required_fields(self):
        for key, meta in STRATEGY_CATALOG.items():
            for field in ("label", "category", "kind", "description"):
                assert meta.get(field), f"{key} missing {field}"

    def test_every_entry_has_a_backtested_flag(self):
        # Must be a real bool the frontend can render a badge from, not
        # missing/None (which would silently hide the "not validated" warning).
        for key, meta in STRATEGY_CATALOG.items():
            assert isinstance(meta.get("backtested"), bool), f"{key} missing/invalid 'backtested' flag"

    def test_backtested_flag_matches_membership_in_backtested_strategies(self):
        from features.fundamental_composites import BACKTESTED_STRATEGIES

        for key, meta in STRATEGY_CATALOG.items():
            assert meta["backtested"] == (key in BACKTESTED_STRATEGIES)

    def test_garp_preset_change_is_recorded_in_changelog(self):
        from features.fundamental_composites import SCREENER_PRESET_CHANGELOG

        assert "garp" in SCREENER_PRESET_CHANGELOG
        assert len(SCREENER_PRESET_CHANGELOG["garp"]) >= 1
