"""
Nomenclature regression tests — this module exists specifically to
prevent the R1-ran-as-R3 mislabeling bug (see
project_strategy_identity_bug_r_vs_m memory) from recurring, and to
regression-test the two REAL collision gaps this session found and fixed
mid-build (R08's vol_target_pct, R12's liquidity_quintile) so a future
edit can't silently reopen either.
"""

import pytest

from momentum_framework.metrics.nomenclature import build_strategy_id


def test_rank_method_is_mandatory():
    """The root-cause fix for the original R1-ran-as-R3 bug: rank_method
    must never be silently inferred."""
    with pytest.raises(ValueError, match="rank_method is required"):
        build_strategy_id(
            strategy_code="R01", band_id=2, top_n=10,
            lookback_months=12, rebalance_cadence_days=21,
        )


def test_unknown_band_rejected():
    with pytest.raises(ValueError, match="not a known M-band"):
        build_strategy_id(
            strategy_code="R01", band_id=999, top_n=10,
            lookback_months=12, rebalance_cadence_days=21,
            rank_method="trailing_return",
        )


def test_skip_months_distinguishes_r01_from_r03():
    """R01 (skip=0) and R03 (skip=1) must never collide — this is the
    exact pair the original bug conflated."""
    r01_id = build_strategy_id(
        strategy_code="R01", band_id=2, top_n=10, lookback_months=12,
        rebalance_cadence_days=21, rank_method="trailing_return", skip_months=0,
    )
    r03_id = build_strategy_id(
        strategy_code="R03", band_id=2, top_n=10, lookback_months=12,
        rebalance_cadence_days=21, rank_method="trailing_return", skip_months=1,
    )
    assert r01_id != r03_id
    assert "R01" in r01_id and "R03" not in r01_id
    assert "R03" in r03_id


def test_vol_target_pct_is_an_identity_field():
    """Regression test for the R08 gap found 2026-09-04: two runs
    differing only in vol_target_pct must not collide."""
    id_15pct = build_strategy_id(
        strategy_code="R08", band_id=2, top_n=10, lookback_months=12,
        rebalance_cadence_days=21, rank_method="trailing_return",
        vol_target_enabled=True, vol_target_pct=0.15,
    )
    id_20pct = build_strategy_id(
        strategy_code="R08", band_id=2, top_n=10, lookback_months=12,
        rebalance_cadence_days=21, rank_method="trailing_return",
        vol_target_enabled=True, vol_target_pct=0.20,
    )
    assert id_15pct != id_20pct


def test_liquidity_quintile_is_an_identity_field():
    """Regression test for the R12 gap found 2026-09-04: the
    QueueGenerator's own duplicate check caught this before it shipped —
    this test locks that fix in place."""
    ids = {
        build_strategy_id(
            strategy_code="R12", band_id=2, top_n=10, lookback_months=1,
            rebalance_cadence_days=21, rank_method="trailing_reversal_1mo",
            liquidity_quintile=q,
        )
        for q in [None, 1, 2, 3, 4, 5]
    }
    assert len(ids) == 6, "All 6 liquidity_quintile variants must produce distinct strategy_ids"


def test_filter_preset_always_present_in_output():
    """filter_preset defaults to all_risk but must never be silently
    dropped from the string — see nomenclature.py's own docstring."""
    sid = build_strategy_id(
        strategy_code="R01", band_id=2, top_n=10, lookback_months=12,
        rebalance_cadence_days=21, rank_method="trailing_return",
    )
    assert "allrisk" in sid

    sid_balanced = build_strategy_id(
        strategy_code="R01", band_id=2, top_n=10, lookback_months=12,
        rebalance_cadence_days=21, rank_method="trailing_return",
        filter_preset="balanced",
    )
    assert sid != sid_balanced


def test_invalid_filter_preset_rejected():
    with pytest.raises(ValueError, match="filter_preset"):
        build_strategy_id(
            strategy_code="R01", band_id=2, top_n=10, lookback_months=12,
            rebalance_cadence_days=21, rank_method="trailing_return",
            filter_preset="not_a_real_preset",
        )
