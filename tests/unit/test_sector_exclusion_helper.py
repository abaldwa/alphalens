"""
tests/unit/test_sector_exclusion_helper.py

Phase: Signal-generator consolidation (UnifiedGeneratorRefactorPlan.md, E1)
Owner: Platform / Fundamental
Consumers: CI / `pytest tests/unit/`

Behaviour of the single sector-exclusion decision. The static gate
(tests/quality/test_sector_exclusion_has_one_owner.py) proves nothing else
re-decides it; these prove it decides correctly.

PIT Assumptions
---------------
None -- PRESET_EXCLUDED_SECTORS is a static declaration, not PIT data.
"""

from __future__ import annotations

import pytest

from features.fundamental_composites import (
    PRESET_EXCLUDED_SECTORS,
    SCREENER_PRESETS,
    is_sector_excluded,
    matches_screener_preset,
)


def test_excluded_sector_is_excluded():
    for preset, sectors in PRESET_EXCLUDED_SECTORS.items():
        for sector in sectors:
            assert is_sector_excluded(preset, sector) is True


def test_unlisted_sector_is_not_excluded():
    for preset in PRESET_EXCLUDED_SECTORS:
        assert is_sector_excluded(preset, "Information Technology") is False


def test_preset_with_no_exclusions_excludes_nothing():
    unexcluded = [p for p in SCREENER_PRESETS if not PRESET_EXCLUDED_SECTORS.get(p)]
    if not unexcluded:
        pytest.skip("every preset declares an exclusion; nothing to assert here")
    for preset in unexcluded:
        assert is_sector_excluded(preset, "Financial Services") is False


def test_unknown_sector_never_excludes():
    """A missing sector means the lookup failed, not that the ticker is in a
    banned sector. Excluding on None would let a data-quality gap silently
    shrink a strategy's universe -- a change to what the strategy holds,
    caused by something that has nothing to do with the strategy."""
    for preset in PRESET_EXCLUDED_SECTORS:
        assert is_sector_excluded(preset, None) is False


def test_exclusions_cover_more_than_the_threshold_presets():
    """PRESET_EXCLUDED_SECTORS spans BOTH threshold screener presets and
    composite-SCORE strategies (moat, sector_leader, qglp, ...), which
    matches_screener_preset cannot even accept -- it raises "Unknown screener
    preset" for them.

    This is precisely why the shared owner is `is_sector_excluded` and not
    the predicate. `/scores` deals in score strategies, so routing it through
    the predicate was never an option; a helper that answers the sector
    question alone is the only thing all the callers can share. Recorded as
    a test because the mismatch is easy to mistake for a bug in the table."""
    score_only = set(PRESET_EXCLUDED_SECTORS) - set(SCREENER_PRESETS)
    assert score_only, (
        "PRESET_EXCLUDED_SECTORS no longer covers any composite-score "
        "strategy. If the two sets have converged, is_sector_excluded could "
        "in principle fold back into the predicate -- re-check this design."
    )
    for preset in sorted(score_only):
        assert is_sector_excluded(preset, "Financial Services") is True
        with pytest.raises(ValueError, match="Unknown screener preset"):
            matches_screener_preset({}, preset, sector="Financial Services")


def test_predicate_and_helper_agree_on_threshold_presets():
    """For the presets matches_screener_preset DOES accept, it must reach the
    same verdict as the helper. If these disagree, the collapse has been
    undone inside the predicate itself -- which the static gate cannot see,
    because the predicate is one of its owners."""
    for preset in sorted(set(PRESET_EXCLUDED_SECTORS) & set(SCREENER_PRESETS)):
        for sector in PRESET_EXCLUDED_SECTORS[preset]:
            # Ratios deliberately absent: an excluded sector must fail on the
            # sector alone, never reaching the threshold comparison.
            assert matches_screener_preset({}, preset, sector=sector) is False
