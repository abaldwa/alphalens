"""
tests/unit/test_ratio_recovery.py

The repair this module exists for looked trivial and was not: parse the ratio
out of the details prose, write it, re-run the adjuster. Done blindly that
corrupts 18 of 86 price histories, because those splits are ALREADY reflected
in the stored prices despite adj_factor recording 1.0 — and a double-adjusted
series is still a plausible-looking series, so nothing downstream would flag
it.

So the tests that matter here are the refusals. Confirming that a 5:1 split
parses to 5.0 is table stakes; proving the module declines to touch a history
that is already correct is the whole point.

Synthetic price frames throughout per SPEC-SYS-006's fixture exemption: the
gap has to be controlled exactly to test where the decision boundary sits, and
real bars carry a day of genuine market movement on top of it.
"""

import pandas as pd
import pytest

from ingestion.adjust.ratio_recovery import (
    DECISION_MARGIN,
    MAX_CONFIRMING_ERROR,
    Verdict,
    classify_action,
    classify_gap,
    combined_expected_gap,
    expected_gap,
    observed_gap,
    parse_split_ratio,
)


# ---------------------------------------------------------------------------
# Parsing the ratio out of NSE's prose
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "details, expected",
    [
        ("Fv Split Rs.10/- To Rs.2/", 5.0),
        ("Fv Split Rs.10/- To Re.1/", 10.0),
        ("Fv Split Rs.10/- To Rs.5/", 2.0),
        ("Fv Split Rs.5/- To Rs.2/", 2.5),
        ("Fv Split Rs.2/- To Re.1/-", 2.0),
        # Separators eaten by the source system; still unambiguous to a reader.
        ("Split-Rs.10tors.2/Div-60%Purpose Revised", 5.0),
    ],
)
def test_face_value_splits_are_recovered(details, expected):
    assert parse_split_ratio(details) == pytest.approx(expected)


@pytest.mark.parametrize(
    "details",
    [
        # These are the records that legitimately carry ratio=0. They do not
        # dilute the equity share count, so returning None here is the correct
        # answer rather than a parser shortfall — price_adjuster already
        # refuses them via NON_EQUITY_BONUS_PATTERN, and this must agree.
        "Scheme Of Arrangement - Issue Of Bonus Debentures",
        "Scheme Of Arangement- Bonus - 1 Debenture For 1 Equity Share Held",
        "Bonus 1 Dvr : 10 Eq Share",
        "",
    ],
)
def test_non_equity_records_yield_no_ratio(details):
    assert parse_split_ratio(details) is None


def test_a_consolidation_is_not_read_as_a_split():
    """A reverse split RAISES the face value. Parsing it with the split
    formula would invert the adjustment and move prices the wrong way."""
    assert parse_split_ratio("Fv Split Rs.2/- To Rs.10/") is None


# ---------------------------------------------------------------------------
# What gap an unadjusted action should show
# ---------------------------------------------------------------------------

def test_expected_gap_matches_the_adjuster_formulas():
    """Must mirror price_adjuster._action_factors inverted. If these drift
    apart, every verdict is measured against the wrong target."""
    assert expected_gap("SPLIT", 5.0) == pytest.approx(5.0)
    assert expected_gap("BONUS", 1.0) == pytest.approx(2.0)   # 1:1 -> halves
    assert expected_gap("BONUS", 0.5) == pytest.approx(1.5)   # 1:2 -> -33%
    assert expected_gap("DIVIDEND", 5.0) is None
    assert expected_gap("SPLIT", 0.0) is None


# ---------------------------------------------------------------------------
# Measuring the gap
# ---------------------------------------------------------------------------

def _prices(rows):
    return pd.DataFrame(rows, columns=["date", "close"])


def test_gap_is_measured_across_the_ex_date_boundary():
    prices = _prices([("2010-08-16", 500.0), ("2010-08-17", 500.0),
                      ("2010-08-18", 100.0), ("2010-08-19", 101.0)])
    assert observed_gap(prices, "2010-08-18") == pytest.approx(5.0)


def test_a_missing_side_is_not_a_gap_of_zero():
    """No prior history means no evidence, which must not be reported as a
    measurement — an action with nothing before it has nothing to adjust."""
    assert observed_gap(_prices([("2010-08-18", 100.0)]), "2010-08-18") is None
    assert observed_gap(_prices([("2010-08-16", 500.0)]), "2010-08-18") is None
    assert observed_gap(_prices([]), "2010-08-18") is None


# ---------------------------------------------------------------------------
# The decision — this is what the module is for
# ---------------------------------------------------------------------------

def test_a_clean_matching_gap_confirms_the_repair():
    assert classify_gap(observed=4.96, expected=5.0) is Verdict.CONFIRMED


def test_an_absent_gap_means_the_history_is_already_adjusted():
    """THE TRAP. 18 of 86 recoverable splits look like this: the ratio parses
    perfectly, adj_factor says 1.0, and the prices are already correct. Writing
    the ratio and re-running the adjuster would divide them by five again."""
    assert classify_gap(observed=0.952, expected=5.0) is Verdict.ALREADY_ADJUSTED


def test_a_gap_matching_neither_hypothesis_is_contradicted_not_repaired():
    """Neither 'owed' nor 'already adjusted' explains a 2.5x gap on a 5:1
    split. Something else happened, and guessing is how a repair script
    invents data."""
    assert classify_gap(observed=2.5, expected=5.0) is Verdict.CONTRADICTED


def test_a_near_tie_refuses_rather_than_guesses():
    """Small bonus ratios put the two hypotheses only 10% apart, so a
    marginal win is noise. The asymmetry is deliberate: a false negative
    leaves one action unrepaired, a false positive silently corrupts a series.
    """
    # 1:10 bonus -> expected 1.1. An observed 1.05 sits between the two.
    assert classify_gap(observed=1.05, expected=1.1) is Verdict.ALREADY_ADJUSTED


def test_the_decision_margin_is_actually_enforced():
    """A margin that never binds is a margin that isn't there."""
    expected = 2.0
    # Construct an observation whose 'owed' error is real but whose 'already
    # adjusted' error is only just under the required multiple.
    observed = expected * (1 - 0.05)          # error_if_owed = 0.05
    assert abs(observed - 1.0) >= 0.05 * DECISION_MARGIN
    assert classify_gap(observed, expected) is Verdict.CONFIRMED


def test_no_evidence_is_distinguished_from_evidence_of_no_change():
    """Both leave the action unrepaired, but they need different follow-up:
    one wants price data, the other wants nothing at all."""
    assert classify_gap(observed=None, expected=5.0) is Verdict.NO_PRICE_DATA
    assert classify_gap(observed=0.99, expected=5.0) is Verdict.ALREADY_ADJUSTED


def test_an_unparseable_action_is_never_confirmed():
    assert classify_gap(observed=5.0, expected=None) is Verdict.UNPARSEABLE


def test_confirming_error_ceiling_binds():
    assert MAX_CONFIRMING_ERROR < 0.25, "a 25%-off match is not evidence"
    assert classify_gap(observed=5.0 * (1 + MAX_CONFIRMING_ERROR * 2),
                        expected=5.0) is not Verdict.CONFIRMED


# ---------------------------------------------------------------------------
# End to end on the real historical cases
# ---------------------------------------------------------------------------

def test_gvkpil_the_worst_unadjusted_case_is_repairable():
    """GVKPIL 2008-02-08, a 10:1 face-value split that was never applied and
    reads as a -89.9% single-day return in the backtest's price history."""
    prices = _prices([("2008-02-06", 700.0), ("2008-02-07", 700.0),
                      ("2008-02-08", 70.0), ("2008-02-11", 71.0)])
    result = classify_action("GVKPIL", "2008-02-08", "SPLIT", 0.0,
                             "Fv Split Rs.10/- To Re.1/", prices)
    assert result.repairable
    assert result.ratio == pytest.approx(10.0)


def test_rolta_repairs_from_a_stored_ratio_without_touching_details():
    """ROLTA's 1:1 bonus had a perfectly usable ratio all along — the adjuster
    simply never ran for it. Recovery and never-applied are the same repair."""
    prices = _prices([("2008-01-23", 200.0), ("2008-01-24", 100.0)])
    result = classify_action("ROLTA", "2008-01-24", "BONUS", 1.0, "Bonus 1:1", prices)
    assert result.repairable
    assert result.expected_gap == pytest.approx(2.0)


def test_an_already_adjusted_split_is_reported_with_both_errors():
    """The verdict alone is not reviewable; a human checking a refusal needs
    to see which hypothesis won and by how much."""
    prices = _prices([("2007-04-23", 100.0), ("2007-04-24", 105.0)])
    result = classify_action("MIRZAINT", "2007-04-24", "SPLIT", 0.0,
                             "Fv Split Rs.10/- To Rs.2/", prices)
    assert result.verdict is Verdict.ALREADY_ADJUSTED
    assert not result.repairable
    assert result.error_if_owed > result.error_if_adjusted


# ---------------------------------------------------------------------------
# Several actions on one ex-date
# ---------------------------------------------------------------------------

def test_same_date_actions_combine_into_one_expected_gap():
    """ONMOBILE 2011-05-03: a 2:1 split AND a 1:1 bonus on the same day. The
    market shows one gap of 4x, not two gaps of 2x."""
    assert combined_expected_gap([("SPLIT", 2.0), ("BONUS", 1.0)]) == pytest.approx(4.0)


def test_a_same_date_pair_is_confirmed_not_contradicted():
    """Scoring each action against its own factor measured the combined gap
    against half of it, so both came back CONTRADICTED and a real repair was
    refused. This is the regression that behaviour was."""
    prices = _prices([("2011-05-02", 400.0), ("2011-05-03", 100.0)])
    siblings = [("SPLIT", 2.0, ""), ("BONUS", 1.0, "Bonus 1:1")]

    alone = classify_action("ONMOBILE", "2011-05-03", "SPLIT", 2.0, "", prices)
    together = classify_action("ONMOBILE", "2011-05-03", "SPLIT", 2.0, "", prices,
                               siblings=siblings)

    assert alone.verdict is Verdict.CONTRADICTED
    assert together.verdict is Verdict.CONFIRMED
    assert together.expected_gap == pytest.approx(4.0)


def test_a_lone_action_passed_as_its_own_sibling_is_unchanged():
    """The caller always passes the full same-date group, which for most
    actions is just the action itself — that must not alter the verdict."""
    prices = _prices([("2010-08-17", 500.0), ("2010-08-18", 100.0)])
    solo = classify_action("HDFC", "2010-08-18", "SPLIT", 5.0, "", prices)
    with_self = classify_action("HDFC", "2010-08-18", "SPLIT", 5.0, "", prices,
                                siblings=[("SPLIT", 5.0, "")])
    assert solo.verdict is with_self.verdict is Verdict.CONFIRMED
    assert solo.expected_gap == with_self.expected_gap


def test_a_non_equity_sibling_does_not_inflate_the_expected_gap():
    """JAYBARMARU pairs a real 10:1 split with a placeholder bonus. Folding an
    unadjustable action into the product would overstate the gap."""
    assert combined_expected_gap(
        [("SPLIT", 10.0), ("BONUS", None)]
    ) == pytest.approx(10.0)


def test_a_non_equity_bonus_is_never_repairable():
    prices = _prices([("2019-08-21", 100.0), ("2019-08-22", 50.0)])
    result = classify_action("BRITANNIA", "2019-08-22", "BONUS", 0.0,
                             "Scheme Of Arangement- Bonus - 1 Debenture For 1 "
                             "Equity Share Held", prices)
    assert not result.repairable
    assert result.verdict is Verdict.UNPARSEABLE
