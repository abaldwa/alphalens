"""
tests/unit/test_units.py

The field names lie, and the check has to survive real data.

`pnl_pct` holds a fraction, `score` holds a fraction, `exit_urgency` holds
0-100. Reading any one of them wrong produces plausible output — a plausible
CAGR, a plausible exit rate — which is why the units are declared rather than
inferred at each call site.

The tests that matter are the two failure modes of a checker like this:
missing a real 100x scaling error, and firing on legitimate data. The second
is what gets a check switched off, so it gets the most attention here.
"""

import pandas as pd

from backtest.units import (
    FIELD_UNITS,
    Unit,
    check_frame,
    describe,
    unit_of,
)


# ---------------------------------------------------------------------------
# The declarations themselves
# ---------------------------------------------------------------------------

def test_the_misleading_names_are_declared_correctly():
    """Verified against the live store: pnl_pct's median matches
    sale_price/buy_price - 1, which makes it a fraction despite the suffix."""
    assert unit_of("pnl_pct") is Unit.FRACTION
    assert unit_of("score") is Unit.FRACTION
    assert unit_of("exit_urgency") is Unit.SCORE_0_100
    assert unit_of("exit_survival_21d") is Unit.PROBABILITY


def test_exit_urgency_is_not_a_probability():
    """The specific confusion this module exists to prevent. Read as a
    probability, every 0-100 urgency exceeds 1.0 and every 'is this urgent'
    comparison silently becomes true."""
    assert unit_of("exit_urgency").typical_max == 100.0
    assert unit_of("exit_urgency") is not Unit.PROBABILITY


def test_an_undeclared_field_returns_none_rather_than_guessing():
    assert unit_of("some_new_column") is None


def test_describe_puts_the_unit_where_a_reader_will_see_it():
    assert describe("pnl_pct") == "pnl_pct (fraction)"
    assert describe("some_new_column") == "some_new_column"


# ---------------------------------------------------------------------------
# Catching a real scaling error
# ---------------------------------------------------------------------------

def test_percent_written_into_a_fraction_field_is_caught():
    """The actual bug: returns multiplied by 100 upstream. Understates or
    overstates every result by 100x while still looking like returns."""
    frame = pd.DataFrame({"pnl_pct": [-5.2, 3.1, 8.4, -2.7, 4.9]})
    violations = check_frame(frame)
    assert len(violations) == 1
    assert violations[0].field == "pnl_pct"
    assert "100" in str(violations[0])


def test_a_probability_holding_a_0_to_100_score_is_caught():
    frame = pd.DataFrame({"exit_survival_21d": [45.0, 62.0, 71.0, 38.0]})
    assert len(check_frame(frame)) == 1


def test_a_negative_probability_is_caught():
    frame = pd.DataFrame({"exit_survival_5d": [-0.4, -0.6, -0.5]})
    assert len(check_frame(frame)) == 1


# ---------------------------------------------------------------------------
# NOT firing on legitimate data — the part that keeps the check switched on
# ---------------------------------------------------------------------------

def test_correctly_scaled_data_passes():
    frame = pd.DataFrame({
        "pnl_pct": [-0.05, 0.12, 0.03, -0.21],
        "exit_urgency": [45.0, 82.0, 12.0, 60.0],
        "exit_survival_21d": [0.4, 0.9, 0.2, 0.7],
        "holding_days": [3, 21, 45, 8],
        "pnl_inr": [-5_000.0, 12_000.0, 3_000.0, -21_000.0],
    })
    assert check_frame(frame) == []


def test_a_single_extreme_trade_is_not_a_unit_error():
    """pnl_pct = 186.1 is real: an 18,610% return from an unadjusted
    corporate action. It is a DATA defect, not a UNIT defect, and a checker
    that failed the run on it would be switched off within a week. Unit errors
    move the whole distribution; data defects move one row."""
    frame = pd.DataFrame({"pnl_pct": [-0.05, 0.12, 186.1, -0.21, 0.03]})
    assert check_frame(frame) == []


def test_unbounded_fields_are_never_flagged():
    """Rupee amounts and multiples have no natural ceiling. Declaring a bound
    would mean flagging a large portfolio as a unit error."""
    frame = pd.DataFrame({
        "initial_capital": [1_000_000.0, 1_000_000.0],
        "pnl_inr": [50_00_000.0, -12_00_000.0],
        "sharpe": [1.4, 2.2],
    })
    assert check_frame(frame) == []


def test_an_undeclared_column_does_not_fail_the_frame():
    """If adding a column breaks the run, nobody adds columns and the
    registry rots instead."""
    assert check_frame(pd.DataFrame({"brand_new_metric": [4_000.0, 9_000.0]})) == []


def test_nulls_and_non_numerics_are_tolerated():
    frame = pd.DataFrame({
        "pnl_pct": [None, 0.05, float("nan"), -0.02],
        "exit_reason": ["stop", "target", "max_hold", "signal"],
    })
    assert check_frame(frame) == []


def test_an_all_null_column_is_not_a_violation():
    """exit_urgency is entirely null in the live store today. An empty column
    is absence of evidence, not a scaling error."""
    assert check_frame(pd.DataFrame({"exit_urgency": [None, None]})) == []


def test_an_empty_frame_is_clean():
    assert check_frame(pd.DataFrame()) == []


# ---------------------------------------------------------------------------
# Registry hygiene
# ---------------------------------------------------------------------------

def test_every_declared_field_maps_to_a_real_unit():
    assert all(isinstance(u, Unit) for u in FIELD_UNITS.values())


def test_fraction_and_percent_ranges_do_not_overlap_where_it_matters():
    """The scaling check works because a correctly-scaled fraction and a
    100x-scaled one land in disjoint places. If these ranges ever converged,
    the check would silently stop discriminating."""
    assert Unit.FRACTION.typical_max < Unit.PERCENT.typical_max


def test_the_violation_message_names_the_field_and_the_measurement():
    """A violation nobody can act on is noise. It has to say which column,
    what was declared, and what was actually seen."""
    violation = check_frame(pd.DataFrame({"pnl_pct": [4.2, 5.1, 3.9]}))[0]
    text = str(violation)
    assert "pnl_pct" in text and "fraction" in text and "4.2" in text
