"""
tests/unit/test_continuity_gate.py

The gate exists because every corporate-action defect in this codebase looked
plausible. An unapplied 10:1 split reads as a -90% day, and -90% days do
happen to small caps — unless you know NSE price bands make a single-session
-90% impossible, in which case it is provably a data defect.

So the tests pin both edges. A gate that fires on real market moves gets
switched off; a gate that misses a 5:1 split does nothing. The exclusions
(circuit locks, known ex-dates) are where a check like this usually goes
wrong, so they get tested as carefully as the detection.
"""

import pandas as pd
import pytest

from ingestion.adjust.continuity_gate import (
    MAX_LEGACY_DAILY_MOVE,
    find_discontinuities,
)


def _bars(rows):
    """rows: (date, close) or (date, close, high, low, volume)."""
    out = []
    for r in rows:
        if len(r) == 2:
            d, c = r
            out.append({"ticker": "T", "date": d, "close": c,
                        "high": c * 1.01, "low": c * 0.99, "volume": 1000})
        else:
            d, c, h, lo, v = r
            out.append({"ticker": "T", "date": d, "close": c,
                        "high": h, "low": lo, "volume": v})
    return pd.DataFrame(out)


def test_an_unapplied_split_is_caught_and_named():
    found = find_discontinuities(_bars([("2008-02-07", 700.0), ("2008-02-08", 70.0)]))
    assert len(found) == 1
    assert found[0].implied_ratio == pytest.approx(10.0)
    assert "10.00x" in str(found[0])


def test_the_implied_ratio_identifies_the_action_immediately():
    """A reader seeing 'implies 5.00x' knows it is an unapplied 5:1 split
    without going to look anything up. That is the point of reporting it."""
    found = find_discontinuities(_bars([("2010-08-17", 500.0), ("2010-08-18", 100.0)]))
    assert found[0].implied_ratio == pytest.approx(5.0)


def test_a_one_to_one_bonus_is_above_the_threshold():
    """The smallest action that must be caught halves the price, at -50%,
    against a 35% threshold. If these ever crossed the gate would be blind to
    the single most common corporate action."""
    found = find_discontinuities(_bars([("2008-01-23", 200.0), ("2008-01-24", 100.0)]))
    assert len(found) == 1
    assert MAX_LEGACY_DAILY_MOVE < 0.50


def test_a_violent_but_possible_move_does_not_fire():
    """-20% is a bad day, not a defect. A gate that flags real market moves is
    a gate somebody turns off."""
    assert find_discontinuities(_bars([("2020-03-22", 100.0), ("2020-03-23", 80.0)])) == []


def test_a_circuit_locked_bar_is_excluded():
    """high == low with volume means the band was hit and the exchange
    enforced the move. It is real, however large."""
    bars = _bars([("2008-10-23", 100.0), ("2008-10-24", 40.0, 40.0, 40.0, 5000)])
    assert find_discontinuities(bars) == []


def test_a_zero_volume_flat_bar_is_not_treated_as_a_circuit_lock():
    """No trades means no price discovery, so high == low proves nothing and
    the exclusion must not apply."""
    bars = _bars([("2008-10-23", 100.0), ("2008-10-24", 40.0, 40.0, 40.0, 0)])
    assert len(find_discontinuities(bars)) == 1


def test_a_known_ex_date_is_suppressed():
    """Before the adjuster runs, a gap on a known ex-date is the expected
    state. Reporting it would bury the unknown ones, which are the point."""
    bars = _bars([("2008-02-07", 700.0), ("2008-02-08", 70.0)])
    assert find_discontinuities(bars, known_action_dates={pd.Timestamp("2008-02-08").date()}) == []


def test_an_unknown_gap_still_fires_when_other_dates_are_known():
    bars = _bars([("2008-02-07", 700.0), ("2008-02-08", 70.0)])
    assert len(find_discontinuities(bars, known_action_dates={pd.Timestamp("2009-01-01").date()})) == 1


def test_upward_jumps_are_caught_too():
    """A reverse split, or an adjustment applied in the wrong direction,
    moves prices up. Only checking for crashes would miss it."""
    assert len(find_discontinuities(_bars([("2013-03-20", 10.0), ("2013-03-21", 137.0)]))) == 1


def test_gaps_are_measured_in_sequence_not_across_a_sort():
    """Unsorted input silently compares unrelated bars, which manufactures
    discontinuities that do not exist."""
    ordered = _bars([("2010-01-04", 100.0), ("2010-01-05", 101.0), ("2010-01-06", 102.0)])
    assert find_discontinuities(ordered.iloc[::-1]) == []


def test_too_few_bars_and_bad_prices_are_not_discontinuities():
    assert find_discontinuities(_bars([("2010-01-04", 100.0)])) == []
    empty = pd.DataFrame(columns=["ticker", "date", "close", "high", "low", "volume"])
    assert find_discontinuities(empty) == []
    assert find_discontinuities(_bars([("2010-01-04", 0.0), ("2010-01-05", 100.0)])) == []
