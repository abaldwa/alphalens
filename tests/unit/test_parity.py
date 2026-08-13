"""
tests/unit/test_parity.py

The gate STEP 6's layer split runs behind: a pure code move must reproduce a
run EXACTLY, and the gate must be able to prove it.

Why this exists alongside a green unit suite: the suite asserts behaviours
somebody thought to assert. It cannot notice that a 17-year run's 3,886th
trade now fills a day later, and it stayed green through every defect this
refactor found — a wrong universe, unreachable exit triggers, tax that never
left the portfolio. Each produced a perfectly plausible CAGR. A pure move that
quietly changed one would be exactly as plausible.

So these tests are mostly about the gate's SENSITIVITY. A comparison that
cannot fail is worse than none, because it certifies the refactor.
"""

import pandas as pd

from backtest.parity import (
    RELATIVE_TOLERANCE,
    compare_equity,
    compare_metrics,
    compare_runs,
    compare_trades,
)


def _trades(rows=None):
    rows = rows or [
        {"ticker": "AAA", "entry_date": "2024-01-02", "exit_price": 110.0, "quantity": 100},
        {"ticker": "BBB", "entry_date": "2024-01-03", "exit_price": 95.0, "quantity": 50},
    ]
    return pd.DataFrame(rows)


def test_identical_runs_report_identical():
    report = compare_runs(_trades(), _trades())
    assert report.identical
    assert "IDENTICAL" in report.summary()


def test_a_single_changed_price_is_caught():
    after = _trades()
    after.loc[0, "exit_price"] = 110.01
    report = compare_runs(_trades(), after)
    assert not report.identical
    assert any("exit_price" in d for d in report.trade_differences)


def test_a_changed_quantity_is_caught():
    after = _trades()
    after.loc[1, "quantity"] = 51
    assert not compare_runs(_trades(), after).identical


def test_reordered_trades_are_not_treated_as_equal():
    """Two runs holding the same positions in a different sequence are NOT the
    same run: sizing depends on cash available at the moment of each buy, so a
    reordering changes quantities downstream even when the ticker set matches.
    Normalising order away would hide exactly that.
    """
    after = _trades().iloc[::-1].reset_index(drop=True)
    assert not compare_runs(_trades(), after).identical


def test_a_missing_trade_is_caught_and_counted():
    after = _trades().iloc[:1]
    report = compare_runs(_trades(), after)
    assert not report.identical
    assert any("trade count" in d for d in report.trade_differences)


def test_an_extra_trade_is_caught():
    after = pd.concat(
        [_trades(), pd.DataFrame([{"ticker": "CCC", "entry_date": "2024-02-01",
                                   "exit_price": 200.0, "quantity": 10}])],
        ignore_index=True,
    )
    assert not compare_runs(_trades(), after).identical


def test_tolerance_is_tight_enough_to_catch_a_real_change():
    """A pure move recomputes identical arithmetic in the same order, so it
    reproduces results exactly. A tolerance loose enough to absorb a genuine
    behavioural change is a tolerance that lets the change through — a paisa
    on a lakh is 1e-7 relative, and must NOT pass."""
    after = _trades()
    after.loc[0, "exit_price"] = 110.0 * (1 + 1e-7)
    assert not compare_runs(_trades(), after).identical


def test_float_noise_within_tolerance_still_passes():
    after = _trades()
    after.loc[0, "exit_price"] = 110.0 * (1 + RELATIVE_TOLERANCE / 10)
    assert compare_runs(_trades(), after).identical


def test_two_nans_compare_equal():
    """NaN != NaN would report every absent survival curve as a difference and
    drown the real ones — the classic route to a comparison being switched off
    because it cries wolf."""
    before = pd.DataFrame([{"ticker": "AAA", "survival": float("nan")}])
    after = pd.DataFrame([{"ticker": "AAA", "survival": float("nan")}])
    assert compare_runs(before, after).identical


def test_nan_against_a_number_is_a_difference():
    before = pd.DataFrame([{"ticker": "AAA", "survival": float("nan")}])
    after = pd.DataFrame([{"ticker": "AAA", "survival": 0.5}])
    assert not compare_runs(before, after).identical


def test_equity_curve_differences_are_reported_with_their_date():
    idx = pd.to_datetime(["2024-01-01", "2024-01-02"])
    before = pd.Series([100.0, 101.0], index=idx)
    after = pd.Series([100.0, 101.5], index=idx)
    diffs = compare_equity(before, after)
    assert len(diffs) == 1
    assert "2024-01-02" in diffs[0]


def test_metric_added_or_removed_is_reported():
    assert compare_metrics({"cagr": 0.1}, {"cagr": 0.1, "sharpe": 1.2}) == [
        "sharpe: absent -> 1.2"
    ]
    assert compare_metrics({"cagr": 0.1, "sharpe": 1.2}, {"cagr": 0.1}) == [
        "sharpe: 1.2 -> absent"
    ]


def test_column_set_changes_are_reported():
    after = _trades().drop(columns=["quantity"])
    report = compare_runs(_trades(), after)
    assert any("columns differ" in d for d in report.trade_differences)


def test_report_detail_truncates_but_says_how_many_were_hidden():
    """A thousand-line report is a report nobody reads, but silently showing
    the first twenty would understate the blast radius."""
    before = pd.DataFrame([{"ticker": f"T{i}", "px": float(i)} for i in range(100)])
    after = pd.DataFrame([{"ticker": f"T{i}", "px": float(i) + 1} for i in range(100)])
    detail = compare_runs(before, after).detail()
    assert "and 80 more" in detail


def test_only_one_field_is_reported_per_diverged_trade():
    """Listing every field of a diverged trade buries the NEXT diverged trade,
    and the first field is enough to identify it."""
    before = _trades()
    after = _trades()
    after.loc[0, "exit_price"] = 999.0
    after.loc[0, "quantity"] = 999
    diffs = compare_trades(before, after)
    assert len([d for d in diffs if d.startswith("trade[0]")]) == 1
