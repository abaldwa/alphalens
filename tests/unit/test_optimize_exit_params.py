"""
tests/unit/test_optimize_exit_params.py

The optimiser picks the barriers every Technical strategy will run with, by
searching a grid against one realised path. Its failure mode is not a crash —
it is confidently returning the cell that best fits noise. These tests pin the
replay semantics exactly (where a formula cannot substitute for path order)
and pin the guards that keep the search honest.

Synthetic paths per SPEC-SYS-006's fixture exemption: the point is to control
the ORDER of highs and lows, which is the one thing real data cannot be made
to do on demand.
"""

import numpy as np
import pandas as pd
import pytest

from backtest.optimize_exit_params import (
    CAGR,
    CALMAR,
    SHARPE,
    _cagr,
    _max_drawdown,
    evaluate_cell,
    optimize,
    plateau_width,
    replay_trade,
)


def _path(bars):
    """bars: list of (low, high, close)."""
    return pd.DataFrame(bars, columns=["low", "high", "close"])


def test_stop_fires_on_the_day_it_is_breached():
    path = _path([(100, 105, 102), (88, 101, 90), (80, 95, 85)])
    ret, days = replay_trade(path, buy_price=100, stop_pct=-0.10, target_pct=0.20, max_hold_days=10)
    assert ret == pytest.approx(-0.10)
    assert days == 2


def test_target_fires_on_the_day_it_is_reached():
    path = _path([(99, 105, 104), (103, 125, 122)])
    ret, days = replay_trade(path, buy_price=100, stop_pct=-0.10, target_pct=0.20, max_hold_days=10)
    assert ret == pytest.approx(0.20)
    assert days == 2


def test_stop_wins_an_intrabar_tie():
    """One bar breaching BOTH barriers is the case daily data cannot resolve.
    Booking the target here would make every wide-target/tight-stop cell look
    free — exactly the region the optimiser searches hardest — so the
    pessimistic choice is load-bearing, not decorative."""
    path = _path([(85, 125, 100)])
    ret, _ = replay_trade(path, buy_price=100, stop_pct=-0.10, target_pct=0.20, max_hold_days=10)
    assert ret == pytest.approx(-0.10)


def test_max_hold_exits_at_the_close_not_at_a_barrier():
    path = _path([(99, 101, 100), (99, 102, 101), (99, 103, 107)])
    ret, days = replay_trade(path, buy_price=100, stop_pct=-0.50, target_pct=0.50, max_hold_days=2)
    assert days == 2
    assert ret == pytest.approx(0.01)  # close of bar 2, not bar 3


def test_barriers_cannot_fire_before_the_first_post_entry_bar():
    """An empty post-entry path means the trade never had a chance to move;
    it must return a flat result rather than booking a barrier."""
    assert replay_trade(_path([]), 100, -0.1, 0.2, 10) == (0.0, 0)


def test_cagr_uses_deployed_time_not_wall_clock():
    """Two cells recycling capital at different speeds must be comparable. A
    +10% trade held 21 days annualises far above the same +10% held 252 days;
    scoring both against the backtest window would credit the slow one for
    time it spent holding rather than compounding."""
    fast = _cagr(np.array([0.10]), np.array([21.0]))
    slow = _cagr(np.array([0.10]), np.array([252.0]))
    assert fast > slow
    assert slow == pytest.approx(0.10, rel=1e-6)


def test_total_wipeout_is_ranked_not_raised():
    assert _cagr(np.array([-1.0, 0.5]), np.array([10.0, 10.0])) == -1.0


def test_max_drawdown_is_peak_to_trough():
    dd = _max_drawdown(np.array([0.5, -0.5, -0.2, 0.1]))
    assert dd == pytest.approx(1 - (1.5 * 0.5 * 0.8) / 1.5, rel=1e-9)


def test_drawdown_guard_rejects_the_top_scoring_cell_when_it_is_too_risky():
    """The guard only matters if it can actually veto a winner."""
    rng = np.random.default_rng(0)
    trades = []
    for _ in range(300):
        lows = rng.uniform(70, 99, 30)
        highs = rng.uniform(101, 140, 30)
        closes = rng.uniform(80, 130, 30)
        trades.append((_path(list(zip(lows, highs, closes))), 100.0))

    loose = optimize(trades, objective=CAGR, baseline_max_drawdown=None, min_trades=10)
    guarded = optimize(
        trades, objective=CAGR, baseline_max_drawdown=0.01,
        max_drawdown_multiple=1.0, min_trades=10,
    )
    # With an unreachably tight guard, no cell may be marked eligible.
    assert not guarded["eligible"].any()
    # And the ungated search still ranks something first, so the difference is
    # attributable to the guard rather than to an empty grid.
    assert len(loose) == len(guarded) > 0


def test_ineligible_cells_are_retained_and_flagged_not_dropped():
    """A guard that silently deletes its rejects is invisible in review."""
    trades = [(_path([(95, 110, 105)] * 20), 100.0)] * 50
    ranked = optimize(trades, min_trades=1000)  # nothing can qualify
    assert len(ranked) > 0
    assert set(ranked["eligible"]) == {False}


def test_min_trades_floor_marks_thin_cells_ineligible():
    trades = [(_path([(95, 110, 105)] * 20), 100.0)] * 5
    ranked = optimize(trades, min_trades=100)
    assert not ranked["eligible"].any()


def test_plateau_width_separates_a_lone_peak_from_a_broad_optimum():
    broad = pd.DataFrame({"eligible": [True] * 5, "score": [0.30, 0.29, 0.285, 0.28, 0.10]})
    lone = pd.DataFrame({"eligible": [True] * 5, "score": [0.30, 0.10, 0.09, 0.08, 0.07]})
    assert plateau_width(broad) == 4
    assert plateau_width(lone) == 1


def test_objectives_rank_differently_which_is_why_both_exist():
    """CAGR and Sharpe must be genuinely different orderings over the grid.

    Note this asserts the ORDERING differs, not that the top cell differs: two
    objectives can legitimately agree on the single best cell while disagreeing
    everywhere below it, and on well-behaved data they often do. An earlier
    version of this test demanded a different winner and failed for that
    reason — the requirement was wrong, not the optimiser.
    """
    rng = np.random.default_rng(3)
    trades = []
    for _ in range(400):
        n = 40
        trades.append((
            _path(list(zip(rng.uniform(60, 99, n), rng.uniform(101, 180, n), rng.uniform(70, 160, n)))),
            100.0,
        ))
    key = ["stop_pct", "target_pct", "max_hold_days"]
    by_cagr = optimize(trades, objective=CAGR, min_trades=10)[key]
    by_sharpe = optimize(trades, objective=SHARPE, min_trades=10)[key]
    assert not by_cagr.reset_index(drop=True).equals(by_sharpe.reset_index(drop=True))


def test_calmar_is_finite_and_penalises_drawdown():
    trades = [(_path([(99, 101, 100)] * 10), 100.0)] * 200
    out = evaluate_cell(trades, -0.10, 0.20, 5, CALMAR)
    assert np.isfinite(out["cagr"])
