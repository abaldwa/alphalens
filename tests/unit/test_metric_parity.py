"""
tests/unit/test_metric_parity.py

T13: rolling windows, year-on-year returns, churn and average winner/loser are
computed IN-ENGINE for every channel, under the same definitions Momentum
already used.

Why parity is the point: Technical previously got these post-hoc from
trade-book CSVs under a DIFFERENT definition. That is worse than not having
them, because the columns line up next to Momentum's and invite a comparison
that is not valid.

Everything here is a FRACTION and every rolling figure is ANNUALISED, matching
the rest of BacktestMetrics.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

from backtest.core.metrics import (
    ROLLING_WINDOW_YEARS,
    avg_winner_loser,
    churn_per_year,
    compute_metrics,
    financial_year_label,
    fy_returns,
    rolling_window_cagrs,
    rolling_window_summary,
)


def _steady_curve(rate_per_day: float = 1.0004, start="2015-04-01", end="2026-03-31"):
    idx = pd.date_range(start, end, freq="B")
    return pd.Series(100_000 * (rate_per_day ** np.arange(len(idx))), index=idx)


# --- rolling windows -------------------------------------------------------


def test_rolling_windows_are_annualised_not_totals():
    """The defining check. On a steady ~11%/yr curve, the 3-year window's
    median must be ~11%/yr — NOT the ~37% total that three years compounds to.
    Reporting the total here is what makes a 3-year and a 5-year figure look
    incomparable when they describe the same strategy."""
    curve = _steady_curve()
    summary = rolling_window_summary(curve, 3)
    assert 0.10 < summary["median_cagr"] < 0.12


def test_rolling_median_agrees_with_the_whole_period_cagr():
    """Internal consistency: for a steady curve the rolling median and the
    headline CAGR must be the same number. A mismatch means one of them is
    being annualised a second time — a defect this project has shipped once."""
    curve = _steady_curve()
    m = compute_metrics(
        equity_curve=curve,
        cash_flows=[("2015-04-01", -100_000.0)],
        trade_pnls=[1.0],
        trade_values=[100.0],
        distinct_tickers=["A"],
        start_date=dt.date(2015, 4, 1),
        end_date=dt.date(2026, 3, 31),
        total_contributed=100_000.0,
    )
    assert abs(m.rolling_returns["3y"]["median_cagr"] - m.cagr) < 0.01


def test_windows_step_by_calendar_time_not_row_index():
    """Index stepping under-samples by ~60x on a rebalance-frequency curve and
    once produced 1-2 windows over a 16-year backtest."""
    curve = _steady_curve()
    windows = rolling_window_cagrs(curve, 3, step_months=3)
    # 11 years of history, 3-year windows, quarterly steps -> ~32 windows.
    assert len(windows) > 25


def test_a_partial_tail_window_is_dropped_not_extrapolated():
    """Extrapolating a stub window invents the strategy's most recent
    performance — the figure a reader trusts most."""
    short = _steady_curve(start="2024-04-01", end="2026-03-31")  # 2 years
    assert rolling_window_cagrs(short, 3) == []
    assert rolling_window_summary(short, 3)["n_windows"] == 0
    assert rolling_window_summary(short, 3)["median_cagr"] is None


def test_all_four_window_lengths_are_reported():
    assert ROLLING_WINDOW_YEARS == (2, 3, 4, 5)


# --- financial years -------------------------------------------------------


def test_financial_year_runs_april_to_march():
    assert financial_year_label("2020-04-01") == "FY2021"
    assert financial_year_label("2020-03-31") == "FY2020"
    assert financial_year_label("2021-01-15") == "FY2021"


def test_yoy_years_chain_to_the_whole_period_return():
    """Each year measured from the previous year's close, so compounding the
    per-year returns reproduces the total. Measuring each year from its own
    first row instead would double-count the gaps."""
    curve = _steady_curve()
    years = fy_returns(curve)
    compounded = 1.0
    for y in years:
        compounded *= 1 + (y["return_pct"] or 0.0)
    total = curve.iloc[-1] / curve.iloc[0]
    assert abs(compounded - total) / total < 0.01


def test_a_mid_year_start_is_flagged_partial():
    """A stub period presented as a year drags every 'share of positive years'
    figure."""
    curve = _steady_curve(start="2015-09-01", end="2020-03-31")
    years = fy_returns(curve)
    assert years[0]["partial"] is True
    assert all(not y["partial"] for y in years[1:])


def test_an_april_start_is_not_partial():
    years = fy_returns(_steady_curve(start="2015-04-01", end="2020-03-31"))
    assert years[0]["partial"] is False


# --- churn and trade quality ----------------------------------------------


def test_churn_is_round_trips_per_year():
    assert abs(churn_per_year(110, "2016-04-01", "2026-03-31") - 11.0) < 0.1


def test_churn_is_none_rather_than_zero_without_trades():
    """Zero churn is a real, meaningful value (a buy-and-hold strategy); no
    trades at all is missing data. They must not render the same."""
    assert churn_per_year(0, "2016-04-01", "2026-03-31") is None
    assert churn_per_year(None, "2016-04-01", "2026-03-31") is None


def test_avg_winner_and_loser_are_trade_outcomes_not_rates():
    """A 4% gain over a three-day hold is 4%, not 380%/yr. The rate rule in
    AGENTS.md explicitly exempts trade-level P&L."""
    win, loss = avg_winner_loser([0.05, -0.02, 0.10, -0.04])
    assert abs(win - 0.075) < 1e-9
    assert abs(loss - (-0.03)) < 1e-9


def test_all_winners_leaves_the_loser_average_none():
    win, loss = avg_winner_loser([0.05, 0.10])
    assert win is not None and loss is None
