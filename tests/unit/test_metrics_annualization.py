"""
Unit tests for backtest/core/metrics.py's cadence-aware annualization.

Why these exist: Sharpe and Sortino annualize by sqrt(periods per year), so
the factor must match the curve. core/metrics.py hardcoded 252, which is
right for the orchestrator's daily equity curves and wrong for a momentum
strategy's per-rebalance curve -- and that single mismatch is why a second
Sharpe implementation grew in momentum_metrics.py rather than this one being
reused. These tests pin the behaviour that lets ONE implementation serve
every channel's cadence.
"""

import numpy as np
import pandas as pd

from backtest.core.metrics import (
    TRADING_DAYS_PER_YEAR,
    infer_periods_per_year,
    sharpe_ratio,
    sortino_ratio,
)


def _returns(n, seed=0):
    return pd.Series(np.random.RandomState(seed).normal(0.002, 0.02, n))


def test_infers_weekly_and_monthly_cadence():
    weekly = pd.date_range("2020-01-01", periods=105, freq="W")
    monthly = pd.date_range("2020-01-01", periods=25, freq="ME")
    assert 50 <= infer_periods_per_year(weekly) <= 54
    assert 11 <= infer_periods_per_year(monthly) <= 13


def test_too_short_to_measure_falls_back_to_daily():
    """Two observations cannot establish a cadence; guessing from them would
    produce a wilder factor than the honest default."""
    assert infer_periods_per_year(pd.date_range("2020-01-01", periods=2)) == float(
        TRADING_DAYS_PER_YEAR
    )
    assert infer_periods_per_year(pd.DatetimeIndex([])) == float(TRADING_DAYS_PER_YEAR)


def test_same_day_stamps_do_not_divide_by_zero():
    same = pd.DatetimeIndex(["2020-01-01"] * 5)
    assert infer_periods_per_year(same) == float(TRADING_DAYS_PER_YEAR)


def test_default_is_unchanged_252_basis():
    """Every existing caller passes nothing and must keep its published
    number -- this parameter is additive, not a silent rebasing."""
    r = _returns(300)
    assert sharpe_ratio(r) == sharpe_ratio(r, TRADING_DAYS_PER_YEAR)


def test_weekly_curve_annualized_at_252_overstates_sharpe():
    """The concrete error being fixed: the same weekly returns read ~2.2x
    higher (sqrt(252/52)) when annualized on the daily basis."""
    r = _returns(104)
    weekly_ppy = infer_periods_per_year(pd.date_range("2020-01-01", periods=105, freq="W"))
    wrong, right = sharpe_ratio(r), sharpe_ratio(r, weekly_ppy)
    assert wrong > right
    assert 2.0 < wrong / right < 2.4


def test_sortino_accepts_a_fractional_cadence():
    """periods_per_year was typed int, but an inferred cadence is fractional
    (52.18 weeks/year). An int-only signature would force a caller to round,
    reintroducing the mismatch this removes."""
    r = _returns(104)
    value, reason = sortino_ratio(r, 52.18)
    assert reason is None
    assert value is not None and np.isfinite(value)
