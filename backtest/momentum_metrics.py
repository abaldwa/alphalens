"""
backtest/momentum_metrics.py

Phase: FeatureBacklog.md ML38 — momentum strategy scoping/implementation
Owner: Platform / Backtest
Consumers: backtest/momentum_backtest.py, scripts/run_momentum_experimentation.py

The 3 metrics ML38's scope calls for per variant: Total Returns, CAGR, and
Churn Factor (reported as both a per-rebalance series and an annualized
average — 2026-07-14 user decision).
"""

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def xirr(cash_flows: List[Tuple[str, float]]) -> Optional[float]:
    """
    Money-weighted annual rate of return for a series of irregularly-timed
    cash flows (2026-07-14, added for the SIP comparison — plain CAGR only
    works for a single lump-sum in/out; a monthly SIP needs XIRR since
    each contribution has its own date).

    cash_flows : [(date_str, amount), ...] — contributions negative
        (money leaving the investor's pocket), the final value positive
        (money that would come back if liquidated on that date). Order
        doesn't matter internally but the first entry's date is used as
        the discounting anchor.

    Returns
    -------
    float or None — None if a rate can't be bracketed (e.g. all cash
    flows are the same sign, so there's no real rate that zeroes the
    NPV), rather than raising or guessing a value.
    """
    if len(cash_flows) < 2:
        raise ValueError("xirr needs at least 2 cash flows")
    dates = [pd.Timestamp(d) for d, _ in cash_flows]
    amounts = [a for _, a in cash_flows]
    anchor = min(dates)

    def npv(rate: float) -> float:
        return sum(a / (1.0 + rate) ** ((d - anchor).days / 365.0) for d, a in zip(dates, amounts))

    lo, hi = -0.9999, 10.0
    f_lo, f_hi = npv(lo), npv(hi)
    if f_lo == 0:
        return lo
    if f_hi == 0:
        return hi
    if (f_lo > 0) == (f_hi > 0):
        return None  # same sign at both ends: can't bracket a root

    for _ in range(200):
        mid = (lo + hi) / 2.0
        f_mid = npv(mid)
        if abs(f_mid) < 1e-6:
            return mid
        if (f_lo > 0) == (f_mid > 0):
            lo, f_lo = mid, f_mid
        else:
            hi, f_hi = mid, f_mid
    return (lo + hi) / 2.0


def total_return(starting_capital: float, ending_value: float) -> float:
    """Net-of-cost total return over the whole run, e.g. 0.42 = +42%."""
    if starting_capital <= 0:
        raise ValueError("starting_capital must be positive")
    return (ending_value / starting_capital) - 1.0


def cagr(starting_capital: float, ending_value: float, start_date: str, end_date: str) -> float:
    """Compounded annual growth rate over the real elapsed calendar time
    between start_date and end_date (ML38: computed over the full
    experimentation period)."""
    if starting_capital <= 0:
        raise ValueError("starting_capital must be positive")
    years = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days / 365.25
    if years <= 0:
        raise ValueError("end_date must be after start_date")
    return (ending_value / starting_capital) ** (1.0 / years) - 1.0


def churn_factor(rebalance_events: List[Dict]) -> Dict:
    """
    rebalance_events: list of dicts, one per rebalance, each with at least
    {"date": iso date str, "n_bought": int, "n_sold": int}.

    Returns
    -------
    dict with:
      per_rebalance: [{"date", "n_bought", "n_sold", "n_transactions"}, ...]
      avg_transactions_per_year: float — mean of each calendar year's total
        (bought + sold) transaction count across the run (the "average
        number of stocks bought+sold through the year" ML38 asked for).
    """
    per_rebalance = [
        {
            "date": event["date"],
            "n_bought": event["n_bought"],
            "n_sold": event["n_sold"],
            "n_transactions": event["n_bought"] + event["n_sold"],
        }
        for event in rebalance_events
    ]
    if not per_rebalance:
        return {"per_rebalance": [], "avg_transactions_per_year": 0.0}

    df = pd.DataFrame(per_rebalance)
    df["year"] = pd.to_datetime(df["date"]).dt.year
    per_year = df.groupby("year")["n_transactions"].sum()
    avg_per_year = float(per_year.mean())

    return {"per_rebalance": per_rebalance, "avg_transactions_per_year": avg_per_year}


_NEAR_ZERO_STD = 1e-9


def sharpe_sortino_calmar(equity_curve: List[Dict], cagr_value: Optional[float]) -> Dict[str, Optional[float]]:
    """
    Sharpe/Sortino/Calmar for a MomentumBacktestResult.equity_curve
    (2026-07-27 user request — computed straight from the equity curve
    every already-completed sweep/experimentation run already has, no
    fresh backtest needed).

    equity_curve : [{"date": iso_str, "total_value": float}, ...] in
        chronological REBALANCE order — NOT daily. backtest/core/
        metrics.py's sharpe_ratio/sortino_ratio hardcode a 252-trading-
        day annualization, which is correct for the unified engine's
        daily equity curve but would misstate a weekly/biweekly/monthly/
        bimonthly/quarterly momentum rebalance schedule. This instead
        infers real periods-per-year from the curve's own average
        calendar-day spacing, so every rebalance frequency annualizes
        correctly.

    cagr_value : this variant's already-computed calendar/365.25 CAGR
        (backtest/momentum_metrics.py::cagr) — reused for Calmar rather
        than recomputed, for consistency with the CAGR already reported
        elsewhere in the same variant dict.
    """
    if len(equity_curve) < 3:
        return {"sharpe": None, "sortino": None, "calmar": None}

    dates = pd.to_datetime([e["date"] for e in equity_curve])
    values = np.array([e["total_value"] for e in equity_curve], dtype=float)

    period_returns = pd.Series(values[1:] / values[:-1] - 1.0)
    period_returns = period_returns[np.isfinite(period_returns)]
    if len(period_returns) < 2:
        return {"sharpe": None, "sortino": None, "calmar": None}

    span_days = (dates[-1] - dates[0]).days
    periods_per_year = (len(dates) - 1) / max(span_days / 365.25, 1e-9)

    std = period_returns.std()
    sharpe = (
        float(period_returns.mean() / std * (periods_per_year**0.5))
        if pd.notna(std) and std > _NEAR_ZERO_STD else None
    )

    downside = period_returns[period_returns < 0]
    if len(downside) == 0:
        sortino = None
    else:
        downside_std = downside.std()
        sortino = (
            float((period_returns.mean() * periods_per_year) / (downside_std * (periods_per_year**0.5)))
            if pd.notna(downside_std) and downside_std > _NEAR_ZERO_STD else None
        )

    running_max = np.maximum.accumulate(values)
    drawdown = (values - running_max) / running_max
    mdd = float(drawdown.min()) if len(drawdown) else 0.0
    calmar = (cagr_value / abs(mdd)) if (cagr_value is not None and abs(mdd) > _NEAR_ZERO_STD) else None

    return {"sharpe": sharpe, "sortino": sortino, "calmar": calmar, "max_drawdown": mdd}


def win_rate(transactions: List[Dict]) -> Optional[float]:
    """Fraction of CLOSED transactions that sold above their buy price.

    transactions : MomentumBacktestResult.transactions — dicts with at
        least {"status", "buy_price", "sell_price"}. Open positions
        (status != "closed") are excluded, since they have no realized
        outcome yet. None if there are no closed transactions to judge.
    """
    closed = [t for t in transactions if t["status"] == "closed"]
    if not closed:
        return None
    wins = sum(1 for t in closed if t["sell_price"] is not None and t["sell_price"] > t["buy_price"])
    return wins / len(closed)
