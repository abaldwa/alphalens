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


def _closed_trade_returns_pct(transactions: List[Dict]) -> List[float]:
    """Simple (non-annualized) per-trade % return -- sell_price/buy_price
    - 1, as a percentage -- for every closed transaction with real
    buy/sell prices. Distinct from trade_cagr (annualized); this is what
    avg_winner_return_pct/avg_loser_return_pct average over."""
    out = []
    for t in transactions:
        if t["status"] != "closed" or t["sell_price"] is None or not t["buy_price"]:
            continue
        out.append((t["sell_price"] / t["buy_price"] - 1.0) * 100.0)
    return out


def avg_winner_return_pct(transactions: List[Dict]) -> Optional[float]:
    """Mean simple % return across closed trades that sold ABOVE their buy
    price (2026-08-09, "average % gain for stocks with positive return").
    None if there are no winning closed trades."""
    winners = [r for r in _closed_trade_returns_pct(transactions) if r > 0]
    return sum(winners) / len(winners) if winners else None


def avg_loser_return_pct(transactions: List[Dict]) -> Optional[float]:
    """Mean simple % return across closed trades that sold AT/BELOW their
    buy price (2026-08-09, "average % loss for stocks with negative
    return"). Reported as a negative number (a loss), consistent with
    return_pct's sign convention elsewhere in this codebase. None if there
    are no losing closed trades."""
    losers = [r for r in _closed_trade_returns_pct(transactions) if r <= 0]
    return sum(losers) / len(losers) if losers else None


def rolling_window_returns(
    equity_curve: List[Dict], window_years: int, step_months: int = 3,
) -> List[Dict]:
    """Every window_years-long rolling-window CAGR from an equity curve,
    window start stepped every step_months (2026-08-09, rolling-return
    consistency reporting -- distinct from the single whole-period CAGR).

    equity_curve : MomentumBacktestResult.equity_curve -- [{"date",
        "total_value"}], one row per trading day. Window starts are taken
        at day-level granularity every ~step_months (approximated as
        21 trading days/month) so this stays O(n) instead of re-scanning
        per calendar month.

    Returns [{"window_start", "window_end", "cagr_pct"}, ...] -- only
    windows that reach a real trading day at/after start + window_years
    are included (the tail of the curve shorter than one window is
    dropped, not padded/extrapolated).
    """
    if len(equity_curve) < 2:
        return []
    df = pd.DataFrame(equity_curve)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    step_days = max(1, step_months * 21)
    window_delta = pd.DateOffset(years=window_years)

    out = []
    for start_idx in range(0, len(df), step_days):
        start_date = df["date"].iloc[start_idx]
        start_value = df["total_value"].iloc[start_idx]
        target_end = start_date + window_delta
        end_candidates = df[df["date"] >= target_end]
        if end_candidates.empty:
            break
        end_row = end_candidates.iloc[0]
        end_date, end_value = end_row["date"], end_row["total_value"]
        if start_value <= 0 or end_value <= 0:
            continue
        years = (end_date - start_date).days / 365.25
        cagr_pct = ((end_value / start_value) ** (1.0 / years) - 1.0) * 100.0
        out.append({
            "window_start": start_date.date().isoformat(),
            "window_end": end_date.date().isoformat(),
            "cagr_pct": cagr_pct,
        })
    return out


def rolling_window_summary(equity_curve: List[Dict], window_years: int) -> Dict[str, Optional[float]]:
    """min/median/max CAGR across all rolling window_years windows -- the
    at-a-glance consistency figures (worst case, typical, best case) shown
    per (band, category) rather than the full window-by-window series."""
    windows = rolling_window_returns(equity_curve, window_years)
    if not windows:
        return {"min_cagr_pct": None, "median_cagr_pct": None, "max_cagr_pct": None, "n_windows": 0}
    values = [w["cagr_pct"] for w in windows]
    return {
        "min_cagr_pct": min(values),
        "median_cagr_pct": float(np.median(values)),
        "max_cagr_pct": max(values),
        "n_windows": len(values),
    }


def return_population_zscores(
    returns_pct: List[Optional[float]], outlier_threshold: float = 3.0,
) -> Dict:
    """Channel-agnostic core of the outlier-detection math: given a list of
    per-trade % returns (None entries pass through as None, e.g. a trade
    with no realized/unrealized return yet), returns each entry's z-score
    plus population summary stats. Extracted from trade_quality_metrics()
    (2026-08-01) so backtest/core/metrics.py::compute_metrics can reuse the
    exact same statistics against backtest.portfolio.Trade's own pnl_pct
    (Technical/Fundamental/Momentum-via-orchestrator channels) instead of
    reimplementing it against a differently-shaped transaction dict.

    Returns
    -------
    dict with:
      zscores               — list, same length/order as returns_pct; None
          where the input was None or the population (<3 non-None values)
          is too small for a meaningful std.
      n_outliers             — count with |zscore| > outlier_threshold.
      max_abs_zscore         — the single most extreme |zscore|, or None.
    """
    valid = [r for r in returns_pct if r is not None]
    mean = float(np.mean(valid)) if len(valid) >= 3 else None
    std = float(np.std(valid)) if len(valid) >= 3 else None

    zscores: List[Optional[float]] = []
    n_outliers = 0
    max_abs_z = None
    for r in returns_pct:
        if r is None or mean is None or std is None or std == 0:
            zscores.append(None)
            continue
        z = (r - mean) / std
        zscores.append(z)
        if max_abs_z is None or abs(z) > max_abs_z:
            max_abs_z = abs(z)
        if abs(z) > outlier_threshold:
            n_outliers += 1

    return {"zscores": zscores, "n_outliers": n_outliers, "max_abs_zscore": max_abs_z}


def trade_quality_metrics(transactions: List[Dict], zscore_outlier_threshold: float = 3.0) -> Dict:
    """Data-quality / summary stats across the FULL transaction ledger
    (open + closed — 2026-08-01 user request: "Total Trades. not just the
    Open Trades"), plus a per-trade return z-score used to auto-flag
    price artifacts like the 2024-07-08..07-31 OHLCV gap that fabricated
    up to +418% "returns" via a stale forward-filled price (see
    backtest/momentum_backtest.py's MAX_FORWARD_FILL_TRADING_DAYS fix —
    this z-score is a second, independent line of defense: even a
    legitimate-looking gap the fill-cap doesn't catch will surface here
    as a statistical outlier for a human to review).

    Mutates each transaction dict in place, adding "return_pct" (realized
    for closed, unrealized mark-to-market for open — None if no sell_price
    yet at all) and "return_zscore" (None if the population is too small
    to compute a meaningful std, i.e. <3 trades with a return).

    Returns
    -------
    dict with:
      total_trades          — len(transactions), open + closed.
      avg_trade_duration_days — mean holding_days across ALL trades (open
          positions use their as-of-run-end holding_days, same convention
          MomentumBacktester already records them with).
      n_outlier_trades       — count with |return_zscore| > zscore_outlier_threshold.
      max_abs_return_zscore  — the single most extreme |return_zscore|, or
          None if it couldn't be computed.
    """
    total_trades = len(transactions)
    durations = [t["holding_days"] for t in transactions if t.get("holding_days") is not None]
    avg_trade_duration_days = (sum(durations) / len(durations)) if durations else None

    returns: List[Optional[float]] = []
    for t in transactions:
        if t.get("sell_price") is not None and t.get("buy_price"):
            r = (t["sell_price"] / t["buy_price"] - 1) * 100
        else:
            r = None
        t["return_pct"] = r
        returns.append(r)

    z_result = return_population_zscores(returns, zscore_outlier_threshold)
    for t, z in zip(transactions, z_result["zscores"]):
        t["return_zscore"] = z

    return {
        "total_trades": total_trades,
        "avg_trade_duration_days": avg_trade_duration_days,
        "n_outlier_trades": z_result["n_outliers"],
        "max_abs_return_zscore": z_result["max_abs_zscore"],
    }
