"""
backtest/core/metrics.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 1
Owner: Platform / Backtest
Consumers: backtest/core/engine.py (once refactored), every channel
adapter, datastore/api/routers/backtest_reports.py (once migrated)

The single metrics module every channel/adapter reports through, closing
the methodology mismatch between backtest/engine.py (trading-day-
annualized CAGR) and backtest/momentum_metrics.py (calendar/365.25 CAGR):
per the 2026-07-20 user-confirmed decision, calendar/365.25 CAGR + XIRR
are canonical everywhere; trading-day-annualized CAGR is kept only as a
secondary/legacy field (`cagr_trading_day_legacy`) for backward
comparability with existing ML `/ml-backtest` reports.

Reuses backtest/momentum_metrics.py's xirr()/churn_factor() rather than
reimplementing them (that module's bisection-based XIRR is
channel-agnostic already — it operates on a plain cash-flow list, no
momentum-specific assumptions).
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from backtest.momentum_metrics import churn_factor, xirr

TRADING_DAYS_PER_YEAR = 252


@dataclass
class BacktestMetrics:
    cagr: Optional[float]  # calendar/365.25 basis — primary
    cagr_trading_day_legacy: Optional[float]  # trading-day-annualized — secondary, for ML report continuity
    xirr: Optional[float]  # money-weighted, handles lump-sum and SIP uniformly
    final_capital: float
    total_contributed: float  # initial capital + all SIP injections (for SIP mode); == initial_capital for lump-sum
    max_drawdown: float
    win_rate: Optional[float]
    profit_factor: Optional[float]
    sortino: Optional[float]
    calmar: Optional[float]
    n_distinct_tickers_traded: int
    turnover_ratio: Optional[float]
    n_trades: int
    benchmark_cagr: Optional[float]  # None + benchmark_status flagged if fold predates 2023-07 (index_ohlcv gap)
    excess_return: Optional[float]
    benchmark_status: str  # "ok" | "insufficient_benchmark_history"
    cash_position_series: List[Dict] = field(default_factory=list)  # [{"date":..., "cash":...}, ...]


def calendar_cagr(starting_capital: float, ending_value: float, start_date, end_date) -> Optional[float]:
    """Primary CAGR basis (calendar/365.25) — correct under both lump-sum and SIP
    when compared against XIRR, since it doesn't assume trading-day density."""
    if starting_capital <= 0:
        return None
    years = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days / 365.25
    if years <= 0:
        return None
    return (ending_value / starting_capital) ** (1.0 / years) - 1.0


def trading_day_cagr(equity_curve: pd.Series) -> Optional[float]:
    """Legacy basis matching backtest/engine.py's existing _cagr_sharpe_from_equity
    methodology — trading-day-count annualized. Kept only for comparability with
    pre-refactor ML report output; never treat as primary."""
    if len(equity_curve) < 2 or equity_curve.iloc[0] <= 0:
        return None
    n_days = len(equity_curve) - 1
    if n_days <= 0:
        return None
    total_return = equity_curve.iloc[-1] / equity_curve.iloc[0]
    if total_return <= 0:
        return None
    years = n_days / TRADING_DAYS_PER_YEAR
    return total_return ** (1.0 / years) - 1.0


def max_drawdown(equity_curve: pd.Series) -> float:
    if len(equity_curve) == 0:
        return 0.0
    running_max = equity_curve.cummax()
    drawdown = (equity_curve - running_max) / running_max
    return float(drawdown.min())


def sortino_ratio(returns: pd.Series, periods_per_year: int = TRADING_DAYS_PER_YEAR) -> Optional[float]:
    """Like Sharpe but only penalizes downside deviation — recommended addition
    (BacktestUmbrellaPlan.md Truthful Review #8) since small/mid-cap Indian
    equity strategies have fat left tails that Sharpe alone under-penalizes."""
    if len(returns) < 2:
        return None
    downside = returns[returns < 0]
    if len(downside) == 0:
        return None
    downside_std = downside.std()
    if downside_std == 0 or np.isnan(downside_std):
        return None
    mean_return = returns.mean()
    return float((mean_return * periods_per_year) / (downside_std * np.sqrt(periods_per_year)))


def calmar_ratio(cagr_value: Optional[float], mdd: float) -> Optional[float]:
    if cagr_value is None or mdd == 0:
        return None
    return cagr_value / abs(mdd)


def win_rate_and_profit_factor(trade_pnls: List[float]) -> Tuple[Optional[float], Optional[float]]:
    if not trade_pnls:
        return None, None
    wins = [p for p in trade_pnls if p > 0]
    losses = [p for p in trade_pnls if p < 0]
    win_rate = len(wins) / len(trade_pnls)
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else None
    return win_rate, profit_factor


def turnover_ratio(trade_values: List[float], avg_portfolio_value: float) -> Optional[float]:
    """Sum of (buy value + sell value) across the run / average portfolio value —
    the second of the two churn definitions pinned down in BacktestUmbrellaPlan.md
    Truthful Review #11 (n_distinct_tickers_traded is the other; both are reported)."""
    if avg_portfolio_value <= 0:
        return None
    return sum(trade_values) / avg_portfolio_value


def benchmark_metrics(
    strategy_cagr: Optional[float], benchmark_equity_curve: Optional[pd.Series],
    start_date, end_date, index_ohlcv_min_date: date = date(2023, 7, 3),
) -> Tuple[Optional[float], Optional[float], str]:
    """
    Returns (benchmark_cagr, excess_return, status). Per the 2026-07-20
    user-confirmed decision, the index_ohlcv gap (real Nifty history only
    from 2023-07-03) is an ACCEPTED gap, not backfilled — this function's
    job is to make that gap explicit (benchmark_status flagged) rather
    than silently returning None with no explanation.
    """
    if pd.Timestamp(start_date) < pd.Timestamp(index_ohlcv_min_date) or benchmark_equity_curve is None:
        return None, None, "insufficient_benchmark_history"
    bench_cagr = calendar_cagr(
        benchmark_equity_curve.iloc[0], benchmark_equity_curve.iloc[-1], start_date, end_date
    )
    if bench_cagr is None or strategy_cagr is None:
        return bench_cagr, None, "insufficient_benchmark_history"
    return bench_cagr, strategy_cagr - bench_cagr, "ok"


def compute_metrics(
    equity_curve: pd.Series,
    cash_flows: List[Tuple[str, float]],  # [(date_str, amount), ...] incl. initial capital, SIP, tax outflows
    trade_pnls: List[float],
    trade_values: List[float],
    distinct_tickers: List[str],
    start_date, end_date,
    total_contributed: float,
    benchmark_equity_curve: Optional[pd.Series] = None,
    cash_position_series: Optional[List[Dict]] = None,
) -> BacktestMetrics:
    """
    Single entry point every adapter's backtest/walk-forward run calls once
    at the end of a run to produce the standardized metrics record (CAGR,
    XIRR, final capital, churn, win rate, max drawdown, cash position —
    the exact set the user specified, plus Sortino/Calmar per Truthful
    Review #8).
    """
    starting_capital = equity_curve.iloc[0] if len(equity_curve) else 0.0
    ending_value = equity_curve.iloc[-1] if len(equity_curve) else 0.0
    returns = equity_curve.pct_change().dropna() if len(equity_curve) > 1 else pd.Series(dtype=float)

    cagr_value = calendar_cagr(starting_capital, ending_value, start_date, end_date)
    xirr_value = xirr(cash_flows) if len(cash_flows) >= 2 else None
    mdd = max_drawdown(equity_curve)
    win_rate, profit_factor = win_rate_and_profit_factor(trade_pnls)
    bench_cagr, excess_return, bench_status = benchmark_metrics(
        cagr_value, benchmark_equity_curve, start_date, end_date
    )

    return BacktestMetrics(
        cagr=cagr_value,
        cagr_trading_day_legacy=trading_day_cagr(equity_curve),
        xirr=xirr_value,
        final_capital=float(ending_value),
        total_contributed=total_contributed,
        max_drawdown=mdd,
        win_rate=win_rate,
        profit_factor=profit_factor,
        sortino=sortino_ratio(returns),
        calmar=calmar_ratio(cagr_value, mdd),
        n_distinct_tickers_traded=len(set(distinct_tickers)),
        turnover_ratio=turnover_ratio(trade_values, float(equity_curve.mean()) if len(equity_curve) else 0.0),
        n_trades=len(trade_pnls),
        benchmark_cagr=bench_cagr,
        excess_return=excess_return,
        benchmark_status=bench_status,
        cash_position_series=cash_position_series or [],
    )


__all__ = [
    "BacktestMetrics", "calendar_cagr", "trading_day_cagr", "max_drawdown", "sortino_ratio",
    "calmar_ratio", "win_rate_and_profit_factor", "turnover_ratio", "benchmark_metrics",
    "compute_metrics", "churn_factor", "xirr",
]
