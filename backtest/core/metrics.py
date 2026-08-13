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

from backtest.momentum_metrics import churn_factor, return_population_zscores, xirr

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
    sharpe: Optional[float]  # 2026-07-26 (REV6 wiring): annualized daily-return Sharpe, rf=0 — deflated_sharpe_ratio's required input; None with < 2 return observations or zero volatility
    sortino: Optional[float]
    sortino_none_reason: Optional[str]  # REV19: why sortino is None, when it is
    calmar: Optional[float]
    calmar_none_reason: Optional[str]  # None when a real value was computed
    n_distinct_tickers_traded: int
    turnover_ratio: Optional[float]
    n_trades: int
    benchmark_cagr: Optional[float]  # None + benchmark_status flagged if fold predates 2023-07 (index_ohlcv gap)
    excess_return: Optional[float]
    benchmark_status: str  # "ok" | "insufficient_benchmark_history"
    # A98: WHICH index the comparison was against. Without it, two runs'
    # excess returns look comparable when they may be measured against
    # different yardsticks — and a report cannot honestly label its own
    # benchmark column. None only for runs predating this field.
    benchmark_index_name: Optional[str] = None
    cash_position_series: List[Dict] = field(default_factory=list)  # [{"date":..., "cash":...}, ...]
    avg_days_held: Optional[float] = None  # mean (exit_date - entry_date).days across closed trades; None if n_trades == 0
    # 2026-08-01 (Technical-strategy Momentum-parity reporting) — n_trades
    # above is closed-only (len(trade_pnls)); total_trades additionally
    # counts still-open positions, matching Momentum's n_closed_trades vs.
    # total_trades distinction (backtest/momentum_metrics.py::trade_quality_metrics).
    total_trades: Optional[int] = None
    avg_trade_duration_days: Optional[float] = None  # mean holding-period across ALL trades (open + closed)
    n_outlier_trades: Optional[int] = None  # count with |return z-score| > 3 among this run's own closed trades
    max_abs_return_zscore: Optional[float] = None


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


_TRADING_DAYS_PER_YEAR = 252

# A degenerate (no-trade / flat-equity) run's std/drawdown comes out as
# float rounding noise (e.g. 1e-16), not exact zero — every `== 0`
# division-by-zero guard below needs this tolerance instead, or the
# guard silently doesn't fire and a mean-noise/std-noise ratio gets
# reported as if it were a real metric (found 2026-07-26 auditing the
# B4 technical template: 0 trades, non-null Sharpe of -0.32).
_NEAR_ZERO_STD = 1e-9


def sharpe_ratio(returns: pd.Series) -> Optional[float]:
    """Annualized daily-return Sharpe (rf=0). None with < 2 return
    observations or zero volatility (division-by-zero guard) — matches
    sortino_ratio's None-on-insufficient-data convention. 2026-07-26
    (REV6 wiring): the required scalar input to overfit_checks.
    deflated_sharpe_ratio; BacktestMetrics had no Sharpe field before this.

    Uses _NEAR_ZERO_STD (not `== 0`) since a degenerate (no-trade, flat
    equity) run's std comes out as float noise like 1e-16, not exact
    zero — an exact-equality guard let a meaningless mean-noise/std-noise
    ratio (e.g. -0.32 on a 0-trade run) through as if it were a real
    Sharpe (found 2026-07-26 auditing the B4 technical template)."""
    if len(returns) < 2:
        return None
    std = returns.std()
    if pd.isna(std) or std < _NEAR_ZERO_STD:
        return None
    return float(returns.mean() / std * (_TRADING_DAYS_PER_YEAR**0.5))


def sortino_ratio(
    returns: pd.Series, periods_per_year: int = TRADING_DAYS_PER_YEAR,
) -> Tuple[Optional[float], Optional[str]]:
    """Like Sharpe but only penalizes downside deviation — recommended addition
    (BacktestUmbrellaPlan.md Truthful Review #8) since small/mid-cap Indian
    equity strategies have fat left tails that Sharpe alone under-penalizes.

    Returns (value, none_reason) — REV19 (2026-07-21 review): a bare `None`
    doesn't distinguish "genuinely no downside, excellent run" from "too few
    observations to compute a real ratio"; none_reason makes that explicit
    for any caller aggregating many runs.
    """
    if len(returns) < 2:
        return None, "insufficient_returns"
    downside = returns[returns < 0]
    if len(downside) == 0:
        return None, "no_downside_periods"
    downside_std = downside.std()
    if np.isnan(downside_std) or downside_std < _NEAR_ZERO_STD:
        return None, "zero_downside_std"
    mean_return = returns.mean()
    return float((mean_return * periods_per_year) / (downside_std * np.sqrt(periods_per_year))), None


def calmar_ratio(cagr_value: Optional[float], mdd: float) -> Tuple[Optional[float], Optional[str]]:
    """Returns (value, none_reason) — see sortino_ratio's docstring for why."""
    if cagr_value is None:
        return None, "no_cagr"
    if abs(mdd) < _NEAR_ZERO_STD:
        return None, "zero_or_undefined_drawdown"
    return cagr_value / abs(mdd), None


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
    start_date, end_date, index_ohlcv_min_date: Optional[date] = None,
) -> Tuple[Optional[float], Optional[float], str]:
    """
    Returns (benchmark_cagr, excess_return, status).

    The 2026-07-20 note here recorded index_ohlcv as holding real Nifty
    history only from 2023-07-03, and this function hard-coded that date as
    a floor: any run starting earlier got (None, None,
    "insufficient_benchmark_history") REGARDLESS of whether real benchmark
    data covered it.

    [BUG FIX 2026-08-08] That floor is stale. index_ohlcv now holds Nifty
    500 continuously from 2012-03-13 — 3,549 distinct trading dates with no
    gap wider than 7 days (verified directly against the table). The cutoff
    was therefore silently nulling benchmark_cagr/excess_return for every
    run starting before mid-2023: a 5-year 2021-start Technical sweep
    reported NO benchmark comparison at all, on 46 strategies, even though
    the full Nifty 500 series for that window was sitting in the DB.

    The cutoff was also redundant as a safety mechanism. The only source of
    benchmark_equity_curve is BacktestEngine._build_benchmark_curve(), which
    already returns None when the index has no real overlap with the run's
    dates, and the None branch below already reports
    "insufficient_benchmark_history". Real coverage is thus decided by the
    data itself rather than by a date constant that has to be maintained by
    hand — so the default is now None (no floor), and CLAUDE.md Absolute
    Rule 6 still holds: absent real index data, this returns None, never a
    synthetic benchmark.

    index_ohlcv_min_date is retained (default None = no floor) so a caller
    with a genuine reason to assert a coverage floor can still pass one.
    """
    if benchmark_equity_curve is None:
        return None, None, "insufficient_benchmark_history"
    if index_ohlcv_min_date is not None and pd.Timestamp(start_date) < pd.Timestamp(index_ohlcv_min_date):
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
    holding_days: Optional[List[float]] = None,
    trade_returns_pct: Optional[List[float]] = None,
    n_open_positions: int = 0,
    holding_days_all: Optional[List[float]] = None,
    benchmark_index_name: Optional[str] = None,
) -> BacktestMetrics:
    """
    Single entry point every adapter's backtest/walk-forward run calls once
    at the end of a run to produce the standardized metrics record (CAGR,
    XIRR, final capital, churn, win rate, max drawdown, cash position —
    the exact set the user specified, plus Sortino/Calmar per Truthful
    Review #8).

    trade_returns_pct : optional (2026-08-01) — per-closed-trade % return
        (e.g. [t.pnl_pct * 100 for t in portfolio.trades], net-of-cost —
        deliberately NOT recomputed from raw buy/sell prices the way
        Momentum's trade_quality_metrics does, since Trade.pnl_pct already
        reflects real transaction costs). Feeds n_outlier_trades/
        max_abs_return_zscore via the same population-z-score math
        Momentum's trade-quality outlier detection uses
        (backtest.momentum_metrics.return_population_zscores) — a second,
        independent defense against fabricated/stale-price trades, same
        motivation as the Momentum fix. None (omit) leaves those fields
        None, unaffected for any caller not yet passing it.
    n_open_positions : still-open position count at run end — added to
        len(trade_pnls) for total_trades (open + closed), since n_trades
        itself stays closed-only for backward compatibility.
    holding_days_all : optional — holding-period days across BOTH closed
        trades and still-open positions (as-of-run-end for the latter),
        feeding avg_trade_duration_days. Falls back to `holding_days`
        (closed-only, same value as avg_days_held) when omitted — no
        behavior change for any existing caller.
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
    sharpe_value = sharpe_ratio(returns)
    sortino_value, sortino_reason = sortino_ratio(returns)
    calmar_value, calmar_reason = calmar_ratio(cagr_value, mdd)

    if trade_returns_pct:
        z_result = return_population_zscores(trade_returns_pct)
        n_outlier_trades = z_result["n_outliers"]
        max_abs_return_zscore = z_result["max_abs_zscore"]
    else:
        n_outlier_trades = None
        max_abs_return_zscore = None

    return BacktestMetrics(
        cagr=cagr_value,
        cagr_trading_day_legacy=trading_day_cagr(equity_curve),
        xirr=xirr_value,
        final_capital=float(ending_value),
        total_contributed=total_contributed,
        max_drawdown=mdd,
        win_rate=win_rate,
        profit_factor=profit_factor,
        sharpe=sharpe_value,
        sortino=sortino_value,
        sortino_none_reason=sortino_reason,
        calmar=calmar_value,
        calmar_none_reason=calmar_reason,
        n_distinct_tickers_traded=len(set(distinct_tickers)),
        turnover_ratio=turnover_ratio(trade_values, float(equity_curve.mean()) if len(equity_curve) else 0.0),
        n_trades=len(trade_pnls),
        benchmark_cagr=bench_cagr,
        excess_return=excess_return,
        benchmark_status=bench_status,
        benchmark_index_name=benchmark_index_name,
        cash_position_series=cash_position_series or [],
        avg_days_held=(float(np.mean(holding_days)) if holding_days else None),
        total_trades=len(trade_pnls) + n_open_positions,
        avg_trade_duration_days=(
            float(np.mean(holding_days_all)) if holding_days_all
            else (float(np.mean(holding_days)) if holding_days else None)
        ),
        n_outlier_trades=n_outlier_trades,
        max_abs_return_zscore=max_abs_return_zscore,
    )


__all__ = [
    "BacktestMetrics", "calendar_cagr", "trading_day_cagr", "max_drawdown", "sharpe_ratio", "sortino_ratio",
    "calmar_ratio", "win_rate_and_profit_factor", "turnover_ratio", "benchmark_metrics",
    "compute_metrics", "churn_factor", "xirr",
]
