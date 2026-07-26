"""
backtest/core/regime_breakdown.py

Per-regime performance breakdown for a completed backtest run — "which
strategy works in which market phase," the whole point of systems/regime/
market_regime.py's Bull/Bear/Sideways segmentation.

Slices the run's equity curve and trades by which market_regimes segment
each date falls into, and reports a compact metrics subset per regime —
deliberately NOT the full core/metrics.py BacktestMetrics set, since
XIRR/turnover/benchmark-comparison don't translate cleanly to an arbitrary
sub-period slice of one continuous run. This reports what does: segment-
local CAGR, max drawdown, win rate, profit factor, and trade count.
"""

from dataclasses import dataclass
from datetime import date as date_type
from typing import Any, Dict, List, Optional

import pandas as pd

from backtest.core.metrics import calendar_cagr, max_drawdown, sharpe_ratio, win_rate_and_profit_factor


@dataclass
class RegimeBreakdownRow:
    regime: str
    start_date: date_type  # clipped to the run's own [start_date, end_date] window
    end_date: date_type
    cagr: Optional[float]
    max_drawdown: float
    win_rate: Optional[float]
    profit_factor: Optional[float]
    n_trades: int
    n_days: int
    # 2026-07-26 (REV4 wiring, backtest/core/post_run_checks.py): this
    # segment's own daily-return Sharpe — added so a run's regime segments
    # can feed BacktestIntegrityChecker.check_08_fold_stability as
    # market-regime-aligned sub-periods instead of arbitrary equal-length
    # calendar slices (model-review 2026-07-26: contiguous calendar slices
    # of one continuous equity curve are autocorrelated and mechanically
    # bias std(fold_sharpes) toward passing; regime-segment boundaries are
    # at least defined by an independent real market-state signal, not the
    # calendar). None when the segment has too few days for a meaningful
    # Sharpe (< 2 return observations).
    sharpe: Optional[float] = None


def compute_regime_breakdown(
    equity_curve: pd.Series,
    trades: List[Any],  # backtest.core.portfolio.Trade — duck-typed on .exit_date/.pnl_inr
    run_start: date_type,
    run_end: date_type,
    regime_segments: List[Dict[str, Any]],  # rows from systems/regime/regime_store.list_regime_segments
) -> List[RegimeBreakdownRow]:
    """One row per regime segment overlapping [run_start, run_end], clipped
    to that window. Trades are attributed to the segment containing their
    exit_date (when a trade closed, not when it opened) — the win/loss is
    realized on exit."""
    if equity_curve.empty or not regime_segments:
        return []

    eq_dates = pd.DatetimeIndex(equity_curve.index).normalize()
    rows: List[RegimeBreakdownRow] = []

    for seg in regime_segments:
        seg_start = max(seg["start_date"], run_start)
        seg_end = min(seg["end_date"], run_end)
        if seg_start > seg_end:
            continue

        mask = (eq_dates.date >= seg_start) & (eq_dates.date <= seg_end)
        seg_curve = equity_curve[mask]
        if seg_curve.empty:
            continue

        seg_cagr = calendar_cagr(float(seg_curve.iloc[0]), float(seg_curve.iloc[-1]), seg_start, seg_end)
        seg_mdd = max_drawdown(seg_curve)
        seg_sharpe = sharpe_ratio(seg_curve.pct_change().dropna())

        seg_trades = [t for t in trades if seg_start <= _as_date(t.exit_date) <= seg_end]
        wr, pf = win_rate_and_profit_factor([t.pnl_inr for t in seg_trades])

        rows.append(
            RegimeBreakdownRow(
                regime=seg["regime"],
                start_date=seg_start,
                end_date=seg_end,
                cagr=seg_cagr,
                max_drawdown=seg_mdd,
                win_rate=wr,
                profit_factor=pf,
                n_trades=len(seg_trades),
                n_days=int(mask.sum()),
                sharpe=seg_sharpe,
            )
        )
    return rows


def _as_date(d: Any) -> date_type:
    if isinstance(d, date_type):
        return d
    return pd.Timestamp(d).date()
