"""
Windowed / "Slider" Backtest Analysis — explicit user instruction
2026-09-04: "if we would have infused the capital on 01-01-2020 with
10,00,000 capital and executed the strategy till 31-12-2024, what is the
CAGR derived... codify this."

Two entry points:
  run_window()        — one arbitrary (start_date, end_date, capital)
                         backtest. Thin, named wrapper over what
                         BacktestConfig + run_native() already support —
                         codified here so "run a strategy over an
                         arbitrary window" has one obvious call site
                         instead of every caller re-deriving it.
  rolling_window_scan() — THE slider mechanism: holds window LENGTH and
                         capital fixed, slides the START DATE across the
                         full history in `step_months` increments, and
                         returns one row of metrics per candidate start
                         date. This is what a UI slider would look up —
                         drag to a start date, read that row's CAGR.

CRITICAL correctness note (learned the hard way this session with R07/R09):
a StrategyAdapter instance accumulates state across a run (_held,
_equity_history, _regime_series, ...). Reusing ONE instance across
multiple windows in rolling_window_scan() would leak state between
windows and silently corrupt every window after the first. Both
functions therefore take a `strategy_factory` — a zero-arg callable that
returns a FRESH instance — never a pre-built strategy object, for
anything that runs more than one window.
"""

from typing import Any, Callable, Dict, List
import logging

import pandas as pd
from dateutil.relativedelta import relativedelta

from momentum_framework.backtesting.adapter import StrategyAdapter
from momentum_framework.backtesting.orchestrator import BacktestConfig, BacktestOrchestrator
from momentum_framework.backtesting.result import BacktestResult

logger = logging.getLogger(__name__)


def run_window(
    strategy: StrategyAdapter,
    start_date: str,
    end_date: str,
    initial_capital: float,
    conn: Any,
) -> BacktestResult:
    """
    One backtest over an arbitrary window — e.g. "capital infused
    2020-01-01, run through 2024-12-31, starting with ₹10,00,000":

        run_window(R01TrailingMomentum(band_id=2, top_n=5,
                   lookback_months=6, rebalance_cadence_days=21),
                   "2020-01-01", "2024-12-31", 1_000_000, conn)

    `strategy` is used ONCE by this function — pass a fresh instance
    (never one already run through another window; see module docstring).
    The signal's own lookback (e.g. 6 months of trailing-return history)
    reaches back before `start_date` automatically — TrailingMomentumSignal
    queries `date <= as_of_date` with no floor at start_date, so the
    first rebalance is correctly informed even though no CAPITAL is
    deployed before start_date.
    """
    config = BacktestConfig(start_date=start_date, end_date=end_date, initial_capital=initial_capital)
    return BacktestOrchestrator(strategy, config).run_native(conn)


def rolling_window_scan(
    strategy_factory: Callable[[], StrategyAdapter],
    conn: Any,
    window_years: float,
    initial_capital: float,
    full_start: str = "2009-01-01",
    full_end: str = "2026-06-30",
    step_months: int = 12,
) -> pd.DataFrame:
    """
    The "slider": CAGR (and other metrics) as a function of chosen start
    date, holding window LENGTH and capital fixed. Slides a
    `window_years`-long window across [full_start, full_end] in
    `step_months` increments, running one FRESH backtest per candidate
    start date (see module docstring on why strategy_factory, not a
    single strategy instance, is required here).

    Returns a DataFrame indexed by window_start with columns:
    window_end, cagr, sharpe_ratio, max_drawdown, trade_count,
    integrity_passed. A window whose computed end would fall after
    full_end is not generated — every row is a COMPLETE window, never a
    truncated one silently passed off as full-length.

    Example — "how would a 5-year window starting anywhere from 2009 to
    2021 have performed, ₹10L capital, stepping the start date yearly":

        df = rolling_window_scan(
            strategy_factory=lambda: R01TrailingMomentum(
                band_id=2, top_n=5, lookback_months=6, rebalance_cadence_days=21),
            conn=conn, window_years=5, initial_capital=1_000_000,
            full_start="2009-01-01", full_end="2026-06-30", step_months=12,
        )
        df.loc["2020-01-01"]  # the exact scenario in the user's request
    """
    if window_years <= 0:
        raise ValueError(f"window_years must be positive, got {window_years}")
    if step_months <= 0:
        raise ValueError(f"step_months must be positive, got {step_months}")

    full_start_ts = pd.Timestamp(full_start)
    full_end_ts = pd.Timestamp(full_end)
    window_delta = relativedelta(years=int(window_years), months=round((window_years % 1) * 12))

    rows: List[Dict[str, Any]] = []
    window_start_ts = full_start_ts
    while True:
        window_end_ts = window_start_ts + window_delta - pd.Timedelta(days=1)
        if window_end_ts > full_end_ts:
            break

        window_start = window_start_ts.strftime("%Y-%m-%d")
        window_end = window_end_ts.strftime("%Y-%m-%d")

        strategy = strategy_factory()  # FRESH instance — see module docstring
        try:
            result = run_window(strategy, window_start, window_end, initial_capital, conn)
            rows.append({
                "window_start": window_start,
                "window_end": window_end,
                "cagr": result.cagr(),
                "sharpe_ratio": result.sharpe(),
                "max_drawdown": result.max_drawdown(),
                "trade_count": result.trade_count,
                "integrity_passed": result.integrity_passed,
            })
        except Exception as e:
            logger.warning(f"Window [{window_start}, {window_end}] failed: {type(e).__name__}: {e}")
            rows.append({
                "window_start": window_start, "window_end": window_end,
                "cagr": None, "sharpe_ratio": None, "max_drawdown": None,
                "trade_count": None, "integrity_passed": False,
            })

        window_start_ts = window_start_ts + relativedelta(months=step_months)

    df = pd.DataFrame(rows).set_index("window_start")
    return df
