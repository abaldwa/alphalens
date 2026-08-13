"""
backtest/trade_filters.py

Phase: 3.x (Technical backtest refactor)
Owner: backtest
Consumers: backtest/derive_exit_params.py, backtest/core/engine.py (universe
           construction), tests/unit/test_trade_filters.py

Tradeability filters applied BEFORE any barrier is derived or any metric is
reported. Everything here answers one question: could this trade actually have
been executed as the backtest claims? A backtest that says yes when the answer
is no does not overstate returns a little — it invents them.

Three filters, in the order they apply.

1. TOP-N BY POINT-IN-TIME ADTV
   The intended universe was always "top 800 by ADTV as of the rebalance
   date". It was never actually in force: the 195 unconstrained Technical runs
   carry universe_spec='curated', min_adtv_cr=None and max_tickers=800 — a CAP
   on how many names a screen may return, not a liquidity RANKING. The
   difference is not academic. The single largest trade in the entire history
   (INDOTECH, +1,493.95%, replicated across six templates) was ranked 1,554th
   by ADTV at entry; SERVOTECH (+1,059%) 1,253rd; JAIBALAJI (+1,038%) 1,305th.
   All three would have been excluded by the filter that was believed to be
   running. The headline outliers were, precisely, the names the universe rule
   existed to keep out.

2. CIRCUIT-LOCKED BARS
   A stock locked at its circuit limit cannot be bought or sold at that price —
   there is no opposing side. Filling at a locked price is free money the
   simulation grants itself. A locked bar is identified by high == low with
   non-zero volume: a real trading day with literally no intraday range, which
   in the Indian market means the band was hit and held. INDOTECH's run
   included locked days at +19.99%, +20.00%, +90.08% and three at +5.00%.
   Universe-wide there are 155,718 such bars across 2,311 tickers (2.2% of all
   bars since 2009-04-01), so this is a systematic effect, not a curiosity.

3. DATA BLACKOUTS INSIDE A HOLDING WINDOW
   No barrier can fire on a day that has no bar. INDOTECH's 601-day trade
   contains a 209-calendar-day gap (2023-06-06 -> 2024-01-01) and a 68-day gap
   ending on the exit date: 126 bars where ~410 trading days belong. The
   position was marked at whatever price appeared when data resumed. This is
   widespread — 795 tickers have 1,860 INTERIOR gappy years (excluding
   listing/delisting partial years), concentrated in 2013, 2014, 2020-21 and
   2024 — so it survives the ADTV filter for some names and needs its own
   check.

Why these are filters and not exclusions-by-name: dropping INDOTECH because
its return is implausible is how a backtest is made to flatter itself, and
config/backtest_exclusions.py rightly forbids it ("justified by 'this ticker's
PRICE HISTORY is unverifiable', never by 'this ticker's RETURNS are
inconvenient'"). Each rule here is a statement about executability that is
decided the same way for every name, whatever its return.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

# Universe size. The rule the runs were believed to be applying.
DEFAULT_TOP_N_BY_ADTV = 800

# ADTV lookback. 21 sessions ~ one trading month: long enough not to be moved
# by a single block trade, short enough to react when a name's liquidity
# genuinely changes.
ADTV_LOOKBACK_SESSIONS = 21

# A holding window missing more than this many consecutive TRADING days is a
# blackout: the position was held through a period the engine could not see,
# so no stop, target or max-hold could have fired in it. Five sessions is one
# calendar week — beyond a long weekend or a single suspension day, and well
# short of the multi-month gaps that motivated the check.
MAX_BLACKOUT_SESSIONS = 5


@dataclass(frozen=True)
class FilterReport:
    """Counts for every filter, always reported rather than silently applied —
    a filter that quietly removes 40% of trades changes every number
    downstream, and that has to be visible in the run record."""

    n_input: int
    n_dropped_illiquid: int
    n_dropped_circuit: int
    n_dropped_blackout: int
    n_output: int

    @property
    def dropped_pct(self) -> float:
        return 100.0 * (self.n_input - self.n_output) / self.n_input if self.n_input else 0.0


def rank_by_adtv(ohlcv: pd.DataFrame, as_of: pd.Timestamp,
                 lookback: int = ADTV_LOOKBACK_SESSIONS) -> pd.Series:
    """Point-in-time ADTV rank (1 = most liquid) using only bars STRICTLY
    BEFORE as_of.

    The strict inequality is the point: including the rebalance date's own bar
    would rank a stock using volume that had not printed when the decision was
    made. That is lookahead, and on a day when a name spikes on news it is
    exactly the kind that promotes the stock you would not have been able to
    buy.
    """
    window = ohlcv[ohlcv["date"] < as_of]
    if window.empty:
        return pd.Series(dtype=float)
    recent = (
        window.sort_values("date")
        .groupby("ticker")
        .tail(lookback)
        .assign(turnover=lambda d: d["close"] * d["volume"])
    )
    adtv = recent.groupby("ticker")["turnover"].mean().sort_values(ascending=False)
    return pd.Series(range(1, len(adtv) + 1), index=adtv.index, name="adtv_rank")


def is_circuit_locked(bars: pd.DataFrame) -> pd.Series:
    """True where a bar is locked at its circuit limit: no intraday range on a
    day that actually traded.

    volume > 0 matters. A bar with high == low AND zero volume is a non-trading
    artifact (a carried-forward price), not a circuit lock. Treating those as
    circuit hits would over-filter dormant small caps for the wrong reason.
    """
    return (bars["high"] == bars["low"]) & (bars["volume"] > 0)


def has_blackout(bars: pd.DataFrame, trading_calendar: pd.DatetimeIndex,
                 start: pd.Timestamp, end: pd.Timestamp,
                 max_gap: int = MAX_BLACKOUT_SESSIONS) -> bool:
    """True if the ticker is missing more than `max_gap` consecutive sessions
    between start and end.

    Measured against the exchange trading calendar, never against calendar
    days: a Diwali week or a long weekend is not a blackout, and counting
    calendar days would flag them while missing a genuine three-week suspension
    that happens to straddle few weekends.
    """
    expected = trading_calendar[(trading_calendar > start) & (trading_calendar <= end)]
    if len(expected) == 0:
        return False
    present = set(pd.to_datetime(bars["date"]))
    missing = [d for d in expected if d not in present]
    if not missing:
        return False

    longest = run = 1
    for prev, cur in zip(missing, missing[1:]):
        # Consecutive in the CALENDAR's terms — adjacent missing entries in
        # `expected`, not adjacent dates.
        run = run + 1 if expected.get_loc(cur) == expected.get_loc(prev) + 1 else 1
        longest = max(longest, run)
    return longest > max_gap


def filter_trades(
    trades: pd.DataFrame,
    adtv_rank_at_entry: Optional[pd.Series] = None,
    top_n: int = DEFAULT_TOP_N_BY_ADTV,
) -> tuple[pd.DataFrame, FilterReport]:
    """Applies the tradeability filters to a closed-trade frame.

    trades must carry: ticker, buy_date, sale_date, and the boolean columns
    entry_circuit_locked / exit_circuit_locked / has_blackout precomputed by
    the caller (they need OHLCV, which this module deliberately does not open —
    DuckDB is single-writer here and a filter helper must not hold a connection).

    adtv_rank_at_entry : optional {index-aligned} rank per trade. When None the
    liquidity filter is SKIPPED and reported as zero drops rather than silently
    passing everything as if it had run — the distinction between "filter
    applied, nothing dropped" and "filter never ran" is exactly what went wrong
    with the top-800 universe.
    """
    n_input = len(trades)
    keep = pd.Series(True, index=trades.index)

    n_illiquid = 0
    if adtv_rank_at_entry is not None:
        illiquid = adtv_rank_at_entry.reindex(trades.index) > top_n
        n_illiquid = int(illiquid.fillna(False).sum())
        keep &= ~illiquid.fillna(False)

    circuit = trades.get("entry_circuit_locked", False) | trades.get("exit_circuit_locked", False)
    circuit = pd.Series(circuit, index=trades.index).fillna(False)
    n_circuit = int((keep & circuit).sum())
    keep &= ~circuit

    blackout = pd.Series(trades.get("has_blackout", False), index=trades.index).fillna(False)
    n_blackout = int((keep & blackout).sum())
    keep &= ~blackout

    out = trades[keep]
    return out, FilterReport(
        n_input=n_input,
        n_dropped_illiquid=n_illiquid,
        n_dropped_circuit=n_circuit,
        n_dropped_blackout=n_blackout,
        n_output=len(out),
    )
