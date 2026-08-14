"""
backtest/adapters/panel_filters.py

Phase: Unified Backtest & Paper Trading Umbrella (A93 groundwork)
Owner: Platform / Backtest
Consumers: backtest/adapters/momentum_adapter.py,
backtest/adapters/fundamental_adapter.py.

The three entry-side filters that need nothing but a price/volume panel:
liquidity floor, circuit-lock proxy, and short-term downtrend. One
implementation each, per AGENTS.md invariant 2.

WHY THIS MODULE EXISTS
----------------------
These three lived as private methods on MomentumAdapter. TechnicalAdapter had
its own copies. FundamentalAdapter had none at all -- and the orchestrator
never passed it the parameters either, so `--min-adtv-cr 1.0` on a fundamental
run was accepted by the CLI, stored in config_json, and rendered in the report
as an applied filter while changing nothing.

That is not a small bug. Measured on 2026-08-14 across all 26 fundamental
presets, control vs filtered: 168 trades vs 168 trades, 0 buys added, 0 buys
removed -- bit-identical. A filter that reports itself as applied and does
nothing is worse than an absent one, because every number downstream inherits
a caveat nobody can see.

Rather than paste a fourth copy into FundamentalAdapter, the implementations
move here and the adapters call them.

ORDERING IS PART OF THE CONTRACT
--------------------------------
apply_entry_filters() runs BEFORE a caller's top_n cut, never after. Filtering
after selection silently shrinks the book: a strategy told to hold 10 names
would hold however many of its top 10 happened to survive, leaving capital
idle and misreporting itself as fully deployed. Selecting from the filtered
pool instead keeps the count intact and is what MomentumAdapter._selection_pool
already did -- this preserves that behaviour rather than inventing one.
"""

from __future__ import annotations

import logging
from datetime import date as date_type
from typing import Dict, List, Optional, Sequence

import pandas as pd

from features.momentum_strategy import trailing_momentum_from_panel

logger = logging.getLogger(__name__)

#: Trailing window for the liquidity estimate, in trading sessions.
DEFAULT_ADTV_LOOKBACK_DAYS = 20
#: Trailing window for the downtrend test, in trading sessions.
DEFAULT_DOWNTREND_LOOKBACK_DAYS = 20


def adtv_series(
    price_panel: Optional[pd.DataFrame], volume_panel: Optional[pd.DataFrame],
    tickers: Sequence[str], as_of_date: date_type,
    lookback_days: int = DEFAULT_ADTV_LOOKBACK_DAYS,
) -> pd.Series:
    """Trailing average daily traded value in INR crore, per ticker.

    price(t) * volume(t) with no forward-fill: a NaN day contributes nothing
    to the mean rather than being imputed. A ticker absent from either panel
    is absent from the result -- callers must treat that as "unknown", never
    as "liquid" (see apply_entry_filters).
    """
    if price_panel is None or volume_panel is None or len(tickers) == 0:
        return pd.Series(dtype=float)
    cols = [t for t in tickers if t in volume_panel.columns and t in price_panel.columns]
    if not cols:
        return pd.Series(dtype=float)
    ts = pd.Timestamp(as_of_date)
    window_prices = price_panel[cols].loc[:ts].tail(lookback_days)
    window_volume = volume_panel[cols].loc[:ts].tail(lookback_days)
    return ((window_prices * window_volume) / 1e7).mean(skipna=True)


def is_circuit_locked(
    price_panel: Optional[pd.DataFrame], ticker: str, as_of_date: date_type,
    circuit_band_pct: Optional[float],
) -> bool:
    """True if the realized 1-day return into `as_of_date` meets or exceeds
    `circuit_band_pct` in either direction.

    A coarse proxy for "this close is probably not a fillable price" -- real
    NSE bands vary 5/10/20% by tier. False on insufficient history: missing
    data must never lock a ticker, or a data gap becomes a trading decision.
    """
    if circuit_band_pct is None or price_panel is None or ticker not in price_panel.columns:
        return False
    idx = price_panel.index
    pos = idx.searchsorted(pd.Timestamp(as_of_date))
    if pos <= 0 or pos >= len(idx):
        return False
    prev_price = price_panel[ticker].iloc[pos - 1]
    cur_price = price_panel[ticker].iloc[pos]
    if pd.isna(prev_price) or pd.isna(cur_price) or prev_price <= 0:
        return False
    return abs((cur_price - prev_price) / prev_price) >= circuit_band_pct


def downtrend_tickers(
    price_panel: Optional[pd.DataFrame], tickers: Sequence[str], as_of_date: date_type,
    downtrend_filter_pct: Optional[float],
    lookback_days: int = DEFAULT_DOWNTREND_LOOKBACK_DAYS,
) -> List[str]:
    """Tickers down by at least `downtrend_filter_pct` over the trailing
    window -- i.e. the ones a buy should not be opened into."""
    if downtrend_filter_pct is None or price_panel is None or len(tickers) == 0:
        return []
    short_term = trailing_momentum_from_panel(
        price_panel, list(tickers), str(as_of_date), lookback_days,
    )
    if short_term.empty:
        return []
    return list(short_term[short_term <= -downtrend_filter_pct].index)


def apply_entry_filters(
    candidates: Sequence[str], as_of_date: date_type, *,
    price_panel: Optional[pd.DataFrame] = None,
    volume_panel: Optional[pd.DataFrame] = None,
    min_adtv_cr: Optional[float] = None,
    circuit_band_pct: Optional[float] = None,
    downtrend_filter_pct: Optional[float] = None,
    adtv_lookback_days: int = DEFAULT_ADTV_LOOKBACK_DAYS,
    downtrend_lookback_days: int = DEFAULT_DOWNTREND_LOOKBACK_DAYS,
) -> List[str]:
    """`candidates` minus everything the three entry filters reject, in the
    order MomentumBacktester.run() applies them (liquidity -> circuit-lock ->
    downtrend). Input order is otherwise preserved, because callers rank
    before or after this and must not have their ordering silently reshuffled.

    Call this BEFORE any top_n cut -- see the module docstring.

    A candidate with no ADTV data is DROPPED when min_adtv_cr is set, not
    kept: "we could not measure this name's liquidity" is not evidence that
    it clears the floor, and treating unknown as passing is how an illiquid
    name reaches a book that reports itself as liquidity-filtered.
    """
    kept = list(candidates)
    if not kept:
        return kept

    if min_adtv_cr is not None:
        adtv = adtv_series(price_panel, volume_panel, kept, as_of_date, adtv_lookback_days)
        kept = [
            t for t in kept
            if t in adtv.index and pd.notna(adtv[t]) and adtv[t] >= min_adtv_cr
        ]
        if not kept:
            return kept

    if circuit_band_pct is not None:
        kept = [
            t for t in kept
            if not is_circuit_locked(price_panel, t, as_of_date, circuit_band_pct)
        ]
        if not kept:
            return kept

    if downtrend_filter_pct is not None:
        falling = set(downtrend_tickers(
            price_panel, kept, as_of_date, downtrend_filter_pct, downtrend_lookback_days,
        ))
        kept = [t for t in kept if t not in falling]

    return kept


def active_filter_summary(
    min_adtv_cr: Optional[float] = None,
    circuit_band_pct: Optional[float] = None,
    downtrend_filter_pct: Optional[float] = None,
) -> Dict[str, float]:
    """The filters actually in force, for a run to record. Only non-None
    entries appear, so a report can state what was applied instead of
    echoing back the parameters it was handed."""
    active = {
        "min_adtv_cr": min_adtv_cr,
        "circuit_band_pct": circuit_band_pct,
        "downtrend_filter_pct": downtrend_filter_pct,
    }
    return {k: v for k, v in active.items() if v is not None}
