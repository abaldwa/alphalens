"""
backtest/core/adtv.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 2
Owner: Platform / Backtest

Shared trailing average-daily-traded-value (ADTV, INR crore) computation —
factored out of backtest/adapters/momentum_adapter.py's `_adtv_cr` (the only
adapter that populated Signal.adtv_cr before this fix) so
fundamental_adapter.py and technical_adapter.py can populate the same field
with the identical real formula, instead of leaving it unset.

[BUG FIX, 4th fundamental-strategies review, item 2] Signal.adtv_cr being
unset for Fundamental/Technical signals meant backtest/core/post_run_checks.
py's check_06_liquidity computed applied_min_adt_inr=0.0 for every real run
through those two channels — MIN_ADT_INR (config/settings.py) was never
actually enforced outside Momentum.
"""

from datetime import date as date_type
from typing import Optional

import pandas as pd


def adtv_cr_for_ticker(
    ticker: str,
    as_of_date: date_type,
    price_panel: Optional[pd.DataFrame],
    volume_panel: Optional[pd.DataFrame],
    adtv_lookback_days: int = 20,
) -> Optional[float]:
    """Real trailing-window average daily traded value (INR crore) for one
    ticker, or None if either panel wasn't supplied or the ticker/date has
    no real data — never fabricated. `price_panel`/`volume_panel` are wide
    DataFrames (date index, ticker columns), same shape momentum_adapter.py
    already builds from the OHLCV panel via `.pivot(index="date",
    columns="ticker", values=...)`.
    """
    if price_panel is None or volume_panel is None:
        return None
    if ticker not in volume_panel.columns or ticker not in price_panel.columns:
        return None
    ts = pd.Timestamp(as_of_date)
    window_prices = price_panel[ticker].loc[:ts].tail(adtv_lookback_days)
    window_volume = volume_panel[ticker].loc[:ts].tail(adtv_lookback_days)
    traded_value_cr = (window_prices * window_volume) / 1e7
    value = traded_value_cr.mean(skipna=True)
    return float(value) if pd.notna(value) else None
