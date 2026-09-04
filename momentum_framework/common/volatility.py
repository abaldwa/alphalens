"""
Volatility Computation — shared realized/downside volatility helpers used
by the R14-R17 position-weighting schemes (common/position_weighting.py).

Kept separate from position_weighting.py because "how do I measure a
ticker's recent volatility" is a reusable question (R07's crash detector
and R08/R09's vol-scaling will need the same daily-return machinery once
ported), while "how do I turn a volatility number into a portfolio
weight" is specific to each weighting scheme.
"""

from typing import Any, List
import numpy as np
import pandas as pd


def daily_returns(conn: Any, tickers: List[str], as_of_date: str, lookback_days: int) -> pd.DataFrame:
    """
    Wide daily-return panel (index=date, columns=ticker) for `tickers`,
    covering the `lookback_days` trading sessions ending on or before
    as_of_date. Missing (ticker, date) cells stay NaN — a real gap
    (not-yet-listed, delisted, holiday for that name), never forward-filled.
    """
    if not tickers:
        return pd.DataFrame()

    placeholders = ",".join("?" for _ in tickers)
    df = conn.execute(
        f"""
        SELECT ticker, date, close
        FROM (
            SELECT ticker, date, close,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM ohlcv_adjusted
            WHERE ticker IN ({placeholders}) AND date <= ?
        )
        WHERE rn <= ?
        ORDER BY ticker, date
        """,
        list(tickers) + [as_of_date, lookback_days + 1],
    ).fetch_df()

    if df.empty:
        return pd.DataFrame()

    wide = df.pivot(index="date", columns="ticker", values="close")
    return wide.pct_change(fill_method=None).dropna(how="all")


def realized_volatility(returns: pd.DataFrame) -> pd.Series:
    """Annualized standard deviation of daily returns, per ticker."""
    if returns.empty:
        return pd.Series(dtype=float)
    return returns.std() * np.sqrt(252)


def realized_variance(returns: pd.DataFrame) -> pd.Series:
    """Annualized variance of daily returns, per ticker."""
    vol = realized_volatility(returns)
    return vol ** 2


def downside_volatility(returns: pd.DataFrame, threshold: float = 0.0) -> pd.Series:
    """
    Annualized standard deviation of NEGATIVE (below-threshold) daily
    returns only, per ticker — the Sortino-style asymmetric risk measure.
    A ticker with zero downside observations gets NaN (dropped by the
    caller), not a fabricated zero — no downside days in the window is
    a real, if unusual, outcome, not "no risk."
    """
    if returns.empty:
        return pd.Series(dtype=float)
    downside = returns.where(returns < threshold)
    return downside.std() * np.sqrt(252)
