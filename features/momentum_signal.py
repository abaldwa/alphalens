"""
features/momentum_signal.py

Phase: FeatureBacklog.md ML38 — momentum strategy scoping/implementation
Owner: Platform / Features
Consumers: backtest/momentum_backtest.py

Trailing price momentum ranking: for a given as_of_date and a set of
candidate tickers, trailing_momentum() returns each ticker's percentage
price return over the trailing N trading days (3/6/9/12 real months,
approximated as 63/126/189/252 trading days — matching the codebase's
existing 21-trading-day-per-month convention used elsewhere, e.g.
signal_63d).

Per ML38's confirmed scope, each of the 3/6/9/12-month lookbacks is its
own independent ranking — never blended into one composite score.
"""

import logging
from typing import Any, List

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

TRADING_DAYS_PER_MONTH = 21
LOOKBACK_MONTHS = [3, 6, 9, 12]


def lookback_trading_days(months: int) -> int:
    return months * TRADING_DAYS_PER_MONTH


def trailing_momentum(
    normalised_conn: Any, tickers: List[str], as_of_date: str, lookback_days: int
) -> pd.Series:
    """
    Trailing `lookback_days`-trading-day percentage return per ticker,
    ending at the most recent real ohlcv_adjusted close on or before
    as_of_date. A ticker without at least lookback_days+1 real closes
    on/before as_of_date is excluded (no partial-window guess).

    Returns
    -------
    pd.Series indexed by ticker, values = pct return (e.g. 0.15 = +15%).
    Empty Series if no ticker has enough history.
    """
    if not tickers:
        return pd.Series(dtype=float)
    placeholders = ",".join("?" for _ in tickers)
    df = normalised_conn.execute(
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
        return pd.Series(dtype=float)

    def _return(group: pd.DataFrame):
        if len(group) < lookback_days + 1:
            return None
        start_close = group.iloc[0]["close"]
        end_close = group.iloc[-1]["close"]
        if start_close is None or start_close == 0:
            return None
        return (end_close / start_close) - 1.0

    returns = df.groupby("ticker").apply(_return, include_groups=False)
    returns = returns.dropna()
    return returns.astype(float)


def load_price_panel(normalised_conn: Any, tickers: List[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    Wide close-price panel (index=date, columns=ticker) for `tickers` over
    [start_date, end_date], loaded once from ohlcv_adjusted and reused
    in-memory for every backtest variant's momentum/price lookups —
    avoids re-querying the DB per rebalance across dozens of variants.
    Missing (ticker, date) cells are real gaps (not-yet-listed, delisted,
    trading holiday for that name, etc.) and stay NaN — never
    forward-filled here; callers decide how to handle a NaN at lookup time.
    """
    if not tickers:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in tickers)
    df = normalised_conn.execute(
        f"""
        SELECT date, ticker, close
        FROM ohlcv_adjusted
        WHERE ticker IN ({placeholders}) AND date >= ? AND date <= ?
        ORDER BY date
        """,
        list(tickers) + [start_date, end_date],
    ).fetch_df()
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    return df.pivot(index="date", columns="ticker", values="close")


def load_volume_panel(normalised_conn: Any, tickers: List[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    Wide adjusted-volume panel (index=date, columns=ticker), same shape and
    loading pattern as load_price_panel above. Used by MomentumBacktester's
    optional ADTV/liquidity filter (Fix 1, FeatureBacklog full-codebase
    review) — real volume from ohlcv_adjusted, never a mock/derived proxy.
    Missing cells stay NaN, same real-gap semantics as load_price_panel.
    """
    if not tickers:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in tickers)
    df = normalised_conn.execute(
        f"""
        SELECT date, ticker, volume
        FROM ohlcv_adjusted
        WHERE ticker IN ({placeholders}) AND date >= ? AND date <= ?
        ORDER BY date
        """,
        list(tickers) + [start_date, end_date],
    ).fetch_df()
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    return df.pivot(index="date", columns="ticker", values="volume")


def trailing_momentum_from_panel(
    price_panel: pd.DataFrame, tickers: List[str], as_of_date: str, lookback_days: int
) -> pd.Series:
    """
    In-memory equivalent of trailing_momentum(), operating on a
    pre-loaded wide price panel (see load_price_panel) instead of hitting
    the DB — the hot path used inside MomentumBacktester's rebalance loop.
    A ticker missing either endpoint close (not yet listed, delisted, or
    simply outside the panel's columns) is excluded, never guessed.
    """
    if price_panel.empty or not tickers:
        return pd.Series(dtype=float)
    as_of_ts = pd.Timestamp(as_of_date)
    available_dates = price_panel.index[price_panel.index <= as_of_ts]
    if len(available_dates) < lookback_days + 1:
        return pd.Series(dtype=float)
    end_date = available_dates[-1]
    start_date = available_dates[-(lookback_days + 1)]

    valid_tickers = [t for t in tickers if t in price_panel.columns]
    if not valid_tickers:
        return pd.Series(dtype=float)

    start_prices = price_panel.loc[start_date, valid_tickers]
    end_prices = price_panel.loc[end_date, valid_tickers]
    valid = start_prices.notna() & end_prices.notna() & (start_prices != 0)
    returns = (end_prices[valid] / start_prices[valid]) - 1.0
    return returns.astype(float)


def top_n_by_momentum(
    normalised_conn: Any, tickers: List[str], as_of_date: str, lookback_days: int, top_n: int
) -> List[str]:
    """Top-`top_n` tickers by trailing momentum as of as_of_date, out of
    `tickers`. Fewer than top_n returned if fewer tickers have enough
    history — never padded/guessed."""
    momentum = trailing_momentum(normalised_conn, tickers, as_of_date, lookback_days)
    if momentum.empty:
        return []
    return momentum.sort_values(ascending=False).head(top_n).index.tolist()


def orthogonalize_momentum_vs_factors(
    momentum: pd.Series,
    market_cap: pd.Series,
    beta: pd.Series,
    min_observations: int = 10,
) -> pd.Series:
    """
    Cross-sectional factor-neutralization (2026-07-19 full-codebase-review
    Fix B3): regress raw momentum scores on log(market_cap) and a
    sector-beta proxy across the candidate universe for one rebalance
    date, and return the OLS residuals — the part of each ticker's
    momentum NOT explained by size or beta. Ranking on the residual
    instead of raw momentum avoids the strategy being a disguised
    small-cap-beta bet (standard factor-neutralization technique).

    Same OLS approach as systems/damodaran_valuation/relative/
    pe_regression.py (np.linalg.lstsq) rather than a new regression
    implementation.

    Parameters
    ----------
    momentum : pd.Series
        ticker -> raw trailing momentum score for this rebalance date.
    market_cap : pd.Series
        ticker -> market cap (any consistent unit, e.g. INR crore) for
        the same date. Tickers missing from this series are dropped from
        the regression (never imputed).
    beta : pd.Series
        ticker -> beta proxy (e.g. SECTOR_UNLEVERED_BETAS lookup) for the
        same date/tickers.
    min_observations : int
        Minimum number of tickers with all three values present required
        to fit the regression (default 10 — a 3-parameter OLS with fewer
        points is unstable, same reasoning as pe_regression.py's
        min_peers). Below this, returns the original `momentum` series
        unchanged (documented fallback, never crashes or fabricates a
        residual from too little data).

    Returns
    -------
    pd.Series
        ticker -> residual momentum (same index as the tickers that had
        all three inputs present). Callers combine this with the original
        `momentum` series (e.g. via .combine_first) if they want tickers
        missing market_cap/beta to still rank on raw momentum rather than
        being dropped entirely.
    """
    df = pd.DataFrame({"momentum": momentum, "market_cap": market_cap, "beta": beta}).dropna()
    df = df[df["market_cap"] > 0]
    if len(df) < min_observations:
        return momentum

    y = df["momentum"].to_numpy(dtype=float)
    log_mcap = np.log(df["market_cap"].to_numpy(dtype=float))
    beta_arr = df["beta"].to_numpy(dtype=float)
    x = np.column_stack([np.ones(len(df)), log_mcap, beta_arr])

    try:
        coef, _, _, _ = np.linalg.lstsq(x, y, rcond=None)
    except np.linalg.LinAlgError:
        return momentum

    residuals = y - x @ coef
    return pd.Series(residuals, index=df.index)
