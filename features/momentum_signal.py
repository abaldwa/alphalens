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
from typing import Any, List, Optional

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

    def _return(group: pd.DataFrame) -> Optional[float]:
        if len(group) < lookback_days + 1:
            return None
        start_close = group.iloc[0]["close"]
        end_close = group.iloc[-1]["close"]
        if start_close is None or start_close == 0:
            return None
        ret: float = (end_close / start_close) - 1.0
        return ret

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


def build_momentum_panel(price_panel: pd.DataFrame, lookback_days: int) -> pd.DataFrame:
    """Precompute trailing momentum for ALL (date, ticker) pairs — a vectorised
    rolling operation that replaces per-rebalance ``trailing_momentum_from_panel()``
    calls with an O(1) ``.loc`` lookup.  Same semantics: a ticker missing either
    endpoint (or with a zero start price) is NaN; the first ``lookback_days``
    rows are all-NaN (insufficient history), matching the empty-Series branch
    that ``trailing_momentum_from_panel`` returns for early dates.

    Returns
    -------
    pd.DataFrame, same shape as *price_panel*, where each cell is
    ``price[ticker, date] / price[ticker, date - lookback_days] - 1``."""
    if price_panel.empty or lookback_days < 1:
        return pd.DataFrame()
    shift = price_panel.shift(lookback_days)
    # Replace zero starts with NaN (div-by-zero → inf, then masked below)
    safe_start = shift.replace(0, np.nan)
    momentum = (price_panel / safe_start) - 1.0
    momentum = momentum.replace([np.inf, -np.inf], np.nan)
    return momentum


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


def trailing_momentum_skip_recent(
    price_panel: pd.DataFrame, tickers: List[str], as_of_date: str,
    total_lookback_days: int, skip_days: int,
) -> pd.Series:
    """
    Trailing momentum computed over (total_lookback_days - skip_days), skipping
    the most recent skip_days of data. Implements Jegadeesh-Titman style
    formation-holding period filters (e.g., 12-7: 12-month lookback, skip 1 month).

    Returns a pd.Series indexed by ticker with the skip-adjusted momentum return.
    Tickers with insufficient history (even after skipping) are excluded.
    """
    if price_panel.empty or not tickers or total_lookback_days <= skip_days:
        return pd.Series(dtype=float)

    as_of_ts = pd.Timestamp(as_of_date)
    available_dates = price_panel.index[price_panel.index <= as_of_ts]

    # Need (skip_days + adjusted_lookback_days + 1) data points
    total_days_needed = total_lookback_days + 1
    if len(available_dates) < total_days_needed:
        return pd.Series(dtype=float)

    # end_date = skip_days back from as_of_date
    end_idx = len(available_dates) - skip_days - 1
    if end_idx < 0:
        return pd.Series(dtype=float)
    end_date = available_dates[end_idx]

    # start_date = total_lookback_days before end_date
    start_idx = end_idx - total_lookback_days
    if start_idx < 0:
        return pd.Series(dtype=float)
    start_date = available_dates[start_idx]

    valid_tickers = [t for t in tickers if t in price_panel.columns]
    if not valid_tickers:
        return pd.Series(dtype=float)

    start_prices = price_panel.loc[start_date, valid_tickers]
    end_prices = price_panel.loc[end_date, valid_tickers]
    valid = start_prices.notna() & end_prices.notna() & (start_prices != 0)
    returns = (end_prices[valid] / start_prices[valid]) - 1.0
    return returns.astype(float)


def pct_of_52wk_high(
    price_panel: pd.DataFrame, tickers: List[str], as_of_date: str,
    lookback_days: int = 252,
) -> pd.Series:
    """
    52-week-high signal: current price as a percentage of the rolling high
    over the lookback window (default 252 trading days ≈ 1 year). Returns
    a score from 0 (at 52-week low) to 1.0+ (at or above 52-week high);
    scores near 1.0 indicate a ticker near its peak, suggesting momentum.

    Per spec 7.5, this is a pure momentum signal independent of conventional
    trailing-return ranking — captures price strength/trend-following behavior
    without explicit return calculation.

    Returns a pd.Series indexed by ticker with pct-of-52wk-high scores.
    Tickers with insufficient history or missing data are excluded.
    """
    if price_panel.empty or not tickers or lookback_days < 1:
        return pd.Series(dtype=float)

    as_of_ts = pd.Timestamp(as_of_date)
    available_dates = price_panel.index[price_panel.index <= as_of_ts]

    if len(available_dates) < lookback_days + 1:
        return pd.Series(dtype=float)

    # Window: from (lookback_days back) to (as_of_date)
    end_idx = len(available_dates) - 1
    start_idx = end_idx - lookback_days
    if start_idx < 0:
        return pd.Series(dtype=float)

    end_date = available_dates[end_idx]
    start_date = available_dates[start_idx]

    valid_tickers = [t for t in tickers if t in price_panel.columns]
    if not valid_tickers:
        return pd.Series(dtype=float)

    # Extract the window [start_date, end_date] for each ticker
    window = price_panel.loc[start_date:end_date, valid_tickers]
    if window.empty:
        return pd.Series(dtype=float)

    # Current price (most recent close, as_of_date)
    current_price = price_panel.loc[end_date, valid_tickers]

    # 52-week high within the window
    high_52wk = window.max()

    # Percent of 52-week high: current / high (scores > 1.0 if current > peak)
    scores = current_price / high_52wk

    # Exclude tickers missing current price or high
    valid = current_price.notna() & high_52wk.notna() & (high_52wk != 0)
    return scores[valid].astype(float)


def downtrend_tickers(
    price_panel: pd.DataFrame, tickers: List[str], as_of_date: str,
    downtrend_filter_pct: Optional[float], lookback_days: int = 20,
) -> List[str]:
    """Tickers down by at least `downtrend_filter_pct` over the trailing
    window — the ones a buy must not be opened into.

    Lives here, beside trailing_momentum_from_panel, because it is a
    threshold TEST on the momentum primitive, not a ranking. That
    distinction is enforced: tests/quality/test_one_generator_per_channel.py
    treats any call to trailing_momentum_from_panel from inside backtest/
    as a second momentum generator, and it is right to — a filter module
    that re-derives momentum is one refactor away from re-deriving the
    selection. Callers in backtest/ import this instead.

    A ticker with no history over the window stays eligible: missing data
    is not evidence of a downtrend, and excluding on it would silently
    shrink the candidate set for exactly the names least covered.
    """
    if downtrend_filter_pct is None or price_panel is None or not len(tickers):
        return []
    short_term = trailing_momentum_from_panel(price_panel, list(tickers), as_of_date, lookback_days)
    if short_term.empty:
        return []
    return list(short_term[short_term <= -downtrend_filter_pct].index)


def top_n_by_momentum(
    normalised_conn: Any, tickers: List[str], as_of_date: str, lookback_days: int, top_n: int
) -> List[str]:
    """Top-`top_n` tickers by trailing momentum as of as_of_date, out of
    `tickers`. Fewer than top_n returned if fewer tickers have enough
    history — never padded/guessed."""
    momentum = trailing_momentum(normalised_conn, tickers, as_of_date, lookback_days)
    if momentum.empty:
        return []
    top: List[str] = momentum.sort_values(ascending=False).head(top_n).index.tolist()
    return top


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


def risk_adjusted_momentum_score(
    price_panel: pd.DataFrame,
    tickers: List[str],
    as_of_date: str,
    volatility_measure: str = "daily_return_stddev",
    use_skip_month: bool = False,
    min_volatility: float = 0.001,
    winsorize_pct: float = 0.05,
) -> pd.Series:
    """
    Risk-adjusted composite momentum combining 12-month and 6-month momentum
    signals, each scaled by its own volatility measure. Implements spec
    section 8: risk_adjusted_composite_momentum.

    The formula is:
        risk_adj_score = (m12 / vol12 + m6 / vol6) / 2

    where m12, m6 are trailing 12-month and 6-month momentum returns, and
    vol12, vol6 are their corresponding volatility measures (either daily
    return std-dev or daily price std-dev).

    Spec 8.4 safeguards:
    - Rejects tickers with insufficient observations (< 126 days for 6mo vol)
    - Enforces volatility floor (default 0.1% daily) to avoid div-by-zero
    - Winsorizes scores at [5th, 95th] percentiles to cap outliers
    - Flags exclusion counts and raw vs winsorized divergence

    Args:
        price_panel: Wide close-price DataFrame (index=date, columns=ticker).
        tickers: List of ticker symbols to score.
        as_of_date: Date string for scoring (inclusive; lookbacks end here).
        volatility_measure: "daily_return_stddev" (default) or "daily_price_stddev".
            - daily_return_stddev: std-dev of daily log-returns (252-day window).
            - daily_price_stddev: std-dev of daily price changes (252-day window).
        use_skip_month: If True, use skip-month lookbacks (12-7, 6-2) instead of
            standard (12mo, 6mo). Phase 2 (R3) only; affects lookback computation.
        min_volatility: Floor volatility (default 0.001 = 0.1%); lower vols clipped.
        winsorize_pct: Winsorization percentile (default 0.05 = 5th/95th).

    Returns
    -------
    pd.Series
        ticker -> risk-adjusted momentum score (higher = stronger signal).
        Tickers with insufficient data, zero/NaN volatility, or extreme scores
        are excluded (never NaN-padded).
    """
    from features.winsorize import winsorize_series

    if price_panel.empty or not tickers:
        return pd.Series(dtype=float)

    as_of_ts = pd.Timestamp(as_of_date)
    available_dates = price_panel.index[price_panel.index <= as_of_ts]

    if len(available_dates) < 252 + 1:
        return pd.Series(dtype=float)

    valid_tickers = [t for t in tickers if t in price_panel.columns]
    if not valid_tickers:
        return pd.Series(dtype=float)

    # Standard lookbacks: 12-month (252 days) and 6-month (126 days)
    # Skip-month variants: 12-7 (245 days) and 6-2 (119 days)
    if use_skip_month:
        lookback_12m = 245  # 12 months - 7 days
        lookback_6m = 119   # 6 months - 2 days
        skip_days_12m = 7
        skip_days_6m = 2
    else:
        lookback_12m = 252
        lookback_6m = 126
        skip_days_12m = 0
        skip_days_6m = 0

    if len(available_dates) < lookback_12m + 1:
        return pd.Series(dtype=float)

    # Compute momentum components
    if use_skip_month:
        m12 = trailing_momentum_skip_recent(
            price_panel, valid_tickers, as_of_date, lookback_12m, skip_days_12m
        )
        m6 = trailing_momentum_skip_recent(
            price_panel, valid_tickers, as_of_date, lookback_6m, skip_days_6m
        )
    else:
        m12 = trailing_momentum_from_panel(
            price_panel, valid_tickers, as_of_date, lookback_12m
        )
        m6 = trailing_momentum_from_panel(
            price_panel, valid_tickers, as_of_date, lookback_6m
        )

    # Compute volatility measures
    if volatility_measure == "daily_return_stddev":
        vol12 = _daily_return_volatility(price_panel, valid_tickers, as_of_date, 252)
        vol6 = _daily_return_volatility(price_panel, valid_tickers, as_of_date, 126)
    elif volatility_measure == "daily_price_stddev":
        vol12 = _daily_price_volatility(price_panel, valid_tickers, as_of_date, 252)
        vol6 = _daily_price_volatility(price_panel, valid_tickers, as_of_date, 126)
    else:
        raise ValueError(f"Unknown volatility_measure: {volatility_measure}")

    # Combine: align on tickers present in all four series
    common_tickers = m12.index.intersection(m6.index).intersection(vol12.index).intersection(vol6.index)
    if len(common_tickers) == 0:
        return pd.Series(dtype=float)

    m12_c = m12.loc[common_tickers]
    m6_c = m6.loc[common_tickers]
    vol12_c = vol12.loc[common_tickers]
    vol6_c = vol6.loc[common_tickers]

    # Apply volatility floor
    vol12_c = vol12_c.clip(lower=min_volatility)
    vol6_c = vol6_c.clip(lower=min_volatility)

    # Compute risk-adjusted scores
    scores = (m12_c / vol12_c + m6_c / vol6_c) / 2.0

    # Winsorize
    scores_winsorized, n_lower, n_upper, n_total = winsorize_series(
        scores, lower_pct=winsorize_pct, upper_pct=(1.0 - winsorize_pct)
    )

    logger.debug(
        f"risk_adjusted_momentum_score: {n_total} tickers scored; "
        f"{n_lower} winsorized at lower, {n_upper} at upper; "
        f"volatility_measure={volatility_measure}"
    )

    return scores_winsorized.astype(float)


def _daily_return_volatility(
    price_panel: pd.DataFrame, tickers: List[str], as_of_date: str, lookback_days: int = 252
) -> pd.Series:
    """
    Daily log-return volatility (std-dev) over the lookback window.
    Used for risk adjustment in risk_adjusted_momentum_score.

    Returns a pd.Series indexed by ticker with annualized volatility estimates.
    Tickers with fewer than 2 valid returns (i.e., < 3 prices) are excluded.
    """
    if price_panel.empty or not tickers or lookback_days < 2:
        return pd.Series(dtype=float)

    as_of_ts = pd.Timestamp(as_of_date)
    available_dates = price_panel.index[price_panel.index <= as_of_ts]

    if len(available_dates) < lookback_days + 1:
        return pd.Series(dtype=float)

    end_idx = len(available_dates) - 1
    start_idx = end_idx - lookback_days
    if start_idx < 0:
        return pd.Series(dtype=float)

    end_date = available_dates[end_idx]
    start_date = available_dates[start_idx]

    valid_tickers = [t for t in tickers if t in price_panel.columns]
    if not valid_tickers:
        return pd.Series(dtype=float)

    # Extract the window
    window = price_panel.loc[start_date:end_date, valid_tickers]
    if window.empty or len(window) < 2:
        return pd.Series(dtype=float)

    # Compute log-returns: ln(price[t] / price[t-1])
    log_returns = np.log(window / window.shift(1))

    # Compute daily std-dev per ticker, annualize by sqrt(252)
    daily_vol = log_returns.std(ddof=1)  # Sample volatility
    annualized_vol = daily_vol * np.sqrt(252)

    # Exclude tickers with NaN or zero volatility
    valid = annualized_vol.notna() & (annualized_vol > 0)
    return annualized_vol[valid].astype(float)


def _daily_price_volatility(
    price_panel: pd.DataFrame, tickers: List[str], as_of_date: str, lookback_days: int = 252
) -> pd.Series:
    """
    Daily price-change volatility (std-dev of absolute price differences)
    over the lookback window. Alternative volatility measure for
    risk_adjusted_momentum_score when volatility_measure="daily_price_stddev".

    Returns a pd.Series indexed by ticker with daily price std-dev (not annualized).
    Tickers with fewer than 2 valid price changes are excluded.
    """
    if price_panel.empty or not tickers or lookback_days < 2:
        return pd.Series(dtype=float)

    as_of_ts = pd.Timestamp(as_of_date)
    available_dates = price_panel.index[price_panel.index <= as_of_ts]

    if len(available_dates) < lookback_days + 1:
        return pd.Series(dtype=float)

    end_idx = len(available_dates) - 1
    start_idx = end_idx - lookback_days
    if start_idx < 0:
        return pd.Series(dtype=float)

    end_date = available_dates[end_idx]
    start_date = available_dates[start_idx]

    valid_tickers = [t for t in tickers if t in price_panel.columns]
    if not valid_tickers:
        return pd.Series(dtype=float)

    # Extract the window
    window = price_panel.loc[start_date:end_date, valid_tickers]
    if window.empty or len(window) < 2:
        return pd.Series(dtype=float)

    # Compute daily price changes: price[t] - price[t-1]
    price_changes = window.diff()

    # Compute daily std-dev per ticker (not annualized)
    daily_vol = price_changes.std(ddof=1)  # Sample volatility

    # Exclude tickers with NaN or zero volatility
    valid = daily_vol.notna() & (daily_vol > 0)
    return daily_vol[valid].astype(float)
