"""
features/volatility_scaling.py

Phase 9 (R9): Individual volatility scaling functions for Moreira-Muir framework.

Exports five position-sizing modes as standalone, testable functions:
- baseline: multiplier = 1.0 (neutral; no scaling) — control/reference mode
- inverse_volatility: size ∝ 1/σ (aggressive in low-vol regimes)
- inverse_variance: size ∝ 1/σ² (quadratic penalty; stable across regimes)
- target_volatility: size ∝ target_σ/σ (Barroso-Santa-Clara; R8 logic)
- downside_volatility: size ∝ 1/downside_σ (asymmetric; penalizes downside only)

Each function takes an equity curve and returns per-date exposure multipliers.
All functions share the same rolling volatility computation pipeline.

Also exports five PER-TICKER equivalents (R0) — see the block below
VOL_SCALING_DISPATCH — that compute a size_multiplier per ticker within a
rebalance basket instead of one portfolio-level scalar.
"""

import logging
from datetime import date as date_type
from typing import Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from features.momentum_signal import _daily_return_volatility

logger = logging.getLogger(__name__)

TRADING_DAYS_PER_YEAR = 252


def baseline(
    equity_curve: pd.Series,
    lookback_days: int = 126,
    leverage_cap: Optional[float] = None,
) -> pd.Series:
    """
    Baseline (neutral) volatility scaling: multiplier = 1.0 (no scaling).

    **Characteristics:**
    - No volatility-based position sizing adjustment
    - Pure momentum signal, unmodified by realized volatility
    - Used as control/reference to measure value-add of vol scaling
    - Optimal for: Comparing vol scaling benefit across strategies

    Parameters
    ----------
    equity_curve : pd.Series
        Portfolio value time series (index=date, values=portfolio_value).
    lookback_days : int
        Ignored (included for API consistency).
    leverage_cap : Optional[float]
        Ignored (included for API consistency).

    Returns
    -------
    pd.Series (index=date) with constant multiplier=1.0 for all dates.
    Same length as equity_curve.

    Raises
    ------
    ValueError
        If equity_curve is empty.

    Example
    -------
    >>> baseline_mult = baseline(equity_curve)
    >>> # Compare: vol_adjusted_size = base_size * inverse_volatility(equity_curve)
    >>>           neutral_size = base_size * baseline(equity_curve)  # always base_size
    """
    if equity_curve.empty:
        raise ValueError("equity_curve cannot be empty")

    # Return constant 1.0 multiplier for all dates
    return pd.Series(1.0, index=equity_curve.index, dtype=float)


def _compute_rolling_volatility(
    equity_curve: pd.Series,
    lookback_days: int,
    downside_only: bool = False,
) -> pd.Series:
    """
    Compute rolling realized volatility (annualized).

    Parameters
    ----------
    equity_curve : pd.Series
        Portfolio value time series (index=date, values=portfolio_value).
    lookback_days : int
        Rolling window for volatility computation (e.g., 126 = ~6 months).
    downside_only : bool
        If True, compute semi-deviation (volatility of negative returns only).
        If False, compute standard volatility (all returns).

    Returns
    -------
    pd.Series (index=date) with rolling annualized volatility.
    Insufficient data (< lookback_days) → NaN.
    """
    daily_returns = equity_curve.pct_change()

    if downside_only:
        # Semi-deviation: std of only negative returns
        negative_returns = daily_returns.clip(upper=0.0)
        rolling_vol_daily = negative_returns.rolling(
            window=lookback_days, min_periods=lookback_days
        ).std()
        logger.debug(
            f"Downside volatility: negative_returns stats: "
            f"mean={negative_returns.mean():.6f}, std={negative_returns.std():.6f}"
        )
    else:
        # Standard volatility: all returns
        rolling_vol_daily = daily_returns.rolling(
            window=lookback_days, min_periods=lookback_days
        ).std()

    # Annualize: vol_annual = vol_daily * sqrt(252)
    rolling_vol_annual = rolling_vol_daily * (TRADING_DAYS_PER_YEAR ** 0.5)

    return rolling_vol_annual


def _process_volatility(
    rolling_vol_annual: pd.Series,
    mode_name: str,
) -> pd.Series:
    """
    Post-process rolling volatility: handle zero/NaN, fill gaps.

    Parameters
    ----------
    rolling_vol_annual : pd.Series
        Raw rolling annualized volatility.
    mode_name : str
        Name of scaling mode (for logging).

    Returns
    -------
    pd.Series with cleaned volatility (no zeros or NaNs).
    """
    # Avoid division by zero
    rolling_vol_annual = rolling_vol_annual.replace(0.0, np.nan)
    mean_vol = rolling_vol_annual.mean()
    rolling_vol_annual = rolling_vol_annual.fillna(mean_vol)

    if rolling_vol_annual.isna().all() or (rolling_vol_annual <= 0).all():
        logger.warning(f"{mode_name}: All vol values are NaN or <= 0, returning 1.0 for all dates")
        return None  # Signal to caller to return neutral multipliers

    logger.debug(
        f"{mode_name}: vol_range=[{rolling_vol_annual.min():.6f}, "
        f"{rolling_vol_annual.max():.6f}], mean={rolling_vol_annual.mean():.6f}"
    )

    return rolling_vol_annual


def inverse_volatility(
    equity_curve: pd.Series,
    lookback_days: int = 126,
    leverage_cap: Optional[float] = None,
) -> pd.Series:
    """
    Moreira-Muir inverse volatility scaling: size ∝ 1/σ.

    **Characteristics:**
    - Aggressive in low-vol regimes (high position sizing)
    - Conservative in high-vol regimes (low position sizing)
    - Optimal for: Low baseline volatility + strong momentum (e.g., M7)
    - Fragile in: High baseline volatility (collapses positioning, misses alpha)

    Parameters
    ----------
    equity_curve : pd.Series
        Portfolio value time series (index=date, values=portfolio_value).
    lookback_days : int
        Rolling window for volatility computation (default: 126 = ~6 months).
    leverage_cap : Optional[float]
        Maximum exposure multiplier. If None, uncapped (Moreira-Muir default).
        Typical range: 2.0-3.0 for risk control.

    Returns
    -------
    pd.Series (index=date) with per-date exposure multipliers.
    Insufficient data → 1.0 (neutral).

    Raises
    ------
    ValueError
        If equity_curve is empty.
    """
    if equity_curve.empty:
        raise ValueError("equity_curve cannot be empty")

    rolling_vol_annual = _compute_rolling_volatility(
        equity_curve, lookback_days, downside_only=False
    )
    rolling_vol_annual = _process_volatility(rolling_vol_annual, "inverse_volatility")

    if rolling_vol_annual is None:
        return pd.Series(1.0, index=equity_curve.index, dtype=float)

    # size ∝ 1 / vol
    multiplier = 1.0 / rolling_vol_annual

    if leverage_cap is not None:
        precap_mean = multiplier.mean()
        multiplier = multiplier.clip(upper=leverage_cap)
        logger.info(
            f"inverse_volatility: pre-cap mean={precap_mean:.4f}, "
            f"capped to {leverage_cap}"
        )
    else:
        logger.info(f"inverse_volatility: uncapped multiplier mean={multiplier.mean():.4f}")

    multiplier = multiplier.fillna(1.0).clip(lower=0.0)
    return multiplier.astype(float)


def inverse_variance(
    equity_curve: pd.Series,
    lookback_days: int = 126,
    leverage_cap: Optional[float] = None,
) -> pd.Series:
    """
    Moreira-Muir inverse variance scaling: size ∝ 1/σ².

    **Characteristics:**
    - Quadratic penalty on volatility (smoother than 1/σ)
    - Most stable across different vol regimes
    - Balances aggression in low-vol with conservatism in high-vol
    - Optimal for: Medium baseline volatility + high momentum (e.g., M8)
    - No catastrophic failures in any regime (most robust single mode)

    Parameters
    ----------
    equity_curve : pd.Series
        Portfolio value time series (index=date, values=portfolio_value).
    lookback_days : int
        Rolling window for volatility computation (default: 126 = ~6 months).
    leverage_cap : Optional[float]
        Maximum exposure multiplier. If None, uncapped (Moreira-Muir default).
        Typical range: 2.0-3.0 for risk control.

    Returns
    -------
    pd.Series (index=date) with per-date exposure multipliers.
    Insufficient data → 1.0 (neutral).

    Raises
    ------
    ValueError
        If equity_curve is empty.
    """
    if equity_curve.empty:
        raise ValueError("equity_curve cannot be empty")

    rolling_vol_annual = _compute_rolling_volatility(
        equity_curve, lookback_days, downside_only=False
    )
    rolling_vol_annual = _process_volatility(rolling_vol_annual, "inverse_variance")

    if rolling_vol_annual is None:
        return pd.Series(1.0, index=equity_curve.index, dtype=float)

    # size ∝ 1 / vol²
    multiplier = 1.0 / (rolling_vol_annual ** 2)

    if leverage_cap is not None:
        precap_mean = multiplier.mean()
        multiplier = multiplier.clip(upper=leverage_cap)
        logger.info(
            f"inverse_variance: pre-cap mean={precap_mean:.4f}, capped to {leverage_cap}"
        )
    else:
        logger.info(f"inverse_variance: uncapped multiplier mean={multiplier.mean():.4f}")

    multiplier = multiplier.fillna(1.0).clip(lower=0.0)
    return multiplier.astype(float)


def target_volatility(
    equity_curve: pd.Series,
    target_vol: float = 0.15,
    lookback_days: int = 126,
    leverage_cap: Optional[float] = 1.0,
) -> pd.Series:
    """
    Barroso-Santa-Clara target volatility scaling (R8 logic): size ∝ target_σ/σ.

    **Characteristics:**
    - De-leverages when realized vol > target, re-leverages when < target
    - Conservative by design (cap=1.0 by default; never above neutral)
    - Stabilizes portfolio volatility around target level
    - Optimal for: Risk management (not return maximization)

    Parameters
    ----------
    equity_curve : pd.Series
        Portfolio value time series (index=date, values=portfolio_value).
    target_vol : float
        Target annualized volatility (default: 0.15 = 15%).
    lookback_days : int
        Rolling window for volatility computation (default: 126 = ~6 months).
    leverage_cap : Optional[float]
        Maximum exposure multiplier (default: 1.0, preventing over-leverage).
        Set to None for uncapped (not recommended; defeats risk-management purpose).

    Returns
    -------
    pd.Series (index=date) with per-date exposure multipliers (0.0-1.0 typically).
    Insufficient data → 1.0 (neutral).

    Raises
    ------
    ValueError
        If equity_curve is empty.
    """
    if equity_curve.empty:
        raise ValueError("equity_curve cannot be empty")

    rolling_vol_annual = _compute_rolling_volatility(
        equity_curve, lookback_days, downside_only=False
    )
    rolling_vol_annual = _process_volatility(rolling_vol_annual, "target_volatility")

    if rolling_vol_annual is None:
        return pd.Series(1.0, index=equity_curve.index, dtype=float)

    # size ∝ target_vol / realized_vol
    multiplier = target_vol / rolling_vol_annual

    if leverage_cap is None:
        leverage_cap = 1.0  # R8 default (conservative)
    multiplier = multiplier.clip(upper=leverage_cap)

    logger.info(
        f"target_volatility (target={target_vol:.4f}): "
        f"capped to {leverage_cap}, mean multiplier={multiplier.mean():.4f}"
    )

    multiplier = multiplier.fillna(1.0).clip(lower=0.0)
    return multiplier.astype(float)


def downside_volatility(
    equity_curve: pd.Series,
    lookback_days: int = 126,
    leverage_cap: Optional[float] = None,
) -> pd.Series:
    """
    Moreira-Muir downside volatility scaling: size ∝ 1/downside_σ.

    **Characteristics:**
    - Only penalizes downside volatility (negative returns)
    - Upside swings don't reduce position sizing
    - Optimal for: High baseline vol with upside drift from momentum (e.g., M9)
    - Fragile in: Low-vol regimes with symmetric vol (over-leverages)
    - **Critical:** Often produces 33%+ performance swaps across bands

    Parameters
    ----------
    equity_curve : pd.Series
        Portfolio value time series (index=date, values=portfolio_value).
    lookback_days : int
        Rolling window for volatility computation (default: 126 = ~6 months).
    leverage_cap : Optional[float]
        Maximum exposure multiplier. If None, uncapped (Moreira-Muir default).
        Typical range: 2.0-3.0 for risk control.

    Returns
    -------
    pd.Series (index=date) with per-date exposure multipliers.
    Insufficient data → 1.0 (neutral).

    Raises
    ------
    ValueError
        If equity_curve is empty.

    Notes
    -----
    Downside volatility (semi-deviation) computed as std(negative_returns only).
    This asymmetric approach assumes upside drift (momentum) is valuable, while
    protecting against tail risk. Works well in trending markets; fails in
    mean-reverting or choppy regimes where upside/downside are symmetric.
    """
    if equity_curve.empty:
        raise ValueError("equity_curve cannot be empty")

    rolling_vol_annual = _compute_rolling_volatility(
        equity_curve, lookback_days, downside_only=True
    )
    rolling_vol_annual = _process_volatility(rolling_vol_annual, "downside_volatility")

    if rolling_vol_annual is None:
        return pd.Series(1.0, index=equity_curve.index, dtype=float)

    # size ∝ 1 / downside_vol
    # Downside vol is typically lower than total vol, so multipliers are naturally higher
    multiplier = 1.0 / rolling_vol_annual

    if leverage_cap is not None:
        precap_mean = multiplier.mean()
        multiplier = multiplier.clip(upper=leverage_cap)
        logger.info(
            f"downside_volatility: pre-cap mean={precap_mean:.4f}, "
            f"capped to {leverage_cap}"
        )
    else:
        logger.info(f"downside_volatility: uncapped multiplier mean={multiplier.mean():.4f}")

    multiplier = multiplier.fillna(1.0).clip(lower=0.0)
    return multiplier.astype(float)


# ---------------------------------------------------------------------------
# Per-ticker (R0) weighting: unlike the five functions above, which compute a
# single portfolio-level exposure multiplier from the equity curve, these
# compute a per-ticker size_multiplier from each ticker's OWN price history,
# normalized so the basket's multipliers average to 1.0 — i.e. a pure
# re-weighting of the fixed equal-weight capital budget across the basket,
# never a leverage decision. Called once per rebalance date, in-strategy
# (backtest/adapters/momentum_adapter.py), not precomputed for the full
# date range up front.
# ---------------------------------------------------------------------------

def _normalize_to_basket_mean(raw: pd.Series, mode_name: str) -> pd.Series:
    """
    Normalize raw per-ticker scores so the basket's multipliers average to
    1.0 (same total capital deployed as plain equal-weight). Tickers with
    no raw score (insufficient history) default to 1.0 and are excluded
    from the mean.
    """
    valid = raw.dropna()
    valid = valid[valid > 0]
    if valid.empty:
        logger.warning(f"{mode_name}: no tickers with valid volatility, defaulting basket to 1.0")
        return pd.Series(1.0, index=raw.index, dtype=float)
    mean_raw = valid.mean()
    normalized = (raw / mean_raw).fillna(1.0)
    normalized[raw.isna() | (raw <= 0)] = 1.0
    return normalized.astype(float)


def _as_of_str(as_of_date: Union[str, date_type]) -> str:
    return as_of_date if isinstance(as_of_date, str) else as_of_date.isoformat()


def baseline_per_ticker(
    price_panel: pd.DataFrame,
    tickers: List[str],
    as_of_date: Union[str, date_type],
    lookback_days: int = 126,
) -> pd.Series:
    """
    Control mode: multiplier = 1.0 for every ticker (plain equal-weight).
    lookback_days is ignored — kept only for signature parity with the
    other four per-ticker modes / WEIGHT_DISPATCH.
    """
    return pd.Series(1.0, index=tickers, dtype=float)


def inverse_volatility_per_ticker(
    price_panel: pd.DataFrame,
    tickers: List[str],
    as_of_date: Union[str, date_type],
    lookback_days: int = 126,
) -> pd.Series:
    """size_i ∝ 1 / vol_i, normalized to basket mean = 1.0."""
    vol = _daily_return_volatility(price_panel, tickers, _as_of_str(as_of_date), lookback_days)
    raw = (1.0 / vol).reindex(tickers)
    return _normalize_to_basket_mean(raw, "inverse_volatility_per_ticker")


def inverse_variance_per_ticker(
    price_panel: pd.DataFrame,
    tickers: List[str],
    as_of_date: Union[str, date_type],
    lookback_days: int = 126,
) -> pd.Series:
    """size_i ∝ 1 / vol_i², normalized to basket mean = 1.0."""
    vol = _daily_return_volatility(price_panel, tickers, _as_of_str(as_of_date), lookback_days)
    raw = (1.0 / (vol ** 2)).reindex(tickers)
    return _normalize_to_basket_mean(raw, "inverse_variance_per_ticker")


def target_volatility_per_ticker(
    price_panel: pd.DataFrame,
    tickers: List[str],
    as_of_date: Union[str, date_type],
    target_vol: float = 0.15,
    lookback_days: int = 126,
) -> pd.Series:
    """
    size_i ∝ target_vol / vol_i, normalized to basket mean = 1.0.
    Uncapped at the per-ticker level: a cap here would limit one name's
    share of the fixed capital budget, not total portfolio leverage — the
    R8 leverage_cap=1.0 default doesn't transfer to this basket-relative
    context.
    """
    vol = _daily_return_volatility(price_panel, tickers, _as_of_str(as_of_date), lookback_days)
    raw = (target_vol / vol).reindex(tickers)
    return _normalize_to_basket_mean(raw, "target_volatility_per_ticker")


def downside_volatility_per_ticker(
    price_panel: pd.DataFrame,
    tickers: List[str],
    as_of_date: Union[str, date_type],
    lookback_days: int = 126,
) -> pd.Series:
    """
    size_i ∝ 1 / downside_vol_i (semi-deviation, negative daily returns
    only), normalized to basket mean = 1.0.
    """
    valid_tickers = [t for t in tickers if t in price_panel.columns]
    downside_vol = pd.Series(dtype=float)
    if valid_tickers:
        as_of_ts = pd.Timestamp(_as_of_str(as_of_date))
        available_dates = price_panel.index[price_panel.index <= as_of_ts]
        if len(available_dates) >= lookback_days + 1:
            end_date = available_dates[-1]
            start_date = available_dates[-(lookback_days + 1)]
            window = price_panel.loc[start_date:end_date, valid_tickers]
            log_returns = np.log(window / window.shift(1))
            negative_returns = log_returns.clip(upper=0.0)
            daily_downside_vol = negative_returns.std(ddof=1)
            downside_vol = (daily_downside_vol * (TRADING_DAYS_PER_YEAR ** 0.5))
            downside_vol = downside_vol[downside_vol.notna() & (downside_vol > 0)]
    raw = (1.0 / downside_vol).reindex(tickers)
    return _normalize_to_basket_mean(raw, "downside_volatility_per_ticker")


VOL_SCALING_DISPATCH: Dict[str, Callable[..., pd.Series]] = {
    "baseline": baseline,
    "inverse_volatility": inverse_volatility,
    "inverse_variance": inverse_variance,
    "target_volatility": target_volatility,
    "downside_volatility": downside_volatility,
}

WEIGHT_DISPATCH: Dict[str, Callable[..., pd.Series]] = {
    "baseline": baseline_per_ticker,
    "inverse_volatility": inverse_volatility_per_ticker,
    "inverse_variance": inverse_variance_per_ticker,
    "target_volatility": target_volatility_per_ticker,
    "downside_volatility": downside_volatility_per_ticker,
}
