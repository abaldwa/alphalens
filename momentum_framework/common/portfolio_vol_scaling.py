"""
Portfolio-Level Volatility Scaling — R08 and R09's exposure multipliers.

Distinct from common/position_weighting.py (R14-R17): those redistribute
a FIXED capital budget across a basket of tickers (per-ticker weights
summing to 1.0). These functions instead scale the WHOLE portfolio's
exposure up or down based on the STRATEGY'S OWN recent realized
volatility (from its equity curve) — a single scalar applied uniformly,
not a basket reallocation. Ported from features/momentum_signal.py
(R08's realized_vol_target_multiplier) and features/volatility_scaling.py
(R09's 4-mode dispatch), same formulas, unchanged.

Every function here takes `equity_curve` (a strategy's own portfolio
value time series, index=date), NOT a cross-section of tickers — hence a
separate module from position_weighting.py despite superficially similar
math (1/vol, 1/vol², target/vol, 1/downside_vol all reappear here too).
"""

from typing import Callable, Dict, Optional
import numpy as np
import pandas as pd

TRADING_DAYS_PER_YEAR = 252


def _rolling_annualized_vol(equity_curve: pd.Series, lookback_days: int,
                             downside_only: bool = False) -> pd.Series:
    """Shared rolling-vol core for every function below."""
    daily_returns = equity_curve.pct_change()
    if downside_only:
        daily_returns = daily_returns.where(daily_returns < 0)
    rolling_vol_daily = daily_returns.rolling(window=lookback_days, min_periods=lookback_days).std()
    return rolling_vol_daily * np.sqrt(TRADING_DAYS_PER_YEAR)


def vol_target_multiplier(
    equity_curve: pd.Series,
    target_vol: float = 0.15,
    lookback_days: int = 126,
    leverage_cap: float = 1.0,
) -> pd.Series:
    """
    R08 (Barroso-Santa-Clara): multiplier = min(target_vol / realized_vol, leverage_cap).
    Insufficient data -> 1.0 (no scaling, never fabricate).
    """
    if equity_curve.empty or len(equity_curve) < lookback_days:
        return pd.Series(1.0, index=equity_curve.index, dtype=float)

    vol = _rolling_annualized_vol(equity_curve, lookback_days)
    vol = vol.replace(0.0, np.nan)
    vol = vol.fillna(vol.mean())
    if vol.isna().all() or (vol <= 0).all():
        return pd.Series(1.0, index=equity_curve.index, dtype=float)

    multiplier = (target_vol / vol).clip(upper=leverage_cap)
    return multiplier.fillna(1.0).astype(float)


def inverse_volatility(equity_curve: pd.Series, lookback_days: int = 126,
                        leverage_cap: Optional[float] = None) -> pd.Series:
    """R09 mode: multiplier ∝ 1/vol. Uncapped by default (Moreira-Muir default)."""
    if equity_curve.empty:
        raise ValueError("equity_curve cannot be empty")
    vol = _rolling_annualized_vol(equity_curve, lookback_days)
    multiplier = 1.0 / vol.replace(0.0, np.nan)
    if leverage_cap is not None:
        multiplier = multiplier.clip(upper=leverage_cap)
    return multiplier.fillna(1.0).clip(lower=0.0).astype(float)


def inverse_variance(equity_curve: pd.Series, lookback_days: int = 126,
                      leverage_cap: Optional[float] = None) -> pd.Series:
    """R09 mode: multiplier ∝ 1/vol²."""
    if equity_curve.empty:
        raise ValueError("equity_curve cannot be empty")
    vol = _rolling_annualized_vol(equity_curve, lookback_days)
    multiplier = 1.0 / (vol.replace(0.0, np.nan) ** 2)
    if leverage_cap is not None:
        multiplier = multiplier.clip(upper=leverage_cap)
    return multiplier.fillna(1.0).clip(lower=0.0).astype(float)


def target_volatility(equity_curve: pd.Series, lookback_days: int = 126,
                       target_vol: float = 0.15,
                       leverage_cap: Optional[float] = None) -> pd.Series:
    """R09 mode: multiplier = target_vol / vol, optionally capped."""
    if equity_curve.empty:
        raise ValueError("equity_curve cannot be empty")
    vol = _rolling_annualized_vol(equity_curve, lookback_days)
    multiplier = target_vol / vol.replace(0.0, np.nan)
    if leverage_cap is not None:
        multiplier = multiplier.clip(upper=leverage_cap)
    return multiplier.fillna(1.0).clip(lower=0.0).astype(float)


def downside_volatility(equity_curve: pd.Series, lookback_days: int = 126,
                         leverage_cap: Optional[float] = None) -> pd.Series:
    """R09 mode: multiplier ∝ 1/downside_vol (Sortino-style, only below-zero days)."""
    if equity_curve.empty:
        raise ValueError("equity_curve cannot be empty")
    vol = _rolling_annualized_vol(equity_curve, lookback_days, downside_only=True)
    multiplier = 1.0 / vol.replace(0.0, np.nan)
    if leverage_cap is not None:
        multiplier = multiplier.clip(upper=leverage_cap)
    return multiplier.fillna(1.0).clip(lower=0.0).astype(float)


#: R09's mode dispatch — mirrors features/volatility_scaling.py::VOL_SCALING_DISPATCH
#: Explicitly typed (rather than inferred) because the 4 functions' exact
#: keyword signatures differ (target_volatility takes an extra target_vol
#: kwarg) — mypy can't infer a uniformly-callable value type otherwise,
#: which broke the **kwargs call site in strategies/r09_mm_volscale.py.
VOL_SCALING_DISPATCH: Dict[str, Callable[..., pd.Series]] = {
    "inverse_volatility": inverse_volatility,
    "inverse_variance": inverse_variance,
    "target_volatility": target_volatility,
    "downside_volatility": downside_volatility,
}
