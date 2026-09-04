"""
Crash Regime Detection — Daniel-Moskowitz-style overlay used by R07.

Direct port of features/momentum_signal.py::crash_regime_detector() —
same formula, unchanged, so R07's crash dates match the legacy engine's
exactly. This is the ONE piece of R07 kept as a faithful line-for-line
translation rather than a framework-native rebuild, because the formula
itself (drawdown + elevated rolling vol, both against a benchmark) has no
"shared ranking" analog to unify with — it's a standalone regime signal,
not a stock-ranking signal.
"""

import pandas as pd


def crash_regime_detector(
    equity_curve: pd.Series,
    drawdown_threshold: float = -0.15,
    vol_percentile_threshold: float = 0.75,
    lookback_days: int = 252,
    vol_lookback_days: int = 20,
) -> pd.Series:
    """
    Detects "crash regime" dates when `equity_curve` is in drawdown AND
    its volatility is elevated relative to its own trailing baseline.

    A date enters crash regime when BOTH:
    1. `equity_curve` is within drawdown_threshold from its running peak
       (e.g., -0.15 = down 15% from the highest value seen so far)
    2. Rolling volatility over vol_lookback_days exceeds the
       vol_percentile_threshold percentile of the trailing lookback_days
       volatility distribution

    [Phase 7 fix, 2026-09-02 — carried over from the legacy port] Pass a
    real market-index equity curve (e.g. Nifty 500 level series), not the
    strategy's own P&L — crash regime is a market-wide signal. R07's
    strategy file below only supports this benchmark-driven mode; the
    legacy adapter's self-referential fallback (using the strategy's own
    equity curve when no benchmark was supplied) is NOT ported — the
    legacy code itself documents benchmark_equity as the preferred mode,
    and self-referential mode requires portfolio-value plumbing the
    framework's StrategyAdapter doesn't have yet (see
    docs/CODE_TRACEABILITY.md's R07 row).

    Returns
    -------
    pd.Series (index=date, dtype=bool). Missing/insufficient data -> False
    (never exclude on unknown regime).
    """
    if equity_curve.empty or len(equity_curve) < lookback_days:
        return pd.Series(dtype=bool)

    running_peak = equity_curve.expanding().max()
    drawdown = (equity_curve - running_peak) / running_peak
    in_drawdown = drawdown <= drawdown_threshold

    daily_returns = equity_curve.pct_change()
    rolling_vol = daily_returns.rolling(window=vol_lookback_days, min_periods=vol_lookback_days).std()
    vol_percentile = rolling_vol.rolling(window=lookback_days, min_periods=1).quantile(vol_percentile_threshold)
    elevated_vol = rolling_vol > vol_percentile

    crash_regime = (in_drawdown & elevated_vol).fillna(False)
    return crash_regime.astype(bool)
