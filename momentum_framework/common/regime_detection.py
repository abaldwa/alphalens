"""
Regime Detection — R09's Bull/Bear/Choppy classification for
regime_switching_enabled (B-027).

Ported from contracts/regime_detector.py::EnsembleRegimeDetector +
EMARegimeDetector/RSIRegimeDetector/VolatilityRegimeDetector — same three
algorithms, same majority-voting ensemble, unchanged formulas.

TWO deliberate departures from the legacy version, both fixes rather than
simplifications:

1. The legacy adapter's own import path
   (`backtest.core.regime_detection.EnsembleRegimeDetector`) DOES NOT
   EXIST anywhere in the codebase — the real class lives in
   `contracts/regime_detector.py`. That means `regime_switching_enabled`
   silently disabled itself in the legacy system too (caught
   `ImportError`, logged a warning, continued without it) — R09-with-
   regime-switching was never actually running as documented. This port
   fixes that: the class exists and is wired up for real.
2. Legacy's `RegimeDetector.load_index_ohlcv()` hardcodes its own DuckDB
   path and its OWN band->index mapping (`MARKET_CAP_BANDS`, a second,
   INCONSISTENT set of index names from `common/benchmark.py`'s —
   e.g. it maps "nifty_150" to ranks (1,150), not this framework's M4).
   This port takes an already-loaded equity_curve/ohlcv Series instead of
   querying internally, and the caller resolves it via
   common/benchmark.py — the ONE band->benchmark mapping now used
   everywhere (R07 and R09 both), per the user's explicit "index equity
   curve attached to a band, not a strategy" instruction.
"""

import numpy as np
import pandas as pd

REGIMES = ("Bull", "Bear", "Choppy")


def _ema_regime(close: pd.Series, short_span: int = 5, long_span: int = 10) -> pd.Series:
    """Bull: close > ema_short > ema_long. Bear: ema_short <= ema_long. Else Choppy."""
    ema_short = close.ewm(span=short_span, adjust=False).mean()
    ema_long = close.ewm(span=long_span, adjust=False).mean()

    regime = pd.Series("Choppy", index=close.index)
    regime[(close > ema_short) & (ema_short > ema_long)] = "Bull"
    regime[ema_short <= ema_long] = "Bear"
    return regime


def _rsi_regime(close: pd.Series, period: int = 14, overbought: float = 70, oversold: float = 30) -> pd.Series:
    """Bull: RSI > overbought. Bear: RSI < oversold. Else Choppy."""
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rsi = 100 - (100 / (1 + gain / loss))

    regime = pd.Series("Choppy", index=close.index)
    regime[rsi > overbought] = "Bull"
    regime[rsi < oversold] = "Bear"
    regime[rsi.isna()] = "Choppy"
    return regime


def _volatility_regime(close: pd.Series, window: int = 21,
                        high_pct: float = 75, low_pct: float = 25) -> pd.Series:
    """Bull: low realized vol. Bear: high realized vol. Else Choppy."""
    returns = close.pct_change()
    vol = returns.rolling(window=window).std() * np.sqrt(252)
    vol_high = vol.rolling(window=window * 2).quantile(high_pct / 100)
    vol_low = vol.rolling(window=window * 2).quantile(low_pct / 100)

    regime = pd.Series("Choppy", index=close.index)
    valid = vol.notna() & vol_high.notna() & vol_low.notna()
    regime[valid & (vol > vol_high)] = "Bear"
    regime[valid & (vol < vol_low)] = "Bull"
    regime[~valid] = "Choppy"
    return regime


def detect_ensemble_regime(close: pd.Series) -> pd.Series:
    """
    Majority-vote across EMA/RSI/volatility regime classifiers.
    Returns a pd.Series (index=date, values in {"Bull","Bear","Choppy"}).
    No majority among the 3 votes -> "Choppy" (conservative default,
    matches legacy's `get_ensemble_regime()`).
    """
    if close.empty:
        return pd.Series(dtype=object)

    votes = pd.DataFrame({
        "ema": _ema_regime(close),
        "rsi": _rsi_regime(close),
        "vol": _volatility_regime(close),
    })

    def _majority(row: pd.Series) -> str:
        counts = row.value_counts()
        if counts.empty:
            return "Choppy"
        if counts.iloc[0] > len(row) / 2:
            return str(counts.index[0])
        return "Choppy"

    return votes.apply(_majority, axis=1)
