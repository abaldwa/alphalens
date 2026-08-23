"""
features/nifty_regime.py

Nifty 50 regime detection based on EMA(5/10) crossovers and RSI(14).

6-state regime model:
  BULL (100%):           Close > EMA(5) AND EMA(5) > EMA(10) AND RSI(14) > 55
  BULL_WEAK (75%):       Close > EMA(5) AND EMA(5) > EMA(10) AND 50 <= RSI(14) <= 55
  CHOPPY (50%):          Close < EMA(5) AND EMA(5) >= EMA(10) AND RSI(14) >= 45
  CHOPPY_BEARISH (25%):  Close < EMA(5) AND EMA(5) >= EMA(10) AND RSI(14) < 45
  BEAR (0%):             EMA(5) < EMA(10) (regardless of price or RSI)
  UNDEFINED (50%):       Default fallback (missing data, edge cases)

Output: DataFrame with columns [date, close, ema_5, ema_10, rsi_14, regime, exposure, ema_crossover]
"""

import logging
from typing import Tuple

import pandas as pd
import talib

logger = logging.getLogger(__name__)

# Regime thresholds (configurable)
RSI_BULLISH = 55
RSI_WEAK_UPPER = 55
RSI_WEAK_LOWER = 50
RSI_CHOPPY = 45

# Regime exposure levels (matched to position sizing)
REGIME_EXPOSURE = {
    'BULL': 1.0,
    'BULL_WEAK': 0.75,
    'CHOPPY': 0.50,
    'CHOPPY_BEARISH': 0.25,
    'BEAR': 0.0,
    'UNDEFINED': 0.50,
}


def compute_nifty_regime(
    nifty_ohlcv: pd.DataFrame,
    ema_short: int = 5,
    ema_long: int = 10,
    rsi_period: int = 14,
) -> pd.DataFrame:
    """
    Compute Nifty 50 regime state (daily).

    Args:
        nifty_ohlcv: DataFrame with columns [date, open, high, low, close, volume]
                     Must be sorted by date ascending.
        ema_short: Period for short EMA (default 5)
        ema_long: Period for long EMA (default 10)
        rsi_period: Period for RSI calculation (default 14)

    Returns:
        DataFrame with columns:
          - date: Trading date
          - close: Nifty 50 close price
          - ema_5: Short EMA value
          - ema_10: Long EMA value
          - rsi_14: RSI(14) value
          - regime: Regime name (BULL, BULL_WEAK, CHOPPY, CHOPPY_BEARISH, BEAR, UNDEFINED)
          - exposure: Exposure multiplier (1.0, 0.75, 0.50, 0.25, 0.0)
          - ema_crossover: True if EMA(5) crosses below EMA(10) on this date
    """
    if nifty_ohlcv.empty:
        logger.warning("Empty OHLCV input; returning empty regime DataFrame")
        return pd.DataFrame()

    # Ensure required columns exist
    required_cols = ['date', 'close', 'high', 'low']
    missing = [c for c in required_cols if c not in nifty_ohlcv.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Create working copy
    df = nifty_ohlcv[['date', 'close', 'high', 'low']].copy()
    df = df.sort_values('date').reset_index(drop=True)

    # Compute EMAs
    df['ema_5'] = talib.EMA(df['close'].values, timeperiod=ema_short)
    df['ema_10'] = talib.EMA(df['close'].values, timeperiod=ema_long)

    # Compute RSI
    df['rsi_14'] = talib.RSI(df['close'].values, timeperiod=rsi_period)

    # Detect EMA crossover: EMA(5) crosses below EMA(10)
    # This happens when: previous EMA(5) > previous EMA(10), and current EMA(5) <= current EMA(10)
    df['ema_5_prev'] = df['ema_5'].shift(1)
    df['ema_10_prev'] = df['ema_10'].shift(1)
    df['ema_crossover'] = (
        (df['ema_5_prev'] > df['ema_10_prev']) & (df['ema_5'] <= df['ema_10'])
    ).fillna(False)

    # Apply regime logic (6-branch decision tree)
    df['regime'] = df.apply(_assign_regime, axis=1)
    df['exposure'] = df['regime'].map(REGIME_EXPOSURE)

    # Return only required columns
    return df[[
        'date', 'close', 'ema_5', 'ema_10', 'rsi_14',
        'regime', 'exposure', 'ema_crossover'
    ]].copy()


def _assign_regime(row: pd.Series) -> str:
    """
    Assign regime for a single date based on OHLCV + indicators.

    Decision tree:
      1. If EMA(5) crosses below EMA(10) → BEAR
      2. If EMA(5) < EMA(10):
         - If RSI(14) < 45 → CHOPPY_BEARISH
         - Else → CHOPPY
      3. Else (EMA(5) >= EMA(10)):
         - If RSI(14) > 55 → BULL
         - Elif RSI(14) >= 50 → BULL_WEAK
         - Else → CHOPPY  (price below EMA but RSI weak)
      4. Fallback → UNDEFINED

    Args:
        row: Series with keys [close, ema_5, ema_10, rsi_14, ema_crossover]

    Returns:
        Regime name as string
    """
    close = row['close']
    ema_5 = row['ema_5']
    ema_10 = row['ema_10']
    rsi_14 = row['rsi_14']
    ema_cross = row['ema_crossover']

    # Handle NaN values (insufficient data for indicators)
    if pd.isna([close, ema_5, ema_10, rsi_14]).any():
        return 'UNDEFINED'

    # Branch 1: EMA crossover detected (immediate BEAR signal)
    if ema_cross:
        return 'BEAR'

    # Branch 2: EMA(5) below EMA(10) (bearish structure)
    if ema_5 < ema_10:
        if rsi_14 < RSI_CHOPPY:
            return 'CHOPPY_BEARISH'
        else:
            return 'CHOPPY'

    # Branch 3: EMA(5) >= EMA(10) (bullish structure)
    # Distinguish based on price position and RSI
    if close > ema_5 and ema_5 >= ema_10:
        # Price above short EMA + bullish structure
        if rsi_14 > RSI_BULLISH:
            return 'BULL'
        elif rsi_14 >= RSI_WEAK_LOWER:
            return 'BULL_WEAK'
        else:
            # Bullish structure but weak RSI → CHOPPY
            return 'CHOPPY'
    elif close <= ema_5 and ema_5 >= ema_10:
        # Price below short EMA but long EMA still bullish → CHOPPY
        if rsi_14 < RSI_CHOPPY:
            return 'CHOPPY_BEARISH'
        else:
            return 'CHOPPY'

    # Fallback (shouldn't reach here if logic is complete)
    return 'UNDEFINED'


def validate_regime(regime_df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Validate regime DataFrame for correctness.

    Checks:
      - All required columns present
      - No NaN in regime/exposure columns
      - Exposure values are valid (0.0, 0.25, 0.50, 0.75, 1.0)
      - Dates are monotonic increasing
      - No duplicate dates

    Args:
        regime_df: Output from compute_nifty_regime()

    Returns:
        (is_valid, message) tuple
    """
    if regime_df.empty:
        return True, "Empty DataFrame (acceptable for zero data)"

    required_cols = ['date', 'regime', 'exposure', 'ema_5', 'ema_10', 'rsi_14']
    missing = [c for c in required_cols if c not in regime_df.columns]
    if missing:
        return False, f"Missing columns: {missing}"

    # Check for NaN in regime/exposure
    nan_regimes = regime_df['regime'].isna().sum()
    nan_exposure = regime_df['exposure'].isna().sum()
    if nan_regimes > 0 or nan_exposure > 0:
        return False, f"NaN values in regime ({nan_regimes}) or exposure ({nan_exposure})"

    # Check regime values
    valid_regimes = set(REGIME_EXPOSURE.keys())
    invalid = regime_df[~regime_df['regime'].isin(valid_regimes)]
    if not invalid.empty:
        return False, f"Invalid regime values: {invalid['regime'].unique()}"

    # Check exposure values
    valid_exposures = {0.0, 0.25, 0.50, 0.75, 1.0}
    invalid_exp = regime_df[~regime_df['exposure'].isin(valid_exposures)]
    if not invalid_exp.empty:
        return False, f"Invalid exposure values: {invalid_exp['exposure'].unique()}"

    # Check date ordering
    if not regime_df['date'].is_monotonic_increasing:
        return False, "Dates are not monotonically increasing"

    # Check for duplicates
    if regime_df['date'].duplicated().any():
        return False, "Duplicate dates found"

    return True, "Valid"
