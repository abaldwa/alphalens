"""
config/regime_features.py

Registry of available regime types for backtest engine integration.
Each regime defines a loader function that fetches regime data from the feature store
and returns it in a standardized format.

Regimes are applied at trade execution time: strategies generate buy/sell signals,
then the engine applies regime exposure scaling based on strategy config.
"""

from dataclasses import dataclass
from datetime import date as date_type
from pathlib import Path
from typing import Callable, Dict, Optional

import pandas as pd

# Path to feature store (relative to project root)
FEATURE_STORE_PATH = Path('feature_store/hybrid/nifty_regime')


@dataclass(frozen=True)
class RegimeData:
    """Standardized regime output format."""
    regime: str  # BULL, BULL_WEAK, CHOPPY, CHOPPY_BEARISH, BEAR, UNDEFINED
    exposure: float  # 0.0, 0.25, 0.5, 0.75, 1.0
    ema_5: float
    ema_10: float
    rsi_14: float
    date: date_type


def load_ema_rsi_v1_regime(start_date: date_type, end_date: date_type) -> pd.DataFrame:
    """
    Load EMA-RSI v1 regime from feature store.

    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)

    Returns:
        DataFrame with columns [date, regime, exposure, ema_5, ema_10, rsi_14]
        Indexed by date for O(1) lookups.

    Raises:
        FileNotFoundError: If no regime files exist for the date range
    """
    regime_path = Path(FEATURE_STORE_PATH) / "hybrid" / "nifty_regime"

    if not regime_path.exists():
        raise FileNotFoundError(f"Regime feature store not found at {regime_path}")

    # Collect all Parquet files in date range
    dfs = []
    current = start_date
    while current <= end_date:
        year_dir = regime_path / str(current.year) / f"{current.month:02d}"
        parquet_file = year_dir / f"nifty_regime_{current.year}-{current.month:02d}.parquet"

        if parquet_file.exists():
            df = pd.read_parquet(parquet_file)
            # Filter to date range
            df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
            dfs.append(df)

        # Move to next month
        if current.month == 12:
            current = date_type(current.year + 1, 1, 1)
        else:
            current = date_type(current.year, current.month + 1, 1)

    if not dfs:
        raise FileNotFoundError(
            f"No regime data found for {start_date} to {end_date} in {regime_path}"
        )

    regime_df = pd.concat(dfs, ignore_index=True)
    regime_df = regime_df.sort_values('date').reset_index(drop=True)

    return regime_df[['date', 'regime', 'exposure', 'ema_5', 'ema_10', 'rsi_14']].copy()


def get_regime_for_date(regime_df: pd.DataFrame, as_of_date: date_type) -> Optional[RegimeData]:
    """
    Lookup regime for a specific date.

    Args:
        regime_df: DataFrame from load_ema_rsi_v1_regime()
        as_of_date: Lookup date

    Returns:
        RegimeData if date found, None otherwise
    """
    matching_rows = regime_df[regime_df['date'] == as_of_date]
    if matching_rows.empty:
        return None

    row = matching_rows.iloc[0]
    return RegimeData(
        regime=str(row['regime']),
        exposure=float(row['exposure']),
        ema_5=float(row['ema_5']),
        ema_10=float(row['ema_10']),
        rsi_14=float(row['rsi_14']),
        date=row['date'] if hasattr(row['date'], 'date') else row['date'].date(),
    )


# Registry of regime types
# Format: regime_type_name -> loader_function
# New regimes can be added here without modifying engine or strategies
REGIME_REGISTRY: Dict[str, Callable[[date_type, date_type], pd.DataFrame]] = {
    'ema_rsi_v1': load_ema_rsi_v1_regime,
    # Future: 'vix_based_v1': load_vix_regime, 'sector_based_v1': load_sector_regime, etc.
}


def validate_regime_type(regime_type: Optional[str]) -> bool:
    """Check if regime type is registered."""
    if regime_type is None:
        return True  # None means "no regime"
    return regime_type in REGIME_REGISTRY
