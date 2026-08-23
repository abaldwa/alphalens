"""
scripts/backfill_nifty_regime.py

Backfill Nifty 50 regime feature to feature_store/hybrid/nifty_regime/.

Usage:
    python3 scripts/backfill_nifty_regime.py [--start-date 2009-01-01] [--end-date 2026-08-20]

Outputs:
    feature_store/hybrid/nifty_regime/YYYY/MM/nifty_regime_YYYY-MM-DD.parquet (daily files)
    One row per trading day, columns: [date, close, ema_5, ema_10, rsi_14, regime, exposure, ema_crossover]

Notes:
    - Loads full Nifty 50 OHLCV history from DuckDB
    - Computes regime for all dates
    - Partitions by year/month for efficient loading
    - Validates output before writing
"""

import argparse
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from tqdm import tqdm

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from features.nifty_regime import compute_nifty_regime, validate_regime

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

FEATURE_STORE_PATH = Path('feature_store/hybrid/nifty_regime')


def fetch_nifty_ohlcv(start_date: date, end_date: date, db_path: Path) -> pd.DataFrame:
    """
    Fetch Nifty 50 OHLCV data from DuckDB.

    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        db_path: Path to DuckDB file

    Returns:
        DataFrame with columns [date, open, high, low, close, volume]
        Sorted by date ascending.
    """
    logger.info(f"Fetching Nifty 50 OHLCV from {start_date} to {end_date}...")

    with get_duckdb_connection(db_path, read_only=True, persist=False) as conn:
        query = """
            SELECT
                date,
                open,
                high,
                low,
                close,
                volume
            FROM index_ohlcv
            WHERE index_name = 'Nifty 50'
              AND date >= ?
              AND date <= ?
            ORDER BY date ASC
        """
        df = conn.execute(query, [start_date, end_date]).fetchdf()

    logger.info(f"Fetched {len(df)} rows")
    return df


def backfill_nifty_regime(
    start_date: date = date(2009, 1, 1),
    end_date: Optional[date] = None,
    db_path: Path = DUCKDB_PATH,
) -> None:
    """
    Compute and backfill Nifty 50 regime feature.

    Args:
        start_date: Start date for backfill (default 2009-01-01)
        end_date: End date for backfill (default today)
        db_path: Path to DuckDB file
    """
    if end_date is None:
        end_date = date.today()

    logger.info(f"Backfilling Nifty 50 regime from {start_date} to {end_date}")

    # Create output directory
    FEATURE_STORE_PATH.mkdir(parents=True, exist_ok=True)

    # Fetch Nifty 50 OHLCV
    nifty_ohlcv = fetch_nifty_ohlcv(start_date, end_date, db_path)

    if nifty_ohlcv.empty:
        logger.error(f"No Nifty 50 data found for {start_date} to {end_date}")
        return

    # Compute regime
    logger.info("Computing regime...")
    regime_df = compute_nifty_regime(nifty_ohlcv)

    # Validate
    is_valid, msg = validate_regime(regime_df)
    if not is_valid:
        logger.error(f"Regime validation failed: {msg}")
        return
    logger.info(f"Regime validation passed: {msg}")

    # Write partitions (year/month)
    logger.info("Writing Parquet partitions...")
    regime_df['year'] = pd.to_datetime(regime_df['date']).dt.year
    regime_df['month'] = pd.to_datetime(regime_df['date']).dt.month

    total = regime_df.groupby(['year', 'month']).ngroups
    for (year, month), group in tqdm(regime_df.groupby(['year', 'month']), total=total):
        partition_dir = FEATURE_STORE_PATH / str(year) / f"{month:02d}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Aggregate all dates in this month into a single file
        # (one row per date, but multiple dates per file)
        output_file = partition_dir / f"nifty_regime_{year}-{month:02d}.parquet"

        # Drop year/month cols, keep only regime data
        output_data = group[[
            'date', 'close', 'ema_5', 'ema_10', 'rsi_14',
            'regime', 'exposure', 'ema_crossover'
        ]]

        output_data.to_parquet(output_file, index=False)
        logger.info(f"Wrote {len(output_data)} rows to {output_file}")

    logger.info(f"Backfill complete: {len(regime_df)} total rows written")
    logger.info(f"Feature store path: {FEATURE_STORE_PATH}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backfill Nifty 50 regime feature')
    parser.add_argument(
        '--start-date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        default=date(2009, 1, 1),
        help='Start date (YYYY-MM-DD, default 2009-01-01)'
    )
    parser.add_argument(
        '--end-date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        default=None,
        help='End date (YYYY-MM-DD, default today)'
    )
    parser.add_argument(
        '--db-path',
        type=Path,
        default=DUCKDB_PATH,
        help=f'Path to DuckDB file (default {DUCKDB_PATH})'
    )

    args = parser.parse_args()
    backfill_nifty_regime(args.start_date, args.end_date, args.db_path)
