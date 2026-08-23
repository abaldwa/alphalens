"""
backtest/diagnose_momentum_signal_quality.py

Cross-sectional signal quality diagnostic for momentum strategies.
Measures information coefficient (IC) of plain 12-month and 12-7 skip-month
momentum signals independently, without portfolio construction or transaction
costs, to isolate pure signal predictiveness.

Spec Section: Phase 6 (R6, spec 7.6 reporting requirement).

Usage:
    python3 backtest/diagnose_momentum_signal_quality.py \
        [--start-date 2010-01-01] [--end-date 2025-12-31] \
        [--horizon-days 21] [--samples 60] \
        [--csv-out reports/r6_momentum_12_7_ic_analysis.csv]

Output:
    CSV: per-band IC tables (12mo plain vs 12-7 skip-month vs incremental)
    stdout: summary statistics and interpretations
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backtest.overfit_checks import information_coefficient
from config.settings import DUCKDB_PATH
from features.momentum_signal import (
    load_price_panel,
    trailing_momentum_from_panel,
    trailing_momentum_skip_recent,
)
from features.momentum_universe import rank_band_tickers

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ICMetrics:
    """IC statistics for one band/signal pair."""
    mean_ic: Optional[float] = None
    std_ic: Optional[float] = None
    min_ic: Optional[float] = None
    max_ic: Optional[float] = None
    count_dates: int = 0
    count_positive_dates: int = 0
    ic_ir: Optional[float] = None  # mean / std, the "IC information ratio"

    @classmethod
    def from_ic_series(cls, ic_series: List[float]) -> 'ICMetrics':
        """Build metrics from a list of IC values (one per date)."""
        if not ic_series:
            return cls()
        arr = np.array(ic_series)
        mean_val = float(np.mean(arr))
        std_val = float(np.std(arr))
        return cls(
            mean_ic=mean_val,
            std_ic=std_val,
            min_ic=float(np.min(arr)),
            max_ic=float(np.max(arr)),
            count_dates=len(ic_series),
            count_positive_dates=sum(1 for ic in ic_series if ic > 0),
            ic_ir=mean_val / std_val if std_val > 1e-6 else None,
        )


def load_forward_returns_from_panel(
    panel: pd.DataFrame,
    tickers: List[str],
    as_of_date: str,
    horizon_days: int,
) -> pd.Series:
    """
    Compute forward N-trading-day returns from a loaded price panel.

    Finds the row index of `as_of_date`, then advances `horizon_days` rows
    forward and computes the return. Returns indexed by ticker.
    """
    if panel.empty or as_of_date not in panel.index:
        return pd.Series(dtype=float)

    as_of_idx = panel.index.get_loc(as_of_date)
    fwd_idx = as_of_idx + horizon_days

    if fwd_idx >= len(panel):
        return pd.Series(dtype=float)

    as_of_prices = panel.loc[as_of_date, tickers]
    fwd_date = panel.index[fwd_idx]
    fwd_prices = panel.loc[fwd_date, tickers]

    # Compute returns, exclude tickers missing either endpoint
    valid = as_of_prices.notna() & fwd_prices.notna() & (as_of_prices != 0)
    returns = (fwd_prices[valid] / as_of_prices[valid]) - 1.0
    return returns.astype(float)


def sample_rebalance_dates(
    panel: pd.DataFrame,
    cadence_days: int = 21,  # ~monthly in trading days
    max_samples: int = 60,
) -> List[str]:
    """
    Sample rebalance dates evenly spaced across the panel's date range.

    Simulates monthly rebalance cadence (approx 21 trading days), returns
    `max_samples` evenly-distributed dates.
    """
    all_dates = panel.index.tolist()
    if len(all_dates) <= max_samples:
        return [str(d.date()) for d in all_dates]

    # Sample at cadence intervals, then evenly distribute to max_samples
    cadence_dates = all_dates[::cadence_days]
    if len(cadence_dates) <= max_samples:
        return [str(d.date()) for d in cadence_dates]

    idx = np.linspace(0, len(cadence_dates) - 1, max_samples).astype(int)
    return [str(cadence_dates[i].date()) for i in idx]


def diagnose_band(
    panel: pd.DataFrame,
    band_id: int,
    band_tickers: List[str],
    rebalance_dates: List[str],
    horizon_days: int = 21,
) -> Dict[str, ICMetrics]:
    """
    Compute IC for both 12mo plain and 12-7 skip-month signals on one band.

    Returns:
        {
            '12mo': ICMetrics(...),
            '12_7_skip': ICMetrics(...),
            'incremental': ICMetrics(...),  # IC(12-7) - IC(12mo)
        }
    """
    lookback_days = 252  # 12 months in trading days
    skip_days = 21  # ~1 month skip

    ic_12mo = []
    ic_12_7 = []
    ic_incremental = []
    dates_with_data = 0

    for date_str in rebalance_dates:
        # Compute both signals on the same universe
        scores_12mo = trailing_momentum_from_panel(panel, band_tickers, date_str, lookback_days)
        scores_12_7 = trailing_momentum_skip_recent(panel, band_tickers, date_str, lookback_days, skip_days)
        fwd_ret = load_forward_returns_from_panel(panel, band_tickers, date_str, horizon_days)

        if fwd_ret.empty or (scores_12mo.empty and scores_12_7.empty):
            continue

        ic_12mo_val = information_coefficient(scores_12mo, fwd_ret)
        ic_12_7_val = information_coefficient(scores_12_7, fwd_ret)

        if ic_12mo_val is not None:
            ic_12mo.append(ic_12mo_val)
        if ic_12_7_val is not None:
            ic_12_7.append(ic_12_7_val)
            if ic_12mo_val is not None:
                ic_incremental.append(ic_12_7_val - ic_12mo_val)

        if ic_12mo_val is not None or ic_12_7_val is not None:
            dates_with_data += 1

    logger.info(
        f"Band {band_id}: {dates_with_data} dates with IC data "
        f"(12mo: {len(ic_12mo)}, 12-7: {len(ic_12_7)}, incremental: {len(ic_incremental)})"
    )

    return {
        '12mo': ICMetrics.from_ic_series(ic_12mo),
        '12_7_skip': ICMetrics.from_ic_series(ic_12_7),
        'incremental': ICMetrics.from_ic_series(ic_incremental),
    }


def main() -> None:
    ap = argparse.ArgumentParser(
        description='Momentum signal quality diagnostic (IC analysis)',
    )
    ap.add_argument(
        '--start-date',
        default='2010-01-04',
        help='Start date for price panel (YYYY-MM-DD)',
    )
    ap.add_argument(
        '--end-date',
        default='2025-12-31',
        help='End date for price panel (YYYY-MM-DD)',
    )
    ap.add_argument(
        '--horizon-days',
        type=int,
        default=21,
        help='Forward horizon in trading days',
    )
    ap.add_argument(
        '--samples',
        type=int,
        default=60,
        help='Target number of rebalance date samples',
    )
    ap.add_argument(
        '--csv-out',
        default='backtest/reports/r6_momentum_12_7_ic_analysis.csv',
        help='Output CSV path',
    )
    args = ap.parse_args()

    logger.info(f'Loading price panel: {args.start_date} to {args.end_date}')

    # Connect to DB for loading price panel and rank bands
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    # Validated bands for analysis: Band 1 (large-caps) and Band 9 (mid-caps)
    # RANK_BANDS is list of (rank_start, rank_end, band_id) tuples
    bands_to_analyze = [1, 9]
    rank_start_by_band = {1: 1, 9: 201}  # From RANK_BANDS definitions
    rank_end_by_band = {1: 50, 9: 250}

    # Load tickers for each band (use start date as the as_of_date)
    all_tickers = set()
    for band_id in bands_to_analyze:
        tickers = rank_band_tickers(
            con,
            args.start_date,
            rank_start_by_band[band_id],
            rank_end_by_band[band_id],
            include_delisted=True,
        )
        all_tickers.update(tickers)
        logger.info(f'Band {band_id}: {len(tickers)} tickers')

    panel = load_price_panel(con, list(all_tickers), args.start_date, args.end_date)

    if panel.empty:
        logger.error(f'Price panel is empty for {args.start_date}:{args.end_date}')
        sys.exit(1)

    logger.info(f'Panel shape: {panel.shape} ({len(panel.index)} dates, {len(panel.columns)} tickers)')

    # Sample rebalance dates
    rebalance_dates = sample_rebalance_dates(panel, cadence_days=21, max_samples=args.samples)
    logger.info(f'Sampled {len(rebalance_dates)} rebalance dates')

    # Diagnose each band
    results = {}
    for band_id in bands_to_analyze:
        logger.info(f'\n--- Band {band_id} ---')
        band_tickers = rank_band_tickers(
            con,
            args.start_date,
            rank_start_by_band[band_id],
            rank_end_by_band[band_id],
            include_delisted=True,
        )
        logger.info(f'Tickers in universe: {len(band_tickers)}')

        results[band_id] = diagnose_band(
            panel,
            band_id,
            band_tickers,
            rebalance_dates,
            args.horizon_days,
        )

    # Write results to CSV
    os.makedirs(os.path.dirname(args.csv_out), exist_ok=True)

    rows = []
    for band_id in bands_to_analyze:
        metrics = results[band_id]
        for signal_name in ['12mo', '12_7_skip', 'incremental']:
            m = metrics[signal_name]
            rows.append({
                'band_id': band_id,
                'signal': signal_name,
                'mean_ic': m.mean_ic,
                'std_ic': m.std_ic,
                'min_ic': m.min_ic,
                'max_ic': m.max_ic,
                'ic_ir': m.ic_ir,
                'n_dates': m.count_dates,
                'n_positive_dates': m.count_positive_dates,
                'pct_positive_dates': 100 * m.count_positive_dates / m.count_dates if m.count_dates > 0 else None,
            })

    df_results = pd.DataFrame(rows)
    df_results.to_csv(args.csv_out, index=False)
    logger.info(f'\nResults written to {args.csv_out}')

    # Print summary
    print('\n' + '='*80)
    print('MOMENTUM SIGNAL QUALITY ANALYSIS (IC Diagnostic)')
    print('='*80)
    for band_id in bands_to_analyze:
        print(f'\n--- Band {band_id} ---')
        for signal_name in ['12mo', '12_7_skip', 'incremental']:
            m = results[band_id][signal_name]
            if m.mean_ic is None:
                print(f'  {signal_name:20s}: No IC data')
                continue
            pct_pos = 100 * m.count_positive_dates / m.count_dates if m.count_dates > 0 else 0
            print(f'  {signal_name:20s}: mean_IC={m.mean_ic:7.4f}, '
                  f'std={m.std_ic:6.4f}, IR={m.ic_ir or np.nan:6.2f}, '
                  f'n_dates={m.count_dates:3d}, pct_pos={pct_pos:5.1f}%')

    print('\n' + '='*80)
    print('Interpretation:')
    print('  - mean_IC: average daily cross-sectional correlation')
    print('  - IR (IC_ratio): mean_IC / std_IC, signal consistency measure')
    print('  - incremental IC: IC(12-7) - IC(12mo), measures added value of skip-month')
    print('  - Typical equity momentum IC range: 0.02–0.08 (annual cross-sectional)')
    print('='*80 + '\n')


if __name__ == '__main__':
    main()
