"""
backfill_momentum_rankings.py (rebalance-date optimized)

Pre-computes momentum rankings ONLY for rebalance dates (every N days).
Huge speedup: ~20x faster by computing for ~200 dates instead of ~4335.
"""

import argparse
import logging
from datetime import date, datetime
from pathlib import Path
from typing import List, Set

import pandas as pd

from datastore.api.db import get_duckdb_connection
from features.momentum_signal import lookback_trading_days
from features.momentum_universe import RANK_BANDS
from strategies.migrations.momentum import variant_name

logger = logging.getLogger(__name__)
REPO = Path(__file__).resolve().parents[1]


def get_rebalance_dates(
    all_dates: List[date],
    rebalance_days: int,
    start_date: date,
) -> Set[date]:
    """Get dates that are rebalance dates for a given cadence."""
    rebalance_set = set()
    for i, d in enumerate(all_dates):
        if d >= start_date and i % rebalance_days == 0:
            rebalance_set.add(d)
    return rebalance_set


def backfill_momentum_rankings(
    bands: List[int],
    lookbacks: List[int],
    rebalances: List[int],
    top_ns: List[int],
    start_date: date,
    end_date: date,
    batch_size: int = 10000,
) -> int:
    """Backfill momentum_rankings for rebalance dates only."""

    with get_duckdb_connection(REPO / "datastore" / "normalised" / "alphalens.duckdb", read_only=False) as conn:
        # Get all tickers and trading dates
        tickers_df = conn.execute("SELECT DISTINCT ticker FROM ohlcv_adjusted ORDER BY ticker").fetch_df()
        all_tickers = tickers_df['ticker'].tolist()

        dates_df = conn.execute(
            "SELECT DISTINCT date FROM ohlcv_adjusted WHERE date >= ? AND date <= ? ORDER BY date",
            [start_date, end_date]
        ).fetch_df()
        all_trading_dates = [pd.to_datetime(d).date() for d in dates_df['date']]

        # Compute rebalance dates for each cadence
        rebalance_dates_by_cadence = {}
        for rebalance_days in rebalances:
            rebalance_dates_by_cadence[rebalance_days] = get_rebalance_dates(all_trading_dates, rebalance_days, start_date)

        total_rebalance_dates = len(set().union(*rebalance_dates_by_cadence.values()))
        logger.info(f"Backfilling {total_rebalance_dates} rebalance dates (across {rebalances}) "
                   f"for {len(bands)} bands × {len(lookbacks)} lookbacks × {len(top_ns)} top_n")
        logger.info(f"Total tickers: {len(all_tickers)} | Total trading dates: {len(all_trading_dates)}")

        # Load all OHLCV data once
        logger.info("Loading OHLCV data...")
        ohlcv_df = conn.execute(
            "SELECT date, ticker, close FROM ohlcv_adjusted WHERE date >= ? AND date <= ? ORDER BY date, ticker",
            [start_date, end_date]
        ).fetch_df()
        logger.info(f"Loaded {len(ohlcv_df)} OHLCV rows")

        # Pivot to price panel
        price_panel = ohlcv_df.pivot(index='date', columns='ticker', values='close')
        price_panel.index = pd.to_datetime(price_panel.index).date
        price_panel_dates = price_panel.index.tolist()

        logger.info(f"Price panel: {price_panel.shape[0]} dates × {price_panel.shape[1]} tickers")

        total_rows = 0
        rows_batch = []
        processed_count = 0

        # Process only rebalance dates
        all_rebalance_dates = sorted(set().union(*rebalance_dates_by_cadence.values()))

        for trading_date in all_rebalance_dates:
            processed_count += 1
            if processed_count % 50 == 0:
                logger.info(f"  Processed {processed_count}/{len(all_rebalance_dates)} rebalance dates")

            try:
                current_idx = price_panel_dates.index(trading_date)
            except ValueError:
                continue

            for band_id in bands:
                band_entry = next((b for b in RANK_BANDS if b[0] == band_id), None)
                if not band_entry:
                    continue

                rank_start, rank_end = band_entry[1], band_entry[2]
                band_tickers = all_tickers

                for lookback_months in lookbacks:
                    lookback_days = lookback_trading_days(lookback_months)
                    start_idx = max(0, current_idx - lookback_days)

                    if current_idx <= start_idx:
                        continue

                    # Compute momentum returns
                    start_prices = price_panel.iloc[start_idx]
                    end_prices = price_panel.iloc[current_idx]
                    momentum_returns = ((end_prices / start_prices) - 1.0)
                    momentum_returns = momentum_returns.dropna()
                    momentum_returns = momentum_returns[[t for t in momentum_returns.index if t in band_tickers]]

                    if momentum_returns.empty:
                        continue

                    ranked = momentum_returns.rank(method="min", ascending=False)

                    # Determine which rebalance cadences apply to this date
                    applicable_cadences = [r for r in rebalances if trading_date in rebalance_dates_by_cadence[r]]

                    for rebalance_days in applicable_cadences:
                        rebalance_str = {5: "5d", 10: "10d", 21: "21d", 63: "63d"}.get(rebalance_days, f"{rebalance_days}d")

                        for top_n in top_ns:
                            variant = variant_name(
                                category="all_risk",
                                band_id=band_id,
                                rank_start=rank_start,
                                rank_end=rank_end,
                                lookback_months=lookback_months,
                                rebalance=rebalance_str,
                                top_n=top_n,
                            )
                            strategy_id = f"momentum:{variant}"

                            # Generate rows
                            for ticker in momentum_returns.index:
                                momentum_return = float(momentum_returns[ticker])
                                momentum_rank = int(ranked[ticker])
                                in_top = momentum_rank <= top_n

                                rows_batch.append({
                                    "date": trading_date,
                                    "strategy_id": strategy_id,
                                    "ticker": ticker,
                                    "momentum_return": momentum_return,
                                    "momentum_rank": momentum_rank,
                                    "in_top_n": in_top,
                                    "band_id": band_id,
                                })

                                if len(rows_batch) >= batch_size:
                                    df_batch = pd.DataFrame(rows_batch)  # noqa: F841
                                    conn.execute(
                                        """INSERT INTO momentum_rankings
                                           (date, strategy_id, ticker, momentum_return, momentum_rank, in_top_n, band_id)
                                           SELECT * FROM df_batch
                                           ON CONFLICT (date, strategy_id, ticker) DO UPDATE SET
                                             momentum_return = EXCLUDED.momentum_return,
                                             momentum_rank = EXCLUDED.momentum_rank,
                                             in_top_n = EXCLUDED.in_top_n"""
                                    )
                                    total_rows += len(rows_batch)
                                    rows_batch = []

        # Final batch
        if rows_batch:
            df_final = pd.DataFrame(rows_batch)  # noqa: F841
            conn.execute(
                """INSERT INTO momentum_rankings
                   (date, strategy_id, ticker, momentum_return, momentum_rank, in_top_n, band_id)
                   SELECT * FROM df_final
                   ON CONFLICT (date, strategy_id, ticker) DO UPDATE SET
                     momentum_return = EXCLUDED.momentum_return,
                     momentum_rank = EXCLUDED.momentum_rank,
                     in_top_n = EXCLUDED.in_top_n"""
            )
            total_rows += len(rows_batch)

    logger.info(f"Backfill COMPLETE: {total_rows} rows inserted")
    return total_rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bands", default="2,4,7,9,10,12", help="Band IDs (comma-separated)")
    ap.add_argument("--lookbacks", default="3,6,9,12", help="Lookback months")
    ap.add_argument("--rebalances", default="5,10,21", help="Rebalance days")
    ap.add_argument("--top-n", default="7,10,15", help="Top-N values")
    ap.add_argument("--start-date", default="2009-01-01", help="Start date (YYYY-MM-DD)")
    ap.add_argument("--end-date", default="2026-06-30", help="End date (YYYY-MM-DD)")
    ap.add_argument("--batch-size", type=int, default=10000, help="Insert batch size")

    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    bands = [int(x) for x in args.bands.split(",")]
    lookbacks = [int(x) for x in args.lookbacks.split(",")]
    rebalances = [int(x) for x in args.rebalances.split(",")]
    top_ns = [int(x) for x in args.top_n.split(",")]
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    backfill_momentum_rankings(bands, lookbacks, rebalances, top_ns, start_date, end_date, args.batch_size)


if __name__ == "__main__":
    main()
