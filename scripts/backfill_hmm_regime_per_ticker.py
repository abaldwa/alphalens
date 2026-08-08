"""
scripts/backfill_hmm_regime_per_ticker.py

Phase: 2.1 (HMM Redesign — per-ticker walk-forward backfill)
Owner: Platform / Features
Consumers: feature parquets (datastore/features/daily/), R1-R4 screener templates,
           ML training, backtest regime segmentation

Walk-forward historical backfill of the 6 hmm_regime_* feature columns for
individual tickers. Redesigned 2026-08-07 per 6-agent model review:
  - 3 observables (de-redundant): daily_return, realized_vol_10d, volume_ratio_20d
  - 3 states: bearish/sideways/bullish
  - Z-score standardization before EM
  - Label anchoring via real-space mean return ranking

Methodology: mirrors backfill_hmm_regime.py's walk-forward pattern but per-ticker:
  - For each ticker, walk forward through the date range
  - Refit a fresh 3-state HMM every --refit-interval-days on the 760-day
    window through each refit date
  - Decode the following block with that fixed model (PIT-correct, no lookahead)
  - Merge the 6 hmm_regime columns into each date's daily feature parquet

Pattern: follows backfill_advanced_technical_top_n.py for ticker-first
processing, staging parquets, checkpointing, and merge logic.

Usage:
    # Top-100 ADTV, smoke test (single worker):
    PYTHONPATH=$PWD .venv/bin/python3 scripts/backfill_hmm_regime_per_ticker.py \
        --from-date 2016-01-01 --to-date 2026-08-06 \
        --top-n-adtv 100 --ticker-batch-size 50 --workers 1 \
        --staging-dir logs/hmm_regime_per_ticker_staging_0_100

    # Full universe (chained tranches, background):
    nohup PYTHONPATH=$PWD .venv/bin/python3 scripts/backfill_hmm_regime_per_ticker.py \
        --from-date 2016-01-01 --to-date 2026-08-06 \
        --top-n-adtv 2500 --ticker-batch-size 50 --workers 3 \
        --staging-dir logs/hmm_regime_per_ticker_staging > logs/backfill_hmm_per_ticker.log 2>&1 &
"""

import argparse
import gc
import logging
import os
import sys
from datetime import date as date_type, timedelta
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# --- Constants matching regime_detector.py (redesigned 2026-08-07) ---
HMM_REGIME_FEATURES = [
    "hmm_regime", "hmm_regime_prob_bullish", "hmm_regime_prob_bearish",
    "hmm_regime_duration", "hmm_regime_transition", "hmm_regime_stability",
]
OBSERVABLE_COLUMNS = ["daily_return", "realized_vol_10d", "volume_ratio_20d"]
N_STATES = 3
REGIME_RANK_NAMES = {0.0: "bearish", 1.0: "sideways", 2.0: "bullish"}

DEFAULT_REFIT_INTERVAL_DAYS = 28
DEFAULT_LOOKBACK_DAYS = 760  # matrix_builder LOOKBACK_CALENDAR_DAYS
DEFAULT_BATCH_SIZE = 50
DEFAULT_WORKERS = 3
MAX_WORKERS = 8

# ---------------------------------------------------------------------------
# Stage 1: walk-forward per ticker (module-level for spawn-picklability)
# ---------------------------------------------------------------------------

def _walk_forward_one_ticker(
    ticker: str,
    ohlcv_full: pd.DataFrame,
    from_date: str,
    to_date: str,
    refit_interval: int,
    n_restarts: int,
    n_iter: int,
) -> Tuple[str, pd.DataFrame]:
    """
    Walk-forward HMM for one ticker over the full date range.

    For each refit date, fit a 3-state standardized HMM on the 760-day
    window through that date, decode the following block with that model.
    Returns (ticker, long-form DataFrame with date + 6 HMM columns).
    """

    from systems.ml_signal_engine.models.hmm.regime_detector import (
        HMMRegimeDetector,
        compute_hmm_observables,
        MIN_OBSERVATIONS,
    )

    # Compute observables once for the full history
    obs_df = compute_hmm_observables(ohlcv_full)
    obs_df["date"] = pd.to_datetime(obs_df["date"])
    obs_df = obs_df.sort_values("date").reset_index(drop=True)

    # Get all trading dates in range
    all_dates = obs_df["date"].unique()
    from_ts = pd.Timestamp(from_date)
    to_ts = pd.Timestamp(to_date)
    target_dates = all_dates[(all_dates >= from_ts) & (all_dates <= to_ts)]
    if len(target_dates) == 0:
        return (ticker, pd.DataFrame(columns=["date", "ticker"] + HMM_REGIME_FEATURES))

    # Build refit grid (every refit_interval trading days)
    refit_dates = target_dates[::refit_interval]

    results = []
    for i, refit_date in enumerate(refit_dates):
        block_end = refit_dates[i + 1] if i + 1 < len(refit_dates) else target_dates[-1] + pd.Timedelta(days=1)
        block_dates = target_dates[(target_dates >= refit_date) & (target_dates < block_end)]
        if len(block_dates) == 0:
            continue

        # Training window: 760 calendar days through refit_date
        train_start = refit_date - pd.Timedelta(days=DEFAULT_LOOKBACK_DAYS)
        train_window = obs_df[(obs_df["date"] <= refit_date) & (obs_df["date"] >= train_start)]

        if len(train_window) < MIN_OBSERVATIONS:
            # Not enough data — fill block with NaN
            for d in block_dates:
                results.append({
                    "date": d, "ticker": ticker,
                    **{col: np.nan for col in HMM_REGIME_FEATURES},
                })
            continue

        detector = HMMRegimeDetector(n_restarts=n_restarts, n_iter=n_iter)
        try:
            detector.fit(train_window)
        except ValueError:
            # Fit failed — fill block with NaN
            for d in block_dates:
                results.append({
                    "date": d, "ticker": ticker,
                    **{col: np.nan for col in HMM_REGIME_FEATURES},
                })
            continue

        # Decode the block
        decode_window = obs_df[obs_df["date"].isin(block_dates)]
        regimes, probs = detector.predict_regime(decode_window)

        for d, rank, prob_row in zip(decode_window["date"], regimes, probs.itertuples(index=False)):
            if pd.isna(rank):
                results.append({
                    "date": d, "ticker": ticker,
                    **{col: np.nan for col in HMM_REGIME_FEATURES},
                })
            else:
                prob_list = list(prob_row) if isinstance(prob_row, tuple) else [0.0] * N_STATES
                results.append({
                    "date": d, "ticker": ticker,
                    "hmm_regime": float(rank),
                    "hmm_regime_prob_bullish": prob_list[2] if len(prob_list) > 2 else np.nan,
                    "hmm_regime_prob_bearish": prob_list[0] if len(prob_list) > 0 else np.nan,
                    "hmm_regime_stability": max(prob_list) if prob_list else np.nan,
                    "hmm_regime_duration": np.nan,
                    "hmm_regime_transition": np.nan,
                })

    if not results:
        return (ticker, pd.DataFrame(columns=["date", "ticker"] + HMM_REGIME_FEATURES))

    df = pd.DataFrame(results)

    # Compute duration and transition (series-global, after all blocks decoded)
    regime_vals = df["hmm_regime"]
    changed = regime_vals.ne(regime_vals.shift(1)) | regime_vals.isna()
    run_id = changed.cumsum()
    df["hmm_regime_duration"] = regime_vals.groupby(run_id).cumcount() + 1
    df["hmm_regime_duration"] = df["hmm_regime_duration"].where(regime_vals.notna())

    prev_regime = regime_vals.shift(1)
    df["hmm_regime_transition"] = regime_vals.ne(prev_regime) & regime_vals.notna() & prev_regime.notna()
    df["hmm_regime_transition"] = df["hmm_regime_transition"].astype(float).where(regime_vals.notna())

    for col in HMM_REGIME_FEATURES:
        df[col] = df[col].astype(np.float64)

    return (ticker, df)


def _walk_forward_one_ticker_star(args: Tuple) -> Tuple[str, pd.DataFrame]:
    """Pickled-args shim for Pool.imap."""
    return _walk_forward_one_ticker(*args)


def run_compute(
    tickers: List[str], ohlcv_by_ticker: dict, from_date: str, to_date: str,
    batch_size: int, workers: int, staging_dir: Path,
    refit_interval: int, n_restarts: int, n_iter: int,
) -> dict:
    """Process tickers in batches, writing staging parquets. Resumable."""
    batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    summary = {"done": 0, "skipped": 0, "empty": 0, "error": 0}

    for batch_idx, batch_tickers in enumerate(batches):
        out_path = staging_dir / f"batch_{batch_idx:05d}.parquet"
        if out_path.exists():
            summary["skipped"] += len(batch_tickers)
            continue

        logger.info("Batch %d/%d: %d tickers (%s..%s)",
                     batch_idx + 1, len(batches), len(batch_tickers),
                     batch_tickers[0], batch_tickers[-1])

        batch_args = [
            (t, ohlcv_by_ticker[t], from_date, to_date, refit_interval, n_restarts, n_iter)
            for t in batch_tickers if t in ohlcv_by_ticker
        ]

        if not batch_args:
            summary["empty"] += len(batch_tickers)
            continue

        if workers <= 1:
            results = [_walk_forward_one_ticker(*a) for a in batch_args]
        else:
            import multiprocessing
            _blas_env = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")
            import os
            _prev = {v: os.environ.get(v) for v in _blas_env}
            try:
                for v in _blas_env:
                    os.environ[v] = "1"
                ctx = multiprocessing.get_context("spawn")
                with ctx.Pool(processes=workers) as pool:
                    results = list(pool.imap(_walk_forward_one_ticker_star, batch_args))
            finally:
                for v, val in _prev.items():
                    if val is None:
                        os.environ.pop(v, None)
                    else:
                        os.environ[v] = val

        parts = [df for ticker, df in results if not df.empty]
        if parts:
            batch_df = pd.concat(parts, ignore_index=True)
            batch_df.to_parquet(out_path, index=False)
            summary["done"] += len(batch_tickers)
            logger.info("  Batch %d written: %d rows, %d tickers", batch_idx + 1, len(batch_df), len(batch_tickers))
        else:
            summary["empty"] += len(batch_tickers)

        del parts, results
        gc.collect()

    return summary


# ---------------------------------------------------------------------------
# Stage 2: merge into daily parquets (per-date, atomic)
# ---------------------------------------------------------------------------

def _already_covers(parquet_path: Path, tickers: set) -> bool:
    """True if any of the tickers already has non-NaN hmm_regime in this date's parquet."""
    if not parquet_path.exists():
        return False
    try:
        df = pd.read_parquet(parquet_path, columns=["ticker", "hmm_regime"])
    except Exception:
        return False
    sub = df[df["ticker"].isin(tickers)]
    if sub.empty:
        return False
    return bool(sub["hmm_regime"].notna().any())


def merge_into_dates(
    master: pd.DataFrame, from_date: str, to_date: str, tickers: List[str],
) -> Tuple[int, int]:
    """Merge HMM columns into each date's parquet. Overwrites (per user consent)."""
    from config.settings import FEATURES_DAILY_DIR

    d = pd.Timestamp(from_date)
    end = pd.Timestamp(to_date)
    n_updated = n_skipped = 0

    while d <= end:
        parquet_path = FEATURES_DAILY_DIR / f"{d.date().isoformat()}.parquet"
        if not parquet_path.exists():
            d += timedelta(days=1)
            continue

        day = master[master["date"] == d].drop(columns=["date"])
        if day.empty:
            d += timedelta(days=1)
            continue

        existing = pd.read_parquet(parquet_path)
        original_columns = list(existing.columns)

        # Drop existing HMM columns then merge new ones
        merged = existing.drop(columns=[c for c in HMM_REGIME_FEATURES if c in existing.columns], errors="ignore")
        merged = merged.merge(day, on="ticker", how="left")
        merged = merged[original_columns]

        tmp_path = parquet_path.with_suffix(".parquet.tmp")
        try:
            merged.to_parquet(tmp_path, index=False)
            os.replace(tmp_path, parquet_path)
            n_updated += 1
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise

        d += timedelta(days=1)

    return n_updated, n_skipped


def run_merge(
    staging_dir: Path, from_date: str, to_date: str, tickers: List[str],
) -> Tuple[int, int]:
    """Load all staging parquets, merge into daily feature parquets."""
    staging_files = sorted(staging_dir.glob("batch_*.parquet"))
    if not staging_files:
        raise FileNotFoundError(f"No batch_*.parquet in {staging_dir}")

    logger.info("Stage 2: loading %d staging files", len(staging_files))
    frames = [pd.read_parquet(p) for p in staging_files]
    master = pd.concat(frames, ignore_index=True)
    del frames
    gc.collect()

    logger.info("Master frame: %d rows, %d tickers, %d dates",
                len(master), master["ticker"].nunique(), master["date"].nunique())

    n_updated, n_skipped = merge_into_dates(master, from_date, to_date, tickers)
    logger.info("Stage 2: %d dates updated, %d skipped", n_updated, n_skipped)
    return n_updated, n_skipped


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--from-date", default="2016-01-01", help="Start date (default: 2016-01-01)")
    parser.add_argument("--to-date", default=None, help="End date (default: today)")
    parser.add_argument("--top-n-adtv", type=int, default=2500, help="Top N tickers by ADTV")
    parser.add_argument("--start-rank", type=int, default=0, help="0-indexed ADTV rank offset")
    parser.add_argument("--ticker-batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--staging-dir", default="logs/hmm_regime_per_ticker_staging")
    parser.add_argument("--refit-interval", type=int, default=DEFAULT_REFIT_INTERVAL_DAYS)
    parser.add_argument("--n-restarts", type=int, default=5)
    parser.add_argument("--n-iter", type=int, default=200)
    parser.add_argument("--stage", choices=["compute", "merge", "both"], default="both")
    args = parser.parse_args()

    workers = min(max(args.workers, 1), MAX_WORKERS)
    to_date = args.to_date or date_type.today().isoformat()
    staging_dir = Path(args.staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    # Resolve universe
    from config.universe import get_top_adtv_tickers
    logger.info("Resolving Top-%d ADTV universe", args.top_n_adtv)
    all_top_n = get_top_adtv_tickers(args.top_n_adtv)
    if not all_top_n:
        sys.exit("get_top_adtv_tickers returned empty")
    if args.start_rank >= len(all_top_n):
        sys.exit(f"--start-rank {args.start_rank} >= universe size {len(all_top_n)}")
    tickers = all_top_n[args.start_rank:]
    logger.info("ADTV ranks %d..%d (%d tickers)", args.start_rank + 1, args.start_rank + len(tickers), len(tickers))

    # Load OHLCV per ticker
    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection

    logger.info("Loading OHLCV for %d tickers from DuckDB...", len(tickers))
    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        ohlcv_by_ticker = {}
        for t in tickers:
            rows = conn.execute(
                "SELECT date, open, high, low, close, volume FROM ohlcv_adjusted "
                "WHERE ticker = ? ORDER BY date", [t]
            ).fetchall()
            if rows:
                ohlcv_by_ticker[t] = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
                ohlcv_by_ticker[t]["ticker"] = t
    logger.info("Loaded OHLCV for %d tickers", len(ohlcv_by_ticker))

    if args.stage in ("compute", "both"):
        summary = run_compute(
            tickers, ohlcv_by_ticker, args.from_date, to_date,
            args.ticker_batch_size, workers, staging_dir,
            args.refit_interval, args.n_restarts, args.n_iter,
        )
        logger.info("Stage 1 complete: %s", summary)

    if args.stage in ("merge", "both"):
        n_updated, n_skipped = run_merge(staging_dir, args.from_date, to_date, tickers)
        logger.info("backfill_hmm_regime_per_ticker complete: %d updated, %d skipped", n_updated, n_skipped)


if __name__ == "__main__":
    main()
