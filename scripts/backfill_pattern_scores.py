#!/usr/bin/env python3
"""
scripts/backfill_pattern_scores.py

Phase: Technical feature backfill (2007-2026)
Owner: Platform / Features
Consumers: per-date feature parquets -> ScreenerEngine (templates T02, T03,
           T18 gate on base_breakout_score / flag_pattern_score)

Fills the 6 `pattern_scores` features across history. Measured 2026-08-10
on the top-800 PIT universe: these columns are ~99-100% populated from
2018 onward but 0% before — the same "backfilled for recent years only"
shape the advanced_technical columns had. base_breakout_score is T18's
entry trigger, so without this those templates match nothing pre-2018 and
would look like failing strategies rather than missing data.

Deliberately a sibling of backfill_advanced_technical_top_n.py rather than
a flag on it: pattern_scores is ~3.8 s/ticker over a 21-year panel vs.
advanced_technical's ~30 s/ticker (and ~2,500 s/ticker with fracdiff), so
it wants its own much shorter run rather than being coupled to a job whose
runtime is dominated by something else. Shares that script's proven
mechanics — prewarmed ticker-sorted OHLCV snapshot, checkpointed per-batch
staging, additive per-date merge.

Usage
-----
    PYTHONPATH=$PWD .venv/bin/python scripts/backfill_pattern_scores.py \
        --from-date 2007-04-01 --to-date 2026-06-30 \
        --ticker-file logs/pit_universe_union.txt \
        --ticker-batch-size 25 --workers 8 \
        --staging-dir logs/pattern_pit_2007_2026_staging
"""

import argparse
import gc
import logging
import sys
from datetime import date as date_type, datetime
from pathlib import Path
from typing import List, Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.backfill_advanced_technical_top_n import (  # noqa: E402
    MAX_WORKERS,
    _prewarm_snapshot,
    merge_into_dates,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _compute_batch(args: Tuple) -> Tuple[str, int, int]:
    """One ticker batch's pattern scores across the full window, staged to
    Parquet. Module-level for spawn-pool picklability, same contract as
    backfill_advanced_technical_top_n._compute_batch."""
    batch_index, tickers, from_date, to_date, staging_dir, snapshot_path = args
    out_path = Path(staging_dir) / f"batch_{batch_index:05d}.parquet"
    if out_path.exists():
        return ("skipped", batch_index, len(tickers))

    from features.matrix_builder import LOOKBACK_CALENDAR_DAYS
    from features.pattern_scores import compute_pattern_scores

    ts_from = pd.Timestamp(from_date)
    ts_to = pd.Timestamp(to_date)
    panel = pd.read_parquet(snapshot_path, filters=[("ticker", "in", set(tickers))])
    if not panel.empty:
        warm_from = ts_from - pd.Timedelta(days=LOOKBACK_CALENDAR_DAYS)
        panel = panel[(panel["date"] >= warm_from) & (panel["date"] <= ts_to)]
    if panel.empty:
        logger.warning(f"batch {batch_index}: no OHLCV for {len(tickers)} tickers — skipping")
        return ("empty", batch_index, len(tickers))

    # all_rows=True is REQUIRED here: the default fills only each ticker's
    # last row (the live daily pipeline's shape), which for a historical
    # backfill would silently leave every date but the most recent NaN.
    scores = compute_pattern_scores(panel, all_rows=True)
    del panel
    scores = scores[scores["date"] >= ts_from].reset_index(drop=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    scores.to_parquet(out_path, index=False)
    n = len(scores)
    del scores
    gc.collect()
    logger.info(f"batch {batch_index}: computed {len(tickers)} tickers, {n} rows -> {out_path}")
    return ("done", batch_index, len(tickers))


def run_compute(
    tickers: List[str], from_date: date_type, to_date: date_type,
    batch_size: int, workers: int, staging_dir: str, snapshot_path: str,
) -> dict:
    from features.matrix_builder import _run_pool_over_chunks

    batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    worker_args = [
        (idx, bt, datetime.combine(from_date, datetime.min.time()),
         datetime.combine(to_date, datetime.min.time()), staging_dir, snapshot_path)
        for idx, bt in enumerate(batches)
    ]
    logger.info(
        f"Stage 1 (pattern scores): {len(tickers)} tickers in {len(batches)} batches "
        f"of <={batch_size}, workers={workers}"
    )
    results = _run_pool_over_chunks(_compute_batch, worker_args, workers)
    summary = {"done": 0, "skipped": 0, "empty": 0}
    for status, _, _ in results:
        summary[status] = summary.get(status, 0) + 1
    logger.info(f"Stage 1 complete: {summary}")
    return summary


def main() -> None:
    from features.pattern_scores import PATTERN_FEATURES

    parser = argparse.ArgumentParser(description="Backfill pattern_scores across history")
    parser.add_argument("--from-date", type=date_type.fromisoformat, required=True)
    parser.add_argument("--to-date", type=date_type.fromisoformat, required=True)
    parser.add_argument("--ticker-file", required=True)
    parser.add_argument("--ticker-batch-size", type=int, default=25)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--staging-dir", default="logs/pattern_pit_staging")
    parser.add_argument("--snapshot-dir", default="backtest/cache/ohlcv_snapshots")
    parser.add_argument("--stage", choices=["compute", "merge", "both"], default="both")
    args = parser.parse_args()

    workers = min(max(args.workers, 1), MAX_WORKERS)
    tickers = [t.strip() for t in Path(args.ticker_file).read_text().splitlines() if t.strip()]
    if not tickers:
        sys.exit(f"--ticker-file {args.ticker_file} is empty — aborting")
    logger.info(f"{len(tickers)} tickers from {args.ticker_file}")

    if args.stage in ("compute", "both"):
        snapshot_path = _prewarm_snapshot(args.from_date, args.to_date, args.snapshot_dir)
        run_compute(
            tickers, args.from_date, args.to_date,
            args.ticker_batch_size, workers, args.staging_dir, snapshot_path,
        )

    if args.stage in ("merge", "both"):
        staging = sorted(Path(args.staging_dir).glob("batch_*.parquet"))
        if not staging:
            sys.exit(f"no staging parquets in {args.staging_dir} — run --stage compute first")
        master = pd.concat([pd.read_parquet(p) for p in staging], ignore_index=True)
        logger.info(f"merge: {len(master)} staged rows across {master['ticker'].nunique()} tickers")
        n_updated, n_skipped = merge_into_dates(
            master, args.from_date, args.to_date, tickers, columns=list(PATTERN_FEATURES),
        )
        logger.info(f"backfill_pattern_scores complete: {n_updated} updated, {n_skipped} skipped")


if __name__ == "__main__":
    main()
