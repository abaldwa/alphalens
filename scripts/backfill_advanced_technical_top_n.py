"""
scripts/backfill_advanced_technical_top_n.py

Phase: 3.1 (Advanced Technical Features) — targeted Top-N ADTV backfill
Owner: Platform / Features
Consumers: backtest/run_orchestrator_backtest.py (Technical Strategies sweep,
           --max-tickers 800), screener templates

Fills the 17 slow `advanced_technical` features (wavelet decomposition,
entropy, fractal dimension, fracdiff, Lyapunov, RQA) that the fast daily
pipeline skips — `ingestion/scheduler/daily_pipeline.py::step_compute_
features` -> `features/matrix_builder.py::build_feature_matrix(
advanced_technical_used_only=True)` leaves every one except `hurst_exp_21d`
NaN (added 2026-08-04). It merges them into the existing per-date feature
parquets for the CURRENT Top-N ADTV universe over a historical date range.

Verified 2026-08-06: all 17 columns are 100% NaN for every date in the last
5 years across the full ~2317-ticker feature store (the "already fully
backfilled under A74" note in backfill_deferred_advanced_technical.py is
stale — those columns were never filled).

Differs from scripts/backfill_deferred_advanced_technical.py in three ways:
  * TICKER-FIRST (all_rows=True, one OHLCV fetch per batch) instead of
    date-by-date — ~600x less OHLCV I/O over a multi-year window (the
    date-by-date script re-fetches the 760-day lookback for every one of
    ~1230 dates).
  * Scoped to get_top_adtv_tickers(n) (default 800) instead of the full
    ~2317-ticker universe. Only the Top-N rows' advanced columns are
    filled; the other rows stay NaN (deliberate — this drives Technical
    Strategies, not ML model training).
  * MEMORY-BOUNDED + CHECKPOINTED: tickers are computed in batches (default
    100), each batch written to a staging parquet and skipped on re-run, so
    a crash (OOM / laptop suspend — see memory notes) loses only the
    in-flight batch. Peak memory is bounded by batch_size x workers, not by
    the whole 5-year window.

Only the 17 deferred columns are merged into each date's parquet; the other
~80 columns and all non-Top-N rows are left byte-for-byte untouched.

Usage
-----
    # Full 5-year Top-800 backfill (background, resumable):
    nohup PYTHONPATH=$PWD .venv/bin/python3 \
        scripts/backfill_advanced_technical_top_n.py \
        --from-date 2021-08-06 --to-date 2026-08-06 \
        --top-n-adtv 800 --ticker-batch-size 100 --workers 2 \
        --staging-dir logs/adv_tech_top800_staging \
        > logs/backfill_advanced_technical_top_n.log 2>&1 &

    # Next 800 (ranks 801..1600 by ADTV), same 5-year window. Must use a
    # DIFFERENT --staging-dir so batch_*.parquet keys don't collide with the
    # first run (which would wrongly skip the second run's batches).
    nohup PYTHONPATH=$PWD .venv/bin/python3 \
        scripts/backfill_advanced_technical_top_n.py \
        --from-date 2021-08-06 --to-date 2026-08-06 \
        --top-n-adtv 1600 --start-rank 800 \
        --ticker-batch-size 100 --workers 2 \
        --staging-dir logs/adv_tech_top_n_staging_800_1600 \
        > logs/backfill_advanced_technical_top_n_800_1600.log 2>&1 &

    # Smoke test (small, single worker):
    .venv/bin/python3 scripts/backfill_advanced_technical_top_n.py \
        --from-date 2026-07-20 --to-date 2026-07-31 \
        --top-n-adtv 20 --ticker-batch-size 20 --workers 1
"""

import argparse
import gc
import logging
import os
import sys
from datetime import date as date_type, datetime, timedelta
from pathlib import Path
from typing import List, Tuple

import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# All advanced_technical columns except hurst_exp_21d — the 17 the fast
# daily path leaves NaN and this script fills back in. Copied verbatim from
# scripts/backfill_deferred_advanced_technical.py so both share one
# definition (drift-safe).
DEFERRED_COLUMNS: List[str] = [
    "wavelet_trend", "wavelet_noise", "wavelet_energy_ratio", "wavelet_regime_signal",
    "hurst_exp_63d",
    "approx_entropy_21d", "sample_entropy_21d", "permutation_entropy_21d",
    "spectral_entropy", "fractal_dimension",
    "fracdiff_d_optimal", "fracdiff_price", "fracdiff_volume",
    "lyapunov_exponent_proxy", "rqa_rec_rate", "time_series_complexity", "nonlinear_trend_strength",
]

DEFAULT_TOP_N = 800
DEFAULT_BATCH_SIZE = 100
DEFAULT_WORKERS = 2
# [2026-08-09] Raised 4 -> 8 for the 2007-2026 PIT-union backfill on a
# 14-core / 14GB host. Each worker holds one batch's OHLCV panel plus its
# derived frame, so peak memory scales with workers x ticker_batch_size,
# NOT with the universe — keep batches small when raising this. 8 is the
# ceiling deliberately: this box has a documented history of systemd-oomd
# kills, and leaving headroom matters more than the last increment of
# throughput.
MAX_WORKERS = 8


# ── Stage 1: compute (module-level worker for spawn-pool picklability) ──────


def _compute_batch(args: Tuple) -> Tuple[str, int, int]:
    """
    Compute the 17 deferred advanced_technical features for one ticker batch
    across the full fetch window (all_rows=True), write the batch's result
    to a staging parquet. Returns (status, batch_index, n_tickers).

    Must be module-level (picklable) for `_run_pool_over_chunks`' spawn pool.
    Each worker opens its OWN DataStoreClient (a client is not picklable
    across spawn) and bulk-fetches OHLCV for the fetch window, filtering to
    this batch's tickers. Peak resident memory is bounded by this batch's
    panel + compute frame, not the whole universe.
    """
    batch_index, tickers, from_date, to_date, staging_dir, snapshot_path = args
    out_path = Path(staging_dir) / f"batch_{batch_index:05d}.parquet"
    if out_path.exists():
        return ("skipped", batch_index, len(tickers))

    # Heavy imports deferred so spawn workers stay lean on import.
    from datastore.client import DataStoreClient
    from features.matrix_builder import LOOKBACK_CALENDAR_DAYS, _fetch_ohlcv_panel
    from features.advanced_technical import compute_advanced_technical_features

    ts_from = pd.Timestamp(from_date)
    ts_to = pd.Timestamp(to_date)
    # Fetch the 760-day warmup before from_date so all_rows=True can fill the
    # first target-date rows (LOOKBACK_CALENDAR_DAYS is matrix_builder's
    # standard lookback, same one the deferred backfill uses).
    fetch_from = (ts_from - pd.Timedelta(days=LOOKBACK_CALENDAR_DAYS)).to_pydatetime()
    fetch_to = ts_to.to_pydatetime()

    if snapshot_path:
        # [OOM FIX 2026-08-10] Every worker used to issue its OWN
        # GET /ohlcv/_bulk for the whole window and materialize the entire
        # full-universe panel before filtering to its 40 tickers. That
        # made peak memory scale with the WINDOW, not with
        # --ticker-batch-size: on a 2007-2026 (21-year) run, 4 concurrent
        # workers each holding a full 21-year panel exhausted a 14GB host
        # in ~90 seconds and the kernel OOM killer took out the desktop
        # session along with the job. Reading the prewarmed snapshot with
        # a pushed-down ticker filter means a worker only ever
        # materializes ITS OWN tickers' rows, so batch size genuinely
        # bounds memory and the bulk fetch happens exactly once (in the
        # parent, before any worker starts).
        panel = pd.read_parquet(
            snapshot_path,
            filters=[("ticker", "in", set(tickers))],
        )
        if not panel.empty:
            panel = panel[(panel["date"] >= ts_from - pd.Timedelta(days=LOOKBACK_CALENDAR_DAYS)) & (panel["date"] <= ts_to)]
    else:
        client = DataStoreClient()
        bulk_panel = None
        loader = getattr(client, "get_ohlcv_bulk", None)
        if callable(loader):
            try:
                bulk_panel = loader(fetch_from, fetch_to)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    f"batch {batch_index}: bulk OHLCV fetch failed, falling back to "
                    f"per-ticker: {exc}"
                )

        panel = _fetch_ohlcv_panel(client, tickers, fetch_from, fetch_to, _bulk_panel=bulk_panel)
        del bulk_panel
    if panel.empty:
        logger.warning(f"batch {batch_index}: no OHLCV for {len(tickers)} tickers — skipping")
        return ("empty", batch_index, len(tickers))

    adv = compute_advanced_technical_features(panel, all_rows=True, used_only=False)
    del panel
    adv = adv[["date", "ticker"] + DEFERRED_COLUMNS]
    # all_rows=True fills every bar with >=16 bars of trailing history,
    # including the warmup window before from_date — keep only the target range.
    adv = adv[adv["date"] >= ts_from].reset_index(drop=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    adv.to_parquet(out_path, index=False)
    del adv
    gc.collect()
    logger.info(
        f"batch {batch_index}: computed {len(tickers)} tickers -> {out_path}"
    )
    return ("done", batch_index, len(tickers))


def run_compute(
    tickers: List[str], from_date: date_type, to_date: date_type,
    batch_size: int, workers: int, staging_dir: str, snapshot_path: str = None,
) -> dict:
    """Chunk `tickers` into batches and compute them (parallel, checkpointed).

    snapshot_path : prewarmed full-window OHLCV Parquet (see
        _prewarm_snapshot). When set, workers read their own tickers'
        rows from it with a pushed-down filter instead of each issuing a
        full-window bulk fetch — the difference between peak memory
        scaling with the WINDOW and scaling with --ticker-batch-size (see
        the OOM note in _compute_batch).
    """
    from features.matrix_builder import _run_pool_over_chunks

    batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    worker_args = [
        (idx, batch_tickers, datetime.combine(from_date, datetime.min.time()),
         datetime.combine(to_date, datetime.min.time()), staging_dir, snapshot_path)
        for idx, batch_tickers in enumerate(batches)
    ]
    logger.info(
        f"Stage 1 (compute): {len(tickers)} tickers in {len(batches)} batches of "
        f"<={batch_size}, workers={workers}, fetch window "
        f"{(pd.Timestamp(from_date) - pd.Timedelta(days=760)).date()}..{to_date}"
    )
    results = _run_pool_over_chunks(_compute_batch, worker_args, workers)
    summary: dict = {"done": 0, "skipped": 0, "empty": 0}
    for status, _idx, _n in results:
        summary[status] = summary.get(status, 0) + 1
    logger.info(f"Stage 1 complete: {summary}")
    return summary


# ── Stage 2: merge (per-date, atomic, idempotent) ───────────────────────────


def _already_covers(parquet_path: Path, top_tickers: List[str]) -> bool:
    """
    True if the date's parquet already has a non-NaN wavelet_trend for ANY of
    the Top-N tickers — proves this date was already backfilled for the
    universe we care about. Reads only the ticker + wavelet_trend columns
    (column pruning via pyarrow) so it stays cheap even on full-universe files.
    """
    if not parquet_path.exists():
        return False
    try:
        df = pd.read_parquet(parquet_path, columns=["ticker", "wavelet_trend"])
    except Exception:
        return False
    sub = df[df["ticker"].isin(top_tickers)]
    if sub.empty:
        return False
    return bool(sub["wavelet_trend"].notna().any())


def _prewarm_snapshot(from_date: date_type, to_date: date_type, snapshot_dir: str) -> str:
    """Fetch the full-window OHLCV panel ONCE, in the parent, before any
    worker starts, and write it as a Parquet partitioned by ticker.

    Written as ONE file SORTED BY TICKER, not as a partitioned dataset.
    Sorting is what makes the filter cheap: each row group then covers a
    contiguous ticker span, so pd.read_parquet(..., filters=[("ticker","in",
    ...)]) prunes by row-group min/max statistics and a worker's resident
    set is proportional to its own batch, not to the 21-year full-universe
    panel. partition_cols=["ticker"] was tried first and fails outright at
    this scale — pyarrow's write_dataset caps concurrently-open files at
    1024 and this universe has ~3,800 tickers ("[Errno 24] Too many open
    files"), and it would also leave thousands of tiny files behind.

    Reuses backtest/core/ohlcv_prewarm.py's already-reviewed read-through
    cache for the fetch itself, so this shares one snapshot with the
    backtest sweep rather than keeping a second copy.
    """
    from backtest.core.ohlcv_prewarm import get_or_fetch_ohlcv_bulk
    from datastore.client import DataStoreClient
    from features.matrix_builder import LOOKBACK_CALENDAR_DAYS

    fetch_from = (pd.Timestamp(from_date) - pd.Timedelta(days=LOOKBACK_CALENDAR_DAYS)).date()
    out_path = Path(snapshot_dir) / f"panel_sorted_{fetch_from.isoformat()}_{to_date.isoformat()}.parquet"
    done_marker = out_path.with_suffix(".parquet.done")
    if done_marker.exists():
        logger.info(f"prewarm: reusing existing sorted panel at {out_path}")
        return str(out_path)

    logger.info(f"prewarm: fetching OHLCV {fetch_from}..{to_date} ONCE (parent process)")
    bulk = get_or_fetch_ohlcv_bulk(
        DataStoreClient(), fetch_from, to_date, Path(snapshot_dir),
    )
    bulk = bulk[(bulk["date"] >= pd.Timestamp(fetch_from)) & (bulk["date"] <= pd.Timestamp(to_date))]
    bulk = bulk.sort_values(["ticker", "date"]).reset_index(drop=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(".parquet.tmp")
    # Modest row groups so a filtered read pulls only the spans it needs;
    # too large and pruning degenerates to reading most of the file.
    bulk.to_parquet(tmp_path, index=False, row_group_size=50_000)
    os.replace(tmp_path, out_path)
    done_marker.write_text(f"{len(bulk)} rows\n")
    logger.info(f"prewarm: wrote {len(bulk)} rows, {bulk['ticker'].nunique()} tickers -> {out_path}")
    del bulk
    gc.collect()
    return str(out_path)


def merge_into_dates(
    master: pd.DataFrame, from_date: date_type, to_date: date_type,
    top_tickers: List[str],
) -> Tuple[int, int]:
    """Merge the master deferred frame into each date's existing parquet.

    Only the 17 deferred columns are replaced; all other columns and all
    non-Top-N rows are left byte-for-byte untouched. Writes atomically via
    a .tmp file + os.replace. Returns (n_updated, n_skipped).
    """
    from config.settings import FEATURES_DAILY_DIR

    d = from_date
    n_updated = n_skipped = 0
    while d <= to_date:
        parquet_path = FEATURES_DAILY_DIR / f"{d.isoformat()}.parquet"
        if not parquet_path.exists():
            d += timedelta(days=1)
            continue
        if _already_covers(parquet_path, top_tickers):
            n_skipped += 1
            d += timedelta(days=1)
            continue

        day = master[master["date"] == pd.Timestamp(d)].drop(columns=["date"])
        existing = pd.read_parquet(parquet_path)
        original_columns = list(existing.columns)
        # ADDITIVE merge: preserve deferred values already present for tickers
        # OUTSIDE this run's slice. The naive drop-then-left-merge would wipe any
        # earlier run's fills (e.g. a second 800-ticker slice clobbers the first).
        # Merge with suffixes and combine_first so we only overwrite where THIS
        # run provides a value; other rows keep whatever they already had.
        merged = existing.merge(day, on="ticker", how="left", suffixes=("", "_new"))
        for col in DEFERRED_COLUMNS:
            new_col = col + "_new"
            merged[col] = merged[new_col].combine_first(merged[col])
            merged = merged.drop(columns=[new_col])
        merged = merged[original_columns]

        tmp_path = parquet_path.with_suffix(".parquet.tmp")
        try:
            merged.to_parquet(tmp_path, index=False)
            os.replace(tmp_path, parquet_path)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
        n_updated += 1
        d += timedelta(days=1)

    return n_updated, n_skipped


def run_merge(
    staging_dir: str, from_date: date_type, to_date: date_type,
    top_tickers: List[str],
) -> Tuple[int, int]:
    """Load all batch staging parquets into one master frame, then merge per-date."""
    from config.settings import FEATURES_DAILY_DIR

    staging = sorted(Path(staging_dir).glob("batch_*.parquet"))
    if not staging:
        raise FileNotFoundError(
            f"no batch_*.parquet staging files found under {staging_dir} — run "
            "Stage 1 (compute) first"
        )
    logger.info(f"Stage 2 (merge): loading {len(staging)} batch staging parquets")
    frames = [pd.read_parquet(p) for p in staging]
    master = pd.concat(frames, ignore_index=True)
    del frames
    gc.collect()
    logger.info(
        f"Stage 2 (merge): master deferred frame {master.shape} "
        f"({master['ticker'].nunique()} tickers x {master['date'].nunique()} dates)"
    )
    n_updated, n_skipped = merge_into_dates(master, from_date, to_date, top_tickers)
    logger.info(
        f"Stage 2 complete: {n_updated} dates updated, {n_skipped} skipped "
        f"(already covered or missing parquet) in {FEATURES_DAILY_DIR}"
    )
    return n_updated, n_skipped


def main() -> None:
    from config.universe import get_top_adtv_tickers

    parser = argparse.ArgumentParser(
        description=(
            "Fill the 17 deferred advanced_technical features for the current "
            "Top-N ADTV universe into existing per-date feature parquets "
            "(ticker-first, memory-bounded, checkpointed)."
        )
    )
    parser.add_argument("--from-date", type=date_type.fromisoformat, required=True)
    parser.add_argument("--to-date", type=date_type.fromisoformat, required=True)
    parser.add_argument("--top-n-adtv", type=int, default=DEFAULT_TOP_N)
    parser.add_argument(
        "--start-rank", type=int, default=0,
        help="0-indexed ADTV-rank offset into the top-n universe to begin at "
             "(e.g. --top-n-adtv 1600 --start-rank 800 fills ranks 801..1600). "
             "Must use a fresh --staging-dir when slicing.",
    )
    parser.add_argument("--ticker-batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument(
        "--staging-dir",
        default="logs/adv_tech_top_n_staging",
        help="Checkpoint dir for per-batch compute results (resumable).",
    )
    parser.add_argument(
        "--stage", choices=["compute", "merge", "both"], default="both",
        help="Which stage(s) to run (default: both).",
    )
    parser.add_argument(
        "--ticker-file", default=None,
        help=(
            "Newline-delimited ticker list to backfill INSTEAD of the current "
            "top-N-by-ADTV snapshot (scripts/build_pit_adtv_universe.py's "
            "--out-union). Required for a point-in-time historical backfill: "
            "get_top_adtv_tickers ranks TODAY's universe, so using it for a "
            "2007-2026 window is survivorship-biased (measured: only 239 of "
            "2007's real top-800 are in today's top-800). Mutually exclusive "
            "with --start-rank, which indexes into the snapshot this replaces."
        ),
    )
    parser.add_argument(
        "--snapshot-dir", default="backtest/cache/ohlcv_snapshots",
        help="Where the one-time prewarmed OHLCV panel lives (shared with the backtest sweep).",
    )
    parser.add_argument(
        "--no-prewarm", action="store_true",
        help=(
            "Skip the shared snapshot and let each worker issue its own full-window "
            "bulk fetch (the pre-2026-08-10 behavior). Only safe for short windows "
            "with 1-2 workers — see the OOM note in _compute_batch."
        ),
    )
    args = parser.parse_args()

    workers = min(max(args.workers, 1), MAX_WORKERS)
    if args.workers > MAX_WORKERS:
        logger.warning(f"--workers {args.workers} > {MAX_WORKERS}; capping to {MAX_WORKERS}")

    if args.from_date > args.to_date:
        parser.error("--from-date must be <= --to-date")

    if args.ticker_file:
        if args.start_rank:
            parser.error("--start-rank indexes the current-ADTV snapshot and is meaningless with --ticker-file")
        tickers = [t.strip() for t in Path(args.ticker_file).read_text().splitlines() if t.strip()]
        if not tickers:
            sys.exit(f"--ticker-file {args.ticker_file} is empty — aborting")
        logger.info(f"Using explicit ticker list: {len(tickers)} tickers from {args.ticker_file}")
    else:
        logger.info(f"Resolving Top-{args.top_n_adtv} ADTV universe (current basis)")
        all_top_n: List[str] = get_top_adtv_tickers(args.top_n_adtv)
        if not all_top_n:
            sys.exit("get_top_adtv_tickers returned an empty universe — aborting")
        if args.start_rank >= len(all_top_n):
            parser.error(
                f"--start-rank {args.start_rank} >= resolved universe size "
                f"{len(all_top_n)} — nothing to do"
            )
        tickers = all_top_n[args.start_rank:]
        logger.info(
            f"ADTV ranks {args.start_rank + 1}..{args.start_rank + len(tickers)} "
            f"({len(tickers)} tickers) resolved"
        )

    if args.stage in ("compute", "both"):
        snapshot_path = None
        if not args.no_prewarm:
            snapshot_path = _prewarm_snapshot(args.from_date, args.to_date, args.snapshot_dir)
        run_compute(
            tickers, args.from_date, args.to_date,
            args.ticker_batch_size, workers, args.staging_dir, snapshot_path,
        )
    if args.stage in ("merge", "both"):
        n_updated, n_skipped = run_merge(
            args.staging_dir, args.from_date, args.to_date, tickers
        )
        logger.info(f"backfill_advanced_technical_top_n complete: {n_updated} updated, {n_skipped} skipped")


if __name__ == "__main__":
    main()
