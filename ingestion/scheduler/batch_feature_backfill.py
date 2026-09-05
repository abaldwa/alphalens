"""
ingestion/scheduler/batch_feature_backfill.py

[2026-09-05] Routes a multi-date compute_features backfill through
scripts/feature_backfill_hybrid.py's ticker-first batch design instead of
running the live, date-first step_compute_features once per missed date.

Why: date-first reloads each ticker's full OHLCV/fundamentals history from
scratch on every call, so its cost scales with (dates x tickers). A real
multi-week scheduler pause (2026-08-14 to 2026-09-05) needed 15 missed
trading days backfilled and the date-first path blew past 90 minutes and
hung (multiprocessing.Pool deadlock) on a single date. The ticker-first
hybrid script loads each ticker's history ONCE and computes every requested
date's features from it in one pass, so cost scales with (tickers) alone —
verified same-day: all 15 dates x 2,317 tickers completed in less time than
date-first took to hang on ONE date.

Consumers: ingestion/scheduler/pipeline_steps.py::run_backfill
"""

import logging
import subprocess
import sys
from datetime import date as date_type
from pathlib import Path
from typing import Dict, List

from ingestion.scheduler.checkpoint import CheckpointManager

logger = logging.getLogger(__name__)

_COMPUTE_FEATURES_STEP = "compute_features"


def run_batch_compute_features(
    gap_dates: List[date_type],
    checkpoint_manager: CheckpointManager,
    workers: int = None,  # type: ignore[assignment]
    timeout_s: int = 4 * 3600,
) -> Dict[date_type, bool]:
    """
    Run scripts/feature_backfill_hybrid.py once, scoped to
    [min(gap_dates), max(gap_dates)], then reconcile the 'compute_features'
    checkpoint for every date in gap_dates against the script's actual
    parquet output — never trust the subprocess's exit code alone, since a
    partial run (e.g. some tickers erroring) can still exit 0.

    Parameters
    ----------
    gap_dates : list of date
        Need not be contiguous trading days — the hybrid script computes
        every trading day in the inclusive range regardless, so any dates
        in that range but NOT in gap_dates simply get (re)computed too;
        harmless, since compute_features is idempotent per date.
    checkpoint_manager : CheckpointManager
    workers : int, optional
        Defaults to config.settings.FEATURE_BACKFILL_HYBRID_WORKERS.
    timeout_s : int
        Hard ceiling on the subprocess. Default 4h — generously above the
        ~15-minutes-for-15-dates observed on a 2,317-ticker universe with
        6 workers, since this runs unattended.

    Returns
    -------
    dict[date, bool]
        True for each gap_date whose compute_features checkpoint was
        marked success; False for failed dates. A date whose parquet is
        missing/empty after the subprocess completes (or the subprocess
        itself failed/timed out) is marked False across the board.
    """
    from config.settings import FEATURE_BACKFILL_HYBRID_WORKERS, FEATURES_DAILY_DIR

    if not gap_dates:
        return {}

    if workers is None:
        workers = FEATURE_BACKFILL_HYBRID_WORKERS

    from_date = min(gap_dates)
    to_date = max(gap_dates)
    script = str(Path(__file__).resolve().parent.parent.parent / "scripts" / "feature_backfill_hybrid.py")

    for d in gap_dates:
        checkpoint_manager.save_checkpoint(d, _COMPUTE_FEATURES_STEP, status="running", is_backfill=True)

    logger.info(
        f"batch_feature_backfill: running feature_backfill_hybrid.py for "
        f"{from_date}..{to_date} ({len(gap_dates)} gap date(s)), workers={workers}"
    )
    cmd = [
        sys.executable, script,
        "--from-date", from_date.isoformat(),
        "--to-date", to_date.isoformat(),
        "--workers", str(workers),
        "--skip-fracdiff",  # matches step_compute_features's own live-path default
        "--force",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
        subprocess_ok = result.returncode == 0
        if not subprocess_ok:
            logger.error(
                f"batch_feature_backfill: feature_backfill_hybrid.py exited "
                f"{result.returncode}: {result.stderr[-3000:]}"
            )
    except subprocess.TimeoutExpired:
        subprocess_ok = False
        logger.error(f"batch_feature_backfill: feature_backfill_hybrid.py timed out after {timeout_s}s")

    outcomes: Dict[date_type, bool] = {}
    for d in gap_dates:
        parquet_path = FEATURES_DAILY_DIR / f"{d.isoformat()}.parquet"
        ok = subprocess_ok and parquet_path.exists() and parquet_path.stat().st_size > 0
        outcomes[d] = ok
        if ok:
            checkpoint_manager.save_checkpoint(d, _COMPUTE_FEATURES_STEP, status="success", is_backfill=True)
        else:
            checkpoint_manager.save_checkpoint(
                d, _COMPUTE_FEATURES_STEP, status="failed", is_backfill=True,
                error_message="batch_feature_backfill: feature_backfill_hybrid.py did not "
                              "produce a non-empty parquet for this date (see logs for the "
                              "subprocess's own stderr)",
            )

    n_ok = sum(outcomes.values())
    logger.info(f"batch_feature_backfill: {n_ok}/{len(gap_dates)} date(s) succeeded")
    return outcomes
