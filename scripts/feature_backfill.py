"""
scripts/feature_backfill.py

Phase: 3 (Deep Learning — feature store preparation)
Specs: SPEC-DS-005, SPEC-PIPE-004
Owner: Platform / ML
Consumers: systems/ml_signal_engine/inference/train_deep_models.py

Backfills the daily feature parquet store (datastore/features/daily/) for a
given date range so that train_deep_models.py has sufficient training data.

PREREQUISITE — DataStore API must be running before you invoke this script:
    .venv/bin/uvicorn datastore.api.main:app --host 127.0.0.1 --port 8000

Start date rationale
--------------------
    OHLCV is available from 2006-01-02. The longest feature lookback is 252
    trading days (1-year moving average), so the first date with fully valid
    technical features is ~2007-01-03. Default start is 2007-01-03.

    Fundamentals, shareholding, F&O, and macro data are only populated for
    recent dates (scrapers run since mid-2026). For dates before those
    scrapers backfill their tables, those feature columns will be NaN — the
    deep-learning models handle this via masking, but expect the historical
    matrices to be primarily technical features.

Processing order: most-recent-first (default)
--------------------
    Processing newest dates first means you have 2023-2026 parquets ready
    (and can kick off a training run) while the backfill continues on older
    dates in the background.

Usage
-----
    # Default: 2007-01-03 → today, most-recent-first
    .venv/bin/python3 scripts/feature_backfill.py

    # Oldest-first (chronological)
    .venv/bin/python3 scripts/feature_backfill.py --chronological

    # Background run with live progress log
    nohup .venv/bin/python3 scripts/feature_backfill.py \\
        > logs/feature_backfill.log 2>&1 &

    tail -f logs/feature_backfill.log

    # Force recompute even if parquet already exists
    .venv/bin/python3 scripts/feature_backfill.py --force

Timing
------
    Each date: ~60-300 s (2492 tickers × API fetch + vectorised feature math).
    Full backfill 2007→2026: ~4,900 dates → estimate 20-80 hours.
    Dates that already have a parquet in datastore/features/daily/ are skipped.
"""

import argparse
import logging
import sys
import time
from datetime import date as date_type, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill daily feature parquets for deep-learning training")
    p.add_argument("--from-date", default="2007-01-03", metavar="YYYY-MM-DD",
                   help="Earliest date to compute (default: 2007-01-03, after 252-day OHLCV warmup)")
    p.add_argument("--to-date", default=None, metavar="YYYY-MM-DD",
                   help="Latest date to compute (default: today)")
    p.add_argument("--chronological", action="store_true",
                   help="Process oldest dates first (default: newest-first so training data is ready sooner)")
    p.add_argument("--force", action="store_true",
                   help="Recompute dates that already have a parquet file")
    p.add_argument("--no-hmm", action="store_true",
                   help="Skip HMM regime-feature fitting (HMM cols become NaN). "
                        "~14x faster per date — recommended for full historical backfill. "
                        "Deep-learning models handle NaN via masking.")
    p.add_argument("--run-id", default=None,
                   help="Identifier for this run's failed-dates manifest file "
                        "(default: a timestamp, e.g. 20260728_101500). The manifest is written "
                        "incrementally to logs/feature_backfill_failed_<run-id>.txt, one failed "
                        "date per line, so a human can grep/retry exact failures without "
                        "re-scanning a potentially huge log.")
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    from config.settings import DUCKDB_PATH, FEATURES_DAILY_DIR
    from config.universe import get_tickers
    from datastore.api.db import get_duckdb_connection
    from ingestion.scheduler.daily_pipeline import step_compute_features

    from_dt = date_type.fromisoformat(args.from_date)
    to_dt = date_type.fromisoformat(args.to_date) if args.to_date else date_type.today()

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        rows = conn.execute(
            "SELECT DISTINCT CAST(date AS VARCHAR) FROM ohlcv_adjusted "
            "WHERE date >= ? AND date <= ? ORDER BY date",
            [from_dt.isoformat(), to_dt.isoformat()],
        ).fetchall()

    all_dates = [date_type.fromisoformat(r[0]) for r in rows]
    logger.info("Found %d trading dates from %s to %s", len(all_dates), from_dt, to_dt)

    FEATURES_DAILY_DIR.mkdir(parents=True, exist_ok=True)
    if args.force:
        pending = all_dates
    else:
        existing = {p.stem for p in FEATURES_DAILY_DIR.glob("*.parquet")}
        pending = [d for d in all_dates if d.isoformat() not in existing]
        skipped = len(all_dates) - len(pending)
        logger.info("%d already have parquets (skipping); %d to compute", skipped, len(pending))

    # Newest-first by default so training data (recent years) is ready sooner
    if not args.chronological:
        pending = list(reversed(pending))

    if not pending:
        logger.info("Nothing to do — all dates have parquets. Use --force to recompute.")
        return

    compute_hmm = not args.no_hmm
    if not compute_hmm:
        logger.info("HMM regime features DISABLED (--no-hmm): hmm_* columns will be NaN")

    # Pre-load all per-ticker data ONCE before the date loop.  For each
    # subsequent date the cache provides in-memory PIT-filtered slices,
    # replacing ~15 M per-date API calls with ~1 500 one-time calls.
    from datastore.client import DataStoreClient
    from features.backfill_cache import BackfillDataCache

    client = DataStoreClient()
    tickers_for_cache = get_tickers()
    backfill_cache = BackfillDataCache(client, tickers_for_cache, to_date=datetime.combine(to_dt, datetime.min.time()))

    # [BUG FIX, 2026-07-28 model-review item 6] A 20-80 hour unattended run
    # with only ERROR-level log lines and a final summary count has real
    # precedent for going badly unnoticed on this project (a June 2026
    # backfill silently failed 4,772/4,785 dates on an unrelated bug, only
    # discovered after the fact). Write each failed date incrementally to a
    # small manifest file, so a human/script can grep/retry the exact
    # failures without re-scanning a huge log.
    run_id = args.run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    logs_dir = Path(__file__).resolve().parent.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = logs_dir / f"feature_backfill_failed_{run_id}.txt"
    logger.info("Failed-dates manifest (if any): %s", manifest_path)

    ok = err = 0
    elapsed_times: list = []

    for i, d in enumerate(pending, start=1):
        t0 = time.monotonic()
        try:
            step_compute_features(d, compute_hmm=compute_hmm, data_cache=backfill_cache)
            elapsed = time.monotonic() - t0
            elapsed_times.append(elapsed)
            remaining = len(pending) - i
            avg = sum(elapsed_times[-20:]) / len(elapsed_times[-20:])  # rolling 20-date avg
            eta_min = remaining * avg / 60
            logger.info(
                "[%d/%d] %s done in %.1fs — %d remaining, ETA ~%.0f min",
                i, len(pending), d, elapsed, remaining, eta_min,
            )
            ok += 1
        except Exception as exc:  # noqa: BLE001
            err += 1
            logger.error("[%d/%d] %s FAILED: %s", i, len(pending), d, exc)
            with open(manifest_path, "a") as f:
                f.write(f"{d.isoformat()}\n")

    total_min = sum(elapsed_times) / 60
    logger.info(
        "Backfill complete: %d succeeded, %d failed, total %.1f min",
        ok, err, total_min,
    )
    if err:
        logger.info("Failed dates written to %s — retry with those dates specifically", manifest_path)


if __name__ == "__main__":
    main()
