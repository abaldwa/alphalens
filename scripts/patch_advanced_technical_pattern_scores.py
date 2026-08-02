"""
scripts/patch_advanced_technical_pattern_scores.py

[2026-08-01] Targeted patch for the `all_rows` bug fix in
features/advanced_technical.py / features/pattern_scores.py (see those
modules' `all_rows` docstrings): the 2026-07-31 "taonly2016" backfill run
(1050 min, 0 failures) wrote every 2016-2026 daily parquet with correct
technical/intraday/pnd columns but 100% NaN advanced_technical/
pattern_scores columns, because panel_staging.py's batch path silently
only filled the LAST row of its multi-year per-ticker panel for those two
categories.

Rather than redo the full 1050-minute run (which mostly re-validated
already-correct technical/intraday/pnd data), this script:
  1. Re-runs features/panel_staging.py's batch staging (now fixed) for the
     requested date range -- this recomputes all 5 categories, but the
     expensive part was always advanced_technical/pattern_scores, not the
     already-fast technical/intraday/pnd.
  2. For each date that already has a parquet in datastore/features/daily/,
     loads it, OVERWRITES ONLY the ADVANCED_TECHNICAL_FEATURES +
     PATTERN_FEATURES columns from the freshly-staged (correct) values,
     and leaves every other column (technical/intraday/pnd/calendar/macro/
     the deliberately-NaN slow categories) byte-for-byte untouched.

No API calls in the per-date merge pass (pure local parquet read/write) --
this is why it's fast compared to the original per-date backfill loop.

Usage:
    .venv/bin/python3 scripts/patch_advanced_technical_pattern_scores.py \\
        --from-date 2016-01-01 --to-date 2026-07-31
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date as date_type
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Patch advanced_technical/pattern_scores columns in-place")
    p.add_argument("--from-date", required=True, metavar="YYYY-MM-DD")
    p.add_argument("--to-date", required=True, metavar="YYYY-MM-DD")
    p.add_argument("--run-id", default=None)
    p.add_argument("--panel-workers", type=int, default=1,
                   help="[2026-08-01] Parallelizes the now-CPU-heavy all_rows=True "
                        "advanced_technical/pattern_scores computation across ticker "
                        "chunks via features/panel_staging.py's panel_workers param. "
                        "Recommended max on this project's 16GB laptop: 8, not 12 "
                        "(documented OOM-dip incident — see that param's docstring).")
    p.add_argument("--skip-staging", action="store_true",
                   help="[2026-08-01] Resume mode: skip stage_batch_panels entirely and "
                        "go straight to the merge/write pass, reusing an existing "
                        "--run-id's already-staged rows (e.g. after a crash/OOM-kill "
                        "mid-merge — drop_staging_run only runs at the very end, so "
                        "staged rows survive a merge-phase death). Requires --run-id to "
                        "point at a run whose staging actually completed; dates with no "
                        "staged rows are skipped with a warning, same as any other gap.")
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    import pandas as pd

    from config.settings import FEATURES_DAILY_DIR
    from config.universe import get_tickers
    from datastore.client import DataStoreClient
    from features import panel_staging
    from features.advanced_technical import ADVANCED_TECHNICAL_FEATURES
    from features.pattern_scores import PATTERN_FEATURES

    patch_cols = ADVANCED_TECHNICAL_FEATURES + PATTERN_FEATURES

    from_dt = date_type.fromisoformat(args.from_date)
    to_dt = date_type.fromisoformat(args.to_date)

    existing = sorted(
        p.stem for p in FEATURES_DAILY_DIR.glob("*.parquet")
        if from_dt.isoformat() <= p.stem <= to_dt.isoformat()
    )
    if not existing:
        logger.info("No existing parquets in range %s..%s — nothing to patch.", from_dt, to_dt)
        return
    logger.info("%d existing parquets in range to patch", len(existing))

    dates = [pd.Timestamp(d) for d in existing]
    tickers = get_tickers()
    run_id = args.run_id or f"patch_advtech_patterns_{int(time.time())}"

    if args.skip_staging:
        if not args.run_id:
            raise SystemExit("--skip-staging requires an explicit --run-id pointing at a completed staging run")
        logger.info("--skip-staging: reusing existing staged rows for run_id=%s (no restage)", run_id)
    else:
        client = DataStoreClient()
        logger.info("Staging (fixed all_rows=True) advanced_technical/pattern_scores for %d dates ...", len(dates))
        t0 = time.monotonic()
        panel_staging.stage_batch_panels(client, tickers, dates, run_id=run_id, panel_workers=args.panel_workers)
        logger.info("Staging complete in %.1f min", (time.monotonic() - t0) / 60)

    from config.settings import FEATURE_PANEL_STAGING_DB_PATH
    from datastore.api.db import get_duckdb_connection

    ok = err = 0
    t0 = time.monotonic()
    # [2026-08-01 perf fix] One connection reused across all `dates` instead
    # of load_staged_panel_for_date's default open-close-per-call — that
    # was measured at ~13.5s/date (almost entirely connection overhead) in
    # this exact merge-only loop, which makes zero API calls otherwise.
    with get_duckdb_connection(FEATURE_PANEL_STAGING_DB_PATH, read_only=False, persist=False) as staging_conn:
        for i, d in enumerate(dates, start=1):
            date_str = d.date().isoformat()
            path = FEATURES_DAILY_DIR / f"{date_str}.parquet"
            try:
                staged = panel_staging.load_staged_panel_for_date(run_id, d, conn=staging_conn)
                if staged is None:
                    logger.warning("[%d/%d] %s: no staged rows — skipping", i, len(dates), date_str)
                    continue

                matrix = pd.read_parquet(path)
                original_columns = list(matrix.columns)
                missing_cols = [c for c in patch_cols if c not in staged.columns]
                if missing_cols:
                    raise RuntimeError(f"staged panel missing columns: {missing_cols}")

                patch = staged[["ticker"] + patch_cols].set_index("ticker")
                matrix = matrix.set_index("ticker")
                matrix.update(patch)
                matrix = matrix.reset_index()[original_columns]

                matrix.to_parquet(path, index=False)
                ok += 1
                if i % 200 == 0 or i == len(dates):
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "[%d/%d] patched through %s — %.1fs elapsed, %.2fs/date avg",
                        i, len(dates), date_str, elapsed, elapsed / i,
                    )
            except Exception as exc:  # noqa: BLE001
                err += 1
                logger.error("[%d/%d] %s FAILED: %s", i, len(dates), date_str, exc)

    panel_staging.drop_staging_run(run_id)
    logger.info("Patch complete: %d succeeded, %d failed", ok, err)


if __name__ == "__main__":
    main()
