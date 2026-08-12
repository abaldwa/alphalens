#!/usr/bin/env python3
"""
scripts/profile_one_feature_date.py

Profiles a single date's feature computation to find where the per-date cost
actually is.

Written 2026-08-12 after two failed guesses. The per-date cost in
scripts/feature_backfill.py sat at ~18s even with panel staging supplying the
five expensive feature categories precomputed. The bulk-OHLCV window cache was
built on the assumption that the repeated ~760-day fetch dominated; measured, it
removed only ~15% (18s -> 15s). The remaining ~15s is unaccounted for, and
guessing again is not a strategy.

Runs the real step_compute_features for one date under cProfile, with staging
loaded exactly as the backfill does, so the profile reflects production cost
rather than a synthetic path.

Usage:
    python scripts/profile_one_feature_date.py 2008-12-04 --run-id cachecheck
"""

import argparse
import cProfile
import io
import pstats
import sys
from datetime import date as date_type
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("target_date")
    p.add_argument("--run-id", default=None,
                   help="panel_staging run_id to load staged rows from (omit = no staging)")
    p.add_argument("--sort", default="cumulative")
    p.add_argument("--rows", type=int, default=35)
    args = p.parse_args()


    from ingestion.scheduler.daily_pipeline import step_compute_features

    d = date_type.fromisoformat(args.target_date)

    staged_panel = None
    if args.run_id:
        from features import panel_staging
        staged_panel = panel_staging.load_staged_panel_for_date(f"feature_backfill_{args.run_id}", d)
        print(f"staged_panel: {'None' if staged_panel is None else staged_panel.shape}")

    prof = cProfile.Profile()
    prof.enable()
    step_compute_features(
        d, compute_hmm=False, staged_panel=staged_panel,
        skip_slow_categories=True, advanced_technical_used_only=True,
        panel_workers=1,
    )
    prof.disable()

    s = io.StringIO()
    pstats.Stats(prof, stream=s).sort_stats(args.sort).print_stats(args.rows)
    print(s.getvalue())


if __name__ == "__main__":
    main()
