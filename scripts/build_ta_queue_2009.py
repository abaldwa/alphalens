#!/usr/bin/env python3
"""
scripts/build_ta_queue_2009.py

Generates the Stage 3 technical queue for the post-backfill rebuild.

Two deliberate changes from backtest/queues/ta_full_history.json:

1. start_date 2007-04-01 -> 2009-04-01.
   Fyers serves no history before 2007-04-02, so 2005-06 is 100% legacy data
   carrying the broken backward-adjustment factors this whole rebuild exists to
   replace. Across that seam, 69 of 473 tickers disagree by >2x and 30 by >5x.
   The engine reinvests a lump capital base, so one bad early entry compounds
   into every later trade — this is not a cosmetic concern.

   A 2008-04-01 start would already clear every real indicator lookback
   (sma_200_weekly_ratio is 50 weekly bars; sma_200 is 200 trading days; 52-week
   high/low — all ~1 year or less). 2009-04-01 was chosen instead (user,
   2026-08-12) to also clear LOOKBACK_CALENDAR_DAYS=760, the full ~2-year window
   the panel builder fetches, so that no input to any staged feature touches the
   seam even indirectly. Costs two years of the nineteen; buys certainty.

2. Both exit variants, not just one.
   The requirement is per-template style exits AND an unconstrained variant.
   `baseline` IS the per-template policy (PerTemplateExitPolicy); the existing
   queue only ran `unconstrained`, so the per-template half was never measured.
   max_hold_days is carried through for baseline and dropped for unconstrained,
   which ignores it (see run_orchestrator_backtest.py --max-hold-days help).

Usage:
    python scripts/build_ta_queue_2009.py
"""

import json
from pathlib import Path

SRC = Path("backtest/queues/ta_full_history.json")
DST = Path("backtest/queues/ta_full_history_2009.json")

START_DATE = "2009-04-01"


def main() -> None:
    src_jobs = json.loads(SRC.read_text())["jobs"]

    jobs = []
    for variant in ("baseline", "unconstrained"):
        for j in src_jobs:
            job = dict(j)
            job["start_date"] = START_DATE
            job["exit_variant"] = variant
            if variant == "unconstrained":
                # Ignored by this variant; carrying it would imply a barrier
                # that is not actually applied.
                job.pop("max_hold_days", None)
            jobs.append(job)

    DST.write_text(json.dumps({"jobs": jobs}, indent=2) + "\n")

    caps = {j["initial_capital"] for j in jobs}
    print(f"wrote {DST}  jobs={len(jobs)}")
    print(f"  templates      : {len({j['template_name'] for j in jobs})}")
    print(f"  variants       : {sorted({j['exit_variant'] for j in jobs})}")
    print(f"  window         : {START_DATE} -> {sorted({j['end_date'] for j in jobs})}")
    print(f"  capital        : {caps}")
    print(f"  max_tickers    : {sorted({j.get('max_tickers') for j in jobs})}")
    print(f"  defer_db_writes: {sorted({j.get('defer_db_writes') for j in jobs})}")


if __name__ == "__main__":
    main()
