#!/usr/bin/env python3
"""
scripts/build_ta_queue_combined_2009.py

Builds the combined technical sweep: all three performance measures in one
queue (user request, 2026-08-12).

    measure 1  CAGR, lump compounding      -> capital_mode="lump"
    measure 2  YoY / FY returns            -> same lump run (derived in report)
    measure 3  annual reset to Rs 10L      -> capital_mode="annual_reset"

WHY THE JOB COUNT IS 390, NOT 260
---------------------------------
For the LUMP run, tax is applied post-hoc by the comparison report, so ONE
trade book serves both LTCG regimes -> 65 templates x 2 exit variants = 130.

For ANNUAL RESET it cannot be. The tax determines how much cash is withdrawn at
each FY boundary; the withdrawal changes next year's capital; capital changes
position sizing; and `can_buy` rejects when cash is short or integer-share
rounding gives qty 0. So the two LTCG regimes take genuinely DIFFERENT TRADES
and must be simulated separately:
    65 templates x 2 exit variants x 2 regimes = 260.

Total 130 + 260 = 390.

Ordering: lump jobs are emitted FIRST so that if the sweep is interrupted, the
two measures that were already working are the ones most likely to be complete.

Usage:
    python scripts/build_ta_queue_combined_2009.py
"""

import json
from pathlib import Path

SRC = Path("backtest/queues/ta_full_history.json")
DST = Path("backtest/queues/ta_combined_2009.json")

START_DATE = "2009-04-01"

# The two regimes the reports carry. Rates/exemptions mirror
# backtest/core/tax.py's constants and the report layer's regime names.
LTCG_REGIMES = [
    {"regime_label": "ltcg_10pct_1L", "ltcg_rate": 0.10, "ltcg_exemption": 100_000.0},
    {"regime_label": "ltcg_12_5pct_1_25L", "ltcg_rate": 0.125, "ltcg_exemption": 125_000.0},
]


def main() -> None:
    src_jobs = json.loads(SRC.read_text())["jobs"]
    jobs = []

    # ---- measures 1 + 2: lump, both exit variants -------------------------
    for variant in ("baseline", "unconstrained"):
        for j in src_jobs:
            job = dict(j)
            job["start_date"] = START_DATE
            job["exit_variant"] = variant
            job["capital_mode"] = "lump"
            if variant == "unconstrained":
                # Ignored by this variant; carrying it would imply a barrier
                # that is not actually applied.
                job.pop("max_hold_days", None)
            jobs.append(job)

    n_lump = len(jobs)

    # ---- measure 3: annual reset, both variants x both regimes ------------
    for regime in LTCG_REGIMES:
        for variant in ("baseline", "unconstrained"):
            for j in src_jobs:
                job = dict(j)
                job["start_date"] = START_DATE
                job["exit_variant"] = variant
                job["capital_mode"] = "annual_reset"
                job["annual_reset_ltcg_rate"] = regime["ltcg_rate"]
                job["annual_reset_ltcg_exemption"] = regime["ltcg_exemption"]
                job["annual_reset_regime_label"] = regime["regime_label"]
                if variant == "unconstrained":
                    job.pop("max_hold_days", None)
                jobs.append(job)

    DST.write_text(json.dumps({"jobs": jobs}, indent=2) + "\n")

    n_reset = len(jobs) - n_lump
    print(f"wrote {DST}")
    print(f"  total jobs        : {len(jobs)}")
    print(f"    lump            : {n_lump}   (measures 1 + 2)")
    print(f"    annual_reset    : {n_reset}   (measure 3, {len(LTCG_REGIMES)} regimes)")
    print(f"  templates         : {len({j['template_name'] for j in jobs})}")
    print(f"  window            : {START_DATE} -> {sorted({j['end_date'] for j in jobs})[0]}")
    print(f"  capital           : {sorted({j['initial_capital'] for j in jobs})}")
    print(f"  defer_db_writes   : {sorted({j.get('defer_db_writes') for j in jobs})}")
    labels = sorted({j.get("annual_reset_regime_label") for j in jobs if j.get("annual_reset_regime_label")})
    print(f"  regimes           : {labels}")

    # Guard the exact failure this file's docstring warns about: if the regime
    # keys were dropped, every annual_reset job would run the engine default and
    # the two regimes would come out identical, looking like a real finding.
    reset_jobs = [j for j in jobs if j["capital_mode"] == "annual_reset"]
    assert all(j.get("annual_reset_regime_label") for j in reset_jobs), "annual_reset job missing its regime label"
    assert len({j["annual_reset_ltcg_rate"] for j in reset_jobs}) == 2, "expected two distinct LTCG rates"
    print("  regime plumbing   : OK (distinct rates present on every annual_reset job)")


if __name__ == "__main__":
    main()
