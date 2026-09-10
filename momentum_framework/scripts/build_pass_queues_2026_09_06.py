"""
Build the 2026-09-06 remediation-campaign job queues, split into passes.

Run this to (re)generate the queue JSON files under
momentum_framework/results/queues/ — it does NOT execute anything, only
writes job lists to disk for a human to review before any pass is run
(see queues/pass_builder.py's module docstring for the pass-1/2/3
rationale).

Usage:
    python -m momentum_framework.scripts.build_pass_queues_2026_09_06
"""

import json
from datetime import date

from momentum_framework.queues.generator import QUEUE_OUTPUT_DIR
from momentum_framework.queues.pass_builder import build_passes

PASS_DESCRIPTIONS = {
    1: (
        "Pass 1 of the 2026-09-06 remediation re-run: every active strategy "
        "(R01,R03,R07-R15,R17 -- R16 retired) x every band it supports, at "
        "each band's SMALLEST top_n (partitioned bands: top_n=5; M13: "
        "top_n=10). Full lookback/cadence/rank_method/crash-config grid per "
        "strategy is unchanged -- only top_n is scoped to pass 1. Reflects "
        "the current fixed defaults: B1 portfolio-weighting fix, R07 "
        "crash_reduce_sizing=0.5, R11/R12/R13 crash_regime_enabled=True, "
        "the shared Nifty-500/-12.5% crash detector, and always-on costs+tax."
    ),
    2: (
        "Pass 2 of the 2026-09-06 remediation re-run: same strategy x band "
        "coverage as pass 1, at each band's SECOND top_n (partitioned "
        "bands: top_n=10; M13: top_n=15). Run only after pass 1 is reviewed."
    ),
    3: (
        "Pass 3 of the 2026-09-06 remediation re-run: M13 ONLY, at its "
        "THIRD top_n (top_n=20) -- partitioned bands have no third top_n "
        "since E2 dropped top_n=15 for them. Run only after pass 2 is "
        "reviewed."
    ),
}


def main() -> None:
    passes = build_passes()
    QUEUE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for pass_number, jobs in passes.items():
        filename = f"campaign_2026_09_06_pass{pass_number}.json"
        out_path = QUEUE_OUTPUT_DIR / filename
        payload = {
            "_description": PASS_DESCRIPTIONS[pass_number],
            "_metadata": {
                "pass_number": pass_number,
                "generated_at": date.today().isoformat(),
                "job_count": len(jobs),
                "strategy_codes": sorted({j["strategy_family"] for j in jobs}),
                "band_ids": sorted({j["rank_band_id"] for j in jobs}),
                "top_n_values": sorted({j["top_n"] for j in jobs}),
                "framework_version": "1.0.0",
            },
            "jobs": jobs,
        }
        out_path.write_text(json.dumps(payload, indent=2))
        print(f"Wrote {out_path} ({len(jobs)} jobs)")


if __name__ == "__main__":
    main()
