"""
backtest/generate_r0_baseline_queue.py

R0: Plain trailing-momentum baseline (no crash overlay, no vol-scaling,
no weight_method) -- equal-weight position sizing, mirrors the
phaseB_r7_6band_4lb_3rebal_3topn.json grid structure exactly, minus the
crash_regime_enabled / crash_reduce_sizing overlay fields.

Grid: 6 bands x 4 lookback x 3 rebalance x 3 top_n = 216 jobs
"""

import json
from pathlib import Path
from typing import Any, Dict, List

BANDS = [2, 4, 7, 9, 10, 12]
LOOKBACK_MONTHS = [3, 6, 9, 12]
REBALANCE_DAYS = [5, 10, 21]
TOP_NS = [7, 10, 15]

START_DATE = "2009-01-01"
END_DATE = "2026-06-30"

OUT_PATH = Path("/home/amit/projects/AlphaLens/backtest/queues/r0_baseline_6band_4lb_3rebal_3topn.json")


def build_jobs() -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    for band_id in BANDS:
        for lookback in LOOKBACK_MONTHS:
            for rebal in REBALANCE_DAYS:
                for top_n in TOP_NS:
                    jobs.append({
                        "kind": "orchestrator",
                        "channel": "momentum",
                        "start_date": START_DATE,
                        "end_date": END_DATE,
                        "rank_band_id": band_id,
                        "top_n": top_n,
                        "rebalance_cadence_days": rebal,
                        "lookback_months": lookback,
                        "strategy_family": "R",
                        "capital_mode": "lump",
                        "initial_capital": 1000000,
                        "max_tickers": 800,
                        "min_history_days": 60,
                        "exit_variant": "baseline",
                        "defer_db_writes": True,
                        "rank_method": "trailing_return",
                    })
    return jobs


def main() -> None:
    jobs = build_jobs()
    payload = {
        "_description": (
            "R0 baseline campaign: plain trailing-momentum, no crash overlay, "
            "no vol-scaling, no weight_method (equal-weight). Mirrors the "
            "phaseB R7 grid (6 bands x 4 lookback x 3 rebalance x 3 top_n = "
            "216 jobs) for apples-to-apples R0 vs R7 comparison. Launched "
            "2026-09-02 to run in parallel with in-progress R7 sweep."
        ),
        "jobs": jobs,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, indent=1))
    print(f"wrote {len(jobs)} jobs to {OUT_PATH}")


if __name__ == "__main__":
    main()
