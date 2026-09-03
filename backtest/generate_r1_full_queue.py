"""
backtest/generate_r1_full_queue.py

R1 (Jegadeesh & Titman 1993 momentum) - FULL CAMPAIGN

6 bands × 4 lookbacks × 3 rebalance cadences × 3 top-N values = 216 jobs
Period: 2009-2026 (full cycle with momentum_rankings cache)
"""

import json
from pathlib import Path
from typing import Any, Dict, List

BANDS = [2, 4, 7, 9, 10, 12]  # rank_band_id (M2-M12)
LOOKBACK_MONTHS = [3, 6, 9, 12]  # J&T spec
SKIP_MONTHS = 1  # Jegadeesh & Titman 1993: skip 1 month to avoid short-term reversal
REBALANCE_CADENCE_DAYS = [5, 10, 21]  # 5-day, 10-day, 21-day (monthly)
TOP_N_VALUES = [7, 10, 15]  # position sizes

START_DATE = "2009-01-01"
END_DATE = "2026-06-30"

OUT_PATH = Path(__file__).resolve().parent / "queues" / "r1_full_campaign_216.json"


def build_jobs() -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []

    for band_id in BANDS:
        for lookback_months in LOOKBACK_MONTHS:
            for rebalance_days in REBALANCE_CADENCE_DAYS:
                for top_n in TOP_N_VALUES:
                    job: Dict[str, Any] = {
                        "kind": "orchestrator",
                        "channel": "momentum",
                        "start_date": START_DATE,
                        "end_date": END_DATE,
                        "rank_band_id": band_id,
                        "top_n": top_n,
                        "lookback_months": lookback_months,
                        "skip_months": SKIP_MONTHS,
                        "rebalance_cadence_days": rebalance_days,
                        "strategy_family": "R",
                        "capital_mode": "lump",
                        "initial_capital": 1_000_000,
                        "max_tickers": 800,
                        "min_history_days": 60,
                        "exit_variant": "baseline",
                        "defer_db_writes": True,
                    }
                    jobs.append(job)

    return jobs


def main() -> None:
    jobs = build_jobs()
    payload = {
        "_description": (
            "R1 (Jegadeesh & Titman 1993 momentum with 1-month skip) — Full Campaign: "
            "3/6/9/12-month lookbacks with 1-month skip × M2/M4/M7/M9/M10/M12 bands × "
            "5d/10d/21d rebalance × top-7/10/15 positions. "
            "2009-2026 full cycle. 216 jobs. With momentum_rankings cache: ~1.5-2hr runtime."
        ),
        "_metadata": {
            "strategy": "R1 (Jegadeesh & Titman 1993 momentum)",
            "phase": "Full Campaign",
            "total_jobs": len(jobs),
            "bands": BANDS,
            "lookback_months": LOOKBACK_MONTHS,
            "skip_months": SKIP_MONTHS,
            "rebalance_cadences_days": REBALANCE_CADENCE_DAYS,
            "top_n_values": TOP_N_VALUES,
            "backtest_period": f"{START_DATE}:{END_DATE}",
            "created_at": "2026-09-03",
        },
        "jobs": jobs,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, indent=1))
    print(f"✅ Generated {len(jobs)} jobs to {OUT_PATH}")
    print(f"   Bands: {len(BANDS)} | Lookbacks: {len(LOOKBACK_MONTHS)} | Rebalance: {len(REBALANCE_CADENCE_DAYS)} | Top-N: {len(TOP_N_VALUES)}")
    print(f"   Scope: {len(BANDS)} × {len(LOOKBACK_MONTHS)} × {len(REBALANCE_CADENCE_DAYS)} × {len(TOP_N_VALUES)} = {len(jobs)} jobs")


if __name__ == "__main__":
    main()
