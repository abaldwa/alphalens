"""
backtest/generate_r1_queue.py

Phase: R1 (Jegadeesh & Titman 1993 momentum)
Owner: Platform / Backtest

R1 core strategy: trailing momentum with 3/6/9/12-month lookbacks (J&T spec).
Full-period backtest across M2/M4/M7/M9/M10/M12 bands, 2009-2026.

Cadence: monthly (30d nominal, calendar-aligned).
Rebalance cadence chosen as 21d to enable month-end portfolio turnover (J&T convention).
No weighting methods — uses equal-weight portfolio.
No leverage caps (momentum channel baseline, no vol scaling).
"""

import json
from pathlib import Path
from typing import Any, Dict, List

BANDS = [2, 4, 7, 9, 10, 12]  # rank_band_id (M2-M12)
LOOKBACK_MONTHS = [3, 6, 9, 12]  # J&T spec

START_DATE = "2009-01-01"
END_DATE = "2026-08-26"
REBALANCE_CADENCE_DAYS = 21  # monthly portfolio turnover (21d ≈ calendar month)

OUT_PATH = Path(__file__).resolve().parent / "queues" / "r1_full_2009_2026.json"


def build_jobs() -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    for band_id in BANDS:
        for lookback_months in LOOKBACK_MONTHS:
            job: Dict[str, Any] = {
                "kind": "orchestrator",
                "channel": "momentum",
                "start_date": START_DATE,
                "end_date": END_DATE,
                "rank_band_id": band_id,
                "top_n": 15,
                "lookback_months": lookback_months,
                "rebalance_cadence_days": REBALANCE_CADENCE_DAYS,
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
            "R1 (Jegadeesh & Titman 1993 momentum): 3/6/9/12-month lookbacks "
            "across M2/M4/M7/M9/M10/M12 bands, 2009-2026, monthly (21d) rebalance, "
            "equal-weight, no leverage. 6 bands x 4 lookbacks = 24 jobs."
        ),
        "_metadata": {
            "strategy": "R1",
            "phase": "Phase 1 (Core momentum)",
            "bands": BANDS,
            "lookback_months": LOOKBACK_MONTHS,
            "backtest_period": f"{START_DATE}:{END_DATE}",
            "created_at": "2026-08-31",
        },
        "jobs": jobs,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, indent=1))
    print(f"wrote {len(jobs)} jobs to {OUT_PATH}")


if __name__ == "__main__":
    main()
