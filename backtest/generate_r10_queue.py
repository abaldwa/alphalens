"""
backtest/generate_r10_queue.py

Phase: R10 (Nigam-Pandey sector momentum)
Owner: Platform / Backtest

R10 strategy: sector-level momentum ranking (industry_momentum rank method).
Full-period backtest across M2/M4/M7/M9/M10/M12 bands, 2009-2026.

Cadence: monthly (quarterly rebalance, 63d).
Lookback: 6 months with 1-month skip (Nigam-Pandey spec).
No leverage fields (baseline momentum channel).
"""

import json
from pathlib import Path
from typing import Any, Dict, List

BANDS = [2, 4, 7, 9, 10, 12]  # rank_band_id (M2-M12)

START_DATE = "2009-01-01"
END_DATE = "2026-08-26"
LOOKBACK_MONTHS = 6
SKIP_MONTHS = 1
REBALANCE_CADENCE_DAYS = 63

OUT_PATH = Path(__file__).resolve().parent / "queues" / "r10_full_2009_2026.json"


def build_jobs() -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    for band_id in BANDS:
        job: Dict[str, Any] = {
            "kind": "orchestrator",
            "channel": "momentum",
            "start_date": START_DATE,
            "end_date": END_DATE,
            "rank_band_id": band_id,
            "lookback_months": LOOKBACK_MONTHS,
            "skip_months": SKIP_MONTHS,
            "rebalance_cadence_days": REBALANCE_CADENCE_DAYS,
            "top_n": 15,
            "capital_mode": "lump",
            "initial_capital": 1_000_000,
            "max_tickers": 800,
            "min_history_days": 60,
            "exit_variant": "baseline",
            "strategy_family": "R",
            "rank_method": "industry_momentum",
            "defer_db_writes": True,
        }
        jobs.append(job)
    return jobs


def main() -> None:
    jobs = build_jobs()
    payload = {
        "_description": (
            "R10 (Nigam-Pandey sector momentum): rank by industry_momentum, "
            "6-month lookback with 1-month skip, quarterly rebalance (63d), "
            "across M2/M4/M7/M9/M10/M12 bands, 2009-2026. 6 bands x 1 config = 6 jobs."
        ),
        "_metadata": {
            "strategy": "R10",
            "phase": "Phase 10",
            "bands": BANDS,
            "lookback_months": LOOKBACK_MONTHS,
            "skip_months": SKIP_MONTHS,
            "rebalance_cadence_days": REBALANCE_CADENCE_DAYS,
            "rank_method": "industry_momentum",
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
