"""
backtest/generate_r12_queue.py

Phase: R12 (Momentum-reversal-liquidity interaction)
Owner: Platform / Backtest

R12 strategy: tests interaction of reversal, momentum, and liquidity factors.
Generates jobs for 5 signal types: reversal_1mo, momentum_3mo, momentum_6mo,
momentum_12mo, momentum_12_7_skip (skip 7 months).
Full-period backtest across M2/M4/M7/M9/M10/M12 bands, 2009-2026.

Cadence: quarterly rebalance (63d).
No leverage fields (baseline momentum channel).
"""

import json
from pathlib import Path
from typing import Any, Dict, List

BANDS = [2, 4, 7, 9, 10, 12]  # rank_band_id (M2-M12)
SIGNAL_TYPES = [
    ("reversal_1mo", 1, None),  # (rank_method, lookback_months, skip_months)
    ("momentum_3mo", 3, None),
    ("momentum_6mo", 6, None),
    ("momentum_12mo", 12, None),
    ("momentum_12_7_skip", 12, 7),
]

START_DATE = "2009-01-01"
END_DATE = "2026-08-26"
REBALANCE_CADENCE_DAYS = 63

OUT_PATH = Path(__file__).resolve().parent / "queues" / "r12_full_2009_2026.json"


def build_jobs() -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    for band_id in BANDS:
        for rank_method, lookback_months, skip_months in SIGNAL_TYPES:
            job: Dict[str, Any] = {
                "kind": "orchestrator",
                "channel": "momentum",
                "start_date": START_DATE,
                "end_date": END_DATE,
                "rank_band_id": band_id,
                "lookback_months": lookback_months,
                "rebalance_cadence_days": REBALANCE_CADENCE_DAYS,
                "top_n": 15,
                "capital_mode": "lump",
                "initial_capital": 1_000_000,
                "max_tickers": 800,
                "min_history_days": 60,
                "exit_variant": "baseline",
                "strategy_family": "M" if "momentum" in rank_method else "R",
                "rank_method": rank_method,
                "defer_db_writes": True,
            }
            if skip_months is not None:
                job["skip_months"] = skip_months
            jobs.append(job)
    return jobs


def main() -> None:
    jobs = build_jobs()
    payload = {
        "_description": (
            "R12 (Momentum-reversal-liquidity): 5 signal types "
            "(reversal_1mo, momentum_3mo, momentum_6mo, momentum_12mo, momentum_12_7_skip) "
            "across M2/M4/M7/M9/M10/M12 bands, 2009-2026, quarterly rebalance. "
            "6 bands x 5 signal types = 30 jobs."
        ),
        "_metadata": {
            "strategy": "R12",
            "phase": "Phase 12",
            "bands": BANDS,
            "signal_types": [st[0] for st in SIGNAL_TYPES],
            "rebalance_cadence_days": REBALANCE_CADENCE_DAYS,
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
