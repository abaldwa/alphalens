"""
backtest/generate_r11_queue.py

Phase: R11 (52-week-high momentum)
Owner: Platform / Backtest

R11 strategy: trend-following momentum based on proximity to 52-week highs (George & Hwang 2004).
Full-period backtest across M2/M4/M7/M9/M10/M12 bands, 2009-2026.

Cadence: monthly (21d for portfolio rebalance).
Lookback: 252 trading days (1 year for 52-week high calculation).
Ranking: pct_of_52wk_high (trend-following, highest = best).
No leverage caps (baseline momentum channel).
"""

import json
from pathlib import Path
from typing import Any, Dict, List

BANDS = [2, 4, 7, 9, 10, 12]  # rank_band_id (M2-M12)

START_DATE = "2009-01-01"
END_DATE = "2026-08-26"
REBALANCE_CADENCE_DAYS = 21

OUT_PATH = Path(__file__).resolve().parent / "queues" / "r11_full_2009_2026.json"


def build_jobs() -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    for band_id in BANDS:
        job: Dict[str, Any] = {
            "kind": "orchestrator",
            "channel": "momentum",
            "start_date": START_DATE,
            "end_date": END_DATE,
            "rank_band_id": band_id,
            "top_n": 15,
            "lookback_days": 252,
            "rebalance_cadence_days": REBALANCE_CADENCE_DAYS,
            "strategy_family": "R",
            "capital_mode": "lump",
            "initial_capital": 1_000_000,
            "max_tickers": 800,
            "min_history_days": 60,
            "exit_variant": "baseline",
            "rank_method": "pct_of_52wk_high",
            "defer_db_writes": True,
        }
        jobs.append(job)
    return jobs


def main() -> None:
    jobs = build_jobs()
    payload = {
        "_description": (
            "R11 (52-week-high momentum): trend-following strategy ranking on proximity "
            "to 52-week highs (George & Hwang 2004), across M2/M4/M7/M9/M10/M12 bands, "
            "2009-2026, monthly (21d) rebalance. 6 bands x 1 config = 6 jobs."
        ),
        "_metadata": {
            "strategy": "R11",
            "phase": "Phase 11",
            "bands": BANDS,
            "rank_method": "pct_of_52wk_high",
            "lookback_days": 252,
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
