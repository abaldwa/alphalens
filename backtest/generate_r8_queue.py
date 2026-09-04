"""
backtest/generate_r8_queue.py

Phase: R8 (Barroso-Santa-Clara vol-target overlay)
Owner: Platform / Backtest

R8 strategy: vol-target overlay on base momentum, using rolling 63-day lookback.
Full-period backtest across M2/M4/M7/M9/M10/M12 bands, 2009-2026.

Cadence: monthly (21d for portfolio rebalance).
Leverage cap: 1.0 (no leverage).
"""

import json
from pathlib import Path
from typing import Any, Dict, List

BANDS = [2, 4, 7, 9, 10, 12]  # rank_band_id (M2-M12)

START_DATE = "2009-01-01"
END_DATE = "2026-08-26"
REBALANCE_CADENCE_DAYS = 21

OUT_PATH = Path(__file__).resolve().parent / "queues" / "r8_full_2009_2026.json"


def build_jobs() -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    for band_id in BANDS:
        job: Dict[str, Any] = {
            "kind": "orchestrator",
            "channel": "momentum",
            "start_date": START_DATE,
            "end_date": END_DATE,
            "rank_band_id": band_id,
            "lookback_months": 12,
            "skip_months": 0,
            "top_n": 10,
            "rank_method": "trailing_return",
            "crash_regime_enabled": False,
            "vol_target_enabled": True,
            "vol_target_pct": 0.15,
            "vol_target_lookback_days": 63,
            "vol_target_leverage_cap": 1.0,
            "rebalance_cadence_days": REBALANCE_CADENCE_DAYS,
            "strategy_family": "R",
            "capital_mode": "lump",
            "initial_capital": 1_000_000,
            "max_tickers": 800,
            "min_history_days": 60,
            "exit_variant": "unconstrained",
            "defer_db_writes": True,
        }
        jobs.append(job)
    return jobs


def main() -> None:
    jobs = build_jobs()
    payload = {
        "_description": (
            "R8 (Barroso-Santa-Clara vol-target overlay): 12-month lookback, "
            "vol-target enabled with 15% target vol and 63d lookback window, "
            "leverage cap 1.0 (no leverage), across M2/M4/M7/M9/M10/M12 bands, "
            "2009-2026. 6 bands x 1 config = 6 jobs."
        ),
        "_metadata": {
            "strategy": "R8",
            "phase": "Phase 8",
            "bands": BANDS,
            "vol_target_enabled": True,
            "vol_target_pct": 0.15,
            "vol_target_lookback_days": 63,
            "vol_target_leverage_cap": 1.0,
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
