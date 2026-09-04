"""
backtest/generate_r9_queue.py

Phase: R9 (Moreira-Muir regime-switching vol scaling)
Owner: Platform / Backtest

R9 strategy: regime-adaptive vol scaling (post-B-027 fix). Single adaptive job
per band that dynamically selects vol-scaling mode based on market regime.
Full-period backtest across M2/M4/M7/M9/M10/M12 bands, 2009-2026.

Cadence: monthly (21d for portfolio rebalance).
Leverage cap: 1.0 (no leverage — BUG FIX from prior 9999.0 cap).
Regime switching: enabled (EMA-RSI regime detector).
"""

import json
from pathlib import Path
from typing import Any, Dict, List

BANDS = [2, 4, 7, 9, 10, 12]  # rank_band_id (M2-M12)

START_DATE = "2009-01-01"
END_DATE = "2026-08-26"
REBALANCE_CADENCE_DAYS = 21

OUT_PATH = Path(__file__).resolve().parent / "queues" / "r9_full_2009_2026.json"


def build_jobs() -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    for band_id in BANDS:
        job: Dict[str, Any] = {
            "kind": "orchestrator",
            "channel": "momentum",
            "start_date": START_DATE,
            "end_date": END_DATE,
            "rank_band_id": band_id,
            "top_n": 10,
            "lookback_months": 12,
            "rank_method": "trailing_return",
            "crash_regime_enabled": False,
            "strategy_family": "R",
            "vol_scaling_mode": "inverse_volatility",
            "vol_scaling_lookback_days": 126,
            "vol_scaling_leverage_cap": 1.0,
            "regime_switching_enabled": True,
            "rebalance_cadence_days": REBALANCE_CADENCE_DAYS,
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
            "R9 (Moreira-Muir regime-switching): Single adaptive job per band "
            "with regime-switching enabled. Dynamically selects vol-scaling mode "
            "(inverse_volatility/downside_volatility/target_volatility) based on "
            "market regime (EMA-RSI detector). Leverage cap 1.0 (BUG FIX). "
            "Across M2/M4/M7/M9/M10/M12 bands, 2009-2026. 6 bands x 1 adaptive = 6 jobs."
        ),
        "_metadata": {
            "strategy": "R9",
            "phase": "Phase 9 + B-027",
            "bands": BANDS,
            "approach": "regime_switching_adaptive",
            "vol_scaling_base_mode": "inverse_volatility",
            "vol_scaling_lookback_days": 126,
            "vol_scaling_leverage_cap": 1.0,
            "regime_switching_enabled": True,
            "backtest_period": f"{START_DATE}:{END_DATE}",
            "created_at": "2026-08-31",
            "notes": "B-027 FIX: replaced 4 separate mode-runs with 1 regime-adaptive job per band. vol_scaling_leverage_cap corrected to 1.0 (was 9999.0).",
        },
        "jobs": jobs,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, indent=1))
    print(f"wrote {len(jobs)} jobs to {OUT_PATH}")


if __name__ == "__main__":
    main()
