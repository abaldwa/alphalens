"""
backtest/generate_r0_isolation_queue.py

Phase: R0 (traditional momentum + volatility-scaled position weighting)
Owner: Platform / Backtest

Apples-to-apples isolation sweep: holds rebalance cadence, lookback, and
basket size (top_n) CONSTANT across all jobs, varying only the per-ticker
weighting scheme (including a `weight_method=None` equal-weight control
per band/top_n cell). This isolates whether the volatility-weighting
mechanism itself adds value once cadence and basket size are held fixed —
unlike backtest/generate_r0_weighting_queue.py's main sweep, which varies
top_n and only compares weighted variants against each other.

Cadence/lookback chosen to match the historical "amazing" R9 leverage runs
(quarterly, 12mo) for a fair like-for-like comparison, but WITHOUT any
vol_scaling_mode / leverage — top_n and weight_method are the only
sizing/selection levers here.

Band -> Nifty-benchmark mapping: see generate_r0_weighting_queue.py.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

BANDS = [2, 4, 7, 9, 10, 12]  # rank_band_id
TOP_NS = [10, 20]
WEIGHT_METHODS: List[Optional[str]] = [
    None,  # equal-weight control
    "inverse_volatility",
    "inverse_variance",
    "target_volatility",
    "downside_volatility",
]

START_DATE = "2009-01-01"
END_DATE = "2026-08-26"
LOOKBACK_MONTHS = 12
REBALANCE_CADENCE_DAYS = 63  # quarterly
WEIGHT_LOOKBACK_DAYS = 126

OHLCV_SNAPSHOT_DIR = "backtest/cache/ohlcv_snapshots/r0_isolation_2009_2026"

OUT_PATH = Path(__file__).resolve().parent / "queues" / "r0_isolation_2009_2026.json"


def build_jobs() -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    for band_id in BANDS:
        for top_n in TOP_NS:
            for weight_method in WEIGHT_METHODS:
                job: Dict[str, Any] = {
                    "kind": "orchestrator",
                    "channel": "momentum",
                    "start_date": START_DATE,
                    "end_date": END_DATE,
                    "rank_band_id": band_id,
                    "top_n": top_n,
                    "lookback_months": LOOKBACK_MONTHS,
                    "rebalance_cadence_days": REBALANCE_CADENCE_DAYS,
                    "rank_method": "trailing_return",
                    "crash_regime_enabled": False,
                    "strategy_family": "M",
                    "capital_mode": "lump",
                    "initial_capital": 1_000_000,
                    "max_tickers": 800,
                    "min_history_days": 60,
                    "exit_variant": "risk_managed",
                    "defer_db_writes": True,
                    "ohlcv_snapshot_dir": OHLCV_SNAPSHOT_DIR,
                }
                if weight_method is not None:
                    job["weight_method"] = weight_method
                    job["weight_lookback_days"] = WEIGHT_LOOKBACK_DAYS
                jobs.append(job)
    return jobs


def main() -> None:
    jobs = build_jobs()
    payload = {
        "_description": (
            "R0 isolation test: cadence (quarterly/63d), lookback (12mo), and "
            "top_n (10, 20) held constant across all jobs; only weight_method "
            "varies (incl. a None/equal-weight control per band+top_n cell), "
            "to isolate whether volatility weighting itself helps once "
            "cadence/basket-size are fixed. 6 bands x 2 top_n x 5 weight "
            "variants (4 weighted + 1 equal-weight control)."
        ),
        "jobs": jobs,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, indent=1))
    print(f"wrote {len(jobs)} jobs to {OUT_PATH}")


if __name__ == "__main__":
    main()
