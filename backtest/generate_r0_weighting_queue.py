"""
backtest/generate_r0_weighting_queue.py

Phase: R0 (traditional momentum + volatility-scaled position weighting)
Owner: Platform / Backtest

Generates the queue for the R0 sweep: 5 rank bands (Nifty-benchmark
proxies) x 2 top_n x 4 per-ticker weighting modes, monthly (21d)
rebalance, 2009-01-01 through today. Uses the corrected weight_method
mechanism in backtest/adapters/momentum_adapter.py (basket-relative
per-ticker re-weighting via Signal.size_multiplier) — NOT the abandoned
rank_method="equal_weight" approach from the earlier, stashed R0 attempt,
which corrupted stock SELECTION (an equal_weight rank_fn returns a
constant score for every ticker, making the top_n cut effectively
arbitrary) rather than just position sizing.

Band -> Nifty-benchmark mapping (features/momentum_universe.py RANK_BANDS):
    M2  (rank   1-75 ) ~ Nifty 50
    M4  (rank  76-160) ~ Nifty Midcap 150
    M7  (rank 161-275) ~ Nifty Midcap 250
    M9  (rank 276-550) ~ Nifty Smallcap 250
    M12 (rank 551-800) ~ Nifty Microcap
"""

import json
from pathlib import Path
from typing import Any, Dict, List

BANDS = [2, 4, 7, 9, 12]  # rank_band_id
TOP_NS = [5, 7]
WEIGHT_METHODS = ["inverse_volatility", "inverse_variance", "target_volatility", "downside_volatility"]

START_DATE = "2009-01-01"
END_DATE = "2026-08-26"
LOOKBACK_MONTHS = 6
REBALANCE_CADENCE_DAYS = 21  # monthly
WEIGHT_LOOKBACK_DAYS = 126

OHLCV_SNAPSHOT_DIR = "backtest/cache/ohlcv_snapshots/r0_weighting_2009_2026"

OUT_PATH = Path(__file__).resolve().parent / "queues" / "r0_volatility_weighting_2009_2026.json"


def build_jobs() -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    for band_id in BANDS:
        for top_n in TOP_NS:
            for weight_method in WEIGHT_METHODS:
                jobs.append({
                    "kind": "orchestrator",
                    "channel": "momentum",
                    "start_date": START_DATE,
                    "end_date": END_DATE,
                    "rank_band_id": band_id,
                    "top_n": top_n,
                    "lookback_months": LOOKBACK_MONTHS,
                    "rebalance_cadence_days": REBALANCE_CADENCE_DAYS,
                    "weight_method": weight_method,
                    "weight_lookback_days": WEIGHT_LOOKBACK_DAYS,
                    "strategy_family": "M",
                    "capital_mode": "lump",
                    "initial_capital": 1_000_000,
                    "max_tickers": 800,
                    "min_history_days": 60,
                    "exit_variant": "baseline",
                    "defer_db_writes": True,
                    "ohlcv_snapshot_dir": OHLCV_SNAPSHOT_DIR,
                })
    return jobs


def main() -> None:
    jobs = build_jobs()
    payload = {
        "_description": (
            "R0: traditional momentum (top_n rank rotation) with per-ticker "
            "volatility-scaled position weighting, replacing equal-weight. "
            "5 bands x 2 top_n x 4 weight modes, monthly rebalance, 2009-2026."
        ),
        "jobs": jobs,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, indent=1))
    print(f"wrote {len(jobs)} jobs to {OUT_PATH}")


if __name__ == "__main__":
    main()
