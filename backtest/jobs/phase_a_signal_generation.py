#!/usr/bin/env python3
"""
Phase A Signal Generation Orchestrator

Generates signals for 5 Nifty-benchmark bands (M12, M9, M7, M4, M2) with:
- Rank method: jt_momentum
- Top-N: 5 (concentrated)
- Rebalance cadence: 21d (monthly)
- Vol scaling: inverse_volatility (4-mode Moreira-Muir, 126-day lookback)
- Backtest window: 2009-01-01 to 2026-06-30

Output: Populates signal_generation_ledger with 5 rows (one per band), status=completed
         Populates strategy_signals table with ~250 dates × 5 bands × ~800 tickers

Owner: Backtest Platform / Phase A
Date: 2026-08-26
"""

import logging
import os
import subprocess
import sys
from datetime import datetime
from typing import Tuple

logger = logging.getLogger(__name__)


# Phase A band configuration
PHASE_A_BANDS = [
    {"band_id": 12, "rank_start": 551, "rank_end": 800},    # Microcap (top 551-800)
    {"band_id": 9,  "rank_start": 401, "rank_end": 550},    # Smallcap (top 401-550)
    {"band_id": 7,  "rank_start": 251, "rank_end": 400},    # Midcap (top 251-400)
    {"band_id": 4,  "rank_start": 51,  "rank_end": 250},    # Largecap (top 51-250)
    {"band_id": 2,  "rank_start": 1,   "rank_end": 50},     # Nifty 50 (top 1-50)
]

BACKTEST_START_DATE = "2009-01-01"
BACKTEST_END_DATE = "2026-06-30"
RANK_METHOD = "jt_momentum"
TOP_N = 5
REBALANCE_CADENCE_DAYS = 21
LOOKBACK_MONTHS = 6
VOL_SCALING_MODE = "inverse_volatility"
VOL_SCALING_LOOKBACK_DAYS = 126
VOL_SCALING_LEVERAGE_CAP = 9999


def run_signal_generation(
    band_id: int,
    rank_start: int,
    rank_end: int,
) -> Tuple[bool, str]:
    """
    Run signal generation for a single band.

    Returns:
        (success: bool, strategy_key: str)
    """
    strategy_key = (
        f"{RANK_METHOD}_M{band_id}_top{TOP_N}_{REBALANCE_CADENCE_DAYS}d_{VOL_SCALING_MODE}"
    )

    logger.info(f"Starting signal generation: {strategy_key}")

    cmd = [
        "python3", "backtest/jobs/generate_signals.py",
        "--band_id", str(band_id),
        "--rank_start", str(rank_start),
        "--rank_end", str(rank_end),
        "--rank_method", RANK_METHOD,
        "--top_n", str(TOP_N),
        "--rebalance_cadence_days", str(REBALANCE_CADENCE_DAYS),
        "--lookback_months", str(LOOKBACK_MONTHS),
        "--backtest_start_date", BACKTEST_START_DATE,
        "--backtest_end_date", BACKTEST_END_DATE,
        "--vol_scaling_mode", VOL_SCALING_MODE,
        "--vol_scaling_lookback_days", str(VOL_SCALING_LOOKBACK_DAYS),
        "--vol_scaling_leverage_cap", str(VOL_SCALING_LEVERAGE_CAP),
    ]

    try:
        # Ensure PYTHONPATH includes project root for imports
        env = os.environ.copy()
        project_root = "/home/amit/projects/AlphaLens"
        env["PYTHONPATH"] = project_root + ":" + env.get("PYTHONPATH", "")

        result = subprocess.run(cmd, cwd=project_root, capture_output=True, text=True, env=env)
        if result.returncode == 0:
            logger.info(f"✅ {strategy_key}: Completed")
            return True, strategy_key
        else:
            logger.error(f"❌ {strategy_key}: Failed")
            logger.error(f"STDOUT:\n{result.stdout}")
            logger.error(f"STDERR:\n{result.stderr}")
            return False, strategy_key
    except Exception as e:
        logger.error(f"❌ {strategy_key}: Exception: {str(e)}")
        return False, strategy_key


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    logger.info("=" * 80)
    logger.info("Phase A Signal Generation Orchestrator")
    logger.info("=" * 80)
    logger.info(f"Bands: {len(PHASE_A_BANDS)}")
    logger.info(f"Config: top{TOP_N}, {REBALANCE_CADENCE_DAYS}d, {VOL_SCALING_MODE}")
    logger.info(f"Window: {BACKTEST_START_DATE} → {BACKTEST_END_DATE}")
    logger.info("")

    results = {}
    start_time = datetime.now()

    for band_config in PHASE_A_BANDS:
        band_id = band_config["band_id"]
        rank_start = band_config["rank_start"]
        rank_end = band_config["rank_end"]

        logger.info(f"\n[{band_id}] Rank {rank_start:>3d}–{rank_end:<3d} ({rank_end - rank_start + 1:>3d} stocks)")

        success, strategy_key = run_signal_generation(band_id, rank_start, rank_end)
        results[strategy_key] = success

    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds() / 60

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("Phase A Signal Generation Summary")
    logger.info("=" * 80)

    completed = sum(1 for v in results.values() if v)
    total = len(results)

    logger.info(f"Completed: {completed}/{total}")
    logger.info(f"Elapsed: {elapsed:.1f} minutes")
    logger.info("")

    for strategy_key, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        logger.info(f"  {status}  {strategy_key}")

    if completed == total:
        logger.info("\n🎉 Phase A signal generation COMPLETE")
        logger.info("Next step: Run Phase A backtests with --skip-signal-generation flag")
        return 0
    else:
        logger.error(f"\n⚠️  Phase A signal generation INCOMPLETE ({total - completed} failures)")
        return 1


if __name__ == "__main__":
    sys.exit(main())
