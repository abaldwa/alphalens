"""
strategies/migrations/r11_52wk_high_momentum.py

Owner: Platform / Architecture (R11 Phase)
Run: PYTHONPATH=$PWD .venv/bin/python -m strategies.migrations.r11_52wk_high_momentum [--dry-run]

Registers R11 (52-week-high momentum) as a reversal/contrarian (mean-reversion) strategy.
Per spec 7.11, R11 tests the 52-week-high effect (George & Hwang 2004) on the
Indian equity market across market-cap bands. B-029 FIX: R11 is REVERSAL (buy oversold),
not trend-following (buy winners).

R11 is config-driven: reuses MomentumAdapter with fixed category/rebalance/top_n
but uses rank_method="pct_of_52wk_high" to rank on proximity to 52-week highs
(reversal/contrarian, selecting LOWEST scores: stocks far from highs, oversold).
Initial validation on bands 1-2, expandable to all 12 bands post-validation.

This migration is append-only and idempotent, matching the discipline of R1/R10/R12.
"""

from __future__ import annotations

import argparse
import logging
from typing import Any, Dict, List, Optional

from strategies.registry import (
    get_strategy,
    register_strategy,
    revise_strategy,
    strategy_key,
)

logger = logging.getLogger(__name__)

SOURCE_REF = "Phase 11: R11 52-week-high momentum (spec 7.11)"

# R11 uses balanced category (default risk management) and focuses on
# 52-week-high effect, holding other parameters constant
CATEGORY = "balanced"
REBALANCE_PERIOD = "monthly"
TOP_N = 15

# Initial validation is against M1 and M2 (bands 1-2)
# Once validated, can expand to all bands 1-12
INITIAL_BAND_IDS = [1, 2]


def build_rows(
    *,
    band_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """Build R11 strategy registry rows for 52-week-high momentum.

    Args:
        band_ids: Which band IDs to register. If None, uses INITIAL_BAND_IDS.
                 Pass [1, 2, 3, ..., 12] to register all bands.

    Returns:
        List of row dicts ready to pass to register_strategy().
    """
    from features.momentum_universe import RANK_BANDS

    if band_ids is None:
        band_ids = INITIAL_BAND_IDS

    # Invert RANK_BANDS list to get (band_id -> rank_start, rank_end)
    band_lookup = {band_id: (rank_start, rank_end) for band_id, rank_start, rank_end in RANK_BANDS}

    rows: List[Dict[str, Any]] = []
    for band_id in band_ids:
        if band_id not in band_lookup:
            logger.warning(f"Band {band_id} not found in RANK_BANDS, skipping")
            continue

        rank_start, rank_end = band_lookup[band_id]

        row: Dict[str, Any] = {
            "strategy_id": f"r11_52wk_high_band{band_id}",
            "phase": "Phase 11",
            "source_ref": SOURCE_REF,
            "strategy_family": "R",
            "category": CATEGORY,
            "rebalance_period": REBALANCE_PERIOD,
            "rank_band_id": band_id,
            "rank_band_name": f"M{band_id}",
            "rank_range": f"{rank_start}-{rank_end}",
            "top_n": TOP_N,
            "adapter": "MomentumAdapter",
            "rank_method": "pct_of_52wk_high",
            "lookback_days": 252,
            "config_hash": None,
            "status": "active",
            "notes": (
                f"R11 52-week-high reversal on band {band_id} (ranks {rank_start}-{rank_end}). "
                "Reversal/contrarian: selects stocks FAR from their 52-week highs (oversold, mean-reversion)."
            ),
        }
        rows.append(row)

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Register R11 (52-week-high momentum) strategies"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print rows without registering",
    )
    parser.add_argument(
        "--all-bands",
        action="store_true",
        help="Register all 12 bands (default: bands 1-2 validation only)",
    )
    args = parser.parse_args()

    band_ids = list(range(1, 13)) if args.all_bands else INITIAL_BAND_IDS
    rows = build_rows(band_ids=band_ids)

    if args.dry_run:
        import json
        print(json.dumps(rows, indent=2))
        logger.info(f"[DRY-RUN] Would register {len(rows)} R11 strategies")
        return

    for row in rows:
        try:
            key = strategy_key(
                strategy_id=row["strategy_id"],
                phase=row["phase"],
            )
            existing = get_strategy(key)
            if existing:
                logger.info(f"Updating {key}...")
                revise_strategy(key, row)
            else:
                logger.info(f"Registering {key}...")
                register_strategy(row)
        except Exception as e:
            logger.error(f"Failed to register {row['strategy_id']}: {e}")
            raise

    logger.info(f"✅ Registered {len(rows)} R11 strategies (bands: {band_ids})")


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    main()
