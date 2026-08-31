"""
strategies/migrations/r13_bollinger_mean_reversion.py

Owner: Platform / Architecture (R13 Phase)
Run: PYTHONPATH=$PWD .venv/bin/python -m strategies.migrations.r13_bollinger_mean_reversion [--dry-run]

Registers R13 (Bollinger Band mean-reversion) as a contrarian strategy.
Per spec 7.13, R13 tests the mean-reversion effect using Bollinger Bands
(Keltner/Bollinger squeeze reversals) on the Indian equity market across
market-cap bands.

R13 is config-driven: reuses MomentumAdapter with rank_method="bollinger_mean_reversion"
that scores stocks based on proximity to lower Bollinger Band (oversold condition).
Initial validation on bands 1-2, expandable to all 12 bands post-validation.

This migration is append-only and idempotent, matching the discipline of R1/R10/R11/R12.
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

SOURCE_REF = "Phase 13: R13 Bollinger Band mean-reversion (spec 7.13)"

# R13 uses balanced category (default risk management) and focuses on
# Bollinger Band oversold mean-reversion, holding other parameters constant
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
    """Build R13 strategy registry rows for Bollinger Band mean-reversion.

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
            "strategy_id": f"r13_bollinger_reversion_band{band_id}",
            "phase": "Phase 13",
            "source_ref": SOURCE_REF,
            "strategy_family": "R",
            "category": CATEGORY,
            "rebalance_period": REBALANCE_PERIOD,
            "rank_band_id": band_id,
            "rank_band_name": f"M{band_id}",
            "rank_range": f"{rank_start}-{rank_end}",
            "top_n": TOP_N,
            "adapter": "MomentumAdapter",
            "rank_method": "bollinger_mean_reversion",
            "lookback_days": 20,
            "config_hash": None,
            "status": "active",
            "notes": (
                f"R13 Bollinger Band mean-reversion on band {band_id} (ranks {rank_start}-{rank_end}). "
                "Contrarian: selects oversold stocks near lower Bollinger Band."
            ),
        }
        rows.append(row)

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Register R13 (Bollinger Band mean-reversion) strategies"
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
        logger.info(f"[DRY-RUN] Would register {len(rows)} R13 strategies")
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

    logger.info(f"✅ Registered {len(rows)} R13 strategies (bands: {band_ids})")


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    main()
