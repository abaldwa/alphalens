"""
strategies/migrations/r10_nigam_pandey_momentum.py

Owner: Platform / Architecture (R10 Phase)
Run: PYTHONPATH=$PWD .venv/bin/python -m strategies.migrations.r10_nigam_pandey_momentum [--dry-run]

Registers R10 (Nigam-Pandey Indian long-only momentum) as a distinct strategy variant
from the M-family (M1-M12). Per spec 7.10, R10 tests a single config combination:
- lookback_months=6
- skip_recent_months=1
- rebalance_frequency=quarterly (63 trading days)

R10 is config-driven (no new adapter code): it reuses MomentumAdapter with
rebalance_cadence_days=63 and skip_months=1 parameters already available.
Initial registration focuses on M1 (band 1) and M2 (band 2) to compare against
M-family and R1/R3 baselines, then broadens to all 12 bands if validation passes.

This migration is append-only and idempotent, matching the discipline of the
M-family and R1 migrations.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from strategies.registry import (
    get_strategy,
    register_strategy,
    revise_strategy,
    strategy_key,
)

logger = logging.getLogger(__name__)

SOURCE_REF = "Phase 10: R10 Nigam-Pandey Indian long-only momentum (spec 7.10)"

# R10 uses balanced category (default risk management) with fixed 6-month lookback,
# 1-month skip, and quarterly rebalance (per spec 7.10)
CATEGORY = "balanced"
LOOKBACK_MONTHS = 6
SKIP_MONTHS = 1
REBALANCE_CADENCE_DAYS = 63  # 21 trading days/month * 3 months
TOP_N = 15

# Initial validation is against M1 and M2 (bands 1-2)
# Once validated, can expand to all bands 1-12
INITIAL_BAND_IDS = [1, 2]


def build_rows(
    *,
    band_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """Build R10 strategy registry rows.

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
        rows.append(
            _row(
                band_id=band_id,
                rank_start=rank_start,
                rank_end=rank_end,
            )
        )

    return rows


def variant_name(
    band_id: int,
    rank_start: int,
    rank_end: int,
) -> str:
    """Build R10 variant name.

    Naming: R10_{band_id}_{rank_start}_{rank_end}

    Distinguishes R10 from M-family (M1-M12) by leading R10 prefix, and omits
    lookback/skip/rebalance since R10 holds those constant for Nigam-Pandey testing.
    """
    return f"R10_{band_id}_{rank_start}_{rank_end}"


def _row(
    *,
    band_id: int,
    rank_start: int,
    rank_end: int,
) -> Dict[str, Any]:
    """Build one R10 registry row."""
    name = variant_name(band_id, rank_start, rank_end)
    label = (
        f"R10 (Nigam-Pandey) - Band {band_id} (rank {rank_start}-{rank_end}) - "
        f"6mo lookback, 1mo skip, quarterly rebalance"
    )

    return {
        "channel": "momentum",
        "name": name,
        "display_label": label,
        "description": (
            f"R10: Nigam-Pandey Indian long-only momentum, market-cap rank band "
            f"{rank_start}-{rank_end} ({CATEGORY} filters), 6-month lookback, "
            f"skip 1 recent month, quarterly rebalance (63 trading days), top {TOP_N} holdings. "
            f"Distinct from M-family to isolate Nigam-Pandey effect with skip-month variant."
        ),
        "category": CATEGORY,
        "definition": {
            "category": CATEGORY,
            "band_id": band_id,
            "rank_start": rank_start,
            "rank_end": rank_end,
            "lookback_months": LOOKBACK_MONTHS,
            "skip_months": SKIP_MONTHS,
            "rebalance_cadence_days": REBALANCE_CADENCE_DAYS,
            "top_n": TOP_N,
            # Phase 0 params
            "rank_method": "trailing_return",  # Default; no custom rank function
        },
        # R10 has no entry predicates: ranks the band's universe by
        # 6-month momentum (with 1-month skip) and buys the top N.
        "entry_criterion": [],
        "exit_criterion": {
            # Same exit logic as M-family: plain list swap on raw momentum
            "variant": "rank_exit",
            "exit_rank": TOP_N,
            "conditions": [],
        },
        # R10 uses same balanced filter set as M-family balanced category
        "filter_ids": [
            "adtv_floor",
            "adtv_capped_sizing",
            "circuit_lock_proxy",
            "quality_gate",
        ],
        "universe_spec": "momentum_rank_band",
        "status": "active",
        "source_ref": SOURCE_REF,
    }


def migrate(
    *,
    db_path: Optional[Path] = None,
    dry_run: bool = False,
    band_ids: Optional[List[int]] = None,
    created_by: str = "R10_Phase",
) -> Dict[str, int]:
    """Register or revise R10 variants. Idempotent.

    Args:
        db_path: Path to backtest DuckDB. If None, uses config default.
        dry_run: If True, log what would be done without writing.
        band_ids: Which bands to register. If None, uses INITIAL_BAND_IDS (M1, M2).
        created_by: Attribution for this migration.

    Returns:
        Stats dict: {registered, revised, unchanged}.
    """
    rows = build_rows(band_ids=band_ids)
    stats = {"registered": 0, "revised": 0, "unchanged": 0}

    for row in rows:
        key = strategy_key(row["channel"], row["name"])
        existing = get_strategy(key, db_path=db_path)

        if existing is None:
            if not dry_run:
                register_strategy(db_path=db_path, created_by=created_by, **row)
            stats["registered"] += 1
            logger.info(f"Registered {key}")
            continue

        changes = _drift(existing, row)
        if not changes:
            stats["unchanged"] += 1
            logger.debug(f"Unchanged {key}")
            continue

        if not dry_run:
            revise_strategy(
                key,
                db_path=db_path,
                created_by=created_by,
                source_ref=SOURCE_REF,
                **changes,
            )
        stats["revised"] += 1
        logger.info(f"Revised {key}")

    return stats


def _drift(existing: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]:
    """Detect fields that have changed since last registration."""
    changes = {}
    for field in (
        "display_label",
        "description",
        "category",
        "definition",
        "entry_criterion",
        "exit_criterion",
        "filter_ids",
    ):
        if existing.get(field) != row[field]:
            changes[field] = row[field]
    return changes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Log without writing")
    parser.add_argument(
        "--all-bands",
        action="store_true",
        help="Register R10 for all 12 bands (not just M1/M2)",
    )
    parser.add_argument("--db-path", type=Path, default=None, help="Path to backtest DuckDB")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    band_ids = None
    if args.all_bands:
        band_ids = list(range(1, 13))
        logger.info("Registering R10 for all 12 bands")
    else:
        logger.info("Registering R10 for M1/M2 bands only (1, 2)")

    stats = migrate(db_path=args.db_path, dry_run=args.dry_run, band_ids=band_ids)

    logger.info(
        "%sR10 registration: registered=%d revised=%d unchanged=%d",
        "[dry-run] " if args.dry_run else "",
        stats["registered"],
        stats["revised"],
        stats["unchanged"],
    )


if __name__ == "__main__":
    main()
