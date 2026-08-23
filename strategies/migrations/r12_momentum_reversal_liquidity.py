"""
strategies/migrations/r12_momentum_reversal_liquidity.py

Owner: Platform / Architecture (R12 Phase)
Run: PYTHONPATH=$PWD .venv/bin/python -m strategies.migrations.r12_momentum_reversal_liquidity [--dry-run]

Registers R12 (momentum/reversal/liquidity interaction) as distinct reversal-signal variants.
Per spec 7.12, R12 tests interaction of existing signals (1-month reversal — new,
3/6/12-month momentum, 12-1, 12-7 — all existing) across liquidity quintiles.

R12 is config-driven and signal-driven: it reuses MomentumAdapter with the new
trailing_reversal_1mo rank method. The momentum variants (3/6/12mo, 12-1, 12-7)
are already covered by M-family registrations; this migration focuses on reversal.

This migration is append-only and idempotent, following the R10/R1 discipline.
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

SOURCE_REF = "Phase 12: R12 Momentum/Reversal/Liquidity Interaction (spec 7.12)"

# R12 reversal uses balanced category (default risk management) with
# 1-month (21-day) lookback, no skip, and quarterly rebalance (per spec 7.12)
CATEGORY = "balanced"
LOOKBACK_DAYS = 21  # 1 month of trading days
TOP_N = 15

# Initial validation is against representative bands (mid-cap band 9, large-cap band 1)
# Once validated, can expand to all bands 1-12
INITIAL_BAND_IDS = [1, 9]


def build_rows(
    *,
    band_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """Build R12 strategy registry rows for reversal variants.

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
    """Build R12 variant name.

    Naming: R12_reversal_1mo_{band_id}_{rank_start}_{rank_end}

    Distinguishes R12 from M-family and other R variants by leading R12 prefix
    and reversal signal identifier.
    """
    return f"R12_reversal_1mo_{band_id}_{rank_start}_{rank_end}"


def _row(
    *,
    band_id: int,
    rank_start: int,
    rank_end: int,
) -> Dict[str, Any]:
    """Build one R12 registry row for reversal signal."""
    name = variant_name(band_id, rank_start, rank_end)
    label = (
        f"R12 (Reversal) - Band {band_id} (rank {rank_start}-{rank_end}) - "
        f"1mo reversal (21-day), quarterly rebalance"
    )

    return {
        "channel": "momentum",
        "name": name,
        "display_label": label,
        "description": (
            f"R12: 1-month reversal (mean reversion), market-cap rank band "
            f"{rank_start}-{rank_end} ({CATEGORY} filters), 21-day lookback, "
            f"quarterly rebalance (63 trading days), top {TOP_N} holdings. "
            f"Tests interaction of reversal signal with liquidity quintiles "
            f"vs existing momentum (3/6/12mo, 12-1, 12-7) signals."
        ),
        "category": CATEGORY,
        "definition": {
            "category": CATEGORY,
            "band_id": band_id,
            "rank_start": rank_start,
            "rank_end": rank_end,
            "lookback_days": LOOKBACK_DAYS,
            "rebalance_cadence_days": 63,  # Quarterly
            "top_n": TOP_N,
            "rank_method": "trailing_reversal_1mo",  # New reversal signal
        },
        # R12 reversal has no entry predicates: ranks the band's universe by
        # 1-month reversal (low returns = strong reversal signal) and buys the top N.
        "entry_criterion": [],
        "exit_criterion": {
            # Same exit logic as M-family: plain list swap on reversal signal
            "variant": "rank_exit",
            "exit_rank": TOP_N,
            "conditions": [],
        },
        # R12 uses same balanced filter set as M-family balanced category
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
    created_by: str = "R12_Phase",
) -> Dict[str, int]:
    """Register or revise R12 reversal variants. Idempotent.

    Args:
        db_path: Path to backtest DuckDB. If None, uses config default.
        dry_run: If True, log what would be done without writing.
        band_ids: Which bands to register. If None, uses INITIAL_BAND_IDS (bands 1, 9).
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
        help="Register R12 for all 12 bands (not just bands 1, 9)",
    )
    parser.add_argument("--db-path", type=Path, default=None, help="Path to backtest DuckDB")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    band_ids = None
    if args.all_bands:
        band_ids = list(range(1, 13))
        logger.info("Registering R12 for all 12 bands")
    else:
        logger.info("Registering R12 for representative bands only (1, 9)")

    stats = migrate(db_path=args.db_path, dry_run=args.dry_run, band_ids=band_ids)

    logger.info(
        "%sR12 registration: registered=%d revised=%d unchanged=%d",
        "[dry-run] " if args.dry_run else "",
        stats["registered"],
        stats["revised"],
        stats["unchanged"],
    )


if __name__ == "__main__":
    main()
