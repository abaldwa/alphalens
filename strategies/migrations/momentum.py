"""
strategies/migrations/momentum.py

Owner: Platform / Architecture (ML41)
Run: PYTHONPATH=$PWD .venv/bin/python -m strategies.migrations.momentum [--dry-run]

Migrates the Momentum strategy definitions into strategy_registry.

Unlike Technical (T15), where 63 named templates map one-to-one onto rows,
Momentum has no named strategies at all. What exists is:

  * four cumulative FILTER PRESETS in
    features/momentum_strategy.py::build_category_presets -- all_risk (no
    filters), balanced (+liquidity, ADTV sizing cap, circuit proxy, quality
    gate), risk_managed (+regime disabling), max_defensive (+size/beta
    orthogonalization);

  * a SWEEP GRID assembled inline in scripts/run_momentum_dynamic_report.py
    from RANK_BANDS x categories x LOOKBACK_MONTHS x REBALANCE_PERIODS x
    TOP_N_OPTIONS;

  * a variant_id f-string built at report time, which is the closest thing
    Momentum has to a strategy identity and exists only inside the report.

So a Momentum "strategy" is the CROSS PRODUCT of a preset and a grid point,
and this migration materialises that cross product as registry rows -- which
is what finally gives Momentum a stable identity (A89) instead of a dict key,
a JSON sweep row, or a generated string, depending on which code path you ask.

Naming
------
`name` reproduces the report's existing variant_id verbatim:

    {category}_b{band}_{rank_start}-{rank_end}_lb{N}mo_{rebalance}_top{N}

so a registry row and a report row can be matched without a translation
table, and every frontend deep link that already exists keeps working.

Filters, unlike Technical
-------------------------
Momentum's filters ARE part of the strategy definition -- the category IS the
filter set -- so unlike T15 these rows do carry filter_ids, resolved against
the A93 registry. That is the difference between a filter chosen per run
(Technical) and a filter that defines the strategy (Momentum), and it is why
attaching them here is truthful where attaching them there was not.

Grid size
---------
5 bands x 4 categories x 4 lookbacks x 5 rebalance periods x 3 top_n = 1,200
rows. Large but bounded, deterministic, and exactly what the sweep runs -- the
registry should describe what is actually backtested, not a tidier subset.
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

SOURCE_REF = (
    "features/momentum_strategy.py::build_category_presets + "
    "scripts/run_momentum_dynamic_report.py sweep grid"
)

# The four cumulative presets, expressed as filter_registry ids rather than as
# kwargs dicts. This mirrors build_category_presets' layering exactly: each
# level is the previous one plus what it adds.
CATEGORY_FILTERS: Dict[str, List[str]] = {
    "all_risk": [],
    "balanced": [
        "adtv_floor",
        "adtv_capped_sizing",
        "circuit_lock_proxy",
        "quality_gate",
    ],
    "risk_managed": [
        "adtv_floor",
        "adtv_capped_sizing",
        "circuit_lock_proxy",
        "quality_gate",
        "hmm_regime",
    ],
    "max_defensive": [
        "adtv_floor",
        "adtv_capped_sizing",
        "circuit_lock_proxy",
        "quality_gate",
        "hmm_regime",
        "size_beta_orthogonalized",
    ],
}

CATEGORY_LABELS = {
    "all_risk": "All Risk",
    "balanced": "Balanced",
    "risk_managed": "Risk-Managed",
    "max_defensive": "Max Defensive",
}

# Momentum ranks its own universe by market cap into bands; it does not use
# the ADTV-ranked universe the other channels screen. Recorded so a report can
# say what universe a number came from rather than leaving it implied.
UNIVERSE_SPEC = "momentum_rank_band"


def build_rows(
    *,
    include_grid: bool = True,
) -> List[Dict[str, Any]]:
    """One payload per (category, band, lookback, rebalance, top_n).

    include_grid=False returns only the four category presets, each with a
    null grid point -- useful for inspecting the preset layering without
    1,200 rows of cross product.
    """
    from features.momentum_universe import RANK_BANDS
    from scripts.run_momentum_dynamic_report import (
        GRACE_CYCLES,
        REBALANCE_PERIODS,
        TOP_N_OPTIONS,
    )
    from features.momentum_signal import LOOKBACK_MONTHS

    rows: List[Dict[str, Any]] = []

    if not include_grid:
        for category, filter_ids in CATEGORY_FILTERS.items():
            rows.append(
                _row(
                    category=category,
                    filter_ids=filter_ids,
                    band_id=None,
                    rank_start=None,
                    rank_end=None,
                    lookback_months=None,
                    rebalance=None,
                    top_n=None,
                    grace_cycles=GRACE_CYCLES,
                )
            )
        return rows

    for band_id, rank_start, rank_end in RANK_BANDS:
        for category, filter_ids in CATEGORY_FILTERS.items():
            for lookback_months in LOOKBACK_MONTHS:
                for rebalance in REBALANCE_PERIODS:
                    for top_n in TOP_N_OPTIONS:
                        rows.append(
                            _row(
                                category=category,
                                filter_ids=filter_ids,
                                band_id=band_id,
                                rank_start=rank_start,
                                rank_end=rank_end,
                                lookback_months=lookback_months,
                                rebalance=rebalance,
                                top_n=top_n,
                                grace_cycles=GRACE_CYCLES,
                            )
                        )
    return rows


def variant_name(
    category: str,
    band_id: int,
    rank_start: int,
    rank_end: int,
    lookback_months: int,
    rebalance: str,
    top_n: int,
) -> str:
    """The report's existing variant_id, reproduced verbatim so registry rows
    and report rows match without a translation table."""
    return (
        f"{category}_b{band_id}_{rank_start}-{rank_end}_"
        f"lb{lookback_months}mo_{rebalance}_top{top_n}"
    )


def _row(
    *,
    category: str,
    filter_ids: List[str],
    band_id: Optional[int],
    rank_start: Optional[int],
    rank_end: Optional[int],
    lookback_months: Optional[int],
    rebalance: Optional[str],
    top_n: Optional[int],
    grace_cycles: int,
) -> Dict[str, Any]:
    if band_id is None:
        name = f"preset_{category}"
        label = f"{CATEGORY_LABELS[category]} (preset)"
    else:
        name = variant_name(
            category, band_id, rank_start, rank_end, lookback_months, rebalance, top_n
        )
        label = (
            f"{CATEGORY_LABELS[category]} - Top{top_n} - {lookback_months}mo - "
            f"{rebalance} - rank {rank_start}-{rank_end}"
        )

    return {
        "channel": "momentum",
        "name": name,
        "display_label": label,
        "description": (
            f"Momentum, {CATEGORY_LABELS[category]} filter preset"
            + (
                ""
                if band_id is None
                else f", market-cap rank band {rank_start}-{rank_end}, "
                f"{lookback_months}-month lookback, {rebalance} rebalance, "
                f"top {top_n} holdings"
            )
        ),
        "category": category,
        "definition": {
            "category": category,
            "band_id": band_id,
            "rank_start": rank_start,
            "rank_end": rank_end,
            "lookback_months": lookback_months,
            "rebalance_frequency": rebalance,
            "top_n": top_n,
            "grace_cycles": grace_cycles,
        },
        # Momentum has no entry predicates: it ranks the band's universe by
        # momentum score and buys the top N. The selection rule is the
        # definition (top_n over a ranked band), not a per-ticker condition,
        # so an empty entry_criterion is the truthful representation rather
        # than a gap.
        "entry_criterion": [],
        "exit_criterion": {
            # Momentum exits by rank: a holding leaves when it falls out of
            # the top N and its grace cycles are exhausted.
            "variant": "rank_grace",
            "grace_cycles": grace_cycles,
            "exit_rank": top_n,
            "conditions": [],
        },
        "filter_ids": filter_ids,
        "universe_spec": UNIVERSE_SPEC,
        "status": "active",
        "source_ref": SOURCE_REF,
    }


def migrate(
    *,
    db_path: Optional[Path] = None,
    dry_run: bool = False,
    include_grid: bool = True,
    created_by: str = "ML41",
) -> Dict[str, int]:
    """Register every variant not already present; revise those that drifted.
    Idempotent, same contract as the Technical migration."""
    rows = build_rows(include_grid=include_grid)
    stats = {"registered": 0, "revised": 0, "unchanged": 0}

    for row in rows:
        key = strategy_key(row["channel"], row["name"])
        existing = get_strategy(key, db_path=db_path)

        if existing is None:
            if not dry_run:
                register_strategy(db_path=db_path, created_by=created_by, **row)
            stats["registered"] += 1
            continue

        changes = _drift(existing, row)
        if not changes:
            stats["unchanged"] += 1
            continue
        if not dry_run:
            revise_strategy(
                key, db_path=db_path, created_by=created_by, source_ref=SOURCE_REF, **changes
            )
        stats["revised"] += 1

    return stats


def _drift(existing: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]:
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
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--presets-only",
        action="store_true",
        help="register the 4 category presets without the 1,200-row sweep grid",
    )
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stats = migrate(
        db_path=args.db_path, dry_run=args.dry_run, include_grid=not args.presets_only
    )
    logger.info(
        "%sregistered=%d revised=%d unchanged=%d",
        "[dry-run] " if args.dry_run else "",
        stats["registered"],
        stats["revised"],
        stats["unchanged"],
    )


if __name__ == "__main__":
    main()
