"""
strategies/migrations/fundamental.py

Owner: Platform / Architecture (F7)
Run: PYTHONPATH=$PWD .venv/bin/python -m strategies.migrations.fundamental [--dry-run]

Migrates the Fundamental strategy definitions into strategy_registry.

Sources, all in features/fundamental_composites.py unless noted:

    STRATEGY_CATALOG          26 strategies, each tagged kind=
                              preset | composite_score | bespoke
    SCREENER_PRESETS          9 threshold sets over z-scored ratios
    SCORE_FUNCTIONS           22 ranking functions
    PRESET_EXCLUDED_SECTORS   per-preset sector exclusions
    BESPOKE_PRESETS           declared in backtest/adapters/fundamental_adapter.py,
                              not in the composites module at all

The three kinds need three different treatments, and pretending otherwise is
how this would go wrong:

  preset          A THRESHOLD SCREEN. SCREENER_PRESETS[p] maps a z-scored
                  ratio to a sign-adjusted threshold, evaluated as
                  `signed_v >= abs(threshold)` where signed_v flips sign when
                  the threshold is negative. That is exactly a predicate list,
                  so these migrate losslessly:

                      threshold >= 0  ->  {feature, "gte", threshold}
                      threshold <  0  ->  {feature, "lte", threshold}

                  e.g. quality_compounder's debt_to_equity: -0.5 means
                  "-v >= 0.5", i.e. v <= -0.5 -- at least half a sector-sigma
                  LESS levered than peers. Encoding that as gte would invert
                  the screen and silently select the most indebted names.

  composite_score A RANKING, not a screen. There is no threshold to express;
                  the strategy is "score every ticker by this function and
                  take the top N". Recorded with an empty entry_criterion and
                  the score function named in the definition -- the same
                  honest-empty as Momentum's rank-and-take-top-N.

  bespoke         Imperative Python with no declarative form at all
                  (piotroski_on_value, margin_of_safety, net_net). Registered
                  with an empty criterion and a `bespoke_ref`, and flagged in
                  the definition so A95's guard test can tell "not yet
                  expressible" apart from "genuinely has no conditions".

Sector exclusions become `not_in` predicates -- the op added to the grammar
for precisely this, since PRESET_EXCLUDED_SECTORS currently lives outside the
condition system entirely as a Python set.

Fundamental persists no signals today; the A94 writer is wired in A95.
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
    "features/fundamental_composites.py + backtest/adapters/fundamental_adapter.py"
)

# Fundamental strategies rebalance on fundamentals, which update quarterly.
EXIT_VARIANT = "rank_rebalance"

# Mirrors backtest/adapters/fundamental_adapter.py's
# _PRESETS_NEEDING_LIQUIDITY_FLOOR. Imported rather than duplicated so the two
# cannot drift while the adapter still owns the runtime behaviour; once the
# adapter reads filter_ids from the registry (A95) its copy is deleted and
# this import goes with it.
from backtest.adapters.fundamental_adapter import (  # noqa: E402
    _PRESETS_NEEDING_LIQUIDITY_FLOOR as PRESETS_NEEDING_LIQUIDITY_FLOOR,
)


def preset_predicates(preset: str) -> List[Dict[str, Any]]:
    """Turn SCREENER_PRESETS[preset] into predicates, preserving the sign
    convention in matches_screener_preset exactly.

    The convention: a NEGATIVE threshold means the ratio is inverted before
    comparison (lower is better), so `-v >= abs(t)`, which is `v <= t`.
    Getting this backwards would invert every "lower is better" screen --
    selecting the most indebted, most expensive names while the label still
    said quality or value.
    """
    from features.fundamental_composites import (
        PRESET_EXCLUDED_SECTORS,
        SCREENER_PRESETS,
    )

    preds: List[Dict[str, Any]] = []
    for feature, threshold in SCREENER_PRESETS[preset].items():
        if threshold < 0:
            preds.append({"feature": feature, "op": "lte", "value": threshold})
        else:
            preds.append({"feature": feature, "op": "gte", "value": threshold})

    excluded = PRESET_EXCLUDED_SECTORS.get(preset)
    if excluded:
        # sorted() so the stored predicate is stable across runs -- a set's
        # iteration order would make the migration look like it drifted every
        # time and inflate the version history.
        preds.append({"feature": "sector", "op": "not_in", "value": sorted(excluded)})
    return preds


def _uncatalogued() -> Dict[str, Dict[str, Any]]:
    """The runnable-but-undeclared names, as STRATEGY_CATALOG-shaped metadata.

    [A95-R1, 2026-08-15] FundamentalAdapter validates --preset against
    SCREENER_PRESETS | SCORE_FUNCTIONS | BESPOKE_PRESETS -- 30 names -- while
    STRATEGY_CATALOG, and therefore this migration, described 26. The other four
    were runnable strategies with no registry row: no declared definition, no
    filter list, no version. A run using one could not be explained by the
    report's Definition card, could not be deployed through A91, and wrote
    ledger signals under a key that resolved to nothing.

    Confirmed 2026-08-14 that this was NOT an incomplete F7 migration: the
    registry mirrored STRATEGY_CATALOG exactly, empty in both directions. These
    four were simply never catalogued.

    Registering them (the user's decision, 2026-08-15, over the alternative of
    keeping them as an explicit escape hatch) makes the registry authoritative
    for every name the adapter will accept, which is what lets that validation
    read the registry instead of the dicts.

    `backtested: False` is asserted rather than guessed -- none of these four
    appear in the 26-strategy fundamental sweep. Categories mirror what the
    catalogue uses for their nearest kin so they group sensibly in the picker.
    """
    return {
        # SCORE_FUNCTIONS-only: rankings, so empty entry_criterion by the same
        # rule composite_score entries already follow.
        "growth": {
            "kind": "composite_score",
            "label": "Growth (raw score)",
            "description": "Rank by the growth composite score. Uncatalogued ranking function, registered by A95-R1 so every runnable preset has a definition.",
            "category": "Growth",
            "backtested": False,
        },
        "quality": {
            "kind": "composite_score",
            "label": "Quality (raw score)",
            "description": "Rank by the quality composite score. Uncatalogued ranking function, registered by A95-R1 so every runnable preset has a definition.",
            "category": "Quality",
            "backtested": False,
        },
        # SCREENER_PRESETS-only: real threshold screens, so these migrate
        # losslessly into predicates via preset_predicates(), sign convention
        # included -- quality_compounder's negative debt_to_equity threshold is
        # exactly the case that docstring warns about.
        "quality_compounder": {
            "kind": "preset",
            "label": "Quality Compounder",
            "description": "Threshold screen for consistently profitable, low-leverage compounders. Uncatalogued preset, registered by A95-R1.",
            "category": "Quality",
            "backtested": False,
        },
        "turnaround": {
            "kind": "preset",
            "label": "Turnaround",
            "description": "Threshold screen for businesses recovering from a weak base. Uncatalogued preset, registered by A95-R1.",
            "category": "Contrarian",
            "backtested": False,
        },
    }


def build_rows() -> List[Dict[str, Any]]:
    """One payload per STRATEGY_CATALOG entry, plus the four uncatalogued but
    runnable names (see _uncatalogued)."""
    from features.fundamental_composites import (
        SCORE_FUNCTIONS,
        SCREENER_PRESETS,
        STRATEGY_CATALOG,
    )

    try:
        from backtest.adapters.fundamental_adapter import BESPOKE_PRESETS
    except ImportError:  # pragma: no cover - adapter import is heavy
        BESPOKE_PRESETS = ()

    catalogue = dict(STRATEGY_CATALOG)
    for name, meta in _uncatalogued().items():
        if name in catalogue:
            # It graduated into STRATEGY_CATALOG upstream. The catalogue wins;
            # carrying a second description here would be the drift this whole
            # migration exists to remove.
            continue
        catalogue[name] = meta

    rows: List[Dict[str, Any]] = []
    for name, meta in catalogue.items():
        kind = meta.get("kind")
        definition: Dict[str, Any] = {
            "kind": kind,
            "backtested": bool(meta.get("backtested")),
        }
        entry: List[Dict[str, Any]] = []

        if kind == "preset":
            if name not in SCREENER_PRESETS:
                raise ValueError(
                    f"{name} is kind=preset but absent from SCREENER_PRESETS"
                )
            entry = preset_predicates(name)
            definition["preset"] = name

        elif kind == "composite_score":
            if name not in SCORE_FUNCTIONS:
                raise ValueError(
                    f"{name} is kind=composite_score but absent from SCORE_FUNCTIONS"
                )
            # A ranking, not a screen: nothing to express as a predicate.
            definition["score_function"] = name

        elif kind == "bespoke":
            # Imperative Python with no declarative form. Flagged so A95's
            # guard can distinguish "not yet expressible" from "no conditions".
            definition["bespoke_ref"] = (
                f"backtest.adapters.fundamental_adapter:{name}"
                if name in BESPOKE_PRESETS
                else f"features.fundamental_composites:{name}"
            )
            definition["not_yet_declarative"] = True

        else:
            raise ValueError(f"{name}: unknown kind {kind!r}")

        rows.append(
            {
                "channel": "fundamental",
                "name": name,
                "display_label": meta.get("label") or name,
                "description": meta.get("description"),
                "category": meta.get("category"),
                "definition": definition,
                "entry_criterion": entry,
                "exit_criterion": {"variant": EXIT_VARIANT, "conditions": []},
                # market_cap_floor, NOT adtv_floor: the adapter gates on
                # LIQUIDITY_FLOOR_MARKET_CAP_CR, which is a size threshold,
                # not a traded-volume one. Both get called "the liquidity
                # floor" in conversation and they are not the same filter.
                #
                # It IS part of these presets' identity rather than run
                # config: _PRESETS_NEEDING_LIQUIDITY_FLOOR is keyed by preset
                # name, so it applies to these three and no others, which a
                # run-level filter could not express.
                "filter_ids": (
                    ["market_cap_floor"]
                    if name in PRESETS_NEEDING_LIQUIDITY_FLOOR
                    else []
                ),
                "status": "active",
                "source_ref": SOURCE_REF,
            }
        )
    return rows


def migrate(
    *,
    db_path: Optional[Path] = None,
    dry_run: bool = False,
    created_by: str = "F7",
) -> Dict[str, int]:
    """Register/revise, idempotently. Same contract as the other migrations."""
    rows = build_rows()
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
        # filter_ids must be compared too, or a corrected filter assignment
        # silently never reaches an already-registered row.
        "filter_ids",
    ):
        if existing.get(field) != row[field]:
            changes[field] = row[field]
    return changes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stats = migrate(db_path=args.db_path, dry_run=args.dry_run)
    logger.info(
        "%sregistered=%d revised=%d unchanged=%d",
        "[dry-run] " if args.dry_run else "",
        stats["registered"],
        stats["revised"],
        stats["unchanged"],
    )
    bespoke = [
        r["name"] for r in build_rows() if r["definition"].get("not_yet_declarative")
    ]
    if bespoke:
        logger.warning(
            "%d strategies have no declarative form and carry a bespoke_ref: %s",
            len(bespoke),
            ", ".join(bespoke),
        )


if __name__ == "__main__":
    main()
