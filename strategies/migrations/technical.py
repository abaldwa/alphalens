"""
strategies/migrations/technical.py

Owner: Platform / Architecture (T15)
Run: PYTHONPATH=$PWD .venv/bin/python -m strategies.migrations.technical [--dry-run]

Migrates the 63 ScreenerTemplates in
systems/technical_analysis/screener/templates.py into strategy_registry rows.

This migration goes FIRST of the four on purpose. Technical's conditions are
already `{"feature", "op", "value"|"feature2"}` dicts, so this is a data move
rather than a rewrite -- which makes it the honest test of whether A92's
predicate grammar can carry real definitions before Momentum and Fundamental
(whose rules are imperative Python) have to bend to it.

What maps where
---------------
    ScreenerTemplate.name              -> name, and strategy_key "technical:<name>"
    .category                          -> category
    .description                       -> display_label
    .conditions                        -> entry_criterion (verbatim)
    .exit_stop_pct/.exit_target_pct/   -> exit_criterion
      .exit_max_hold_days
    TEMPLATE_STYLE[name]               -> definition.style
    .key_display_features              -> definition.key_display_features

The exit params on the dataclass are populated at import time from
STYLE_EXIT_PARAMS via TEMPLATE_STYLE, so reading them off the template picks
up the style table without this module having to re-derive it.

Two things this migration deliberately does NOT do
--------------------------------------------------
1. It does not delete templates.py. The screener, the alert checker and the
   backtest adapter all still import TEMPLATES; removing it is A95's job,
   after those readers move to the registry. A migration that breaks the live
   screener to prove a point is not a migration.

2. It does not invent filter_ids. Technical's filters are per-RUN job fields
   (min_adtv_cr, circuit_band_pct, ...), not per-template properties, so
   attaching them to a template row would assert something untrue. They
   attach when the run configuration migrates.

Exit policy variant
-------------------
A template carries stop/target/max-hold NUMBERS but not a POLICY -- the policy
(`unconstrained`, `risk_managed`, `trailing`, ...) is chosen per backtest run
from EXIT_POLICY_VARIANTS. The registry needs a variant, so rows are written
with "per_template", meaning "use this template's own barriers", which is
exactly what backtest/config/derived_exit_params.json + per_template_exit_policy.py
already implement. A run overriding the policy still can.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from strategies.predicates import PredicateError, validate_predicates
from strategies.registry import (
    RegistryError,
    get_strategy,
    register_strategy,
    revise_strategy,
    strategy_key,
)

logger = logging.getLogger(__name__)

SOURCE_REF = "systems/technical_analysis/screener/templates.py"

# "use this template's own stop/target/max-hold", as implemented by
# systems/ml_signal_engine/models/exit/per_template_exit_policy.py.
PER_TEMPLATE_EXIT = "per_template"


def build_rows() -> List[Dict[str, Any]]:
    """Read templates.py and produce one registry payload per template.

    Pure -- no DB access -- so the shape can be tested without a database and
    inspected with --dry-run.
    """
    from systems.technical_analysis.screener.templates import (
        TEMPLATE_STYLE,
        TEMPLATES,
    )

    rows: List[Dict[str, Any]] = []
    for t in TEMPLATES:
        conditions = [dict(c) for c in t.conditions]
        # Validate before writing. A template whose conditions the grammar
        # cannot express is a finding about the grammar, not something to
        # paper over -- surface it with the template name attached.
        try:
            validate_predicates(conditions, where=f"{t.name}.conditions")
        except PredicateError as exc:
            raise RegistryError(f"template {t.name} has an invalid condition: {exc}") from exc

        rows.append(
            {
                "channel": "technical",
                "name": t.name,
                "display_label": t.description,
                "description": t.description,
                "category": t.category,
                "definition": {
                    "template_name": t.name,
                    "style": TEMPLATE_STYLE.get(t.name),
                    "key_display_features": list(t.key_display_features or []),
                },
                "entry_criterion": conditions,
                "exit_criterion": {
                    "variant": PER_TEMPLATE_EXIT,
                    "stop_pct": t.exit_stop_pct,
                    "target_pct": t.exit_target_pct,
                    "max_hold_days": t.exit_max_hold_days,
                    "conditions": [],
                },
                "status": "active",
                "source_ref": SOURCE_REF,
            }
        )
    return rows


def migrate(
    *,
    db_path: Optional[Path] = None,
    dry_run: bool = False,
    created_by: str = "T15",
) -> Dict[str, int]:
    """Register every template not already present; revise those whose
    definition has drifted. Idempotent.

    Returns {"registered": n, "revised": n, "unchanged": n}.
    """
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
                key,
                db_path=db_path,
                created_by=created_by,
                source_ref=SOURCE_REF,
                **changes,
            )
        stats["revised"] += 1
        logger.info("%s drifted in: %s", key, sorted(changes))

    return stats


def _drift(existing: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]:
    """Fields whose stored value differs from templates.py. Only these are
    passed to revise_strategy, so an unchanged template writes no new version
    -- otherwise re-running the migration would inflate the version history
    and make every run's recorded version meaningless."""
    changes = {}
    for field in (
        "display_label",
        "description",
        "category",
        "definition",
        "entry_criterion",
        "exit_criterion",
    ):
        if existing.get(field) != row[field]:
            changes[field] = row[field]
    return changes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would change without writing",
    )
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stats = migrate(db_path=args.db_path, dry_run=args.dry_run)
    prefix = "[dry-run] would have" if args.dry_run else ""
    logger.info(
        "%s registered=%d revised=%d unchanged=%d",
        prefix,
        stats["registered"],
        stats["revised"],
        stats["unchanged"],
    )


if __name__ == "__main__":
    main()
