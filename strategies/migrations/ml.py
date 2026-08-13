"""
strategies/migrations/ml.py

Owner: Platform / Architecture (ML42)
Run: PYTHONPATH=$PWD .venv/bin/python -m strategies.migrations.ml [--dry-run]

Migrates the ML signal models into strategy_registry, keyed to their trained
artifacts in datastore/models/registry.json.

This is the least like the other three, and the differences are real rather
than cosmetic:

  * An ML strategy has no expressible entry criterion. The rule is "the model
    said so", learned from data and living in a pickle, not a threshold over
    named columns. entry_criterion is empty and the definition names the
    model, its horizon, and the artifact it was trained into -- the same
    honest-empty as Momentum's rank-and-take-top-N and Fundamental's
    composite scores, for a different reason.

  * Definitions are only half the story. An ML strategy is (architecture +
    trained weights), and the weights change on every retrain. So each row
    records the artifact path and last_trained_date from registry.json, and
    a retrain is a REVISION -- a new version, with the old one still readable
    by the runs that used it. Without that, "what did the model that produced
    this backtest actually look like" has no answer.

  * ml_adapter.py deliberately does not implement generate_signals(): it
    wraps the frozen backtest/engine.py as a black box. That exemption is
    recorded on each row as `emits_signals: False` rather than being quietly
    papered over, because A87 (generate once, simulate many) and A94 (persist
    every signal) both assume the seam exists. Deciding whether ML keeps the
    exemption is an open question this migration surfaces rather than settles.

The existing ml_signals table is NOT migrated here. It has a genuinely richer
per-ticker schema (conformal bounds, pnd_*, hmm_*, exit_urgency) than the
ledger's generic context_json, and collapsing it would lose columns that
serving code reads today. Reconciliation is part of A95, once there is a
reader to reconcile against.
"""

from __future__ import annotations

import argparse
import json
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

SOURCE_REF = "systems/ml_signal_engine/models/ + datastore/models/registry.json"

# The models that actually produce a tradeable directional signal. The
# registry.json file also holds hmm_market (a regime input, expressed as the
# hmm_regime FILTER in A93, not a strategy), pnd_detector (a pre-trade
# exclusion), conformal_signal5d (an uncertainty wrapper over signal_5d), and
# tft/bilstm (deep models not currently serving). Registering those as
# strategies would claim they can be deployed and backtested standalone,
# which is not true of any of them.
SIGNAL_MODELS: Dict[str, Dict[str, Any]] = {
    "signal_5d": {
        "label": "ML Signal - 5 day",
        "horizon_days": 5,
        "module": "systems.ml_signal_engine.models.signal.signal_5d",
    },
    "signal_21d": {
        "label": "ML Signal - 21 day",
        "horizon_days": 21,
        "module": "systems.ml_signal_engine.models.signal.signal_21d",
    },
    "signal_63d": {
        "label": "ML Signal - 63 day",
        "horizon_days": 63,
        "module": "systems.ml_signal_engine.models.signal.signal_63d",
    },
    "multibagger": {
        "label": "ML Multibagger",
        "horizon_days": None,
        "module": "systems.ml_signal_engine.models.multibagger",
    },
}

# Non-strategy artifacts, recorded so the exclusion is a decision on the
# record rather than an omission someone has to rediscover.
NON_STRATEGY_ARTIFACTS = {
    "hmm_market": "a regime input; expressed as the hmm_regime filter (A93)",
    "pnd_detector": "a pre-trade exclusion, not a signal source",
    "conformal_signal5d": "an uncertainty wrapper over signal_5d, not a separate strategy",
    "tft": "deep model, not currently serving",
    "bilstm": "deep model, not currently serving",
}

EXIT_VARIANT = "ml_exit_model"


def load_artifacts(registry_path: Optional[Path] = None) -> Dict[str, Any]:
    """Read datastore/models/registry.json. Missing file is not fatal -- a
    checkout without trained artifacts should still be able to register the
    model definitions, just without artifact provenance."""
    if registry_path is None:
        registry_path = Path("datastore/models/registry.json")
    if not registry_path.exists():
        logger.warning("model registry not found at %s; rows will carry no artifact provenance", registry_path)
        return {}
    return json.loads(registry_path.read_text())


def build_rows(registry_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """One payload per serving signal model."""
    artifacts = load_artifacts(registry_path)

    rows: List[Dict[str, Any]] = []
    for name, meta in SIGNAL_MODELS.items():
        art = artifacts.get(name) or {}
        rows.append(
            {
                "channel": "ml",
                "name": name,
                "display_label": meta["label"],
                "description": (
                    "Learned directional signal"
                    + (
                        f" over a {meta['horizon_days']}-day horizon"
                        if meta["horizon_days"]
                        else ""
                    )
                    + ". The entry rule is the trained model, not a threshold "
                    "over named columns, so it has no declarative criterion."
                ),
                "category": "ml_signal",
                "definition": {
                    "model_name": name,
                    "horizon_days": meta["horizon_days"],
                    "module": meta["module"],
                    # Artifact provenance: which trained weights this version
                    # of the strategy refers to. A retrain revises the row.
                    "artifact_path": art.get("saved_path"),
                    "last_trained_date": art.get("last_trained_date"),
                    "training_interval_days": art.get("training_interval_days"),
                    # ml_adapter.py wraps the frozen engine rather than
                    # implementing generate_signals(). A87 and A94 both assume
                    # that seam; record the exemption instead of hiding it.
                    "emits_signals": False,
                    "signal_seam_exemption": (
                        "backtest/adapters/ml_adapter.py wraps backtest/engine.py "
                        "as a black box and does not implement "
                        "StrategyAdapter.generate_signals(). Whether this "
                        "exemption survives A87/A94 is an open decision."
                    ),
                },
                "entry_criterion": [],
                "exit_criterion": {"variant": EXIT_VARIANT, "conditions": []},
                "filter_ids": [],
                "status": "active",
                "source_ref": SOURCE_REF,
            }
        )
    return rows


def migrate(
    *,
    db_path: Optional[Path] = None,
    registry_path: Optional[Path] = None,
    dry_run: bool = False,
    created_by: str = "ML42",
) -> Dict[str, int]:
    """Register/revise idempotently. A retrain changes last_trained_date and
    artifact_path, which shows up as drift and produces a new version -- which
    is the intent: the strategy genuinely changed."""
    rows = build_rows(registry_path)
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
        if "definition" in changes:
            logger.info(
                "%s: artifact changed (retrain) -> new version, old one still readable", key
            )
    return stats


def _drift(existing: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]:
    changes = {}
    for field in ("display_label", "description", "category", "definition", "exit_criterion"):
        if existing.get(field) != row[field]:
            changes[field] = row[field]
    return changes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--registry-path", type=Path, default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stats = migrate(
        db_path=args.db_path, registry_path=args.registry_path, dry_run=args.dry_run
    )
    logger.info(
        "%sregistered=%d revised=%d unchanged=%d",
        "[dry-run] " if args.dry_run else "",
        stats["registered"],
        stats["revised"],
        stats["unchanged"],
    )
    for name, why in NON_STRATEGY_ARTIFACTS.items():
        logger.info("not registered as a strategy - %s: %s", name, why)


if __name__ == "__main__":
    main()
