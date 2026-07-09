"""
datastore/api/routers/models.py

Phase: 3.x (Backlog item #6 refactor)
Specs: SPEC-DS-004
Owner: Platform / DataStore
Consumers: dashboard, systems/ml_signal_engine

GET /api/v1/models — model registry query.

[AS BUILT, item #6] Moved out of datastore/api/main.py (previously the
last inline route left over from before P1.7's router-file reorganization
— see main.py's module docstring) into its own router file, same path,
same tags, same behavior, wired into main.py the same way as every other
router. Pure refactor — MODEL_REGISTRY_PATH is still the single JSON file
written by train_all_phase1.py/retrain_phase2.py; no DuckDB/SQLite table
backs this.
"""

import json
import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from config.settings import MODEL_REGISTRY_PATH
from datastore.api import schemas

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/models", tags=["Models"])


@router.get("", response_model=schemas.ModelRegistry)
async def get_models(
    model_name: Optional[str] = Query(
        None, description="Filter by model name (optional)"
    ),
) -> schemas.ModelRegistry:
    """
    Query model registry.

    SPEC-DS-004: Returns metadata for all trained models (versions, features,
    validation accuracy, hyperparameters).

    Args:
        model_name: If provided, return only this model's versions

    Returns:
        ModelRegistry with all models and latest-by-name index

    Raises:
        HTTPException 404: If model_name provided but not found
    """
    # train_all_phase1.py / retrain_phase2.py write one entry per model to
    # MODEL_REGISTRY_PATH (datastore/models/registry.json) keyed by model
    # name — no DuckDB/SQLite table backs this, the JSON file IS the
    # registry. Older entries (hmm_market, conformal_signal5d) predate this
    # endpoint's contract and only carry saved_path/saved_at — these still
    # surface (version/model_type default to "unknown") rather than being
    # silently dropped.
    if not MODEL_REGISTRY_PATH.exists():
        raise HTTPException(status_code=404, detail="Model registry not found")

    raw_registry = json.loads(MODEL_REGISTRY_PATH.read_text())
    models: List[schemas.ModelMetadata] = []
    for key, entry in raw_registry.items():
        name = entry.get("name", key)
        if model_name and name != model_name:
            continue
        created_at_raw = entry.get("created_at") or entry.get("saved_at")
        models.append(
            schemas.ModelMetadata(
                name=name,
                version=entry.get("version", "unknown"),
                model_type=entry.get("model_type", "unknown"),
                created_at=created_at_raw,
                features_used=entry.get("feature_names", []),
                accuracy_on_validation=entry.get("accuracy_on_validation"),
                # additional_metrics is Dict[str, float] — registry.json's
                # "diagnostics" is a nested dict of dicts (class ratios,
                # best params, per-class F1), not a flat float map, so it
                # doesn't fit this schema field as-is.
                additional_metrics=None,
                hyperparameters=entry.get("hyperparams"),
                training_samples=entry.get("training_samples"),
                training_time_seconds=entry.get("training_time_seconds"),
            )
        )

    if model_name and not models:
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found in registry")

    latest_by_name: dict = {}
    for m in models:
        existing = latest_by_name.get(m.name)
        if existing is None or m.created_at > existing.created_at:
            latest_by_name[m.name] = m

    return schemas.ModelRegistry(
        models=models,
        total_models=len(models),
        latest_model_by_name=latest_by_name,
    )
