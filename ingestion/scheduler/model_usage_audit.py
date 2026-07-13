"""
ingestion/scheduler/model_usage_audit.py

Phase: Pipeline & Monitoring Remediation, Phase 4 (A53)
Owner: Platform / ML Signal Engine
Consumers: datastore/api/routers/ops.py (Jobs & Models "trained but
    unused" panel)

"Trained but unused" detector: the class of bug behind A38 (TFT/BiLSTM
trained-eventually but daily_inference.py never called them) and A40
(StackingEnsemble fully dormant) — a model can sit in registry.json with
a real last_trained_date and simply never be read by anything downstream.
This module doesn't fix A38/A40 (out of this remediation's declared
scope — see FeatureBacklog.md), it detects the *class* of gap so it can't
silently recur for a future model.

CONSUMERS below is a curated map (model_name -> human-readable consumer,
or None if nothing currently consumes it), not a static-analysis result —
tracing "does any code path actually import+call this model" precisely
would require following dynamic joblib.load()/torch.load() paths keyed by
string filenames, which isn't reliably discoverable via AST alone. This
mirrors ingestion/scheduler/exception_catalog.py's approach: a maintained
registry that's easy to audit by eye, checked by a test that at least
every _MODEL_TRAINING_SCRIPT_MAP key has a CONSUMERS entry (so a newly
added model can't be silently forgotten from this map either).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

# model_name -> consumer description, or None if nothing reads it today.
CONSUMERS: dict = {
    "hmm_market": "systems/ml_signal_engine/inference/daily_inference.py::_step_hmm",
    "pnd_detector": "systems/ml_signal_engine/inference/daily_inference.py (P&D step)",
    "signal_5d": "systems/ml_signal_engine/inference/daily_inference.py (signal_5d step)",
    "signal_21d": "systems/ml_signal_engine/inference/daily_inference.py (signal_21d step)",
    "signal_63d": "systems/ml_signal_engine/inference/daily_inference.py (signal_63d step)",
    "meta_labeler": "systems/ml_signal_engine/inference/daily_inference.py (meta-labeling step)",
    "conformal_signal5d": "systems/ml_signal_engine/inference/daily_inference.py::_load_conformal",
    "exit_signal": "systems/ml_signal_engine/inference/daily_inference.py::_load_exit_model",
    "multibagger": "systems/ml_signal_engine/inference/score_multibagger.py",
    # A38/A40 (FeatureBacklog.md): trained (once A38's first real run
    # happens) but not read by daily_inference.py or StackingEnsemble —
    # tracked as an open gap, not fixed by this detector.
    "tft": None,
    "bilstm": None,
}


@dataclass(frozen=True)
class UnusedModelFinding:
    model_name: str
    last_trained_date: Optional[str]


def find_trained_but_unused_models(registry_path: Path) -> List[UnusedModelFinding]:
    """
    Return every model in registry.json that has a last_trained_date but
    whose CONSUMERS entry is None (or missing entirely — an unmapped
    model is also worth flagging, not silently ignored).

    Parameters
    ----------
    registry_path : Path
        Path to datastore/models/registry.json.

    Returns
    -------
    list of UnusedModelFinding
        Empty if registry_path doesn't exist or nothing is unused.

    Raises
    ------
    None
    """
    if not registry_path.exists():
        return []

    try:
        registry = json.loads(registry_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(f"find_trained_but_unused_models: could not read {registry_path}: {exc}")
        return []

    findings = []
    for model_name, meta in registry.items():
        last_trained = meta.get("last_trained_date") if isinstance(meta, dict) else None
        if not last_trained:
            continue  # never actually trained — not this detector's concern
        consumer = CONSUMERS.get(model_name, "UNMAPPED — add to model_usage_audit.py's CONSUMERS")
        if consumer is None or consumer.startswith("UNMAPPED"):
            findings.append(UnusedModelFinding(model_name=model_name, last_trained_date=last_trained))

    return findings
