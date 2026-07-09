"""
systems/ml_signal_engine/inference/train_multibagger.py

Phase: 2.4 (Multibagger Detection System M-08)
Specs: SPEC-MODEL-001, SPEC-MODEL-002, SPEC-MODEL-005
Owner: ml_signal_engine / inference
Consumers: operator CLI (`python3 -m systems.ml_signal_engine.inference.train_multibagger`),
scheduled weekday model-training job (ingestion/scheduler/pipeline_scheduler.py's
_MODEL_TRAINING_SCRIPT_MAP["multibagger"]).

2026-07-05: multibagger previously had no standalone periodic-retrain
entry point — score_multibagger.py only trains inline as a one-off
fallback when no cached artifact exists yet (see its own module
docstring, backlog #27) and explicitly does not decide when to retrain.
This script is that missing entry point: it calls the already-real
load_multibagger_training_data_from_db() (real OHLCV -> real
features.multibagger panel -> real forward-looking 2x-in-3-years labels,
P&D-excluded via the real cached PnDDetector) and MultibaggerModel.
train_full() (real LightGBM lambdarank + Platt calibration + Random
Survival Forest — see multibagger_model.py's own module docstring for
the full "AS BUILT" reconciliation), then saves the result via
train_all_phase1.py's _save_model() so it lands in the SAME
datastore/models/multibagger/ + registry.json convention every other
model in this project uses (SPEC-MODEL-005 versioned filename +
`_current.pkl` + registry entry), which is exactly what
score_multibagger.py's cached-artifact loader already expects.

There is no synthetic-data mode: load_multibagger_training_data_from_db
raises if ohlcv_adjusted is empty/insufficient or no ticker meets the 2x
threshold, rather than fabricating positives.
"""

import argparse
import logging
from typing import Dict

from config.settings import MODELS_DIR
from config.timezone import now_ist
from systems.ml_signal_engine.inference.train_all_phase1 import _save_model
from systems.ml_signal_engine.models.multibagger.multibagger_model import (
    MultibaggerModel,
    load_multibagger_training_data_from_db,
)

logger = logging.getLogger(__name__)


def train_multibagger(
    lookback_days: int = 1260,
    label_window_days: int = 756,
    snapshot_stride_days: int = 5,
    n_estimators: int = 200,
    save: bool = True,
    seed: int = 42,
) -> Dict:
    """
    Train (or retrain) M-08's MultibaggerModel end-to-end on real data and
    persist it via the standard {name}_v{YYYYMMDD}_fold0.pkl + `_current.pkl`
    + registry.json convention.

    Parameters
    ----------
    lookback_days : int
        Real OHLCV history window passed to
        load_multibagger_training_data_from_db(). Default 1260 (~5 years).
    label_window_days : int
        Forward-runway requirement for censored (event=0) snapshots — see
        load_multibagger_training_data_from_db's own docstring. Default
        756 (~3 years, SPEC-MODEL-001).
    snapshot_stride_days : int
        Per-ticker snapshot subsampling stride. Default 5 (~weekly).
    n_estimators : int
        LightGBM ranker / Random Survival Forest tree count.
    save : bool
        If True (default), persist the trained model + update registry.json.
    seed : int
        RNG seed.

    Returns
    -------
    dict
        {"registry": {...}, "diagnostics": {...}} — diagnostics is
        MultibaggerModel.train_full()'s own return value (training_samples,
        positive_rate, event_rate, rsf_concordance_index).

    Raises
    ------
    RuntimeError
        Propagated from load_multibagger_training_data_from_db if
        ohlcv_adjusted has no real data / no confirmed multibaggers in
        the requested window — there is no synthetic-data fallback.
    """
    run_date = now_ist()
    logger.info(
        "Multibagger retrain starting: real OHLCV (lookback=%d days), "
        "label_window=%d days, snapshot_stride=%d days",
        lookback_days, label_window_days, snapshot_stride_days,
    )

    X, y, duration_months, event, groups, _pnd_scores = load_multibagger_training_data_from_db(
        lookback_days=lookback_days,
        label_window_days=label_window_days,
        snapshot_stride_days=snapshot_stride_days,
    )

    model = MultibaggerModel(random_state=seed, n_estimators=n_estimators)
    diagnostics = model.train_full(X, y, duration_months, event, groups=groups)
    logger.info(
        "Multibagger trained: %d samples, positive_rate=%.4f, event_rate=%.4f, rsf_concordance=%.4f",
        diagnostics["training_samples"], diagnostics["positive_rate"],
        diagnostics["event_rate"], diagnostics["rsf_concordance_index"],
    )

    registry: Dict = {}
    if save:
        _save_model(model, "multibagger", run_date, registry, metadata_extra={"diagnostics": diagnostics})

        import json

        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        registry_path = MODELS_DIR / "registry.json"
        existing = {}
        if registry_path.exists():
            try:
                existing = json.loads(registry_path.read_text())
            except (json.JSONDecodeError, OSError):
                existing = {}
        existing.update(registry)
        registry_path.write_text(json.dumps(existing, indent=2, default=str))
        logger.info("Updated model registry: %s", registry_path)

    return {"registry": registry, "diagnostics": diagnostics}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Train/retrain M-08 MultibaggerModel on real data")
    parser.add_argument("--lookback-days", type=int, default=1260)
    parser.add_argument("--label-window-days", type=int, default=756)
    parser.add_argument("--snapshot-stride-days", type=int, default=5)
    parser.add_argument("--n-estimators", type=int, default=200)
    args = parser.parse_args()

    result = train_multibagger(
        lookback_days=args.lookback_days,
        label_window_days=args.label_window_days,
        snapshot_stride_days=args.snapshot_stride_days,
        n_estimators=args.n_estimators,
    )
    diag = result["diagnostics"]
    print(
        f"\nMultibagger trained: {diag['training_samples']} samples, "
        f"positive_rate={diag['positive_rate']:.4f}, event_rate={diag['event_rate']:.4f}, "
        f"rsf_concordance={diag['rsf_concordance_index']:.4f}"
    )


if __name__ == "__main__":
    main()
