"""
systems/ml_signal_engine/inference/train_deep_models.py

Unified CLI entry point for overnight deep model training.

Usage
-----
  # Quick smoke-test (2 epochs, ~30s):
  .venv/bin/python3 -m systems.ml_signal_engine.inference.train_deep_models \\
      --model tft --folds 2 --quick

  # Background overnight training (redirect stdout+stderr with shell, not .venv/bin/nohup):
  nohup .venv/bin/python3 -m systems.ml_signal_engine.inference.train_deep_models \\
      --model tft --folds 5 > logs/tft_training.log 2>&1 &

  # Train both models sequentially:
  nohup .venv/bin/python3 -m systems.ml_signal_engine.inference.train_deep_models \\
      --model all --folds 5 > logs/deep_training.log 2>&1 &

Models
------
  tft    : M-11 Temporal Fusion Transformer  (tft_model.py)
  bilstm : M-12 BiLSTM with Mamba-2 / attention fallback (bilstm_model.py)
  all    : train tft then bilstm sequentially

Output
------
  Saved to --output-dir (default: datastore/models/)
  One .pt + .json file per fold, named:
    tft_signal_{horizon}d_v{version}_fold{N}.pt
    bilstm_signal_{horizon}d_v{version}_fold{N}.pt
"""

import argparse
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def _update_registry(model_name: str, output_dir: str, result: dict, horizon_days: int) -> None:
    """
    Write/update a datastore/models/registry.json entry for `model_name`
    (SPEC-MODEL-005), mirroring train_all_phase1.py::_save_model's
    read-merge-write convention so pipeline_scheduler.py's overdue-retrain
    check (last_trained_date/training_interval_days) and
    scripts/model_training_status.py both see tft/bilstm the same way
    they see every other model. schedule_overnight_training() itself does
    not touch the registry (see its docstring) — this is that write.

    No-ops (does not touch the registry) if `result["folds_trained"] == 0`
    — a run that trained nothing (e.g. insufficient data) must not
    overwrite a real prior last_trained_date with today's date.
    """
    if result.get("folds_trained", 0) == 0:
        logger.warning(f"{model_name}: 0 folds trained — registry.json left unchanged")
        return

    from config.settings import DEFAULT_TRAINING_INTERVAL_DAYS
    from config.timezone import now_ist

    registry_path = Path(output_dir) / "registry.json"
    existing = {}
    if registry_path.exists():
        try:
            existing = json.loads(registry_path.read_text())
        except (json.JSONDecodeError, OSError):
            existing = {}

    run_date = now_ist()
    existing[model_name] = {
        "saved_path": result.get("last_model_path"),
        "saved_at": run_date.isoformat(),
        "last_trained_date": run_date.date().isoformat(),
        "training_interval_days": DEFAULT_TRAINING_INTERVAL_DAYS,
        "folds_trained": result["folds_trained"],
        "horizon_days": horizon_days,
    }
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(json.dumps(existing, indent=2, default=str))
    logger.info(f"Updated model registry for '{model_name}': {registry_path}")


def _train_tft(args: argparse.Namespace) -> None:
    try:
        from systems.ml_signal_engine.models.deep.tft_model import (
            schedule_overnight_training,
        )
    except ImportError as exc:
        logger.error("Could not import TFTSignalModel: %s", exc)
        sys.exit(1)

    logger.info(
        "Starting TFT training: horizon=%dd  folds=%d  quick=%s  output=%s",
        args.horizon, args.folds, args.quick, args.output_dir,
    )
    result = schedule_overnight_training(
        feature_parquet_dir="datastore/features/daily",
        model_output_dir=args.output_dir,
        horizon_days=args.horizon,
        n_folds=args.folds,
        quick=args.quick,
    )
    _update_registry("tft", args.output_dir, result, args.horizon)
    logger.info("TFT training complete: %s", result)


def _train_bilstm(args: argparse.Namespace) -> None:
    try:
        from systems.ml_signal_engine.models.deep.bilstm_model import (
            schedule_overnight_training,
        )
    except ImportError as exc:
        logger.error("Could not import BiLSTMSignalModel: %s", exc)
        sys.exit(1)

    logger.info(
        "Starting BiLSTM training: horizon=%dd  folds=%d  quick=%s  output=%s",
        args.horizon, args.folds, args.quick, args.output_dir,
    )
    result = schedule_overnight_training(
        feature_parquet_dir="datastore/features/daily",
        model_output_dir=args.output_dir,
        horizon_days=args.horizon,
        n_folds=args.folds,
        quick=args.quick,
    )
    _update_registry("bilstm", args.output_dir, result, args.horizon)
    logger.info("BiLSTM training complete: %s", result)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Overnight deep model training (TFT / BiLSTM)"
    )
    parser.add_argument(
        "--model",
        choices=["tft", "bilstm", "all"],
        default="all",
        help="Which model(s) to train (default: all)",
    )
    parser.add_argument(
        "--folds", type=int, default=5,
        help="Number of walk-forward folds (default: 5)",
    )
    parser.add_argument(
        "--horizon", type=int, default=21,
        help="Prediction horizon in days (default: 21)",
    )
    parser.add_argument(
        "--output-dir", default="datastore/models",
        help="Directory to save trained weights (default: datastore/models)",
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="Smoke-test mode: 2 epochs × 50 samples (~30s per model)",
    )
    args = parser.parse_args()

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    if args.model in ("tft", "all"):
        _train_tft(args)

    if args.model in ("bilstm", "all"):
        _train_bilstm(args)

    logger.info("All requested training jobs finished.")


if __name__ == "__main__":
    main()
