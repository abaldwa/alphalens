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
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


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
    schedule_overnight_training(
        feature_parquet_dir="datastore/features/daily",
        model_output_dir=args.output_dir,
        horizon_days=args.horizon,
        n_folds=args.folds,
        quick=args.quick,
    )
    logger.info("TFT training complete.")


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
    schedule_overnight_training(
        feature_parquet_dir="datastore/features/daily",
        model_output_dir=args.output_dir,
        horizon_days=args.horizon,
        n_folds=args.folds,
        quick=args.quick,
    )
    logger.info("BiLSTM training complete.")


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
