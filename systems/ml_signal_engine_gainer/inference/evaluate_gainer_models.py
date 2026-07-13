"""
systems/ml_signal_engine_gainer/inference/evaluate_gainer_models.py

GAINER EXPERIMENT: comparison report for the 3 short-horizon gainer
signal models (5%/6d, 10%/21d, 20%/63d), run under BOTH validation
schemes so the gap between them quantifies time-based leakage vs
genuine cross-stock generalization:

  1. Calendar-year walk-forward (WalkForwardValidator.split_data_purged) —
     purge+embargo applied so no training row's label window overlaps
     the test fold.
  2. Stock-level k-fold (stock_level_kfold) — split by ticker, all time
     periods mixed, eliminating time-based leakage entirely.

For each scheme, reports per model:
  - precision / recall / F1 on the positive (BUY) class
  - % negative-predicted: share of all scored Day-0s the model calls
    negative (no-buy)
  - near-miss magnitude: for the negative-predicted subset, the
    distribution (mean/median/p75/p90) of the actual forward max return
    realized, compared to the model's target_pct — quantifies how many
    "negative" calls were actually close misses vs genuine non-movers.
"""

import argparse
import logging
from typing import Dict

import pandas as pd
from sklearn.metrics import precision_score, recall_score, f1_score

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from features.technical import CORE_TECHNICAL_FEATURES
from systems.ml_signal_engine.inference.train_all_phase1 import load_benchmark_from_db
from systems.ml_signal_engine_gainer.inference.train_gainer_signals import (
    DEFAULT_TICKER_CHUNK_SIZE,
    GAINER_SIGNAL_TARGETS,
    _build_gainer_training_dataset_chunked,
)
from systems.ml_signal_engine_gainer.training.walk_forward import WalkForwardValidator, stock_level_kfold

logger = logging.getLogger(__name__)


def _fit_and_score(train_df: pd.DataFrame, test_df: pd.DataFrame, target, optuna_trials: int, seed: int) -> Dict:
    """Chronological 80/20 carve-out of train_df for HPO/threshold-tuning (never test_df),
    then score on test_df — same train/val/test discipline as production's train_full."""
    validator = WalkForwardValidator(n_folds=2)
    n_years = train_df["date"].dt.year.nunique() if "date" in train_df.columns else 0
    if n_years >= 2:
        train_only, val_df = validator.get_train_validation_split(train_df, val_fraction=0.2)
    else:
        # stock-level fold has no meaningful chronological structure to carve within
        # a ticker-disjoint train set — fall back to a random 80/20 row split.
        shuffled = train_df.sample(frac=1.0, random_state=seed)
        cut = int(len(shuffled) * 0.8)
        train_only, val_df = shuffled.iloc[:cut], shuffled.iloc[cut:]

    model = target.model_cls(optuna_trials=optuna_trials, random_state=seed)
    model.train_full(
        train_only[CORE_TECHNICAL_FEATURES], train_only["_label"],
        val_df[CORE_TECHNICAL_FEATURES], val_df["_label"],
        returns_train=train_only["_return"], returns_val=val_df["_return"],
    )

    preds = model.predict(test_df[CORE_TECHNICAL_FEATURES])
    y_true = test_df["_label"].astype(int)
    y_pred_positive = (preds == 1).astype(int)

    precision = precision_score(y_true, y_pred_positive, zero_division=0)
    recall = recall_score(y_true, y_pred_positive, zero_division=0)
    f1 = f1_score(y_true, y_pred_positive, zero_division=0)
    pct_negative_predicted = float((preds != 1).mean())

    negative_mask = preds != 1
    near_miss_returns = test_df.loc[negative_mask, "_return"]
    near_miss_stats = {
        "n": int(negative_mask.sum()),
        "mean": float(near_miss_returns.mean()) if len(near_miss_returns) else float("nan"),
        "median": float(near_miss_returns.median()) if len(near_miss_returns) else float("nan"),
        "p75": float(near_miss_returns.quantile(0.75)) if len(near_miss_returns) else float("nan"),
        "p90": float(near_miss_returns.quantile(0.90)) if len(near_miss_returns) else float("nan"),
        "target_pct": target.target_pct,
    }

    return {
        "precision": precision, "recall": recall, "f1": f1,
        "pct_negative_predicted": pct_negative_predicted,
        "near_miss": near_miss_stats,
    }


def run_comparison(
    lookback_days: int = 1260, optuna_trials: int = 10, seed: int = 42, db_path=None,
    tickers: list = None, ticker_chunk_size: int = DEFAULT_TICKER_CHUNK_SIZE,
) -> Dict:
    db_path = db_path or DUCKDB_PATH
    with get_duckdb_connection(db_path, read_only=True, persist=False) as conn:
        dates_df = conn.execute(
            "SELECT DISTINCT date FROM ohlcv_adjusted WHERE date >= CURRENT_DATE - INTERVAL (?) DAY ORDER BY date",
            [lookback_days],
        ).df()
    if dates_df.empty:
        raise RuntimeError("ohlcv_adjusted is empty — run ingestion/backfill_runner.py first")
    dates = pd.DatetimeIndex(pd.to_datetime(dates_df["date"]))
    benchmark = load_benchmark_from_db(dates=dates, db_path=db_path)

    report: Dict = {}
    for target in GAINER_SIGNAL_TARGETS:
        # Reuses the same checkpoint namespace as train_gainer_signals.py — if that
        # training run already completed, this is a disk read, not a recompute.
        combined = _build_gainer_training_dataset_chunked(
            target, benchmark, lookback_days, db_path=db_path, tickers=tickers, ticker_chunk_size=ticker_chunk_size,
        )
        target_report = {}

        # --- Scheme 1: calendar-year walk-forward, purged + embargoed ---
        validator = WalkForwardValidator(n_folds=2)
        n_years = combined["date"].dt.year.nunique()
        if n_years > 2:
            wf_folds = validator.split_data_purged(
                combined, label_horizon_days=target.horizon_days, embargo_days=target.horizon_days, n_folds=2,
            )
            train_df, test_df = wf_folds[-1]
            if not train_df.empty and not test_df.empty:
                target_report["walk_forward"] = _fit_and_score(train_df, test_df, target, optuna_trials, seed)
            else:
                logger.warning(f"{target.name}: walk-forward fold empty after purge/embargo — skipped")
        else:
            logger.warning(f"{target.name}: not enough distinct years for walk-forward — skipped")

        # --- Scheme 2: stock-level k-fold ---
        sk_folds = stock_level_kfold(combined, n_folds=5, random_state=seed)
        train_df, test_df = sk_folds[0]
        if not train_df.empty and not test_df.empty:
            target_report["stock_kfold"] = _fit_and_score(train_df, test_df, target, optuna_trials, seed)

        report[target.name] = target_report
        logger.info(f"{target.name}: {target_report}")

    return report


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="[GAINER EXPERIMENT] walk-forward vs stock-kfold comparison")
    parser.add_argument("--lookback-days", type=int, default=1260)
    parser.add_argument("--optuna-trials", type=int, default=10)
    args = parser.parse_args()

    report = run_comparison(lookback_days=args.lookback_days, optuna_trials=args.optuna_trials)
    for name, schemes in report.items():
        print(f"\n=== {name} ===")
        for scheme_name, metrics in schemes.items():
            print(
                f"  [{scheme_name}] precision={metrics['precision']:.3f} recall={metrics['recall']:.3f} "
                f"f1={metrics['f1']:.3f} pct_negative_predicted={metrics['pct_negative_predicted']:.3f}"
            )
            nm = metrics["near_miss"]
            print(
                f"    near-miss (n={nm['n']}, target={nm['target_pct']:.2%}): "
                f"mean={nm['mean']:.3%} median={nm['median']:.3%} p75={nm['p75']:.3%} p90={nm['p90']:.3%}"
            )


if __name__ == "__main__":
    main()
