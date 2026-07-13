"""
systems/ml_signal_engine_gainer/inference/evaluate_gainer_multibagger.py

GAINER EXPERIMENT: survival-appropriate comparison report for the 3
multibagger variants (2x/12mo, 3x/24mo, 5x/36mo), run under BOTH
validation schemes — mirrors evaluate_gainer_models.py's short-horizon
report, but multibagger is a survival model (LightGBM lambdarank +
Platt-calibrated mb_probability + Random Survival Forest), so precision/
recall on a fixed decision threshold isn't the right metric. Instead:

  - Harrell's concordance index (sksurv.metrics.concordance_index_censored):
    using mb_probability as the risk score against (event, duration_months)
    on the held-out test fold — the survival-analysis analogue of AUC,
    answers "does the model rank stocks that multibag SOONER above ones
    that multibag later or never, on data it never trained on."
  - Event-hit-rate @ top-N: of the top-N stocks by mb_probability in the
    test fold, what fraction actually achieved the target multiple —
    directly answers the original "signals generated vs correct signals"
    validation question, using the SAME two leakage-bounding schemes as
    the short-horizon report (calendar walk-forward w/ purge+embargo, and
    stock-level k-fold) so the gap between them quantifies how much of
    the apparent hit-rate is time-leakage vs genuine generalization to
    unseen stocks — this is the concrete answer to "how do we address
    signals hitting stocks already in the training set."

Reuses the SAME parquet checkpoints train_multibagger.py already writes
(load_all_checkpoints with the identical pipeline_name/stage key) — if
that training run already completed, this is a disk read, not a
recompute of features/labels.
"""

import argparse
import logging
from typing import Dict

import numpy as np
import pandas as pd
from sksurv.metrics import concordance_index_censored

from systems.ml_signal_engine_gainer.inference.checkpoint_utils import load_all_checkpoints
from systems.ml_signal_engine_gainer.inference.train_multibagger import MULTIBAGGER_TARGETS
from systems.ml_signal_engine_gainer.models.multibagger.multibagger_model import (
    MultibaggerModel,
    load_multibagger_training_data_from_db,
)
from systems.ml_signal_engine_gainer.training.walk_forward import WalkForwardValidator, stock_level_kfold
from features.multibagger import MULTIBAGGER_FEATURES

logger = logging.getLogger(__name__)

TOP_N = 20


def _fit_and_score_survival(train_df: pd.DataFrame, test_df: pd.DataFrame, seed: int, n_estimators: int) -> Dict:
    model = MultibaggerModel(random_state=seed, n_estimators=n_estimators)
    groups = train_df.groupby("date", sort=True).size().tolist()
    model.train_full(
        train_df[MULTIBAGGER_FEATURES], train_df["label"].astype(int),
        train_df["duration_months"].astype(float), train_df["event"].astype(int), groups=groups,
    )

    proba = model.predict(test_df[MULTIBAGGER_FEATURES])
    event_test = test_df["event"].astype(bool).to_numpy()
    duration_test = test_df["duration_months"].astype(float).to_numpy()

    try:
        c_index = concordance_index_censored(event_test, duration_test, proba.to_numpy())[0]
    except ValueError as exc:
        logger.warning("concordance_index_censored failed (%s) — reporting NaN", exc)
        c_index = float("nan")

    ranked = test_df.assign(_proba=proba.to_numpy()).sort_values("_proba", ascending=False)
    top_n = ranked.head(TOP_N)
    hit_rate = float(top_n["event"].astype(int).mean()) if len(top_n) else float("nan")
    base_rate = float(test_df["event"].astype(int).mean())

    return {
        "concordance_index": float(c_index),
        "top_n_hit_rate": hit_rate,
        "top_n_n": len(top_n),
        "base_event_rate": base_rate,
        "n_test": len(test_df),
    }


def _aggregate(fold_results: list) -> Dict:
    """Mean/std across folds for each numeric metric, plus the raw per-fold list."""
    if not fold_results:
        return {}
    keys = ["concordance_index", "top_n_hit_rate", "base_event_rate", "n_test"]
    agg = {}
    for k in keys:
        vals = [r[k] for r in fold_results if not (isinstance(r[k], float) and np.isnan(r[k]))]
        agg[f"{k}_mean"] = float(np.mean(vals)) if vals else float("nan")
        agg[f"{k}_std"] = float(np.std(vals)) if len(vals) > 1 else 0.0
    agg["n_folds"] = len(fold_results)
    agg["folds"] = fold_results
    return agg


def run_multibagger_comparison(
    lookback_days: int = 1260, n_estimators: int = 200, seed: int = 42,
    snapshot_stride_days: int = 5, cooldown_days: int = 15, tickers: list = None,
    walk_forward_n_folds: int = 5, stock_kfold_n_folds: int = 5,
) -> Dict:
    """
    walk_forward_n_folds/stock_kfold_n_folds: number of folds to run and
    average for EACH scheme (was hardcoded to a single fold each — walk_forward
    used only the last of 2 calendar folds, stock_kfold used only fold[0]).
    Both schemes now report mean/std across all folds actually available
    (a scheme may yield fewer usable folds than requested if a fold has no
    positives after purge/embargo — see per-fold warnings).
    """
    report: Dict = {}
    for target in MULTIBAGGER_TARGETS:
        # Ensures checkpoints exist (a no-op disk read if train_multibagger.py already ran this target).
        load_multibagger_training_data_from_db(
            lookback_days=lookback_days, label_window_days=target.label_window_days,
            min_return_multiplier=target.return_multiplier, label_window_years=target.window_years,
            snapshot_stride_days=snapshot_stride_days, cooldown_days=cooldown_days,
            tickers=tickers, pipeline_name=target.name,
        )
        stage = f"stride{snapshot_stride_days}_cooldown{cooldown_days}_mult{target.return_multiplier}_win{target.window_years}"
        merged = load_all_checkpoints(target.name, stage)
        if merged.empty:
            logger.warning(f"{target.name}: no checkpointed data — skipped")
            continue
        merged["date"] = pd.to_datetime(merged["date"])

        target_report = {}

        # --- Scheme 1: calendar-year walk-forward, purged + embargoed, ALL usable folds ---
        n_years = merged["date"].dt.year.nunique()
        label_horizon_days = int(target.window_years * 252)
        wf_results = []
        if n_years > walk_forward_n_folds:
            validator = WalkForwardValidator(n_folds=walk_forward_n_folds)
            wf_folds = validator.split_data_purged(
                merged, label_horizon_days=label_horizon_days, embargo_days=label_horizon_days,
                n_folds=walk_forward_n_folds,
            )
            for i, (train_df, test_df) in enumerate(wf_folds):
                if not train_df.empty and not test_df.empty and train_df["event"].sum() > 0:
                    wf_results.append(_fit_and_score_survival(train_df, test_df, seed, n_estimators))
                else:
                    logger.warning(f"{target.name}: walk-forward fold {i+1}/{len(wf_folds)} empty/no positives — skipped")
        else:
            logger.warning(
                f"{target.name}: only {n_years} distinct years, need > {walk_forward_n_folds} for "
                f"{walk_forward_n_folds} expanding folds — skipped"
            )
        if wf_results:
            target_report["walk_forward"] = _aggregate(wf_results)

        # --- Scheme 2: stock-level k-fold, ALL folds ---
        sk_folds = stock_level_kfold(merged, n_folds=stock_kfold_n_folds, random_state=seed)
        sk_results = []
        for i, (train_df, test_df) in enumerate(sk_folds):
            if not train_df.empty and not test_df.empty and train_df["event"].sum() > 0:
                sk_results.append(_fit_and_score_survival(train_df, test_df, seed, n_estimators))
            else:
                logger.warning(f"{target.name}: stock-kfold fold {i+1}/{len(sk_folds)} empty/no positives — skipped")
        if sk_results:
            target_report["stock_kfold"] = _aggregate(sk_results)

        report[target.name] = target_report
        logger.info(
            f"{target.name}: wf_folds={len(wf_results)} "
            f"wf_concordance={target_report.get('walk_forward', {}).get('concordance_index_mean')} | "
            f"sk_folds={len(sk_results)} "
            f"sk_concordance={target_report.get('stock_kfold', {}).get('concordance_index_mean')}"
        )

    return report


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="[GAINER EXPERIMENT] multibagger walk-forward vs stock-kfold comparison")
    parser.add_argument("--lookback-days", type=int, default=1260)
    parser.add_argument("--n-estimators", type=int, default=200)
    parser.add_argument("--tickers", type=str, default=None)
    args = parser.parse_args()

    tickers = args.tickers.split(",") if args.tickers else None
    report = run_multibagger_comparison(lookback_days=args.lookback_days, n_estimators=args.n_estimators, tickers=tickers)
    for name, schemes in report.items():
        print(f"\n=== {name} ===")
        for scheme_name, metrics in schemes.items():
            print(
                f"  [{scheme_name}] n_folds={metrics['n_folds']} "
                f"concordance={metrics['concordance_index_mean']:.3f}+/-{metrics['concordance_index_std']:.3f} "
                f"top{TOP_N}_hit_rate={metrics['top_n_hit_rate_mean']:.3f}+/-{metrics['top_n_hit_rate_std']:.3f} "
                f"(base_event_rate={metrics['base_event_rate_mean']:.4f})"
            )


if __name__ == "__main__":
    main()
