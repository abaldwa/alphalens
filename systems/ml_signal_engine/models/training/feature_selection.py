"""
systems/ml_signal_engine/models/training/feature_selection.py

Phase: 3.3 (Stacking + Feature Validation)
Specs: SPEC-FEAT-001, SPEC-FEAT-005, SPEC-MODEL-003
Owner: ml_signal_engine / training
Consumers: RESEARCH TOOL ONLY — not in daily pipeline

M-14: TabNet Feature Selection Validator.

Purpose
-------
Run TabNet attention-based feature importance ONCE on the full dataset,
then cross-validate with LightGBM SHAP importance. A feature is flagged
for pruning ONLY if BOTH methods agree it is unimportant (consensus rule).

Critical constraint (02_models.md M-14):
  "This is a research tool. Do NOT retrain production models until
  Phase 3 backtest validates pruning."

Pruned features are written to:
  1. A JSON artifact: datastore/features/metadata/pruned_features.json
  2. alphalens_docs/01_features.md — each pruned feature appended with
     "Phase 3 pruned — <reason>" in its table row.

Usage
-----
  from systems.ml_signal_engine.models.training.feature_selection import run_feature_selection
  result = run_feature_selection(X_df, y_series, feature_names=list(X_df.columns))
  result.save_artifacts()

Or CLI:
  python -m systems.ml_signal_engine.models.training.feature_selection \
      --parquet datastore/features/daily/2026-*.parquet \
      --target 5d_label

TabNet note:
  Requires pytorch-tabnet>=4.1 and torch>=2.12.
  If torch is not available, TabNet importance is skipped and only SHAP
  importance is used (consensus then falls back to SHAP-only pruning,
  logged as WARNING).

SHAP note:
  LightGBM SHAP values are computed on a single fold (last 20% of data
  chronologically, never shuffled) to avoid lookahead bias.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Importance thresholds ─────────────────────────────────────────────────────

# Feature importance percentile below which a feature is "unimportant"
_TABNET_UNIMPORTANT_PCT: float = 10.0    # bottom 10% of TabNet attention
_SHAP_UNIMPORTANT_PCT: float = 10.0      # bottom 10% of mean |SHAP|

# Minimum number of training samples for TabNet (too few = unstable importance)
_MIN_TABNET_SAMPLES: int = 1000


# ── Result dataclass ──────────────────────────────────────────────────────────


@dataclass
class FeatureSelectionResult:
    """
    Output of run_feature_selection().

    Attributes
    ----------
    pruned_features : List[str] — features flagged by BOTH TabNet AND SHAP
    tabnet_importances : Dict[str, float] — TabNet attention scores
    shap_importances : Dict[str, float] — mean |SHAP| values
    tabnet_available : bool — False if torch/tabnet not installed
    n_features_evaluated : int
    n_features_pruned : int
    rationale : Dict[str, str] — {feature_name: "TabNet rank N + SHAP rank M"}
    generated_at : str
    """

    pruned_features: List[str] = field(default_factory=list)
    tabnet_importances: Dict[str, float] = field(default_factory=dict)
    shap_importances: Dict[str, float] = field(default_factory=dict)
    tabnet_available: bool = True
    n_features_evaluated: int = 0
    n_features_pruned: int = 0
    rationale: Dict[str, str] = field(default_factory=dict)
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def save_artifacts(
        self,
        metadata_dir: str = "datastore/features/metadata",
        features_doc: str = "alphalens_docs/01_features.md",
    ) -> None:
        """
        Persist result to JSON and annotate features doc.

        JSON: datastore/features/metadata/pruned_features.json
        Markdown: alphalens_docs/01_features.md (adds "Phase 3 pruned" annotation)
        """
        meta_path = Path(metadata_dir)
        meta_path.mkdir(parents=True, exist_ok=True)
        out_path = meta_path / "pruned_features.json"
        payload = {
            "generated_at": self.generated_at,
            "n_features_evaluated": self.n_features_evaluated,
            "n_features_pruned": self.n_features_pruned,
            "tabnet_available": self.tabnet_available,
            "pruned_features": self.pruned_features,
            "rationale": self.rationale,
            "tabnet_importances": {k: round(v, 6) for k, v in self.tabnet_importances.items()},
            "shap_importances": {k: round(v, 6) for k, v in self.shap_importances.items()},
        }
        with open(out_path, "w") as f:
            json.dump(payload, f, indent=2)
        logger.info("Pruned feature list saved to %s", out_path)

        doc_path = Path(features_doc)
        if doc_path.exists() and self.pruned_features:
            _annotate_features_doc(doc_path, self.pruned_features, self.rationale)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "n_features_evaluated": self.n_features_evaluated,
            "n_features_pruned": self.n_features_pruned,
            "tabnet_available": self.tabnet_available,
            "pruned_features": self.pruned_features,
            "rationale": self.rationale,
        }


# ── Main entry point ──────────────────────────────────────────────────────────


def run_feature_selection(
    X: pd.DataFrame,
    y: pd.Series,
    feature_names: Optional[List[str]] = None,
    tabnet_epochs: int = 50,
    n_shap_samples: int = 500,
    random_state: int = 42,
) -> FeatureSelectionResult:
    """
    Run TabNet + SHAP feature importance. Prune features only if BOTH agree.

    Parameters
    ----------
    X : pd.DataFrame (n_samples, n_features)
        Feature matrix. NaN will be imputed with column median.
    y : pd.Series (n_samples,)
        Target labels. {-1, 0, 1} or {0, 1, 2}.
    feature_names : list of str, optional
        Feature names. Defaults to X.columns.
    tabnet_epochs : int
        Number of TabNet training epochs (fewer = faster, less stable).
    n_shap_samples : int
        Number of samples to use for SHAP (subsampled for speed).
    random_state : int

    Returns
    -------
    FeatureSelectionResult

    Spec References
    ---------------
    M-14 (02_models.md): "Run TabNet ONCE on full dataset. Cross-validate
    with SHAP: prune only if BOTH agree it's unimportant."
    SPEC-FEAT-005: feature importance cross-validation.
    """
    if feature_names is None:
        feature_names = list(X.columns)

    # Impute NaN (TabNet does not tolerate NaN)
    X_clean = X[feature_names].copy()
    for col in X_clean.columns:
        med = X_clean[col].median()
        X_clean[col] = X_clean[col].fillna(med if not np.isnan(med) else 0.0)

    y_arr = np.array(y)
    # Remap {-1, 0, 1} → {0, 1, 2} for classifiers
    y_clf = np.where(y_arr == -1, 0, np.where(y_arr == 1, 2, 1))

    n_samples, n_features = X_clean.shape
    logger.info(
        "Feature selection: %d samples, %d features. TabNet epochs=%d, SHAP samples=%d",
        n_samples, n_features, tabnet_epochs, n_shap_samples,
    )

    # ── TabNet importance ──────────────────────────────────────────────────
    tabnet_importances, tabnet_available = _compute_tabnet_importance(
        X_clean.values.astype(np.float32),
        y_clf,
        feature_names,
        epochs=tabnet_epochs,
        random_state=random_state,
    )

    # ── SHAP importance ────────────────────────────────────────────────────
    shap_importances = _compute_shap_importance(
        X_clean, y_arr, feature_names, n_samples=n_shap_samples, random_state=random_state
    )

    # ── Consensus pruning ──────────────────────────────────────────────────
    pruned, rationale = _find_consensus_pruned(
        feature_names, tabnet_importances, shap_importances, tabnet_available
    )

    result = FeatureSelectionResult(
        pruned_features=pruned,
        tabnet_importances=tabnet_importances,
        shap_importances=shap_importances,
        tabnet_available=tabnet_available,
        n_features_evaluated=n_features,
        n_features_pruned=len(pruned),
        rationale=rationale,
    )

    if pruned:
        logger.info(
            "Feature selection complete. %d/%d features flagged for pruning: %s",
            len(pruned), n_features, pruned,
        )
        logger.warning(
            "RESEARCH TOOL: Do NOT retrain production models until Phase 3 backtest "
            "validates pruning (02_models.md M-14)."
        )
    else:
        logger.info("Feature selection complete. No features flagged for pruning.")

    return result


# ── TabNet importance ─────────────────────────────────────────────────────────


def _compute_tabnet_importance(
    X: np.ndarray,
    y: np.ndarray,
    feature_names: List[str],
    epochs: int,
    random_state: int,
) -> Tuple[Dict[str, float], bool]:
    """
    Train TabNet and extract feature importance (attention mask).

    Returns
    -------
    (importances_dict, tabnet_available)
      importances_dict: {feature_name: importance_score}
      tabnet_available: False if torch/tabnet import fails
    """
    try:
        from pytorch_tabnet.tab_model import TabNetClassifier
    except ImportError:
        logger.warning(
            "pytorch_tabnet not available. TabNet importance skipped. "
            "Consensus pruning will use SHAP-only (more conservative)."
        )
        return {f: 0.0 for f in feature_names}, False

    n = len(X)
    if n < _MIN_TABNET_SAMPLES:
        logger.warning(
            "Only %d samples < %d minimum for TabNet. Using uniform importance.",
            n, _MIN_TABNET_SAMPLES,
        )
        return {f: 1.0 / len(feature_names) for f in feature_names}, True

    # Chronological split (SPEC-BT-001: no random splits on time-series)
    split_idx = int(n * 0.8)
    X_tr, X_val = X[:split_idx], X[split_idx:]
    y_tr, y_val = y[:split_idx], y[split_idx:]

    # Ensure both sets have all classes
    unique_train = set(np.unique(y_tr))
    unique_val = set(np.unique(y_val))
    if len(unique_train) < 2 or len(unique_val) < 2:
        logger.warning("Insufficient class diversity for TabNet. Using uniform importance.")
        return {f: 1.0 / len(feature_names) for f in feature_names}, True

    try:
        clf = TabNetClassifier(
            n_d=16,
            n_a=16,
            n_steps=3,
            gamma=1.3,
            seed=random_state,
            verbose=0,
        )
        clf.fit(
            X_tr, y_tr,
            eval_set=[(X_val, y_val)],
            max_epochs=epochs,
            patience=10,
            batch_size=min(1024, split_idx),
            virtual_batch_size=min(256, split_idx // 4),
        )
        # `feature_importances_` is the aggregated attention mask (n_features,)
        importance_arr = clf.feature_importances_
        importances = {
            f: float(importance_arr[i]) for i, f in enumerate(feature_names)
        }
        logger.info(
            "TabNet trained. Top-5 features: %s",
            sorted(importances, key=importances.get, reverse=True)[:5],
        )
        return importances, True
    except Exception as exc:
        logger.warning("TabNet training failed: %s. Using uniform importance.", exc)
        return {f: 1.0 / len(feature_names) for f in feature_names}, False


# ── SHAP importance ───────────────────────────────────────────────────────────


def _compute_shap_importance(
    X: pd.DataFrame,
    y: np.ndarray,
    feature_names: List[str],
    n_samples: int,
    random_state: int,
) -> Dict[str, float]:
    """
    Compute mean |SHAP| values using a LightGBM classifier.

    Chronological split: last 20% as evaluation fold (no random shuffle).
    SHAP computed on a random subsample of the test fold for speed.

    Returns
    -------
    Dict[str, float] — {feature_name: mean_abs_shap}
    """
    try:
        import shap
        import lightgbm as lgb
    except ImportError:
        logger.warning("shap or lightgbm not available. SHAP importance unavailable.")
        return {f: 1.0 / len(feature_names) for f in feature_names}

    X_arr = X[feature_names].values.astype(np.float32)
    n = len(X_arr)
    split_idx = int(n * 0.8)

    X_tr, X_te = X_arr[:split_idx], X_arr[split_idx:]
    y_tr = y[:split_idx]

    # Remap for LightGBM multiclass
    y_tr_clf = np.where(y_tr == -1, 0, np.where(y_tr == 1, 2, 1))

    clf = lgb.LGBMClassifier(
        n_estimators=100,
        num_leaves=31,
        learning_rate=0.1,
        random_state=random_state,
        verbose=-1,
    )
    clf.fit(X_tr, y_tr_clf)

    # Subsample test fold for SHAP
    rng = np.random.default_rng(random_state)
    idx = rng.choice(len(X_te), size=min(n_samples, len(X_te)), replace=False)
    X_shap = X_te[idx]

    try:
        explainer = shap.TreeExplainer(clf)
        shap_values = explainer.shap_values(X_shap)
        # shap_values: list of n_classes arrays, each (n_samples, n_features)
        if isinstance(shap_values, list):
            mean_abs = np.mean([np.abs(sv).mean(axis=0) for sv in shap_values], axis=0)
        else:
            mean_abs = np.abs(shap_values).mean(axis=0)

        importances = {f: float(mean_abs[i]) for i, f in enumerate(feature_names)}
        logger.info(
            "SHAP computed. Top-5 features: %s",
            sorted(importances, key=importances.get, reverse=True)[:5],
        )
        return importances
    except Exception as exc:
        logger.warning("SHAP computation failed: %s. Using uniform importance.", exc)
        return {f: 1.0 / len(feature_names) for f in feature_names}


# ── Consensus pruning ─────────────────────────────────────────────────────────


def _find_consensus_pruned(
    feature_names: List[str],
    tabnet_importances: Dict[str, float],
    shap_importances: Dict[str, float],
    tabnet_available: bool,
) -> Tuple[List[str], Dict[str, str]]:
    """
    Flag features where BOTH TabNet AND SHAP consider them unimportant.

    When TabNet is unavailable, falls back to SHAP-only pruning with
    a stricter threshold (bottom 5% instead of 10%) and logs WARNING.

    Returns
    -------
    (pruned_features, rationale_dict)
    """
    n = len(feature_names)
    tabnet_vals = np.array([tabnet_importances.get(f, 0.0) for f in feature_names])
    shap_vals = np.array([shap_importances.get(f, 0.0) for f in feature_names])

    # Percentile thresholds
    tabnet_pct = _TABNET_UNIMPORTANT_PCT
    shap_pct = _SHAP_UNIMPORTANT_PCT

    tabnet_thresh = np.percentile(tabnet_vals, tabnet_pct)
    shap_thresh = np.percentile(shap_vals, shap_pct)

    pruned = []
    rationale: Dict[str, str] = {}

    for i, f in enumerate(feature_names):
        tabnet_rank = int(np.sum(tabnet_vals < tabnet_vals[i])) + 1
        shap_rank = int(np.sum(shap_vals < shap_vals[i])) + 1

        tabnet_low = tabnet_vals[i] <= tabnet_thresh
        shap_low = shap_vals[i] <= shap_thresh

        if tabnet_available:
            if tabnet_low and shap_low:
                pruned.append(f)
                rationale[f] = (
                    f"TabNet rank {tabnet_rank}/{n} (score={tabnet_vals[i]:.6f}) + "
                    f"SHAP rank {shap_rank}/{n} (mean|shap|={shap_vals[i]:.6f})"
                )
        else:
            # SHAP-only fallback: stricter 5th percentile
            shap_strict_thresh = np.percentile(shap_vals, 5.0)
            if shap_vals[i] <= shap_strict_thresh:
                pruned.append(f)
                rationale[f] = (
                    f"SHAP-only (TabNet unavailable): rank {shap_rank}/{n} "
                    f"(mean|shap|={shap_vals[i]:.6f})"
                )

    return pruned, rationale


# ── Features doc annotation ───────────────────────────────────────────────────


def _annotate_features_doc(
    doc_path: Path,
    pruned_features: List[str],
    rationale: Dict[str, str],
) -> None:
    """
    Annotate alphalens_docs/01_features.md with pruned feature notes.

    For each pruned feature found in a Markdown table row, appends
    "Phase 3 pruned — <reason>" to the description column.
    """
    try:
        content = doc_path.read_text()
        lines = content.split("\n")
        modified = False

        for i, line in enumerate(lines):
            # Table rows start with | and contain a backtick feature name
            if "|" not in line or "`" not in line:
                continue
            # Already annotated
            if "Phase 3 pruned" in line:
                continue
            for feat in pruned_features:
                # Match the feature name inside backticks
                if f"`{feat}`" in line:
                    # Append annotation to last table column
                    annotation = f" Phase 3 pruned — {rationale.get(feat, 'consensus TabNet+SHAP')}"
                    # Add to the last | ... | column
                    if line.rstrip().endswith("|"):
                        lines[i] = line.rstrip()[:-1] + annotation + " |"
                    else:
                        lines[i] = line + annotation
                    modified = True
                    logger.info("Annotated feature '%s' in %s", feat, doc_path.name)

        if modified:
            doc_path.write_text("\n".join(lines))
            logger.info("01_features.md updated with %d pruned feature annotations.", len(pruned_features))
        else:
            logger.warning(
                "No feature names from pruned list found in %s table rows. "
                "Manual annotation may be required.",
                doc_path.name,
            )
    except Exception as exc:
        logger.warning("Could not annotate features doc: %s", exc)


# ── CLI entry point ───────────────────────────────────────────────────────────


def main() -> None:
    import argparse
    import glob

    parser = argparse.ArgumentParser(description="M-14: TabNet + SHAP feature selection")
    parser.add_argument("--parquet", type=str, nargs="+", help="Feature Parquet file(s) or glob pattern")
    parser.add_argument("--target", type=str, default="label_5d", help="Label column name")
    parser.add_argument("--tabnet-epochs", type=int, default=50)
    parser.add_argument("--shap-samples", type=int, default=500)
    parser.add_argument("--out-dir", type=str, default="datastore/features/metadata")
    parser.add_argument("--features-doc", type=str, default="alphalens_docs/01_features.md")
    args = parser.parse_args()

    if not args.parquet:
        parser.error("--parquet is required")

    paths = []
    for pattern in args.parquet:
        paths.extend(glob.glob(pattern))
    if not paths:
        raise FileNotFoundError(f"No files matched: {args.parquet}")

    logger.info("Loading %d Parquet files...", len(paths))
    frames = [pd.read_parquet(p) for p in sorted(paths)]
    df = pd.concat(frames, ignore_index=True).sort_values("date")

    if args.target not in df.columns:
        raise ValueError(f"Target column '{args.target}' not in DataFrame.")

    id_cols = {"date", "ticker", args.target}
    feature_cols = [c for c in df.columns if c not in id_cols]
    X = df[feature_cols].copy()
    y = df[args.target]

    result = run_feature_selection(X, y, feature_names=feature_cols, tabnet_epochs=args.tabnet_epochs)
    result.save_artifacts(metadata_dir=args.out_dir, features_doc=args.features_doc)

    import pprint
    pprint.pprint(result.to_dict())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
