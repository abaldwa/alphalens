"""
systems/ml_signal_engine/models/pnd/pnd_detector.py

Phase: 1.3 (P&D Detection)
Specs: SPEC-MODEL-006, SPEC-MODEL-004, SPEC-SOLID-003, SPEC-SOLID-004
Owner: ml_signal_engine / pnd
Consumers: ingestion/scheduler/daily_pipeline (P&D pre-filter, runs BEFORE
           any buy signal — SPEC-MODEL-006), systems/ml_signal_engine

M-06: Pump & Dump pre-filter. LightGBM (primary, supervised) + an
IsolationForest anomaly layer (secondary, unsupervised — catches patterns
that don't resemble the labeled training distribution, per the prompt's
"LightGBM primary + IsolationForest anomaly layer"). SMOTETomek rebalances
the expected 1-3% positive rate on the training fold only (SPEC-MODEL-004).

Training data honesty (Phase 1 gap, documented not hidden): no NSE
circular archive of confirmed P&D cases is ingested as of Phase 1 — there
is no `ingestion/scrapers/nse_circulars.py` or equivalent. `train()`
accepts whatever (X, y) the caller supplies; `generate_synthetic_training_
data()` below is a stand-in data source (synthetic P&D-pattern positives +
synthetic normal-trading negatives, both built from features/pnd_features.py
itself) used only by this module's own tests and as a documented fallback
for an empty/missing real archive. A trained PnDDetector is only as
trustworthy as its training data — replacing the synthetic positives with
real confirmed cases is real follow-up ingestion work, not a code change
here.
"""

import logging
from typing import Any, Dict, List, Optional

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from imblearn.combine import SMOTETomek
from sklearn.ensemble import IsolationForest

from config.settings import PND_BLOCK_THRESHOLD, PND_FLAG_THRESHOLD
from contracts.interfaces import IClassificationModel
from features.pnd_features import PND_FEATURES, compute_pnd_features

logger = logging.getLogger(__name__)

PND_PHASES = ["normal", "accumulation", "pump", "dump", "aftermath"]

# LightGBM probability vs IsolationForest anomaly weighting in the final
# pnd_score — LightGBM is the "primary" signal per the build prompt, the
# anomaly layer a secondary catch-all for out-of-distribution patterns.
_LGBM_WEIGHT = 0.7
_ANOMALY_WEIGHT = 0.3


class PnDDetector(IClassificationModel):
    """
    SPEC-MODEL-006: P&D pre-filter. score > PND_BLOCK_THRESHOLD (60) is a
    hard block — checked before any buy signal reaches the user. score >
    PND_FLAG_THRESHOLD (40) is a flag-only warning. Both thresholds are
    read from config.settings at predict time, never hardcoded here.
    """

    def __init__(self, random_state: int = 42, lgbm_params: Optional[Dict[str, Any]] = None) -> None:
        self.random_state = random_state
        self._lgbm_params = lgbm_params or {
            "n_estimators": 200,
            "max_depth": 5,
            "learning_rate": 0.05,
            "random_state": random_state,
            "verbose": -1,
        }
        self._lgbm: Optional[lgb.LGBMClassifier] = None
        self._iso_forest: Optional[IsolationForest] = None
        self._anomaly_min: Optional[float] = None
        self._anomaly_max: Optional[float] = None
        self._feature_names: List[str] = list(PND_FEATURES)
        self._trained_at: Optional[pd.Timestamp] = None
        self._training_samples: Optional[int] = None
        self._class_ratio_before: Optional[float] = None
        self._class_ratio_after: Optional[float] = None

    def train(self, X: pd.DataFrame, y: pd.Series, sample_weight: Optional[pd.Series] = None) -> None:
        """
        Train LightGBM (on SMOTETomek-resampled data) and IsolationForest
        (on the original, unresampled data — anomaly detection should
        learn what "normal" looks like, not a synthetically rebalanced
        distribution).

        Parameters
        ----------
        X : pd.DataFrame
            Columns: PND_FEATURES (or a superset containing them). NaN
            rows are dropped before training (LightGBM tolerates NaN at
            inference, but SMOTETomek's neighbor search cannot).
        y : pd.Series
            Binary labels, 1 = confirmed/synthetic P&D, 0 = normal.
        sample_weight : pd.Series, optional
            Unused by LightGBM here (class imbalance is handled via
            SMOTETomek instead, per SPEC-MODEL-004) — accepted only for
            IModel interface compliance.

        Spec References
        ----------------
        SPEC-MODEL-004: "SMOTE on training data ONLY... Class weight
        logging: positive/negative ratio before and after resampling."

        Raises
        ------
        ValueError
            If X/y are empty after dropping NaN rows, or shapes mismatch.
        """
        if len(X) != len(y):
            raise ValueError(f"X has {len(X)} rows, y has {len(y)} — must match")

        frame = X[self._feature_names].copy()
        frame["_y"] = y.to_numpy()
        frame = frame.dropna()
        if frame.empty:
            raise ValueError("no valid (non-NaN) training rows after dropping NaN")

        X_clean = frame[self._feature_names]
        y_clean = frame["_y"].astype(int)

        positive_before = int(y_clean.sum())
        self._class_ratio_before = positive_before / len(y_clean)
        logger.info(
            f"P&D training class ratio before resampling: {positive_before}/{len(y_clean)} "
            f"({self._class_ratio_before:.2%} positive)"
        )

        if positive_before == 0 or positive_before == len(y_clean):
            logger.warning("Training data has only one class — skipping SMOTETomek")
            X_resampled, y_resampled = X_clean, y_clean
        else:
            smote_tomek = SMOTETomek(random_state=self.random_state)
            X_resampled, y_resampled = smote_tomek.fit_resample(X_clean, y_clean)

        positive_after = int(np.asarray(y_resampled).sum())
        self._class_ratio_after = positive_after / len(y_resampled)
        logger.info(
            f"P&D training class ratio after resampling: {positive_after}/{len(y_resampled)} "
            f"({self._class_ratio_after:.2%} positive)"
        )

        self._lgbm = lgb.LGBMClassifier(**self._lgbm_params)
        self._lgbm.fit(X_resampled, y_resampled)

        # Anomaly layer trains on the original (unresampled) distribution.
        self._iso_forest = IsolationForest(n_estimators=200, random_state=self.random_state, contamination="auto")
        self._iso_forest.fit(X_clean)
        anomaly_scores = -self._iso_forest.score_samples(X_clean)  # higher = more anomalous
        self._anomaly_min = float(anomaly_scores.min())
        self._anomaly_max = float(anomaly_scores.max())

        self._trained_at = pd.Timestamp.now()
        self._training_samples = len(X_clean)

    def _lgbm_proba_positive(self, X: pd.DataFrame) -> np.ndarray:
        if self._lgbm is None:
            raise RuntimeError("predict called before train()")
        proba = self._lgbm.predict_proba(X[self._feature_names])
        positive_idx = list(self._lgbm.classes_).index(1)
        return proba[:, positive_idx]

    def _anomaly_score_normalized(self, X: pd.DataFrame) -> np.ndarray:
        raw = -self._iso_forest.score_samples(X[self._feature_names])
        span = (self._anomaly_max - self._anomaly_min) or 1e-9
        return np.clip((raw - self._anomaly_min) / span, 0.0, 1.0)

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """Binary prediction (1 = pnd_block) from the combined pnd_score, not the LightGBM-only label."""
        full = self.predict_full(X)
        return full["pnd_block"].astype(int).rename(None)

    def predict_proba(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        SPEC-MODEL-001 (IClassificationModel): per-class probabilities.

        Returns
        -------
        pd.DataFrame
            Columns 'normal', 'pnd' (LightGBM's own class probabilities —
            not blended with the anomaly layer; see predict_full for the
            blended pnd_score used for blocking decisions).
        """
        if self._lgbm is None:
            raise RuntimeError("predict_proba called before train()")
        pnd_proba = self._lgbm_proba_positive(X)
        return pd.DataFrame({"normal": 1 - pnd_proba, "pnd": pnd_proba}, index=X.index)

    def predict_full(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Full P&D pre-filter output (SPEC-MODEL-006).

        Parameters
        ----------
        X : pd.DataFrame
            Columns: PND_FEATURES (or a superset). Rows with any NaN
            feature get pnd_score=NaN, pnd_phase='normal', pnd_block=False,
            pnd_flag=False — fail-open is NOT acceptable for a safety
            filter by silently passing NaN through as a block; fail-open
            here means "can't score it" surfaces as NaN, not as a false
            "safe" signal, so callers must treat NaN pnd_score as
            "uninspectable, do not auto-buy" upstream, not as "normal".

        Returns
        -------
        pd.DataFrame
            Columns: pnd_score (0-100, float), pnd_phase (one of
            PND_PHASES), pnd_block (bool), pnd_flag (bool). Index matches X.

        Spec References
        ----------------
        SPEC-MODEL-006: pnd_block = pnd_score > PND_BLOCK_THRESHOLD (60,
        from config.settings, never hardcoded); pnd_flag = pnd_score >
        PND_FLAG_THRESHOLD (40).

        Raises
        ------
        RuntimeError
            If called before train().
        """
        if self._lgbm is None or self._iso_forest is None:
            raise RuntimeError("predict_full called before train()")

        valid = X[self._feature_names].notna().all(axis=1)
        pnd_score = pd.Series(np.nan, index=X.index, dtype=np.float64)
        phase = pd.Series("normal", index=X.index, dtype=object)

        if valid.any():
            X_valid = X.loc[valid]
            lgbm_p = self._lgbm_proba_positive(X_valid)
            anomaly_p = self._anomaly_score_normalized(X_valid)
            blended = _LGBM_WEIGHT * lgbm_p + _ANOMALY_WEIGHT * anomaly_p
            scores = blended * 100.0
            pnd_score.loc[valid] = scores
            phase.loc[valid] = _classify_phase(X_valid, scores)

        pnd_block = pnd_score > PND_BLOCK_THRESHOLD
        pnd_flag = pnd_score > PND_FLAG_THRESHOLD
        # NaN comparisons are already False via pandas — explicit for readability/auditability.
        pnd_block = pnd_block.where(pnd_score.notna(), False)
        pnd_flag = pnd_flag.where(pnd_score.notna(), False)

        return pd.DataFrame(
            {"pnd_score": pnd_score, "pnd_phase": phase, "pnd_block": pnd_block, "pnd_flag": pnd_flag},
            index=X.index,
        )

    def save(self, path: str) -> None:
        """SPEC-MODEL-005: versioned model file (caller chooses the {name}_v{date}.pkl path)."""
        if self._lgbm is None:
            raise RuntimeError("save called before train()")
        payload = {
            "lgbm": self._lgbm,
            "iso_forest": self._iso_forest,
            "anomaly_min": self._anomaly_min,
            "anomaly_max": self._anomaly_max,
            "feature_names": self._feature_names,
            "random_state": self.random_state,
            "trained_at": self._trained_at,
            "training_samples": self._training_samples,
            "class_ratio_before": self._class_ratio_before,
            "class_ratio_after": self._class_ratio_after,
        }
        joblib.dump(payload, path)

    def load(self, path: str) -> None:
        payload = joblib.load(path)
        self._lgbm = payload["lgbm"]
        self._iso_forest = payload["iso_forest"]
        self._anomaly_min = payload["anomaly_min"]
        self._anomaly_max = payload["anomaly_max"]
        self._feature_names = payload["feature_names"]
        self.random_state = payload["random_state"]
        self._trained_at = payload["trained_at"]
        self._training_samples = payload["training_samples"]
        self._class_ratio_before = payload["class_ratio_before"]
        self._class_ratio_after = payload["class_ratio_after"]

    def metadata(self) -> Dict[str, Any]:
        return {
            "name": "PnDDetector",
            "version": "1.3.0",
            "created_at": self._trained_at,
            "features_count": len(self._feature_names),
            "hyperparams": self._lgbm_params,
            "training_samples": self._training_samples,
            "class_ratio_before_resampling": self._class_ratio_before,
            "class_ratio_after_resampling": self._class_ratio_after,
        }


def _classify_phase(X: pd.DataFrame, scores: np.ndarray) -> np.ndarray:
    """
    Rule-based P&D phase classification from already-computed PND_FEATURES
    (the prompt names the 5 phases but gives no formula — these rules are
    a documented, reasonable first cut, not a trained classifier; expect
    to revisit once real labeled P&D episodes are available to validate
    phase transitions against, not just the binary score).

    Priority order (first match wins): dump > aftermath > pump >
    accumulation > normal. Only rows with an elevated score (>
    PND_FLAG_THRESHOLD) are eligible for a non-'normal' phase.
    """
    elevated = scores > PND_FLAG_THRESHOLD
    is_dumping = elevated & (X["reversal_after_spike_flag"].to_numpy() > 0)
    is_aftermath = (
        elevated
        & ~is_dumping
        & (X["price_acceleration_5d"].to_numpy() < 0)
        & (X["consecutive_up_days"].to_numpy() == 0)
    )
    is_pumping = (
        elevated & ~is_dumping & ~is_aftermath
        & (X["price_acceleration_5d"].to_numpy() > 0)
        & (X["consecutive_up_days"].to_numpy() > 0)
    )
    is_accumulating = elevated & ~is_dumping & ~is_aftermath & ~is_pumping

    return np.select(
        [is_dumping, is_aftermath, is_pumping, is_accumulating],
        ["dump", "aftermath", "pump", "accumulation"],
        default="normal",
    )


def _make_synthetic_pnd_ohlcv(ticker: str, n_days: int, seed: int) -> pd.DataFrame:
    """One synthetic P&D-pattern OHLCV series: quiet base, volume+price spike, collapse."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start="2023-01-01", periods=n_days)
    base_days = int(n_days * 0.5)
    pump_days = int(n_days * 0.2)
    dump_days = n_days - base_days - pump_days

    base_price = 20 + rng.uniform(0, 30)  # small-cap price range, typical P&D target
    base_rets = rng.normal(0.0, 0.01, base_days)
    pump_rets = rng.uniform(0.04, 0.08, pump_days)  # near-daily-circuit moves
    dump_rets = rng.uniform(-0.10, -0.03, dump_days)
    rets = np.concatenate([base_rets, pump_rets, dump_rets])
    close = base_price * np.cumprod(1 + rets)

    base_vol = rng.uniform(50_000, 150_000, base_days)
    pump_vol = base_vol.mean() * rng.uniform(8, 15, pump_days)
    dump_vol = base_vol.mean() * rng.uniform(3, 8, dump_days)
    volume = np.concatenate([base_vol, pump_vol, dump_vol])

    base_delivery = rng.uniform(40, 70, base_days)
    pump_delivery = rng.uniform(2, 10, pump_days)  # delivery collapses during the pump
    dump_delivery = rng.uniform(5, 20, dump_days)
    delivery_pct = np.concatenate([base_delivery, pump_delivery, dump_delivery])

    open_ = close * (1 + rng.normal(0, 0.003, n_days))
    high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.003, n_days)))
    low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.003, n_days)))

    return pd.DataFrame(
        {
            "date": dates,
            "ticker": ticker,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "delivery_pct": delivery_pct,
        }
    )


def _make_synthetic_normal_ohlcv(ticker: str, n_days: int, seed: int) -> pd.DataFrame:
    """One synthetic normal-trading OHLCV series (no P&D pattern) — same shape as
    tests/unit/test_features_technical.py's generator, kept local to avoid a test->prod import."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start="2023-01-01", periods=n_days)
    base_price = 100 + rng.uniform(0, 900)
    rets = rng.normal(0.0003, 0.02, n_days)
    close = base_price * np.cumprod(1 + rets)
    open_ = close * (1 + rng.normal(0, 0.005, n_days))
    high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.005, n_days)))
    low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.005, n_days)))
    volume = rng.integers(100_000, 5_000_000, n_days).astype(float)
    delivery_pct = rng.uniform(30, 80, n_days)
    return pd.DataFrame(
        {
            "date": dates,
            "ticker": ticker,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "delivery_pct": delivery_pct,
        }
    )


def generate_synthetic_training_data(
    n_positive: int = 20, n_negative: int = 980, n_days: int = 90, seed: int = 0
) -> tuple:
    """
    Build a synthetic (X, y) training set: synthetic P&D-pattern positives
    + synthetic normal-trading negatives (~2% positive rate by default,
    inside the prompt's expected 1-3% range).

    Documented Phase-1 stand-in for a real NSE circular archive — see
    module docstring. Each synthetic series is run through features.
    pnd_features.compute_pnd_features() so X has the exact same shape/
    distribution properties a real caller would pass to train().

    Parameters
    ----------
    n_positive, n_negative : int
        Number of synthetic P&D / normal tickers to generate.
    n_days : int
        Trading days per synthetic ticker (>= 60 to populate every
        PND_FEATURES column without NaN warm-up gaps).
    seed : int
        Base RNG seed (offset per ticker for independence).

    Returns
    -------
    (X, y) : (pd.DataFrame, pd.Series)
        X has PND_FEATURES columns (last trading day of each synthetic
        ticker only — one row per ticker, matching how a daily pipeline
        would score "today"). y is 1 for P&D-pattern tickers, 0 otherwise.

    Raises
    ------
    None
    """
    frames = []
    for i in range(n_positive):
        frames.append(_make_synthetic_pnd_ohlcv(f"PND{i:04d}", n_days, seed + i))
    for i in range(n_negative):
        frames.append(_make_synthetic_normal_ohlcv(f"NORM{i:04d}", n_days, seed + n_positive + i))

    ohlcv = pd.concat(frames, ignore_index=True)
    features = compute_pnd_features(ohlcv)
    last_per_ticker = features.sort_values("date").groupby("ticker", sort=False).tail(1)

    y = last_per_ticker["ticker"].str.startswith("PND").astype(int)
    y.index = last_per_ticker.index
    X = last_per_ticker[PND_FEATURES]
    return X.reset_index(drop=True), y.reset_index(drop=True)
