"""
systems/ml_signal_engine/models/multibagger/multibagger_model.py

Phase: 2.4 (Multibagger Detection System M-08)
Specs: SPEC-MODEL-001, SPEC-MODEL-002, SPEC-SOLID-003, SPEC-SOLID-004
Owner: ml_signal_engine / multibagger
Consumers: systems/ml_signal_engine/inference (weekly watchlist job)

M-08: MultibaggerModel. LightGBM (`lambdarank` objective, primary) ranks
stocks by probability of a 2x+ return within 3 years; a calibration step
(Platt-scaling LogisticRegression on the ranker's raw scores) converts the
unbounded ranking score into a genuine `mb_probability` in [0, 1]. A
Random Survival Forest (scikit-survival) is fit on time-to-multibag
(event=achieved 2x, duration=months) to produce survival curves at
6/12/18/24/36 months. `mb_tier` and `mb_archetype` are deterministic,
rule-based derivations (see _classify_tier/_classify_archetype) — see
build-prompt reconciliation below for why these are NOT separately
ML-trained.

[AS BUILT] Algorithm: the build prompt says "LightGBM lambdarank
(primary) + Random Survival Forest" — 02_models.md additionally lists
CatBoost as a third ensemble member. CatBoost is NOT implemented here;
the prompt's literal two-algorithm list governs (same "prompt text over
older doc" precedent applied throughout this phase).

[AS BUILT] Inputs: the build prompt is explicit and arithmetically
self-consistent — "109 features: 76 technical + 33 multibagger-specific —
NO fundamental features in Phase 2" (76 + 33 = 109). 02_models.md's
"two-tower" architecture (a second tower of 28 fundamental + 12
governance features, fused via concatenation) is the doc's OWN stated
"Option B, later" — explicitly deferred here per the prompt's "NO
fundamental features in Phase 2" instruction, not silently dropped.

[AS BUILT] Label construction: the build prompt's literal instruction is
a SINGLE BINARY label ("1 if stock returned 2x+ within 3 years; 0
otherwise") — not 02_models.md's separate 5-class ('2x'/'3x'/'5x'/'10x'/
'none') multi-tier scan over a 5-year window. Only the binary label is
actually trained on (build_binary_labels below); `mb_tier` is then a
deterministic mapping from the single calibrated `mb_probability` onto
fixed thresholds (_classify_tier) — an honest reflection of what this
model actually learned, not an implied (but never built) 5-class
classifier. `mb_archetype` is similarly rule-based
(_classify_archetype), the same precedent
systems/ml_signal_engine/models/pnd/pnd_detector.py already set with its
own `_classify_phase` for a categorical sub-output with no separate
training-label instructions in its build prompt.

[AS BUILT] P&D exclusion ("Validates: P&D episodes excluded from positive
labels"): the build prompt names `forensic_composite` (M-09's classical
forensic score) as the exclusion signal, but M-09 has not been built yet
in this codebase (Phase 2.5, still pending — see BuildLog.md's Phase 2
prompt list). `build_binary_labels` uses the REAL, already-built P&D
signal instead — systems/ml_signal_engine/models/pnd/pnd_detector.py's
`pnd_score`, thresholded at config.settings.PND_FLAG_THRESHOLD (40) —
the same real signal and threshold this phase's own test deliverable
checks ("top-20 list excludes any stock with pnd_score > 40"). Swapping
in `forensic_composite` once M-09 exists is a data-source change, not an
interface change (`build_binary_labels`'s `pnd_scores` parameter accepts
any 0-100 manipulation-likelihood score, regardless of source).

load_multibagger_training_data_from_db() below is the only supported
training-data source: a real binary "2x within label_window_days" label
computed from ohlcv_adjusted, joined against features.multibagger's real
feature computation. There is no synthetic-data fallback — it raises if
the database is empty/unreachable or if no ticker meets the return
threshold, rather than fabricating positives. See BuildLog.md "Real data
sourcing — Multibagger".
"""

import logging
from typing import Any, Dict, List, Optional

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sksurv.ensemble import RandomSurvivalForest
from sksurv.util import Surv

from config.settings import PND_FLAG_THRESHOLD
from contracts.interfaces import ISurvivalModel

logger = logging.getLogger(__name__)

MB_TIERS = ["none", "2x", "3x", "5x", "10x"]
MB_ARCHETYPES = ["long_base_breakout", "post_crash_recovery", "quiet_accumulator", "sector_rotation_leader"]
SURVIVAL_HORIZONS_MONTHS = (6, 12, 18, 24, 36)
LABEL_WINDOW_YEARS = 3

# build prompt: "Top-20 weekly watchlist ... mb_probability > 0.30"
WATCHLIST_PROBABILITY_THRESHOLD = 0.30
WATCHLIST_SIZE = 20

# Deterministic mb_probability -> mb_tier thresholds (see module docstring's
# label-construction reconciliation) — only a single binary target is
# actually trained on, so tier is a documented, fixed mapping from that
# one calibrated probability, not a separately-trained 5-class model.
_TIER_THRESHOLDS = (
    (0.80, "10x"),
    (0.60, "5x"),
    (0.45, "3x"),
    (0.30, "2x"),
)

MB_OUTPUT_COLUMNS = [
    "mb_probability", "mb_tier", "mb_archetype",
    "mb_survival_6m", "mb_survival_12m", "mb_survival_18m", "mb_survival_24m", "mb_survival_36m",
]

def _classify_tier(probability: pd.Series) -> pd.Series:
    tier = pd.Series("none", index=probability.index, dtype=object)
    for threshold, label in reversed(_TIER_THRESHOLDS):
        tier.loc[probability >= threshold] = label
    tier.loc[probability.isna()] = np.nan
    return tier


def _classify_archetype(X: pd.DataFrame) -> pd.Series:
    """
    Rule-based archetype classifier on the 33 multibagger features — same
    "deterministic categorical sub-output, no separate training-label
    instructions" precedent as pnd_detector.py's `_classify_phase`.

    Priority order (first match wins): a sharp post-crash recovery beats a
    long base, which beats quiet accumulation; sector_rotation_leader is
    the default for a strong-but-not-otherwise-distinctive mover.
    """
    archetype = pd.Series("sector_rotation_leader", index=X.index, dtype=object)

    recovery = X.get("recovery_from_correction", pd.Series(np.nan, index=X.index))
    base_len = X.get("base_length_days", pd.Series(np.nan, index=X.index))
    base_tight = X.get("base_tightness_pct", pd.Series(np.nan, index=X.index))
    quiet = X.get("quiet_accumulation_score", pd.Series(np.nan, index=X.index))

    archetype.loc[(quiet >= 60) & (base_tight <= 12)] = "quiet_accumulator"
    archetype.loc[(base_len >= 100) & (base_tight <= 10)] = "long_base_breakout"
    archetype.loc[recovery >= 80] = "post_crash_recovery"

    all_nan = recovery.isna() & base_len.isna() & base_tight.isna() & quiet.isna()
    archetype.loc[all_nan] = np.nan
    return archetype


def build_binary_labels(
    prices: pd.DataFrame,
    pnd_scores: Optional[pd.Series] = None,
    window_years: int = LABEL_WINDOW_YEARS,
    pnd_threshold: float = PND_FLAG_THRESHOLD,
) -> pd.DataFrame:
    """
    Build prompt's literal label: binary 1 if a stock-date snapshot's
    forward max return over `window_years` reaches 2x (+100%), else 0.
    P&D-flagged episodes are excluded from positive labels (downgraded to
    0) — see module docstring's P&D-exclusion reconciliation.

    Parameters
    ----------
    prices : pd.DataFrame
        Long-format: date, ticker, close. One row per (ticker, date),
        trading-day granularity, "confirmed historical data only" (no
        forward-looking price beyond what's actually in this frame).
    pnd_scores : pd.Series, optional
        Same index as `prices`, 0-100 P&D-likelihood score (e.g.
        pnd_detector.py's pnd_score) at each (ticker, date) snapshot. A
        positive label is downgraded to 0 where pnd_scores > pnd_threshold.
        If omitted, no P&D exclusion is applied (documented, not silent —
        logged as a warning).
    window_years : int
    pnd_threshold : float

    Returns
    -------
    pd.DataFrame
        Columns: date, ticker, label (0/1), max_return, event (0/1, same
        as label — kept distinct for clarity at survival-model call
        sites), duration_months (months until the 2x level was first
        reached, or `window_years * 12` if never reached / right-censored
        — see SPEC-MODEL-001's >= 756 trading day minimum for this to be
        meaningful).

    Spec References
    ----------------
    SPEC-MODEL-002: "P&D episodes excluded from positive labels in
    multibagger model."
    SPEC-MODEL-001: "Multibagger: >= 756 trading days for meaningful
    label coverage" — rows without a full forward window are correctly
    right-censored (event=0), not dropped; the CALLER is responsible for
    excluding snapshots without enough trailing history to be meaningful.

    Raises
    ------
    None
    """
    if pnd_scores is None:
        logger.warning("build_binary_labels: no pnd_scores supplied — P&D exclusion is skipped")

    window_days = int(window_years * 252)
    df = prices.sort_values(["ticker", "date"]).reset_index(drop=True)

    records = []
    for ticker, g in df.groupby("ticker", sort=False):
        g = g.reset_index(drop=True)
        close = g["close"].to_numpy(dtype=np.float64)
        n = len(close)
        for i in range(n):
            fwd = close[i:i + window_days]
            if len(fwd) < 2:
                continue
            fwd_returns = fwd / close[i] - 1.0
            max_return = float(np.nanmax(fwd_returns))
            hit_idx = np.argmax(fwd_returns >= 1.0) if (fwd_returns >= 1.0).any() else None
            label = int(max_return >= 1.0)
            duration_months = (
                float(hit_idx) / 252.0 * 12.0 if hit_idx is not None else min(len(fwd), window_days) / 252.0 * 12.0
            )
            records.append(
                {
                    "date": g["date"].iloc[i], "ticker": ticker, "label": label,
                    "max_return": max_return, "duration_months": max(duration_months, 0.5),
                }
            )

    out = pd.DataFrame(records)
    if out.empty:
        return out.assign(event=pd.Series(dtype=int))

    if pnd_scores is not None:
        pnd_lookup = pd.DataFrame(
            {"date": prices["date"], "ticker": prices["ticker"], "pnd_score": pnd_scores.to_numpy()}
        )
        out = out.merge(pnd_lookup, on=["date", "ticker"], how="left")
        pnd_excluded = (out["label"] == 1) & (out["pnd_score"].fillna(0) > pnd_threshold)
        if pnd_excluded.any():
            logger.info(f"build_binary_labels: {int(pnd_excluded.sum())} P&D-flagged positives downgraded to 0")
        out.loc[pnd_excluded, "label"] = 0
        out = out.drop(columns=["pnd_score"])

    out["event"] = out["label"]
    return out


class MultibaggerModel(ISurvivalModel):
    """M-08: LightGBM lambdarank (calibrated to mb_probability) + Random Survival Forest."""

    def __init__(self, random_state: int = 42, n_estimators: int = 200) -> None:
        self.random_state = random_state
        self.n_estimators = n_estimators

        self._ranker: Optional[lgb.LGBMRanker] = None
        self._calibrator: Optional[LogisticRegression] = None
        self._rsf: Optional[RandomSurvivalForest] = None
        self._imputer: Optional[SimpleImputer] = None
        self._feature_names: Optional[List[str]] = None
        self._trained_at = None
        self._training_samples: Optional[int] = None
        self._event_time_bounds: Optional[tuple] = None

    # ===== NaN handling (same pattern as base_signal_model.py / exit_signal.py) =====
    def _impute_fit(self, X: pd.DataFrame) -> pd.DataFrame:
        self._imputer = SimpleImputer(strategy="median", keep_empty_features=True)
        imputed = self._imputer.fit_transform(X)
        return pd.DataFrame(imputed, columns=X.columns, index=X.index)

    def _impute_transform(self, X: pd.DataFrame) -> pd.DataFrame:
        if self._imputer is None:
            raise RuntimeError("predict called before train()/train_full()")
        imputed = self._imputer.transform(X)
        return pd.DataFrame(imputed, columns=X.columns, index=X.index)

    # ===== IModel =====
    def train(self, X: pd.DataFrame, y: pd.Series, sample_weight: Optional[pd.Series] = None) -> None:
        """
        IModel-compliant simple fit: lambdarank ranker (single group = the
        whole dataset, a degenerate ranking — every row competes against
        every other row) + Platt-scaling calibration. No Random Survival
        Forest fit — use train_full() for the complete pipeline.

        Raises
        ------
        ValueError
            If X/y are empty, shapes mismatch, or y has values outside {0, 1}.
        """
        if len(X) != len(y):
            raise ValueError(f"X has {len(X)} rows, y has {len(y)} rows")
        valid = y.notna()
        if not valid.any():
            raise ValueError("no rows with a non-NaN label")
        if not set(y.dropna().unique()).issubset({0, 1}):
            raise ValueError("y must be binary {0, 1}")

        self._feature_names = list(X.columns)
        X_valid, y_valid = X.loc[valid, self._feature_names], y.loc[valid].astype(int)
        X_imputed = self._impute_fit(X_valid)

        self._fit_ranker_and_calibrator(X_imputed, y_valid, groups=[len(X_imputed)])
        self._trained_at = pd.Timestamp.now()
        self._training_samples = len(X_imputed)

    def _fit_ranker_and_calibrator(self, X: pd.DataFrame, y: pd.Series, groups: List[int]) -> None:
        self._ranker = lgb.LGBMRanker(
            objective="lambdarank", metric="ndcg", ndcg_eval_at=[10, 20],
            n_estimators=self.n_estimators, max_depth=5, learning_rate=0.05,
            random_state=self.random_state, verbose=-1,
        )
        self._ranker.fit(X, y, group=groups)

        raw_scores = self._ranker.predict(X).reshape(-1, 1)
        self._calibrator = LogisticRegression(random_state=self.random_state)
        if y.nunique() < 2:
            logger.warning("Only one class present — calibrator will be degenerate (constant probability)")
        self._calibrator.fit(raw_scores, y)

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """IModel: calibrated mb_probability per row (the primary ranking-derived target)."""
        if self._ranker is None:
            raise RuntimeError("predict called before train()/train_full()")
        X_imputed = self._impute_transform(X[self._feature_names])
        raw_scores = self._ranker.predict(X_imputed).reshape(-1, 1)
        proba = self._calibrator.predict_proba(raw_scores)[:, 1]
        return pd.Series(proba, index=X.index).clip(0, 1)

    def save(self, path: str) -> None:
        if self._ranker is None:
            raise RuntimeError("save called before train()/train_full()")
        joblib.dump(
            {
                "ranker": self._ranker, "calibrator": self._calibrator, "rsf": self._rsf,
                "imputer": self._imputer, "feature_names": self._feature_names,
                "random_state": self.random_state, "n_estimators": self.n_estimators,
                "trained_at": self._trained_at, "training_samples": self._training_samples,
                "event_time_bounds": self._event_time_bounds,
            },
            path,
        )

    def load(self, path: str) -> None:
        payload = joblib.load(path)
        self._ranker = payload["ranker"]
        self._calibrator = payload["calibrator"]
        self._rsf = payload["rsf"]
        self._imputer = payload["imputer"]
        self._feature_names = payload["feature_names"]
        self.random_state = payload["random_state"]
        self.n_estimators = payload["n_estimators"]
        self._trained_at = payload["trained_at"]
        self._training_samples = payload["training_samples"]
        self._event_time_bounds = payload["event_time_bounds"]

    def metadata(self) -> Dict[str, Any]:
        return {
            "name": "MultibaggerModel",
            "version": "2.4.0",
            "created_at": self._trained_at,
            "features_count": len(self._feature_names) if self._feature_names else 0,
            "training_samples": self._training_samples,
            "tiers": MB_TIERS,
            "archetypes": MB_ARCHETYPES,
        }

    # ===== Full M-08 pipeline =====
    def train_full(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        duration_months: pd.Series,
        event: pd.Series,
        groups: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """
        Full M-08 pipeline: lambdarank ranker + Platt-scaling calibration
        (-> mb_probability) + Random Survival Forest (-> survival curves).

        Parameters
        ----------
        X : pd.DataFrame
            76 technical + 33 multibagger features (109 total — see module
            docstring; any feature set is actually accepted, matching
            every other model in this project's "trains on whatever
            columns X has" convention).
        y : pd.Series
            Binary label in {0, 1} — build_binary_labels()'s output,
            P&D-excluded.
        duration_months : pd.Series
            Months until the 2x level was reached (event=1) or the
            censoring point (event=0) — build_binary_labels()'s
            duration_months column.
        event : pd.Series
            1 if the 2x level was reached within the observation window, 0
            if censored.
        groups : list of int, optional
            Lambdarank group sizes (e.g. one group per as-of-date
            snapshot — rows within a group are ranked against each
            other). Defaults to one group = the whole dataset.

        Returns
        -------
        dict
            Diagnostics: training_samples, positive_rate, event_rate,
            rsf_concordance_index (in-sample, a sanity check only).

        Spec References
        ----------------
        SPEC-MODEL-001: ">= 756 trading days for meaningful label coverage."
        SPEC-MODEL-002: P&D exclusion (enforced upstream by
        build_binary_labels, not re-checked here).

        Raises
        ------
        ValueError
            If inputs are misaligned/empty, y has values outside {0, 1},
            or `groups` doesn't sum to len(X).
        """
        lengths = {len(X), len(y), len(duration_months), len(event)}
        if len(lengths) != 1:
            raise ValueError("X/y/duration_months/event must all be the same length")
        valid = y.notna() & duration_months.notna() & event.notna()
        if not valid.any():
            raise ValueError("no rows with complete (non-NaN) y/duration_months/event")
        if not set(y.dropna().unique()).issubset({0, 1}):
            raise ValueError("y must be binary {0, 1}")

        groups = groups or [int(valid.sum())]
        if sum(groups) != int(valid.sum()):
            raise ValueError(f"groups sums to {sum(groups)}, expected {int(valid.sum())} valid rows")

        self._feature_names = list(X.columns)
        X_valid = X.loc[valid, self._feature_names]
        y_valid = y.loc[valid].astype(int)
        duration_valid = duration_months.loc[valid].clip(lower=0.5)
        event_valid = event.loc[valid].astype(bool)

        X_imputed = self._impute_fit(X_valid)
        self._fit_ranker_and_calibrator(X_imputed, y_valid, groups=groups)

        surv_y = Surv.from_arrays(event=event_valid.to_numpy(), time=duration_valid.to_numpy())
        self._rsf = RandomSurvivalForest(
            n_estimators=self.n_estimators, random_state=self.random_state, min_samples_leaf=5, n_jobs=-1
        )
        self._rsf.fit(X_imputed, surv_y)
        self._event_time_bounds = (float(self._rsf.unique_times_.min()), float(self._rsf.unique_times_.max()))

        self._trained_at = pd.Timestamp.now()
        self._training_samples = len(X_imputed)

        try:
            rsf_score = float(self._rsf.score(X_imputed, surv_y))
        except Exception:
            rsf_score = np.nan

        return {
            "training_samples": self._training_samples,
            "positive_rate": float(y_valid.mean()),
            "event_rate": float(event_valid.mean()),
            "rsf_concordance_index": rsf_score,
        }

    def predict_survival(self, X: pd.DataFrame, time_horizon_days: int = 20) -> pd.DataFrame:
        """ISurvivalModel contract: day-by-day survival probability (probability the 2x level
        has NOT yet been reached), for interface compliance — predict_full's
        mb_survival_* columns are the actually-used, milestone-granularity output."""
        if self._rsf is None:
            raise RuntimeError("predict_survival called before train_full()")
        X_imputed = self._impute_transform(X[self._feature_names])
        days = np.arange(1, time_horizon_days + 1, dtype=np.float64)
        months = days / 30.4375
        sf_values = self._survival_at(X_imputed, months)
        return pd.DataFrame(sf_values, columns=[f"day_{d}" for d in range(1, time_horizon_days + 1)], index=X.index)

    def _survival_at(self, X_imputed: pd.DataFrame, times_months: np.ndarray) -> np.ndarray:
        """Evaluate the fitted RSF's survival function at `times_months`, clipped to the
        domain the forest was actually fit on (sksurv raises outside that range)."""
        lo, hi = self._event_time_bounds
        clipped = np.clip(times_months, lo, hi)
        step_fns = self._rsf.predict_survival_function(X_imputed, return_array=False)
        return np.array([fn(clipped) for fn in step_fns])

    def predict_full(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        The build prompt's required output contract.

        Parameters
        ----------
        X : pd.DataFrame
            Same feature set as train_full's X.

        Returns
        -------
        pd.DataFrame
            Columns: mb_probability (float, 0-1), mb_tier (str, one of
            MB_TIERS), mb_archetype (str, one of MB_ARCHETYPES),
            mb_survival_6m/_12m/_18m/_24m/_36m (float, probability the
            stock has NOT yet reached the 2x level by that many months —
            standard survival-analysis S(t) semantics, the inverse of
            "probability of having multibagged by time t").

        Raises
        ------
        RuntimeError
            If called before train_full() (train()-only is insufficient —
            no RSF fit).
        """
        if self._ranker is None or self._rsf is None:
            raise RuntimeError("predict_full called before train_full() (train()-only is insufficient)")

        X_aligned = X[self._feature_names]
        X_imputed = self._impute_transform(X_aligned)

        probability = self.predict(X)
        tier = _classify_tier(probability)
        archetype = _classify_archetype(X_aligned)

        survival = self._survival_at(X_imputed, np.array(SURVIVAL_HORIZONS_MONTHS, dtype=np.float64))

        out = pd.DataFrame(index=X.index)
        out["mb_probability"] = probability
        out["mb_tier"] = tier
        out["mb_archetype"] = archetype
        for i, months in enumerate(SURVIVAL_HORIZONS_MONTHS):
            out[f"mb_survival_{months}m"] = survival[:, i]

        return out[MB_OUTPUT_COLUMNS]


def generate_weekly_watchlist(
    scores: pd.DataFrame,
    is_monday: bool,
    pnd_scores: Optional[pd.Series] = None,
    top_n: int = WATCHLIST_SIZE,
    probability_threshold: float = WATCHLIST_PROBABILITY_THRESHOLD,
    pnd_threshold: float = PND_FLAG_THRESHOLD,
) -> Optional[pd.DataFrame]:
    """
    Build prompt: "Weekly run schedule (Monday only)" + "Top-20 weekly
    watchlist generation: sort by mb_probability, take top 20 with
    mb_probability > 0.30" + the test deliverable's "top-20 list excludes
    any stock with pnd_score > 40".

    Parameters
    ----------
    scores : pd.DataFrame
        MultibaggerModel.predict_full()'s output, indexed by ticker (or
        any unique row key) — must contain at least 'mb_probability'.
    is_monday : bool
        Explicit flag rather than deriving today's weekday internally —
        keeps this function pure/directly testable (build prompt's test
        deliverable: "model only scores when is_monday=True").
    pnd_scores : pd.Series, optional
        Same index as `scores`. Rows with pnd_score > pnd_threshold are
        excluded regardless of mb_probability.
    top_n : int
    probability_threshold : float
    pnd_threshold : float

    Returns
    -------
    pd.DataFrame or None
        None if not is_monday (no scoring run at all this call — SPEC-
        MODEL-001's weekly cadence, "long-horizon signals don't change
        day-to-day"). Otherwise up to `top_n` rows, descending by
        mb_probability, filtered to mb_probability > probability_threshold
        and (if pnd_scores given) pnd_score <= pnd_threshold.

    Spec References
    ----------------
    SPEC-MODEL-001.

    Raises
    ------
    None
    """
    if not is_monday:
        return None

    eligible = scores[scores["mb_probability"] > probability_threshold].copy()
    if pnd_scores is not None:
        eligible = eligible.loc[pnd_scores.reindex(eligible.index).fillna(0) <= pnd_threshold]

    return eligible.sort_values("mb_probability", ascending=False).head(top_n)


def load_multibagger_training_data_from_db(
    db_path=None,
    lookback_days: int = 1260,
    label_window_days: int = 756,
    min_return_multiplier: float = 2.0,
) -> tuple:
    """
    Build real (X, y, duration_months, event, groups, pnd_scores) from DB.

    Label construction: a ticker is a positive (y=1) if its close price
    `label_window_days` ago is available AND its current price is at least
    `min_return_multiplier`× that past price (i.e. it has already delivered
    a confirmed multibag over the look-back window). This is the same binary
    "2x within 3 years" label the model is designed to predict.

    Parameters
    ----------
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.
    lookback_days : int
        Total OHLCV history to load (calendar days). Default 1260 (~5 years).
    label_window_days : int
        Trading days to look back when computing achieved returns. Default
        756 (~3 years of trading days). Must be <= lookback_days.
    min_return_multiplier : float
        Return threshold for a "positive" label. Default 2.0 (2x / 100%).

    Returns
    -------
    (X, y, duration_months, event, groups, pnd_scores)

    Raises
    ------
    FileNotFoundError
        If the DuckDB database is missing.
    RuntimeError
        If ohlcv_adjusted / compute_multibagger_features yields no rows, or
        no ticker meets min_return_multiplier (there is no synthetic-data
        fallback — see BuildLog.md "Real data sourcing — Multibagger").
    """
    from pathlib import Path as _Path

    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection
    from features.multibagger import MULTIBAGGER_FEATURES, compute_multibagger_features

    db_path = db_path or DUCKDB_PATH
    if not _Path(db_path).exists():
        raise FileNotFoundError(
            f"DuckDB not found at {db_path}. Multibagger training requires real OHLCV "
            "history — run ingestion/backfill_runner.py first. See BuildLog.md "
            "'Real data sourcing — Multibagger'."
        )

    with get_duckdb_connection(db_path) as conn:
        ohlcv = conn.execute(
            """
            SELECT date, ticker, open, high, low, close, volume,
                   COALESCE(delivery_pct, 0.0) AS delivery_pct
            FROM ohlcv_adjusted
            WHERE date >= CURRENT_DATE - INTERVAL (?) DAY
            ORDER BY ticker, date
            """,
            [lookback_days],
        ).df()

    if ohlcv.empty:
        raise RuntimeError(
            "ohlcv_adjusted is empty. Multibagger training requires real OHLCV history — "
            "run ingestion/backfill_runner.py first. See BuildLog.md 'Real data sourcing — Multibagger'."
        )

    ohlcv["date"] = pd.to_datetime(ohlcv["date"])

    # --- Compute achieved-return label per ticker ---
    # For each ticker: compare its most recent close to its close
    # label_window_days trading rows ago. If that ratio >= min_return_multiplier => positive.
    sorted_ohlcv = ohlcv.sort_values(["ticker", "date"])
    first_close = sorted_ohlcv.groupby("ticker")["close"].nth(0)
    last_close = sorted_ohlcv.groupby("ticker")["close"].nth(-1)
    row_counts = sorted_ohlcv.groupby("ticker")["date"].count()

    # Only label tickers with sufficient history for the label window
    eligible_tickers = row_counts[row_counts >= label_window_days].index
    first_in_window = (
        sorted_ohlcv[sorted_ohlcv["ticker"].isin(eligible_tickers)]
        .groupby("ticker")
        .apply(lambda g: g.iloc[-(label_window_days):]["close"].iloc[0])
    )
    achieved_return = last_close.reindex(first_in_window.index) / first_in_window
    is_positive_ticker = achieved_return >= min_return_multiplier

    # Duration: months from the start of the label window to "event" (if any)
    def _duration_months(ticker):
        g = sorted_ohlcv[sorted_ohlcv["ticker"] == ticker]
        if len(g) < label_window_days:
            return float(LABEL_WINDOW_YEARS * 12)
        start = g.iloc[-(label_window_days):]["date"].iloc[0]
        end = g["date"].iloc[-1]
        return max((end - start).days / 30.44, 0.5)

    # --- Compute multibagger features (latest snapshot per ticker) ---
    from config.universe import load_universe_raw

    try:
        universe = load_universe_raw()
        sector_map = dict(zip(universe["ticker"], universe["sector"]))
    except Exception:
        sector_map = {}

    features_df = compute_multibagger_features(ohlcv, sector_map=sector_map)
    if features_df.empty:
        raise RuntimeError(
            "compute_multibagger_features returned no rows from real OHLCV data. "
            "See BuildLog.md 'Real data sourcing — Multibagger'."
        )

    latest = features_df.sort_values("date").groupby("ticker", sort=False).tail(1).reset_index(drop=True)

    # Align labels to the latest-snapshot tickers
    y_series = latest["ticker"].map(is_positive_ticker).fillna(False).astype(int)
    duration_list = [_duration_months(t) for t in latest["ticker"]]
    duration_months = pd.Series(duration_list)
    event = y_series.copy()
    # pnd_scores: real P&D scores require a trained PnDDetector, loaded and scored by
    # the caller (e.g. inference/train_all_phase1.py) and passed through to
    # build_binary_labels separately. NaN here (not 0.0) is deliberate — a real "no
    # P&D risk" score of 0 and "not yet computed" must not be conflated.
    pnd_scores = pd.Series(np.full(len(latest), np.nan))

    X = latest[MULTIBAGGER_FEATURES]
    groups = [len(latest)]  # single snapshot = one ranking group

    n_pos = y_series.sum()
    n_neg = (y_series == 0).sum()
    logger.info(
        "Multibagger real training data: %d tickers (%d positive / %d negative) from ohlcv_adjusted "
        "(label_window=%d days, threshold=%.1fx)",
        len(X), n_pos, n_neg, label_window_days, min_return_multiplier,
    )

    if n_pos == 0:
        raise RuntimeError(
            f"No confirmed multibaggers found in {label_window_days}-day window with "
            f"{min_return_multiplier}x threshold. There is no synthetic-data fallback. "
            "Use a longer lookback_days/label_window_days, or expand the trading "
            "universe via config/build_universe.py --full-nse. See BuildLog.md "
            "'Real data sourcing — Multibagger'."
        )

    return (
        X.reset_index(drop=True),
        y_series.reset_index(drop=True),
        duration_months.reset_index(drop=True),
        event.reset_index(drop=True),
        groups,
        pnd_scores.reset_index(drop=True),
    )
