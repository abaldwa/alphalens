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
import os
from typing import Any, Dict, List, Optional

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier
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
    return_multiplier: float = 2.0,
) -> pd.DataFrame:
    """
    GAINER EXPERIMENT (copy of multibagger_model.py, not used by
    production): binary 1 if a stock-date snapshot's forward max return
    over `window_years` reaches `return_multiplier` - 1 (e.g. 2.0 -> +100%
    / 2x, 3.0 -> +200% / 3x, 5.0 -> +400% / 5x), else 0. P&D-flagged
    episodes are excluded from positive labels (downgraded to 0).

    [GAINER change vs production]: production's build_binary_labels
    hardcodes the 2x/+100% threshold (`fwd_returns >= 1.0`) regardless of
    any multiplier argument — the return_multiplier param here is real
    and actually used, so this same function serves the 2x/12mo,
    3x/24mo, and 5x/36mo variants by varying (return_multiplier,
    window_years) per call, per the plan's parametrization item.

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
    return_multiplier : float
        Target multiple, e.g. 2.0 for 2x, 3.0 for 3x, 5.0 for 5x.

    Returns
    -------
    pd.DataFrame
        Columns: date, ticker, label (0/1), max_return, event (0/1, same
        as label — kept distinct for clarity at survival-model call
        sites), duration_months (months until the target multiple was
        first reached, or `window_years * 12` if never reached /
        right-censored — see SPEC-MODEL-001's >= 756 trading day minimum
        for this to be meaningful).

    Raises
    ------
    None
    """
    if pnd_scores is None:
        logger.warning("build_binary_labels: no pnd_scores supplied — P&D exclusion is skipped")
    if return_multiplier <= 1.0:
        raise ValueError("return_multiplier must be > 1.0 (e.g. 2.0 for 2x)")

    target_return = return_multiplier - 1.0
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
            hit_idx = np.argmax(fwd_returns >= target_return) if (fwd_returns >= target_return).any() else None
            label = int(max_return >= target_return)
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


def dedup_runs(labels: pd.DataFrame, cooldown_days: int = 15) -> pd.DataFrame:
    """
    GAINER EXPERIMENT: run-level event deduplication, addressing the
    overlapping-event leakage the user flagged — a single multi-year
    multibagger run otherwise contributes one positive row per labeled
    snapshot date (dozens of near-duplicate positives from the same
    underlying event). Keeps the FIRST positive (event=1) Day-0 of each
    run per ticker, then suppresses further positives for that ticker
    until `cooldown_days` trading days after the run resolves (its
    labeled date + duration_months' worth of days). Negative (event=0)
    rows are untouched — every non-event Day-0 remains an independent
    negative example.

    Parameters
    ----------
    labels : pd.DataFrame
        build_binary_labels()'s output (date, ticker, label, max_return,
        duration_months, event), NOT yet stride-subsampled.
    cooldown_days : int
        Trading days of suppression after a run resolves before the same
        ticker can be flagged as a new positive event again.

    Returns
    -------
    pd.DataFrame
        Same columns as `labels`, with redundant same-run positive rows
        dropped (negatives untouched).
    """
    if labels.empty:
        return labels

    df = labels.sort_values(["ticker", "date"]).reset_index(drop=True)
    keep = np.ones(len(df), dtype=bool)

    for ticker, g in df.groupby("ticker", sort=False):
        idx = g.index.to_numpy()
        event = g["event"].to_numpy()
        duration_days = (g["duration_months"].to_numpy() / 12.0 * 252.0).astype(int)
        suppressed_until = -1  # positional index within this ticker's rows
        pos_in_ticker = 0
        for local_i, (row_idx, is_event, dur) in enumerate(zip(idx, event, duration_days)):
            if is_event == 1:
                if local_i <= suppressed_until:
                    keep[row_idx] = False
                else:
                    suppressed_until = local_i + dur + cooldown_days
            pos_in_ticker += 1

    n_dropped = int((~keep).sum())
    if n_dropped:
        logger.info(f"dedup_runs: dropped {n_dropped} redundant same-run positive snapshots (cooldown={cooldown_days}d)")
    return df.loc[keep].reset_index(drop=True)


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
        if y.nunique() < 2:
            # same defect fix as production multibagger_model.py — LogisticRegression.fit()
            # raises on single-class y instead of the "degenerate" output the old warning promised.
            logger.warning("Only one class present — calibrator will be degenerate (constant probability)")
            self._calibrator = DummyClassifier(strategy="constant", constant=y.iloc[0])
        else:
            self._calibrator = LogisticRegression(random_state=self.random_state)
        self._calibrator.fit(raw_scores, y)

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """IModel: calibrated mb_probability per row (the primary ranking-derived target)."""
        if self._ranker is None:
            raise RuntimeError("predict called before train()/train_full()")
        X_imputed = self._impute_transform(X[self._feature_names])
        raw_scores = self._ranker.predict(X_imputed).reshape(-1, 1)
        proba_matrix = self._calibrator.predict_proba(raw_scores)
        if proba_matrix.shape[1] < 2:
            proba = proba_matrix[:, 0] if bool(self._calibrator.classes_[0]) else 1.0 - proba_matrix[:, 0]
        else:
            proba = proba_matrix[:, 1]
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
        # [backlog #26/#27, 2026-07-04] Two independent memory fixes, both
        # confirmed live against the real ~57k-row training set the #27
        # labeling fix now produces (vs. ~1,138 rows pre-fix):
        #
        # 1. n_jobs=-1 previously spun up one worker process per CPU core
        #    (14 on the machine this was found on), each holding its own
        #    copy of the training matrix. Capped rather than removed —
        #    still parallel, just bounded.
        #
        # 2. min_samples_leaf=5 (fine for ~1,138 rows) lets trees grow to
        #    ~57448/5 ≈ 11,000+ leaves each on the new row count, and with
        #    the #27 fix giving real event-time granularity (vs. the old
        #    bug's ~36-41mo clustering) there are now far more distinct
        #    survival-curve leaf values to store per tree. Verified live:
        #    even after fix #1, RSF.fit() alone (before the scoring loop
        #    even starts) still grew to ~7GB RSS and was killed by an
        #    external memory monitor protecting the host. Scaling
        #    min_samples_leaf with the actual training-row count (instead
        #    of a fixed small constant tuned for the old, much smaller
        #    dataset) bounds tree size/leaf count regardless of how many
        #    labeled snapshots load_multibagger_training_data_from_db()
        #    produces in the future.
        rsf_n_jobs = min(4, os.cpu_count() or 1)
        rsf_min_samples_leaf = max(5, len(X_imputed) // 1000)
        self._rsf = RandomSurvivalForest(
            n_estimators=self.n_estimators, random_state=self.random_state,
            min_samples_leaf=rsf_min_samples_leaf, n_jobs=rsf_n_jobs,
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


def _score_pnd_panel(ohlcv: pd.DataFrame) -> pd.Series:
    """
    Real per-(ticker, date) P&D likelihood scores over a full historical
    OHLCV panel, for use as `build_binary_labels`'s `pnd_scores` argument
    (see backlog #27 — previously this was all-NaN, meaning no positive
    label was ever actually P&D-excluded during training).

    Reuses the already-built, already-trained artifacts rather than
    inventing a new scoring path: `features/pnd_features.py`'s
    `compute_pnd_features()` already returns one row per (ticker, date)
    (mirrors `compute_multibagger_features`'s shape), and
    `PnDDetector.predict_full()` already accepts an arbitrary-length
    feature matrix and returns a `pnd_score` per row — the same
    `MODELS_DIR/pnd_detector/pnd_detector_current.pkl` cached artifact
    `daily_inference.py`'s `_step_pnd_filter` loads is reused here instead
    of retraining a second PnDDetector.

    Returns an all-NaN Series (same index as `ohlcv`, one entry per
    (ticker, date) row) if the cached PnDDetector artifact doesn't exist
    yet — documented via a warning, not silently defaulted to "0 risk".
    """
    from config.settings import MODELS_DIR
    from features.pnd_features import PND_FEATURES, compute_pnd_features
    from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector

    pnd_model_path = MODELS_DIR / "pnd_detector" / "pnd_detector_current.pkl"
    if not pnd_model_path.exists():
        logger.warning(
            "_score_pnd_panel: no cached PnDDetector at %s — pnd_scores will be all-NaN "
            "(no P&D exclusion applied to multibagger training labels). Run "
            "train_all_phase1.py to produce this artifact.",
            pnd_model_path,
        )
        return pd.Series(np.full(len(ohlcv), np.nan), index=ohlcv.index)

    try:
        pnd_features = compute_pnd_features(ohlcv)
        pnd_model = PnDDetector()
        pnd_model.load(str(pnd_model_path))
        scored = pnd_model.predict_full(pnd_features[PND_FEATURES])
        pnd_lookup = pnd_features[["date", "ticker"]].assign(pnd_score=scored["pnd_score"].to_numpy())
        merged = ohlcv[["date", "ticker"]].merge(pnd_lookup, on=["date", "ticker"], how="left")
        return pd.Series(merged["pnd_score"].to_numpy(), index=ohlcv.index)
    except Exception as exc:
        logger.warning("_score_pnd_panel: PnDDetector scoring failed (%s) — pnd_scores will be all-NaN", exc)
        return pd.Series(np.full(len(ohlcv), np.nan), index=ohlcv.index)


def load_multibagger_training_data_from_db(
    db_path=None,
    lookback_days: int = 1260,
    label_window_days: int = 756,
    min_return_multiplier: float = 2.0,
    label_window_years: float = LABEL_WINDOW_YEARS,
    snapshot_stride_days: int = 5,
    cooldown_days: int = 15,
    tickers: list = None,
    pipeline_name: str = "multibagger_default",
    ticker_chunk_size: int = 150,
) -> tuple:
    """
    GAINER EXPERIMENT (copy, not used by production): build real (X, y,
    duration_months, event, groups, pnd_scores) from DB.

    [GAINER changes vs production]:
    1. `min_return_multiplier` is now REAL — threaded into
       build_binary_labels(return_multiplier=...) instead of being a
       dead signature-compatibility-only parameter. Combined with the
       new `label_window_years`, this lets the same function serve the
       2x/12mo, 3x/24mo, 5x/36mo variants by varying both together
       (window_years no longer hardcoded to LABEL_WINDOW_YEARS=3).
    2. `dedup_runs()` (cooldown_days) is applied BEFORE the stride
       subsampling below, collapsing each multi-year positive run down
       to its first-qualifying Day-0 instead of contributing ~150
       near-duplicate positive snapshots per run — the run-level event
       dedup requested to address training-set leakage.
    3. Processing is now CHUNKED BY TICKER (ticker_chunk_size, default
       150) instead of loading OHLCV + computing features for the whole
       universe at once — bounds peak memory to roughly one chunk's
       worth of data regardless of universe size. Each chunk's fully
       processed (featured, labeled, deduped, subsampled) rows are
       persisted to a parquet checkpoint (checkpoint_utils.py)
       IMMEDIATELY after that chunk finishes, keyed by `pipeline_name`
       (e.g. "multibagger_3x_24m") — an OOM kill mid-run loses at most
       one chunk's work, and a re-invocation skips every
       already-completed chunk instead of recomputing the whole
       universe.

    `snapshot_stride_days` subsamples snapshot dates (every Nth trading
    day per ticker, default 5 = ~weekly) rather than every single trading
    day — a documented row-count/training-time tradeoff: multibagger
    features and labels change slowly day-to-day (long-horizon signal),
    so daily-granularity snapshots would multiply row count ~5x for
    negligible extra event-time resolution.

    Parameters
    ----------
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.
    lookback_days : int
        Total OHLCV history to load (calendar days). Default 1260 (~5 years).
    label_window_days : int
        Trading days of forward runway required (used only to filter out
        censored (event=0) snapshots too close to the end of history to be
        meaningful — SPEC-MODEL-001's ">= 756 trading days" applies to the
        forward window here, not a trailing one as in the old
        implementation). Positive (event=1) rows are always kept regardless
        of how much forward runway they had. Default 756 (~3 years).
    min_return_multiplier : float
        Return threshold for a "positive" label. Default 2.0 (2x / 100%).
    snapshot_stride_days : int
        Keep every Nth trading day per ticker as a labeled snapshot. Default 5.
    tickers : list[str], optional
        Restrict training to this ticker subset instead of the full
        universe (still internally chunked at ticker_chunk_size). If
        None, the full universe's distinct tickers are queried from
        ohlcv_adjusted and chunked.
    pipeline_name : str
        Checkpoint namespace — use a distinct name per (multiplier,
        window_years) variant (e.g. "multibagger_2x_12m") so different
        variants' checkpoints never collide.
    ticker_chunk_size : int
        Tickers processed per chunk. Default 150 — moderated to bound
        peak memory; lower it further if a chunk still OOMs.

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
    from config.universe import load_universe_raw
    from datastore.api.db import get_duckdb_connection
    from features.multibagger import MULTIBAGGER_FEATURES, compute_multibagger_features
    from systems.ml_signal_engine_gainer.inference.checkpoint_utils import (
        checkpoint_path, load_all_checkpoints, load_checkpoint, save_checkpoint, ticker_chunks,
    )

    db_path = db_path or DUCKDB_PATH
    if not _Path(db_path).exists():
        raise FileNotFoundError(
            f"DuckDB not found at {db_path}. Multibagger training requires real OHLCV "
            "history — run ingestion/backfill_runner.py first. See BuildLog.md "
            "'Real data sourcing — Multibagger'."
        )

    try:
        universe = load_universe_raw()
        sector_map = dict(zip(universe["ticker"], universe["sector"]))
    except Exception:
        sector_map = {}

    with get_duckdb_connection(db_path, read_only=True, persist=False) as conn:
        if tickers:
            all_tickers = list(tickers)
        else:
            all_tickers = conn.execute(
                "SELECT DISTINCT ticker FROM ohlcv_adjusted WHERE date >= CURRENT_DATE - INTERVAL (?) DAY",
                [lookback_days],
            ).df()["ticker"].tolist()

        if not all_tickers:
            raise RuntimeError(
                "ohlcv_adjusted has no tickers in the requested lookback window. Multibagger "
                "training requires real OHLCV history — run ingestion/backfill_runner.py first."
            )

        stage = f"stride{snapshot_stride_days}_cooldown{cooldown_days}_mult{min_return_multiplier}_win{label_window_years}"
        chunk_list = list(ticker_chunks(all_tickers, chunk_size=ticker_chunk_size))
        logger.info(
            "load_multibagger_training_data_from_db[%s]: %d tickers in %d chunks of <=%d",
            pipeline_name, len(all_tickers), len(chunk_list), ticker_chunk_size,
        )

        for chunk_idx, chunk_tickers in enumerate(chunk_list):
            ckpt = checkpoint_path(pipeline_name, stage, str(chunk_idx))
            cached = load_checkpoint(ckpt)
            if cached is not None:
                logger.info("chunk %d/%d: loaded from checkpoint (%d rows)", chunk_idx + 1, len(chunk_list), len(cached))
                continue

            ohlcv_chunk = conn.execute(
                """
                SELECT date, ticker, open, high, low, close, volume,
                       COALESCE(delivery_pct, 0.0) AS delivery_pct
                FROM ohlcv_adjusted
                WHERE date >= CURRENT_DATE - INTERVAL (?) DAY
                  AND ticker = ANY(?)
                ORDER BY ticker, date
                """,
                [lookback_days, chunk_tickers],
            ).df()
            if ohlcv_chunk.empty:
                logger.warning("chunk %d/%d: no OHLCV rows for %d tickers — skipping", chunk_idx + 1, len(chunk_list), len(chunk_tickers))
                save_checkpoint(pd.DataFrame(), ckpt)
                continue

            ohlcv_chunk["date"] = pd.to_datetime(ohlcv_chunk["date"])
            ohlcv_chunk = ohlcv_chunk.sort_values(["ticker", "date"]).reset_index(drop=True)

            features_chunk = compute_multibagger_features(ohlcv_chunk, sector_map=sector_map)
            if features_chunk.empty:
                logger.warning("chunk %d/%d: compute_multibagger_features returned no rows — skipping", chunk_idx + 1, len(chunk_list))
                save_checkpoint(pd.DataFrame(), ckpt)
                continue
            features_chunk["date"] = pd.to_datetime(features_chunk["date"])

            pnd_score_chunk = _score_pnd_panel(ohlcv_chunk)

            prices_chunk = ohlcv_chunk[["date", "ticker", "close"]].copy()
            labels_chunk = build_binary_labels(
                prices_chunk, pnd_scores=pnd_score_chunk, window_years=label_window_years,
                return_multiplier=min_return_multiplier,
            )
            if labels_chunk.empty:
                save_checkpoint(pd.DataFrame(), ckpt)
                continue

            forward_days_available = prices_chunk.groupby("ticker")["date"].transform(lambda s: np.arange(len(s))[::-1])
            labels_chunk = labels_chunk.merge(
                prices_chunk.assign(_fwd_days=forward_days_available)[["date", "ticker", "_fwd_days"]],
                on=["date", "ticker"], how="left",
            )
            labels_chunk = labels_chunk[
                (labels_chunk["event"] == 1) | (labels_chunk["_fwd_days"] >= label_window_days)
            ].drop(columns=["_fwd_days"])
            if labels_chunk.empty:
                save_checkpoint(pd.DataFrame(), ckpt)
                continue

            labels_chunk = dedup_runs(labels_chunk, cooldown_days=cooldown_days)
            row_idx_within_ticker = labels_chunk.groupby("ticker").cumcount()
            labels_chunk = labels_chunk.loc[(row_idx_within_ticker % snapshot_stride_days) == 0].reset_index(drop=True)

            merged_chunk = labels_chunk.merge(features_chunk, on=["date", "ticker"], how="inner")
            pnd_lookup_chunk = prices_chunk.assign(pnd_score=pnd_score_chunk.to_numpy())[["date", "ticker", "pnd_score"]]
            merged_chunk = merged_chunk.merge(pnd_lookup_chunk, on=["date", "ticker"], how="left")

            save_checkpoint(merged_chunk, ckpt)
            logger.info("chunk %d/%d done: %d labeled rows from %d tickers", chunk_idx + 1, len(chunk_list), len(merged_chunk), len(chunk_tickers))

        merged = load_all_checkpoints(pipeline_name, stage)

    if merged.empty:
        raise RuntimeError(
            "No labeled (ticker, date) snapshots survived chunked processing (labeling/forward-runway/"
            "feature-overlap all yielded nothing). Use a longer lookback_days. There is no "
            "synthetic-data fallback. See BuildLog.md 'Real data sourcing — Multibagger'."
        )
    merged = merged.sort_values(["ticker", "date"]).reset_index(drop=True)

    X = merged[MULTIBAGGER_FEATURES]
    y_series = merged["label"].astype(int)
    duration_months = merged["duration_months"].astype(float)
    event = merged["event"].astype(int)
    # groups: one lambdarank group per as-of snapshot date (rows sharing a date compete).
    groups = merged.groupby("date", sort=True).size().tolist()
    # pnd_score was already merged into each chunk's checkpoint (see the chunk loop above) —
    # pull it straight from `merged` rather than re-deriving it from a full-panel prices/pnd_score_series
    # that no longer exists now that processing is chunked.
    pnd_scores = merged["pnd_score"]

    n_pos = int(y_series.sum())
    n_neg = int((y_series == 0).sum())
    logger.info(
        "Multibagger real training data: %d labeled (ticker, date) snapshots across %d tickers "
        "(%d positive / %d negative) from ohlcv_adjusted (label_window=%d days, threshold=%.1fx, "
        "snapshot_stride=%d days, median duration_months=%.1f)",
        len(X), merged["ticker"].nunique(), n_pos, n_neg, label_window_days, min_return_multiplier,
        snapshot_stride_days, float(duration_months.median()),
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
