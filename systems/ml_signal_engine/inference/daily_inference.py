"""
systems/ml_signal_engine/inference/daily_inference.py

Phase: 1.7 (DataStore API Full + Daily Pipeline + Dashboard)
Specs: SPEC-MODEL-006, SPEC-PIPE-005, SPEC-SYS-002, SPEC-DS-002, SPEC-DS-004
Owner: ml_signal_engine / inference
Consumers: ingestion/scheduler/daily_pipeline.py (step_run_models)

Daily inference orchestration: HMM -> PSI check -> P&D filter -> Signals
-> MetaLabel -> Exit -> write to DataStore. P&D filter runs before
Signals (SPEC-MODEL-006: P&D pre-filter takes priority over a buy call —
a P&D-blocked ticker is never even scored by the signal models). The PSI
check is a data-quality GATE that runs right after HMM and before any
signal-relevant inference: PSI > PSI_SEVERE_THRESHOLD (0.25) halts the
run entirely (SPEC-PIPE-005/SPEC-ALERT-001's "halt + retrain" tier) —
running signal models on a drifted feature distribution would silently
produce untrustworthy output, so this script refuses to proceed rather
than write bad signals.

SPEC-DS-002 ("Consumer systems use httpx to call API, never import db
modules"): all writes go through DataStore's POST /api/v1/signals/ml/write
via httpx, never a direct DuckDB INSERT. Inputs (feature matrix, P&D
feature matrix, market proxy OHLCV, position context) are passed in by
the caller (ingestion/scheduler/daily_pipeline.py's step_run_models) as
already-loaded DataFrames — daily_inference.py itself never reads a
database file or Parquet path directly, keeping it a pure orchestration
layer over already-fetched data, independently testable without any I/O.

[AS BUILT, 2026-07-04] FutureDevelopment.md #14: conformal_signal5d,
signal_21d/signal_63d and #16's shap_top5_json are now wired into
_step_signals_and_meta (see _load_conformal/_compute_shap_top5) — this
docstring previously said conformal was intentionally left out; that is
no longer true as of this change (see BuildLog.md "P1.7" for the
original Phase 1 gap this superseded).
"""

import json
import logging
import time
from datetime import date as date_type
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import joblib
import numpy as np
import pandas as pd
import shap

from config.settings import DATASTORE_API_BASE_URL, EXIT_URGENT_THRESHOLD, MODELS_DIR, PSI_SEVERE_THRESHOLD
from config.timezone import now_ist
from features.pnd_features import PND_FEATURES
from features.technical import CORE_TECHNICAL_FEATURES
from ingestion.quality.drift_monitor import PSIMonitor
from ingestion.quality.structured_logger import log_pipeline_step
from systems.ml_signal_engine.models.exit.exit_signal import ExitSignalModel
from systems.ml_signal_engine.models.exit.rule_based_exit_policy import RuleBasedExitPolicy
from systems.ml_signal_engine.models.hmm.regime_detector import compute_hmm_observables
from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector
from systems.ml_signal_engine.models.signal.base_signal_model import CLASS_NAMES, BaseSignalModel
from systems.ml_signal_engine.models.deep.stacking import StackingMetaLearner
from systems.ml_signal_engine.models.signal.meta_labeler import MetaLabeler
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel
from systems.ml_signal_engine.models.signal.signal_21d import Signal21DModel
from systems.ml_signal_engine.models.signal.signal_63d import Signal63DModel

logger = logging.getLogger(__name__)

HMM_MODEL_NAME = "hmm_market"
# [AS BUILT] train_all_phase1.py saves under MODELS_DIR/"hmm"/"hmm_market_v{date}.pkl"
# — directory named after the model TYPE, filename prefixed with the
# specific name. Every other Phase 1 model's directory name matches its
# HMM_MODEL_NAME-equivalent exactly (e.g. signal_5d/signal_5d_current.pkl),
# so this one mismatch was missed until a live pipeline run against real
# data hit FileNotFoundError looking under MODELS_DIR/"hmm_market"/ (which
# never existed) instead of MODELS_DIR/"hmm"/.
HMM_MODEL_DIR_NAME = "hmm"
HMM_MARKET_TICKER = "MARKET"
PND_MODEL_NAME = "pnd_detector"
SIGNAL_MODEL_NAME = "signal_5d"
SIGNAL_21D_MODEL_NAME = "signal_21d"
SIGNAL_63D_MODEL_NAME = "signal_63d"
META_MODEL_NAME = "meta_labeler"
# 2026-07-19 full-codebase-review Fix A3: the models actually scored in
# this file's chunk loop that produce a 3-class (sell/hold/buy)
# probability output StackingMetaLearner can combine — meta_labeler is
# excluded (binary act/no_act, not a 3-class distribution) and tft/bilstm
# are excluded (never loaded in this file; see _load_stacking_ensemble's
# docstring for why a full-5-model trained artifact may not match this
# narrower set, handled defensively at the call site).
STACKING_ENSEMBLE_BASE_MODELS = [SIGNAL_MODEL_NAME, SIGNAL_21D_MODEL_NAME, SIGNAL_63D_MODEL_NAME]
STACKING_ENSEMBLE_MODEL_NAME = "stacking_ensemble"
EXIT_MODEL_NAME = "exit_signal"
CONFORMAL_MODEL_DIR_NAME = "conformal"
CONFORMAL_SIGNAL5D_NAME = "conformal_signal5d"
# FutureDevelopment.md #16: top-5 |SHAP value| features to keep per row —
# shap_top5_json exists in the ml_signals schema/API contract (SPEC-DS-004)
# but nothing computed it until this file wired shap.TreeExplainer in.
SHAP_TOP_N = 5

REGIME_RANK_NAMES = {0.0: "bearish", 1.0: "sideways", 2.0: "volatile", 3.0: "bullish"}

# Must match exit_signal.load_exit_training_data_from_db()'s schema —
# duplicated rather than imported from backtest/engine.py to avoid an
# inference-layer -> backtest-layer dependency for one constant list.
EXIT_CONTEXT_COLUMNS = [
    "entry_price", "days_held", "unrealised_pnl_pct", "days_to_next_earnings",
    "drawdown_from_peak", "momentum_3m", "pnd_score", "hmm_regime",
]


class DailyInferenceHalted(RuntimeError):
    """Raised when the daily inference run halts before completion (e.g. PSI > 0.25)."""


def _load_model(model_cls, name: str, models_dir: Path):
    path = models_dir / name / f"{name}_current.pkl"
    model = model_cls()
    model.load(str(path))
    return model


def _load_exit_model(models_dir: Path):
    """Load the trained ExitSignalModel, falling back to RuleBasedExitPolicy
    if no trained model exists yet (A39, FeatureBacklog.md). Without this,
    _step_exit raised FileNotFoundError the first time a position was open
    and no ExitSignalModel had ever been trained, halting the entire daily
    inference pipeline — mirrors the fallback scripts/run_daily_paper_trading.py
    already uses via _load_exit_policy()."""
    path = models_dir / EXIT_MODEL_NAME / f"{EXIT_MODEL_NAME}_current.pkl"
    if path.exists():
        model = ExitSignalModel()
        model.load(str(path))
        return model
    logger.warning(
        "No trained ExitSignalModel found at %s — falling back to RuleBasedExitPolicy", path
    )
    return RuleBasedExitPolicy()


def _load_hmm(models_dir: Path):
    hmm_dir = models_dir / HMM_MODEL_DIR_NAME
    path = hmm_dir / f"{HMM_MODEL_NAME}_current.pkl"
    if path.exists():
        return joblib.load(path)
    # train_all_phase1.py (P1.5) saves hmm_market as a plain joblib.dump,
    # not the {name}_v{date}_fold0.pkl + {name}_current.pkl pair every
    # other model uses (see that module's "save" comment) — fall back to
    # the most recent versioned file if no _current.pkl was ever written.
    candidates = sorted(hmm_dir.glob(f"{HMM_MODEL_NAME}_v*.pkl"))
    if not candidates:
        raise FileNotFoundError(f"no hmm_market model found under {hmm_dir}")
    return joblib.load(candidates[-1])


def _load_stacking_ensemble(models_dir: Path):
    """
    Load the most recently trained StackingMetaLearner artifact, if one
    exists (2026-07-19 full-codebase-review Fix A3 — wires the previously
    dead-code ensemble into live inference).

    scripts/train_stacking.py saves date-versioned files
    (stacking_meta_v{YYYYMMDD}.pkl/.json under `models_dir`, no
    `_current.pkl` symlink convention like every other model here — same
    globbing fallback pattern as _load_hmm above, since YYYYMMDD version
    strings sort correctly lexicographically.

    Returns None (not an exception) if no artifact has ever been trained
    — this is the expected, common case (train_stacking.py's own module
    docstring documents it as NOT wired into any unattended retrain
    trigger due to its OOM history training the full 5-model TFT/BiLSTM
    set), and the caller must treat a missing ensemble as "skip this
    step today," not a hard failure blocking signal_5d/meta_labeler.
    """
    candidates = sorted(models_dir.glob("stacking_meta_v*.pkl"))
    if not candidates:
        return None
    path = candidates[-1]
    # StackingMetaLearner.load() appends ".pkl" itself — strip it back off.
    stem = str(path)[: -len(".pkl")]
    model = StackingMetaLearner(base_model_names=STACKING_ENSEMBLE_BASE_MODELS)
    model.load(stem)
    return model


def _load_conformal(models_dir: Path):
    """
    Load the trained ConformalPredictor (wraps signal_5d's Q50 regressor,
    see uncertainty/conformal.py) that calibrates conformal_lower/upper for
    signal_5d rows.

    Same "no _current.pkl written, fall back to newest versioned file"
    situation _load_hmm already documents: train_all_phase1.py's conformal
    branch only ever joblib.dump()s a {name}_v{date}.pkl (see that
    function's step 6 comment), never a *_current.pkl symlink/copy.

    Raises
    ------
    FileNotFoundError
        If no conformal_signal5d model has ever been trained/saved.
    """
    conf_dir = models_dir / CONFORMAL_MODEL_DIR_NAME
    candidates = sorted(conf_dir.glob(f"{CONFORMAL_SIGNAL5D_NAME}_v*.pkl"))
    if not candidates:
        raise FileNotFoundError(f"no {CONFORMAL_SIGNAL5D_NAME} model found under {conf_dir}")
    return joblib.load(candidates[-1])


def _compute_shap_top5(signal_model: "BaseSignalModel", X: pd.DataFrame, direction: pd.Series) -> Dict[str, str]:
    """
    FutureDevelopment.md #16: top-5 |SHAP value| features per ticker for
    signal_5d's LightGBM classifier, serialized as JSON for the
    shap_top5_json column.

    Uses the SAME imputed matrix the model itself scores on (X[self.
    _feature_names] through _impute_transform) so SHAP values line up with
    what actually drove predict()/predict_signals()'s output — not a
    separately-imputed (and therefore inconsistent) copy.

    Parameters
    ----------
    signal_model : Signal5DModel
        Already-loaded/trained; must expose `_lgbm` (the base LightGBM
        classifier), `_feature_names`, and `_impute_transform`.
    X : pd.DataFrame
        The eligible feature frame (indexed by ticker), same as passed to
        predict_signals/predict.
    direction : pd.Series
        signal_model.predict(X)'s encoded class per ticker (same index as
        X) — SHAP values are taken for each ticker's OWN predicted class,
        not a fixed class, so "top 5 features" means "top 5 features
        driving the call actually made for that ticker."

    Returns
    -------
    dict
        ticker -> JSON string '[{"feature": ..., "value": ...}, ...]'
        (length SHAP_TOP_N, sorted by descending |value|).
    """
    feature_names = signal_model._feature_names
    X_imputed = signal_model._impute_transform(X[feature_names])
    X_imputed_df = pd.DataFrame(X_imputed, columns=feature_names, index=X.index)

    explainer = shap.TreeExplainer(signal_model._lgbm)
    shap_values = explainer.shap_values(X_imputed_df)  # (n_samples, n_features, n_classes) for multiclass LGBM

    out: Dict[str, str] = {}
    for i, ticker in enumerate(X.index):
        class_idx = int(direction.loc[ticker])
        row_values = shap_values[i, :, class_idx] if shap_values.ndim == 3 else shap_values[i, :]
        order = np.argsort(-np.abs(row_values))[:SHAP_TOP_N]
        top5 = [{"feature": feature_names[j], "value": float(row_values[j])} for j in order]
        out[ticker] = json.dumps(top5)
    return out


def _write_signal(client: httpx.Client, api_base_url: str, payload: Dict[str, Any]) -> None:
    response = client.post(f"{api_base_url}/api/v1/signals/ml/write", json=payload, timeout=10.0)
    response.raise_for_status()


def _step_hmm(
    market_ohlcv: pd.DataFrame, run_date: date_type, client: httpx.Client, api_base_url: str, models_dir: Path
) -> Optional[str]:
    """Decode today's market-wide regime, write it (ticker='MARKET') to DataStore. Returns the regime name, or None."""
    hmm_model = _load_hmm(models_dir)
    observables = compute_hmm_observables(market_ohlcv)
    regimes, probabilities = hmm_model.predict_regime(observables)

    if regimes.empty or pd.isna(regimes.iloc[-1]):
        logger.warning("daily_inference: HMM could not decode a regime for the latest observation")
        return None

    rank = float(regimes.iloc[-1])
    regime_prob = float(probabilities.iloc[-1].max()) if probabilities is not None else None
    regime_name = REGIME_RANK_NAMES.get(rank, str(rank))

    _write_signal(
        client, api_base_url,
        {
            "date": run_date.isoformat(), "ticker": HMM_MARKET_TICKER, "model_name": HMM_MODEL_NAME,
            "model_version": "1.0", "hmm_regime": regime_name, "hmm_regime_prob": regime_prob,
        },
    )
    return regime_name


def _step_psi_check(feature_matrix: pd.DataFrame, psi_baseline: Optional[dict]) -> Dict[str, Any]:
    """
    SPEC-PIPE-005/SPEC-ALERT-001: top-50-feature PSI vs baseline.
    Returns {'status': 'ok'|'warning'|'halt'|'skipped', 'worst_feature', 'worst_psi'}.
    'skipped' (not a halt) if no baseline has been computed yet —
    ingestion/quality/baseline_runner.py hasn't been run (e.g. first-ever
    pipeline run) is a data-availability gap, not a reason to block
    inference on day one.

    A55 (2026-07-11): deliberately NOT chunked, unlike _step_pnd_filter/
    _step_signals_and_meta below. PSI (population stability index) is a
    genuinely cross-sectional statistic — it compares today's full
    per-feature DISTRIBUTION against the baseline distribution, not a
    per-ticker value. Splitting the universe into chunks and computing PSI
    per chunk would compare each chunk's much-smaller, non-representative
    sub-distribution against the baseline and silently produce wrong (and
    inconsistent-across-chunks) drift numbers — exactly the kind of
    statistic A47's matrix_builder.py audit excluded from chunking
    (fundamental/mf_holdings/multibagger, for the same "real cross-ticker
    aggregation" reason). CORE_TECHNICAL_FEATURES x ~2,317 tickers is a
    small, bounded DataFrame slice (tens of MB at most) — nowhere near the
    per-ticker model-scoring steps' memory footprint — so there is no
    memory-pressure reason to chunk it even if it were safe to.
    """
    monitor = PSIMonitor()
    try:
        baseline = psi_baseline if psi_baseline is not None else monitor.load_baseline()
    except FileNotFoundError:
        logger.warning("daily_inference: no PSI baseline found yet — skipping drift check")
        return {"status": "skipped", "worst_feature": None, "worst_psi": None}

    numeric_cols = [c for c in CORE_TECHNICAL_FEATURES if c in feature_matrix.columns]
    results = monitor.check_drift(feature_matrix[numeric_cols], baseline=baseline)
    if not results:
        return {"status": "ok", "worst_feature": None, "worst_psi": None}

    worst_feature, worst = max(results.items(), key=lambda kv: kv[1]["psi"])
    return {"status": worst["status"], "worst_feature": worst_feature, "worst_psi": worst["psi"]}


def _step_pnd_filter(
    pnd_feature_matrix: pd.DataFrame, run_date: date_type, client: httpx.Client, api_base_url: str, models_dir: Path
) -> set:
    """SPEC-MODEL-006: score every ticker, write each row, return the set of blocked tickers.

    A55 (2026-07-11): scored/written in ticker CHUNKS (same
    resource_guard.adaptive_chunk_size pattern as A47's
    _compute_chunked_ticker_independent_panels in features/matrix_builder.py)
    instead of one full-universe predict_full() call. pnd_model.predict_full
    is per-ticker-independent (no cross-ticker groupby/rank), so chunking
    changes nothing about its output — only how much of `out` is held in
    memory at once. The model object itself is loaded ONCE outside the loop
    (it is small and constant-size regardless of ticker count; reloading it
    per chunk would be pure waste, not a memory fix)."""
    from ingestion.scheduler.resource_guard import adaptive_chunk_size

    from config.settings import PIPELINE_MEMORY_CEILING_MB, SCREENER_BATCH_EXPORT_CHUNK_SIZE

    pnd_model = _load_model(PnDDetector, PND_MODEL_NAME, models_dir)
    feature_cols = [c for c in PND_FEATURES if c in pnd_feature_matrix.columns]
    indexed = pnd_feature_matrix.set_index("ticker")[feature_cols]
    tickers = list(indexed.index)

    blocked = set()
    i = 0
    while i < len(tickers):
        chunk_size = adaptive_chunk_size(SCREENER_BATCH_EXPORT_CHUNK_SIZE, ceiling_mb=PIPELINE_MEMORY_CEILING_MB)
        chunk_tickers = tickers[i : i + chunk_size]
        i += chunk_size

        out = pnd_model.predict_full(indexed.loc[chunk_tickers])
        for ticker, row in out.iterrows():
            if bool(row["pnd_block"]):
                blocked.add(ticker)
            _write_signal(
                client, api_base_url,
                {
                    "date": run_date.isoformat(), "ticker": ticker, "model_name": PND_MODEL_NAME,
                    "model_version": "1.0",
                    "pnd_score": None if pd.isna(row["pnd_score"]) else float(row["pnd_score"]),
                    "pnd_phase": row["pnd_phase"], "pnd_block": bool(row["pnd_block"]),
                },
            )
        del out
    return blocked


def _step_signals_and_meta(
    feature_matrix: pd.DataFrame, blocked_tickers: set, run_date: date_type, client: httpx.Client,
    api_base_url: str, models_dir: Path,
) -> pd.DataFrame:
    """
    SPEC-MODEL-006: blocked tickers are excluded before scoring, not
    scored-then-discarded — a blocked ticker's signal_5d row is never
    computed or written at all.

    Returns an empty DataFrame (no error) if every ticker is excluded —
    e.g. every ticker is P&D-blocked that day, or the universe passed in
    is empty — rather than letting sklearn's "0 samples" ValueError
    propagate (caught via a live smoke test: a synthetic, unrealistic
    P&D feature matrix blocked all 10 test tickers and crashed here).

    A55 (2026-07-11, real production OOM incident — see BuildLog.md): this
    is the step that actually blew up alphalens-scheduler.service's memory
    ceiling during a 6-day catch-up backfill (~2,317-ticker full universe).
    It is the heaviest per-run step by far — 5 models (signal_5d, meta,
    signal_21d, signal_63d, conformal) each scoring the FULL eligible
    cross-section at once, plus a SHAP TreeExplainer pass (shap_values
    returns an (n_samples, n_features, n_classes) array — for ~150
    features x 3 classes x 2,317 tickers that is a large dense float64
    array held in memory all at once), all before a single row is written.
    Scoring/SHAP/writing is now done in ticker CHUNKS (same
    resource_guard.adaptive_chunk_size pattern as A47/A55's _step_pnd_filter
    above) — each model's predict_signals/predict/predict_full/SHAP calls
    are per-row-independent (no cross-ticker aggregation), so chunking the
    ROWS they're called on cannot change any individual row's output, only
    how many rows' intermediate arrays are held in memory simultaneously.
    Models themselves are loaded ONCE outside the chunk loop — model
    objects are constant-size regardless of ticker count, so reloading
    them per chunk would add I/O cost for no memory benefit.
    """
    eligible = feature_matrix[~feature_matrix["ticker"].isin(blocked_tickers)].set_index("ticker")
    if eligible.empty:
        logger.warning("daily_inference: no eligible (non-P&D-blocked) tickers to score today")
        return pd.DataFrame()

    # ML24 (2026-07-11): tag every written row with whether its ticker was in
    # the ADTV-curated set the live model was actually trained on — models
    # still score the full universe (pooled panel models, not per-ticker
    # artifacts), this is purely an out-of-distribution flag for the UI.
    from config.training_universe import load_current_training_universe

    try:
        training_universe_set = set(load_current_training_universe())
    except Exception as exc:
        logger.warning(f"daily_inference: could not load training universe, in_training_universe left null today ({exc})")
        training_universe_set = None

    signal_model = _load_model(Signal5DModel, SIGNAL_MODEL_NAME, models_dir)
    meta_model = _load_model(MetaLabeler, META_MODEL_NAME, models_dir)

    # 2026-07-19 full-codebase-review Fix A3: optional, best-effort —
    # a missing/never-trained ensemble artifact (the common case; see
    # _load_stacking_ensemble's docstring) must never block signal_5d/
    # meta_labeler from being scored and written.
    try:
        ensemble_model = _load_stacking_ensemble(models_dir)
    except Exception as exc:
        ensemble_model = None
        logger.info(f"daily_inference: no usable stacking ensemble artifact, skipping ensemble combination today ({exc})")

    # FutureDevelopment.md #14: signal_21d/signal_63d are trained and
    # present in the model registry (datastore/models/registry.json) but,
    # until this change, were never invoked from the daily per-ticker
    # loop — only signal_5d was scored. Each is optional at the per-run
    # level (a missing/corrupt model file for one horizon must not stop
    # signal_5d/meta from being scored and written), so each load is
    # wrapped individually.
    longer_horizon_models: Dict[str, Any] = {}
    for name, cls in ((SIGNAL_21D_MODEL_NAME, Signal21DModel), (SIGNAL_63D_MODEL_NAME, Signal63DModel)):
        try:
            longer_horizon_models[name] = _load_model(cls, name, models_dir)
        except Exception as exc:
            logger.warning(f"daily_inference: could not load {name} model, skipping it today ({exc})")

    # FutureDevelopment.md #14: conformal_signal5d is trained/registered
    # but was never invoked to populate conformal_lower/conformal_upper.
    # [AS BUILT] train_all_phase1.py's conformal branch calibrated this
    # model against CORE_TECHNICAL_FEATURES only (70 cols) — the Q50
    # regressor it wraps was fit on that narrower set (n_features_in_ == 70
    # on the trained artifact, confirmed by inspection), NOT the full
    # 150-column feature set signal_5d/meta_labeler use post-2026-06-23
    # retrain (see this function's own 2026-07-02 bug-fix comment below
    # for that exact prior mismatch). Passing the full 150-col frame here
    # would reproduce the same "silent 80-extra-columns" bug against a
    # sklearn-style estimator that actually validates column count. So
    # conformal scoring always slices back down to CORE_TECHNICAL_FEATURES.
    try:
        conformal = _load_conformal(models_dir)
    except Exception as exc:
        conformal = None
        logger.warning(f"daily_inference: could not load conformal_signal5d model, skipping it today ({exc})")

    # [BUG FIX, 2026-07-02] Previously pre-filtered to CORE_TECHNICAL_FEATURES
    # (70 cols, Phase 1 technical-only) before calling predict_signals/
    # predict_full, which internally do X[self._feature_names] against
    # each model's own saved training feature list. Since the 2026-06-23
    # retrain, signal_5d/meta_labeler were trained on the full 150-column
    # feature set (technical + fundamental + governance + MF + F&O —
    # everything in ALL_FEATURE_COLUMNS a given ticker/date has), so the
    # CORE_TECHNICAL_FEATURES-only X was missing 80 columns the models
    # actually need -> hard KeyError, run_models failed for every date
    # since 2026-06-23 (see BuildLog.md). Fix: pass the full eligible
    # feature frame through — each model selects its own needed subset
    # via self._feature_names internally, so passing extra columns is
    # harmless and keeps this call site correct regardless of which
    # feature subset any given model version was trained on.
    X = eligible

    from ingestion.scheduler.resource_guard import adaptive_chunk_size

    from config.settings import PIPELINE_MEMORY_CEILING_MB, SCREENER_BATCH_EXPORT_CHUNK_SIZE

    conformal_cols = [c for c in CORE_TECHNICAL_FEATURES if c in X.columns]

    tickers = list(X.index)
    result_chunks: List[pd.DataFrame] = []
    i = 0
    while i < len(tickers):
        chunk_size = adaptive_chunk_size(SCREENER_BATCH_EXPORT_CHUNK_SIZE, ceiling_mb=PIPELINE_MEMORY_CEILING_MB)
        chunk_tickers = tickers[i : i + chunk_size]
        i += chunk_size

        Xc = X.loc[chunk_tickers]

        proba = signal_model.predict_signals(Xc)
        # threshold-based call (SPEC-MODEL-007), not a bare probability argmax
        direction = signal_model.predict(Xc)
        meta_out = meta_model.predict_full(Xc)

        # FutureDevelopment.md #16: SHAP top-5 per ticker for signal_5d's
        # predicted class. One bad chunk's SHAP failure must not block
        # signal_5d/meta from being written for that chunk — this is
        # enrichment, not a required column (shap_top5_json is Optional
        # in the schema).
        try:
            shap_top5 = _compute_shap_top5(signal_model, Xc, direction)
        except Exception as exc:
            shap_top5 = {}
            logger.warning(f"daily_inference: SHAP computation failed, shap_top5_json left null today ({exc})")

        # FutureDevelopment.md #14: conformal_lower/upper for signal_5d,
        # scored against CORE_TECHNICAL_FEATURES only (see the load-time
        # comment above for why it can't take the full 150-col X).
        conformal_intervals = None
        if conformal is not None:
            try:
                conformal_intervals = conformal.predict_interval(Xc[conformal_cols])
            except Exception as exc:
                logger.warning(
                    f"daily_inference: conformal scoring failed, conformal_lower/upper left null today ({exc})"
                )

        for ticker in Xc.index:
            payload = {
                "date": run_date.isoformat(), "ticker": ticker, "model_name": SIGNAL_MODEL_NAME,
                "model_version": "1.0",
                "signal_direction": CLASS_NAMES[int(direction.loc[ticker])],
                "buy_prob": float(proba.loc[ticker, "signal_buy_prob"]),
                "hold_prob": float(proba.loc[ticker, "signal_hold_prob"]),
                "sell_prob": float(proba.loc[ticker, "signal_sell_prob"]),
                "q10_return": float(proba.loc[ticker, "signal_q10"]),
                "q50_return": float(proba.loc[ticker, "signal_q50"]),
                "q90_return": float(proba.loc[ticker, "signal_q90"]),
            }
            if training_universe_set is not None:
                payload["in_training_universe"] = ticker in training_universe_set
            if conformal_intervals is not None and ticker in conformal_intervals.index:
                payload["conformal_lower"] = float(conformal_intervals.loc[ticker, "conformal_lower"])
                payload["conformal_upper"] = float(conformal_intervals.loc[ticker, "conformal_upper"])
            if ticker in shap_top5:
                payload["shap_top5_json"] = shap_top5[ticker]
            _write_signal(client, api_base_url, payload)
            _write_signal(
                client, api_base_url,
                {
                    "date": run_date.isoformat(), "ticker": ticker, "model_name": META_MODEL_NAME,
                    "model_version": "1.0",
                    "meta_label": "act" if bool(meta_out.loc[ticker, "meta_label_act"]) else "no_act",
                    "meta_prob": float(meta_out.loc[ticker, "meta_label_prob"]),
                },
            )

        # FutureDevelopment.md #14: signal_21d/signal_63d scoring — same
        # BUY/HOLD/SELL + Q10/Q50/Q90 output contract as signal_5d, written
        # as their own (date, ticker, model_name) rows (SPEC-DS-004), never
        # blended into the signal_5d row itself.
        # 2026-07-19 full-codebase-review Fix A3: captured per-horizon
        # proba frames so the ensemble-combine step below can build a
        # {model_name: ndarray} input from whichever of signal_21d/
        # signal_63d actually scored successfully this chunk (signal_5d's
        # `proba` above is always available since its load isn't wrapped
        # in try/except like the longer-horizon models are).
        horizon_probas: Dict[str, pd.DataFrame] = {}

        for name, model in longer_horizon_models.items():
            try:
                lh_proba = model.predict_signals(Xc)
                lh_direction = model.predict(Xc)
            except Exception as exc:
                logger.warning(f"daily_inference: {name} scoring failed for this batch, skipping it today ({exc})")
                continue
            horizon_probas[name] = lh_proba

            # ML24 (2026-07-11): this module's own comment previously claimed
            # signal_21d/signal_63d SHAP was "already wired in" — false; SHAP
            # was only ever computed for signal_5d above. _compute_shap_top5
            # only touches _lgbm/_feature_names/_impute_transform, all present
            # on every BaseSignalModel subclass, so it's reusable as-is here.
            try:
                lh_shap_top5 = _compute_shap_top5(model, Xc, lh_direction)
            except Exception as exc:
                lh_shap_top5 = {}
                logger.warning(f"daily_inference: {name} SHAP computation failed, shap_top5_json left null today ({exc})")

            for ticker in Xc.index:
                lh_payload = {
                    "date": run_date.isoformat(), "ticker": ticker, "model_name": name, "model_version": "1.0",
                    "signal_direction": CLASS_NAMES[int(lh_direction.loc[ticker])],
                    "buy_prob": float(lh_proba.loc[ticker, "signal_buy_prob"]),
                    "hold_prob": float(lh_proba.loc[ticker, "signal_hold_prob"]),
                    "sell_prob": float(lh_proba.loc[ticker, "signal_sell_prob"]),
                    "q10_return": float(lh_proba.loc[ticker, "signal_q10"]),
                    "q50_return": float(lh_proba.loc[ticker, "signal_q50"]),
                    "q90_return": float(lh_proba.loc[ticker, "signal_q90"]),
                }
                if training_universe_set is not None:
                    lh_payload["in_training_universe"] = ticker in training_universe_set
                if ticker in lh_shap_top5:
                    lh_payload["shap_top5_json"] = lh_shap_top5[ticker]
                _write_signal(
                    client, api_base_url,
                    lh_payload,
                )

        # 2026-07-19 full-codebase-review Fix A3: combine signal_5d/21d/63d
        # into a stacking-ensemble row, only when (a) a trained artifact
        # exists and (b) all three of its expected base models scored
        # successfully this chunk — a partial set (e.g. signal_21d failed
        # to load today) skips ensemble combination for this chunk rather
        # than feeding predict_ensemble a subset it wasn't trained on.
        if ensemble_model is not None and all(m in horizon_probas for m in (SIGNAL_21D_MODEL_NAME, SIGNAL_63D_MODEL_NAME)):
            try:
                base_predictions = {
                    SIGNAL_MODEL_NAME: proba[["signal_sell_prob", "signal_hold_prob", "signal_buy_prob"]].to_numpy(),
                    SIGNAL_21D_MODEL_NAME: horizon_probas[SIGNAL_21D_MODEL_NAME][
                        ["signal_sell_prob", "signal_hold_prob", "signal_buy_prob"]
                    ].to_numpy(),
                    SIGNAL_63D_MODEL_NAME: horizon_probas[SIGNAL_63D_MODEL_NAME][
                        ["signal_sell_prob", "signal_hold_prob", "signal_buy_prob"]
                    ].to_numpy(),
                }
                ensemble_out = ensemble_model.predict_ensemble(base_predictions)
                for i, ticker in enumerate(Xc.index):
                    _write_signal(
                        client, api_base_url,
                        {
                            "date": run_date.isoformat(), "ticker": ticker,
                            "model_name": STACKING_ENSEMBLE_MODEL_NAME, "model_version": "1.0",
                            "signal_direction": CLASS_NAMES[int(ensemble_out.predict_class()[i])],
                            "buy_prob": float(ensemble_out.final_buy_prob[i]),
                            "hold_prob": float(ensemble_out.final_hold_prob[i]),
                            "sell_prob": float(ensemble_out.final_sell_prob[i]),
                        },
                    )
            except Exception as exc:
                # Same defensive isolation as every other model in this
                # function — e.g. a trained artifact whose base_model_names
                # don't match STACKING_ENSEMBLE_BASE_MODELS (a full
                # 5-model tft/bilstm-inclusive artifact, per
                # _load_stacking_ensemble's docstring) must not block
                # signal_5d/meta_labeler from being written.
                logger.warning(f"daily_inference: stacking ensemble combination failed for this batch, skipping it today ({exc})")

        result_chunks.append(proba.join(meta_out))
        del proba, direction, meta_out, shap_top5, conformal_intervals, Xc

    if not result_chunks:
        return pd.DataFrame()
    return pd.concat(result_chunks)


def _step_exit(
    position_context: pd.DataFrame, run_date: date_type, client: httpx.Client, api_base_url: str, models_dir: Path
) -> List[str]:
    """Score held positions for exit urgency/type/survival. Returns tickers flagged urgent (SPEC-MODEL-002, M-07)."""
    if position_context.empty:
        return []

    exit_model = _load_exit_model(models_dir)
    cols = [c for c in EXIT_CONTEXT_COLUMNS if c in position_context.columns]
    out = exit_model.predict_full(position_context.set_index("ticker")[cols])

    urgent = []
    for ticker, row in out.iterrows():
        if float(row["exit_urgency"]) > EXIT_URGENT_THRESHOLD:
            urgent.append(ticker)
        _write_signal(
            client, api_base_url,
            {
                "date": run_date.isoformat(), "ticker": ticker, "model_name": EXIT_MODEL_NAME, "model_version": "1.0",
                "exit_urgency": float(row["exit_urgency"]), "exit_type": row["exit_type"],
                "exit_survival_5d": float(row["exit_survival_5d"]),
                "exit_survival_21d": float(row["exit_survival_21d"]),
                "exit_survival_63d": float(row["exit_survival_63d"]),
            },
        )
    return urgent


def run_daily_inference(
    run_date: date_type,
    feature_matrix: pd.DataFrame,
    pnd_feature_matrix: pd.DataFrame,
    market_ohlcv: pd.DataFrame,
    position_context: Optional[pd.DataFrame] = None,
    api_base_url: Optional[str] = None,
    psi_baseline: Optional[dict] = None,
    models_dir: Optional[Path] = None,
    client: Optional[httpx.Client] = None,
) -> Dict[str, Any]:
    """
    Run the full daily inference sequence: HMM -> PSI check -> P&D filter
    -> Signals -> MetaLabel -> Exit -> write to DataStore.

    Parameters
    ----------
    run_date : date
    feature_matrix : pd.DataFrame
        Today's cross-section: columns ticker + CORE_TECHNICAL_FEATURES
        (a subset of features.matrix_builder.ALL_FEATURE_COLUMNS — see
        train_all_phase1.py's same scoping decision).
    pnd_feature_matrix : pd.DataFrame
        Today's cross-section: columns ticker + PND_FEATURES.
    market_ohlcv : pd.DataFrame
        Recent history (>= MIN_OBSERVATIONS rows) of a market proxy index,
        long-format OHLCV — feeds the market-wide HMM.
    position_context : pd.DataFrame, optional
        Currently held positions: columns ticker + EXIT_CONTEXT_COLUMNS.
        Empty/omitted if no positions are held (exit step is then a no-op).
    api_base_url : str, optional
        Defaults to config.settings.DATASTORE_API_BASE_URL.
    psi_baseline : dict, optional
        Injected for testability; defaults to PSIMonitor().load_baseline().
    models_dir : Path, optional
        Defaults to config.settings.MODELS_DIR. Overridable for tests so a
        run never touches the real production model registry.
    client : httpx.Client, optional
        Injected for testability (SPEC-SOLID-005, same DI pattern as
        datastore.client.DataStoreClient) — e.g. an
        httpx.Client(transport=httpx.ASGITransport(app=...)) wired
        straight to an in-process FastAPI app in integration tests,
        instead of a real TCP connection. Defaults to a plain
        httpx.Client() hitting api_base_url over the network.

    Returns
    -------
    dict
        run_date, halted (bool), halt_reason (str|None), regime (str|None),
        psi (dict), pnd_blocked (list[str]), tickers_scored (int),
        urgent_exits (list[str]), step_timings_s (dict), completed_at.

    Spec References
    ----------------
    SPEC-MODEL-006, SPEC-PIPE-005, SPEC-SYS-002, SPEC-DS-002, SPEC-DS-004.

    Raises
    ------
    None — a PSI halt is reported in the return value (halted=True), not
    raised, so the caller (ingestion/scheduler/daily_pipeline.py's
    step_run_models) can checkpoint/alert without a bare except.
    """
    api_base_url = api_base_url or DATASTORE_API_BASE_URL
    models_dir = models_dir or MODELS_DIR
    position_context = position_context if position_context is not None else pd.DataFrame(columns=["ticker"])
    started_at = now_ist()
    timings: Dict[str, float] = {}
    result: Dict[str, Any] = {
        "run_date": run_date.isoformat(), "halted": False, "halt_reason": None, "regime": None,
        "psi": None, "pnd_blocked": [], "tickers_scored": 0, "urgent_exits": [], "step_timings_s": timings,
    }

    owns_client = client is None
    http_client = client or httpx.Client()
    try:
        t0 = time.monotonic()
        try:
            result["regime"] = _step_hmm(market_ohlcv, run_date, http_client, api_base_url, models_dir)
            log_pipeline_step("hmm", "success", stocks=1, duration_s=time.monotonic() - t0)
        except Exception as exc:
            log_pipeline_step("hmm", "failed", stocks=0, duration_s=time.monotonic() - t0, error=str(exc))
            logger.warning(f"daily_inference: HMM step failed, continuing without a regime ({exc})")
        timings["hmm"] = time.monotonic() - t0

        t0 = time.monotonic()
        psi = _step_psi_check(feature_matrix, psi_baseline)
        result["psi"] = psi
        timings["psi_check"] = time.monotonic() - t0
        log_pipeline_step("psi_check", "success", stocks=len(feature_matrix), duration_s=timings["psi_check"])
        if psi["status"] == "halt":
            result["halted"] = True
            result["halt_reason"] = (
                f"PSI drift halt: {psi['worst_feature']} PSI={psi['worst_psi']:.3f} "
                f"> {PSI_SEVERE_THRESHOLD} (SPEC-PIPE-005/SPEC-ALERT-001)"
            )
            logger.error(f"daily_inference: HALTED — {result['halt_reason']}")
            result["completed_at"] = now_ist().isoformat()
            return result
        if psi["status"] == "warning":
            logger.warning(
                f"daily_inference: PSI warning — {psi['worst_feature']} PSI={psi['worst_psi']:.3f} "
                "(SPEC-ALERT-001: halve position sizing)"
            )

        t0 = time.monotonic()
        try:
            blocked = _step_pnd_filter(pnd_feature_matrix, run_date, http_client, api_base_url, models_dir)
        except Exception as exc:
            log_pipeline_step("pnd_filter", "failed", stocks=0, duration_s=time.monotonic() - t0, error=str(exc))
            raise
        result["pnd_blocked"] = sorted(blocked)
        timings["pnd_filter"] = time.monotonic() - t0
        log_pipeline_step("pnd_filter", "success", stocks=len(pnd_feature_matrix), duration_s=timings["pnd_filter"])

        t0 = time.monotonic()
        try:
            scored = _step_signals_and_meta(feature_matrix, blocked, run_date, http_client, api_base_url, models_dir)
        except Exception as exc:
            log_pipeline_step("signals_meta", "failed", stocks=0, duration_s=time.monotonic() - t0, error=str(exc))
            raise
        result["tickers_scored"] = len(scored)
        timings["signals_meta"] = time.monotonic() - t0
        log_pipeline_step("signals_meta", "success", stocks=len(scored), duration_s=timings["signals_meta"])

        t0 = time.monotonic()
        try:
            result["urgent_exits"] = _step_exit(position_context, run_date, http_client, api_base_url, models_dir)
        except Exception as exc:
            log_pipeline_step("exit", "failed", stocks=0, duration_s=time.monotonic() - t0, error=str(exc))
            raise
        timings["exit"] = time.monotonic() - t0
        log_pipeline_step("exit", "success", stocks=len(position_context), duration_s=timings["exit"])
    finally:
        if owns_client:
            http_client.close()

    result["completed_at"] = now_ist().isoformat()
    result["total_duration_s"] = (now_ist() - started_at).total_seconds()
    logger.info(
        f"daily_inference: completed for {run_date.isoformat()} in {result['total_duration_s']:.1f}s "
        f"({result['tickers_scored']} scored, {len(result['pnd_blocked'])} P&D-blocked, "
        f"{len(result['urgent_exits'])} urgent exits)"
    )
    return result
