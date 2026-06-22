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

[AS BUILT] Conformal (P1.5's ConformalPredictor) is not wired into this
orchestration for the same reason backtest/engine.py (P1.6) left it out:
it calibrates return-magnitude regression intervals and there is no
return-regression estimator in this classification-based entry pipeline
for it to wrap yet. Documented as a known Phase 1 gap, not a silent
omission — see BuildLog.md "P1.7".
"""

import logging
import time
from datetime import date as date_type
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import joblib
import pandas as pd

from config.settings import DATASTORE_API_BASE_URL, EXIT_URGENT_THRESHOLD, MODELS_DIR, PSI_SEVERE_THRESHOLD
from config.timezone import now_ist
from features.pnd_features import PND_FEATURES
from features.technical import CORE_TECHNICAL_FEATURES
from ingestion.quality.drift_monitor import PSIMonitor
from ingestion.quality.structured_logger import log_pipeline_step
from systems.ml_signal_engine.models.exit.exit_signal import ExitSignalModel
from systems.ml_signal_engine.models.hmm.regime_detector import compute_hmm_observables
from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector
from systems.ml_signal_engine.models.signal.base_signal_model import CLASS_NAMES
from systems.ml_signal_engine.models.signal.meta_labeler import MetaLabeler
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel

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
META_MODEL_NAME = "meta_labeler"
EXIT_MODEL_NAME = "exit_signal"

REGIME_RANK_NAMES = {0.0: "bearish", 1.0: "sideways", 2.0: "volatile", 3.0: "bullish"}

# Must match exit_signal.generate_synthetic_training_data()'s schema (P1.6) —
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
    """SPEC-MODEL-006: score every ticker, write each row, return the set of blocked tickers."""
    pnd_model = _load_model(PnDDetector, PND_MODEL_NAME, models_dir)
    feature_cols = [c for c in PND_FEATURES if c in pnd_feature_matrix.columns]
    out = pnd_model.predict_full(pnd_feature_matrix.set_index("ticker")[feature_cols])

    blocked = set()
    for ticker, row in out.iterrows():
        if bool(row["pnd_block"]):
            blocked.add(ticker)
        _write_signal(
            client, api_base_url,
            {
                "date": run_date.isoformat(), "ticker": ticker, "model_name": PND_MODEL_NAME, "model_version": "1.0",
                "pnd_score": None if pd.isna(row["pnd_score"]) else float(row["pnd_score"]),
                "pnd_phase": row["pnd_phase"], "pnd_block": bool(row["pnd_block"]),
            },
        )
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
    """
    eligible = feature_matrix[~feature_matrix["ticker"].isin(blocked_tickers)].set_index("ticker")
    if eligible.empty:
        logger.warning("daily_inference: no eligible (non-P&D-blocked) tickers to score today")
        return pd.DataFrame()

    signal_model = _load_model(Signal5DModel, SIGNAL_MODEL_NAME, models_dir)
    meta_model = _load_model(MetaLabeler, META_MODEL_NAME, models_dir)

    feat_cols = [c for c in CORE_TECHNICAL_FEATURES if c in eligible.columns]
    X = eligible[feat_cols]

    proba = signal_model.predict_signals(X)
    direction = signal_model.predict(X)  # threshold-based call (SPEC-MODEL-007), not a bare probability argmax
    meta_out = meta_model.predict_full(X)

    for ticker in X.index:
        _write_signal(
            client, api_base_url,
            {
                "date": run_date.isoformat(), "ticker": ticker, "model_name": SIGNAL_MODEL_NAME, "model_version": "1.0",
                "signal_direction": CLASS_NAMES[int(direction.loc[ticker])],
                "buy_prob": float(proba.loc[ticker, "signal_buy_prob"]),
                "hold_prob": float(proba.loc[ticker, "signal_hold_prob"]),
                "sell_prob": float(proba.loc[ticker, "signal_sell_prob"]),
                "q10_return": float(proba.loc[ticker, "signal_q10"]),
                "q50_return": float(proba.loc[ticker, "signal_q50"]),
                "q90_return": float(proba.loc[ticker, "signal_q90"]),
            },
        )
        _write_signal(
            client, api_base_url,
            {
                "date": run_date.isoformat(), "ticker": ticker, "model_name": META_MODEL_NAME, "model_version": "1.0",
                "meta_label": "act" if bool(meta_out.loc[ticker, "meta_label_act"]) else "no_act",
                "meta_prob": float(meta_out.loc[ticker, "meta_label_prob"]),
            },
        )

    return proba.join(meta_out)


def _step_exit(
    position_context: pd.DataFrame, run_date: date_type, client: httpx.Client, api_base_url: str, models_dir: Path
) -> List[str]:
    """Score held positions for exit urgency/type/survival. Returns tickers flagged urgent (SPEC-MODEL-002, M-07)."""
    if position_context.empty:
        return []

    exit_model = _load_model(ExitSignalModel, EXIT_MODEL_NAME, models_dir)
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
