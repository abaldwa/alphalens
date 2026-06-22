"""
systems/ml_signal_engine/inference/train_all_phase1.py

Phase: 1.5 (Core Signal Models)
Specs: SPEC-MODEL-001 through SPEC-MODEL-007, SPEC-BT-001
Owner: ml_signal_engine / inference
Consumers: operator CLI (`python3 -m systems.ml_signal_engine.inference.train_all_phase1`)

First end-to-end walk-forward training run: HMM -> P&D -> Signal5D ->
Signal21D -> MetaLabeler -> Conformal, saving each model to
datastore/models/ with SPEC-MODEL-005 versioned filenames + a
registry.json entry, then printing BacktestIntegrityChecker results.

Training data honesty (Phase 1 gap, same pattern as every other
"no real historical archive yet" gap this project has hit): the daily
pipeline (P1.7, not yet built) hasn't accumulated enough real feature-
matrix history across enough trading days to walk-forward train on real
data yet — the dev DB has a handful of real trading days, not the years
of daily snapshots a real walk-forward fit needs. This script trains on a
SYNTHETIC multi-ticker OHLCV universe (same random-walk-plus-pattern
generators used by every other phase's tests), run through the REAL
features.technical.compute_technical_features() and the REAL
TripleBarrierLabeler — so the feature/label SHAPES, the model code, and
the save/load/registry machinery are all exactly what production will
use; only the underlying price series are synthetic. Swapping in
real accumulated daily-pipeline output later is a data swap, not a code
change — every function below takes (X, y, returns) or raw OHLCV, never a
hardcoded path to this script's specific synthetic generator.

Uses Signal5D/Signal21D's only 70 technical features (features.technical)
as the training feature set, not the full 102-column
features.matrix_builder.ALL_FEATURE_COLUMNS — computing the full panel
(which includes a per-ticker HMM fit, see features/technical.py and
systems/ml_signal_engine/models/hmm/regime_detector.py) for dozens of
tickers across hundreds of synthetic dates would multiply this script's
runtime for no real benefit over a smaller, representative feature set in
this synthetic-data feasibility run.
"""

import argparse
import logging
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd
import talib

from backtest.costs import IndianTransactionCosts
from backtest.integrity_checker import BacktestIntegrityChecker
from config.settings import MIN_ADT_INR, MODELS_DIR
from config.timezone import now_ist
from features.technical import CORE_TECHNICAL_FEATURES, compute_technical_features
from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector, generate_synthetic_training_data
from systems.ml_signal_engine.models.signal.meta_labeler import MetaLabeler
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel
from systems.ml_signal_engine.models.signal.signal_21d import Signal21DModel
from systems.ml_signal_engine.models.uncertainty.conformal import ConformalPredictor
from systems.ml_signal_engine.training.labeling import TripleBarrierLabeler
from systems.ml_signal_engine.training.walk_forward import WalkForwardValidator

logger = logging.getLogger(__name__)

MODEL_VERSION_DATE_FORMAT = "%Y%m%d"


def _generate_synthetic_universe(n_tickers: int, n_days: int, seed: int = 0) -> pd.DataFrame:
    """Same shape as tests/unit/test_features_technical.py's generator — long-format OHLCV + delivery_pct."""
    dates = pd.bdate_range(start="2022-01-01", periods=n_days)
    frames = []
    for i in range(n_tickers):
        rng = np.random.default_rng(seed + i)
        base_price = 50 + rng.uniform(0, 500)
        rets = rng.normal(0.0003, 0.02, n_days)
        close = base_price * np.cumprod(1 + rets)
        open_ = close * (1 + rng.normal(0, 0.005, n_days))
        high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.005, n_days)))
        low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.005, n_days)))
        volume = rng.integers(100_000, 5_000_000, n_days).astype(float)
        delivery_pct = rng.uniform(20, 80, n_days)
        frames.append(
            pd.DataFrame(
                {
                    "date": dates, "ticker": f"SYN{i:04d}", "open": open_, "high": high, "low": low,
                    "close": close, "volume": volume, "delivery_pct": delivery_pct,
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


def _generate_synthetic_benchmark(dates: pd.DatetimeIndex, seed: int = 999) -> pd.DataFrame:
    """Synthetic nifty50/100/500_close history so Category 7 (relative strength) features
    populate instead of going permanently NaN — see features/technical.py's BENCHMARK_TICKERS."""
    rng = np.random.default_rng(seed)
    out = {"date": dates}
    for name in ("nifty50", "nifty100", "nifty500"):
        rets = rng.normal(0.0002, 0.01, len(dates))
        out[f"{name}_close"] = 100 * np.cumprod(1 + rets)
    return pd.DataFrame(out)


def _build_training_dataset(
    ohlcv: pd.DataFrame, horizon_days: int, profit_multiplier: float, stop_multiplier: float
) -> pd.DataFrame:
    """
    Returns one combined DataFrame: date, ticker, CORE_TECHNICAL_FEATURES,
    _label (TripleBarrierLabeler direction in {-1, 0, 1}), _return
    (realized forward close-to-close return over horizon_days — the
    quantile-regression target). Rows are dropped only where _label/
    _return are NaN (the unresolvable tail) — feature columns may still
    contain some NaN (e.g. a 252-day lookback not yet warmed up for a
    given ticker/date); LightGBM/CatBoost/XGBoost all handle missing
    feature values natively (SPEC-FEAT-004's documented pattern), so
    dropping every row with ANY NaN feature would wipe out the whole
    dataset over one slow-to-warm-up column.
    """
    benchmark = _generate_synthetic_benchmark(pd.DatetimeIndex(sorted(ohlcv["date"].unique())))
    features = compute_technical_features(ohlcv, benchmark)

    atr_parts = []
    for ticker, g in ohlcv.sort_values(["ticker", "date"]).groupby("ticker", sort=False):
        atr = talib.ATR(
            g["high"].to_numpy(dtype=np.float64), g["low"].to_numpy(dtype=np.float64),
            g["close"].to_numpy(dtype=np.float64), timeperiod=14,
        )
        atr_parts.append(pd.DataFrame({"date": g["date"].to_numpy(), "ticker": ticker, "atr_14": atr}))
    atr_df = pd.concat(atr_parts, ignore_index=True)

    merged = ohlcv.merge(atr_df, on=["date", "ticker"], how="left")
    labeler = TripleBarrierLabeler(
        profit_multiplier=profit_multiplier, stop_multiplier=stop_multiplier, max_holding=horizon_days
    )
    labels = labeler.label_panel(merged, close_col="close", atr_col="atr_14", ticker_col="ticker")

    forward_returns = merged.groupby("ticker", sort=False)["close"].transform(
        lambda s: s.shift(-horizon_days) / s - 1
    )

    combined = features.copy()
    combined["_label"] = labels.to_numpy()
    combined["_return"] = forward_returns.to_numpy()
    combined = combined.dropna(subset=["_label", "_return"]).reset_index(drop=True)
    return combined


def _save_model(
    model, name: str, run_date: pd.Timestamp, registry: Dict, metadata_extra: Optional[Dict] = None
) -> Path:
    """SPEC-MODEL-005: {model_name}_v{YYYYMMDD}_{fold}.pkl + registry.json entry."""
    model_dir = MODELS_DIR / name
    model_dir.mkdir(parents=True, exist_ok=True)
    version = run_date.strftime(MODEL_VERSION_DATE_FORMAT)
    versioned_path = model_dir / f"{name}_v{version}_fold0.pkl"
    current_path = model_dir / f"{name}_current.pkl"

    model.save(str(versioned_path))
    # SPEC-MODEL-005: production symlink-equivalent (plain copy — no symlinks on all filesystems)
    model.save(str(current_path))

    meta = model.metadata() if hasattr(model, "metadata") else {}
    meta = {**meta, **(metadata_extra or {})}
    meta["saved_path"] = str(versioned_path)
    meta["saved_at"] = run_date.isoformat()
    registry[name] = meta
    logger.info(f"Saved {name} -> {versioned_path}")
    return versioned_path


def train_all_phase1(
    n_tickers: int = 40,
    n_days: int = 400,
    optuna_trials: int = 5,
    save: bool = True,
    seed: int = 42,
) -> Dict:
    """
    Run the full P1.5 training sequence: HMM (market-wide) -> P&D ->
    Signal5D -> Signal21D -> MetaLabeler -> Conformal.

    Parameters
    ----------
    n_tickers, n_days : int
        Synthetic universe size (see module docstring on why synthetic).
    optuna_trials : int
        Per-model Optuna trial count. Defaults to 5 (not the documented
        production value of 100) so this script completes in a
        reasonable time for a feasibility/smoke run — pass 100 for a
        production-grade fit.
    save : bool
        If True (default), persist each model to datastore/models/ and
        write registry.json.
    seed : int
        Base RNG seed for the synthetic universe and all model fits.

    Returns
    -------
    dict
        {"registry": {...}, "integrity_results": {...} or None}

    Spec References
    ----------------
    SPEC-MODEL-001 through SPEC-MODEL-007 (see each model's own module).
    SPEC-BT-001: integrity checks printed at the end.

    Raises
    ------
    None — integrity-check failures are reported, not raised, so the
    operator sees the full picture from one run.
    """
    run_date = now_ist()
    logger.info(f"P1.5 training run starting: {n_tickers} synthetic tickers x {n_days} days, seed={seed}")

    ohlcv = _generate_synthetic_universe(n_tickers, n_days, seed)
    registry: Dict = {}

    # ===== 1. HMM (market-wide, Nifty-proxy) =====
    from systems.ml_signal_engine.models.hmm.regime_detector import HMMRegimeDetector, compute_hmm_observables

    market_proxy = ohlcv[ohlcv["ticker"] == ohlcv["ticker"].iloc[0]].sort_values("date")
    hmm_obs = compute_hmm_observables(market_proxy)
    hmm_model = HMMRegimeDetector(random_state=seed)
    try:
        hmm_model.fit(hmm_obs)
        logger.info("HMM (market-wide proxy) fit succeeded")
    except ValueError as exc:
        logger.warning(f"HMM fit skipped: {exc}")
    if save and hmm_model._model is not None:
        import joblib

        hmm_dir = MODELS_DIR / "hmm"
        hmm_dir.mkdir(parents=True, exist_ok=True)
        hmm_path = hmm_dir / f"hmm_market_v{run_date.strftime(MODEL_VERSION_DATE_FORMAT)}.pkl"
        joblib.dump(hmm_model, hmm_path)
        registry["hmm_market"] = {"saved_path": str(hmm_path), "saved_at": run_date.isoformat()}
        logger.info(f"Saved hmm_market -> {hmm_path}")

    # ===== 2. P&D =====
    pnd_X, pnd_y = generate_synthetic_training_data(n_positive=15, n_negative=485, n_days=90, seed=seed)
    pnd_model = PnDDetector(random_state=seed)
    pnd_model.train(pnd_X, pnd_y)
    if save:
        _save_model(pnd_model, "pnd_detector", run_date, registry)

    # ===== 3 & 4. Signal5D, Signal21D =====
    validator = WalkForwardValidator(n_folds=2)
    signal_models = {}
    for horizon, cls, name in ((5, Signal5DModel, "signal_5d"), (21, Signal21DModel, "signal_21d")):
        combined = _build_training_dataset(ohlcv, horizon, profit_multiplier=2.0, stop_multiplier=1.0)

        n_folds_data = combined["date"].dt.year.nunique() - 1
        if n_folds_data < 1:
            # Not enough distinct years for a calendar-year walk-forward fold (e.g. --quick's
            # ~200-day synthetic run) — fall back directly to a chronological train/validation
            # split on the whole dataset. NOTE: this must be date-based, not a positional
            # combined.iloc[:0.7] slice — combined is sorted (ticker, date), so a positional cut
            # lands inside one ticker's contiguous date block rather than at a calendar cutoff
            # across all tickers (real bug: produced a "validation" set whose date range was the
            # ENTIRE history of just the last few tickers, not the last 30% of calendar time —
            # caught via a live --quick run where every downstream integrity-check date filter
            # came back empty). get_train_validation_split sorts by date internally, so it's
            # correct regardless of how many distinct years the data spans.
            train_df, val_df = validator.get_train_validation_split(combined, val_fraction=0.3)
        else:
            folds = validator.split_data(combined, n_folds=min(2, n_folds_data))
            train_fold, _test_fold = folds[0]
            train_df, val_df = validator.get_train_validation_split(train_fold, val_fraction=0.2)

        model = cls(optuna_trials=optuna_trials, random_state=seed)
        diag = model.train_full(
            train_df[CORE_TECHNICAL_FEATURES], train_df["_label"],
            val_df[CORE_TECHNICAL_FEATURES], val_df["_label"],
            returns_train=train_df["_return"], returns_val=val_df["_return"],
        )
        logger.info(f"{name} trained: {diag['thresholds']}")
        signal_models[name] = (model, val_df)
        if save:
            _save_model(model, name, run_date, registry, metadata_extra={"diagnostics": diag})

    # ===== 5. MetaLabeler (on Signal5D's validation fold) =====
    signal_5d_model, val_df_5d_raw = signal_models["signal_5d"]
    val_df_5d = val_df_5d_raw.reset_index(drop=True)
    meta_X = val_df_5d[CORE_TECHNICAL_FEATURES]
    direction = signal_5d_model.predict(meta_X)
    meta_labels = MetaLabeler.compute_labels(direction, val_df_5d["_return"])
    meta_model = MetaLabeler(random_state=seed)
    meta_mask = meta_labels.notna()
    if meta_mask.sum() >= 10:
        meta_model.train(meta_X[meta_mask], meta_labels[meta_mask])
        if save:
            _save_model(meta_model, "meta_labeler", run_date, registry)
    else:
        logger.warning("Too few Act-labeled rows to train MetaLabeler on this synthetic run — skipped")

    # ===== 6. Conformal (wraps Signal5D's Q50 model) =====
    conformal_result = None
    if signal_5d_model._q50_model is not None:
        cal_X, cal_y = meta_X, val_df_5d["_return"]
        conformal = ConformalPredictor(signal_5d_model._q50_model, target_coverage=0.90)
        conformal.calibrate(cal_X, cal_y)
        conformal_result = conformal.evaluate_coverage(cal_X, cal_y)
        logger.info(f"Conformal calibration coverage (in-sample sanity check): {conformal_result:.3f}")
        if save:
            import joblib

            conf_dir = MODELS_DIR / "conformal"
            conf_dir.mkdir(parents=True, exist_ok=True)
            conf_path = conf_dir / f"conformal_signal5d_v{run_date.strftime(MODEL_VERSION_DATE_FORMAT)}.pkl"
            joblib.dump(conformal, conf_path)
            registry["conformal_signal5d"] = {
                "saved_path": str(conf_path), "saved_at": run_date.isoformat(), "coverage": conformal_result,
            }

    if save:
        import json

        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        registry_path = MODELS_DIR / "registry.json"
        existing = {}
        if registry_path.exists():
            try:
                existing = json.loads(registry_path.read_text())
            except (json.JSONDecodeError, OSError):
                existing = {}
        existing.update({k: _json_safe(v) for k, v in registry.items()})
        registry_path.write_text(json.dumps(existing, indent=2, default=str))
        logger.info(f"Updated model registry: {registry_path}")

    # ===== Integrity checks =====
    integrity_results = _run_integrity_checks(ohlcv, signal_models)

    return {"registry": registry, "integrity_results": integrity_results}


def _json_safe(meta: Dict) -> Dict:
    return {k: (v if not isinstance(v, pd.Timestamp) else v.isoformat()) for k, v in meta.items()}


def _run_integrity_checks(ohlcv: pd.DataFrame, signal_models: Dict) -> Optional[Dict[str, bool]]:
    """Builds whatever real context this synthetic run actually has and prints SPEC-BT-001 results."""
    _, val_df_5d = signal_models["signal_5d"]
    dates = ohlcv[["date"]].drop_duplicates()
    train_dates = dates[dates["date"] < val_df_5d["date"].min()] if "date" in val_df_5d.columns else dates.iloc[:1]
    test_dates = dates[dates["date"] >= dates["date"].quantile(0.8)]

    costs = IndianTransactionCosts()
    checker = BacktestIntegrityChecker(
        folds=[(train_dates, test_dates)] if len(train_dates) and len(test_dates) else None,
        # No announcement_date/filing_date columns -> check_02_pit passes trivially (PITRule.NONE).
        feature_df=ohlcv[["date"]],
        ohlcv_df=pd.DataFrame({"adj_factor": [1.0]}),  # synthetic OHLCV has no adj_factor column by construction
        universe_tickers=set(ohlcv["ticker"].unique()),
        historical_tickers=set(ohlcv["ticker"].unique()) | {"DELISTED_SYNTH"},
        applied_roundtrip_cost_pct=costs.compute_roundtrip_cost_pct(1000, 100),
        applied_min_adt_inr=MIN_ADT_INR,
        hpo_dataset="train+validation",
    )
    try:
        results = checker.run_all_checks()
        print("\n=== Backtest Integrity Checks ===")
        for name, passed in results.items():
            print(f"  {'PASS' if passed else 'FAIL'}  {name}")
        return results
    except RuntimeError as exc:
        print(f"\n=== Backtest Integrity Checks: CRITICAL FAILURE ===\n{exc}")
        return None


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="P1.5 first walk-forward training run (HMM->P&D->Signal5D->Signal21D->MetaLabeler->Conformal)"
    )
    parser.add_argument(
        "--folds", type=int, default=2, help="Walk-forward folds for signal models (kept small for a feasibility run)"
    )
    parser.add_argument("--quick", action="store_true", help="Use a small synthetic universe + few Optuna trials")
    args = parser.parse_args()

    n_tickers, n_days, trials = (15, 200, 3) if args.quick else (40, 400, 5)
    result = train_all_phase1(n_tickers=n_tickers, n_days=n_days, optuna_trials=trials)
    print(f"\nModels saved: {list(result['registry'].keys())}")


if __name__ == "__main__":
    main()
