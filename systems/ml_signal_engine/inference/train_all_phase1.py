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

Trains exclusively on REAL OHLCV pulled from ohlcv_adjusted in the DuckDB
database, covering all 2492 active NSE stocks (or as many as are available).
Real benchmark series (NIFTYBEES, NIF100BEES, MONIFTY500) are loaded from
the same table. There is no synthetic-data mode: if the database is
missing or insufficient, this raises a clear error instead of falling
back to fabricated OHLCV — see BuildLog.md "Real data sourcing" entries.

Uses Signal5D/Signal21D's 70 technical features (features.technical)
as the training feature set, not the full 102-column
features.matrix_builder.ALL_FEATURE_COLUMNS.
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
from config.settings import DUCKDB_PATH, MIN_ADT_INR, MODELS_DIR
from config.timezone import now_ist
from features.technical import BENCHMARK_TICKERS, CORE_TECHNICAL_FEATURES, compute_technical_features
from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector, load_pnd_training_data_from_db
from systems.ml_signal_engine.models.signal.meta_labeler import MetaLabeler
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel
from systems.ml_signal_engine.models.signal.signal_21d import Signal21DModel
from systems.ml_signal_engine.models.uncertainty.conformal import ConformalPredictor
from systems.ml_signal_engine.training.labeling import TripleBarrierLabeler
from systems.ml_signal_engine.training.walk_forward import WalkForwardValidator

logger = logging.getLogger(__name__)

MODEL_VERSION_DATE_FORMAT = "%Y%m%d"

# Minimum trading days of history needed per ticker before we include it in training.
# Tickers with fewer rows than this are dropped to avoid warming-up NaN contamination.
_MIN_HISTORY_DAYS = 252


def load_ohlcv_from_db(
    db_path: Optional[Path] = None,
    lookback_days: int = 1260,
    max_tickers: Optional[int] = None,
) -> pd.DataFrame:
    """
    Load real OHLCV from ohlcv_adjusted for all active tickers.

    Parameters
    ----------
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.
    lookback_days : int
        Number of calendar days of history to pull (counting back from the
        most recent date in the table). Default 1260 (~5 years).
    max_tickers : int, optional
        If set, limit to the `max_tickers` most liquid tickers (by row count
        in the window). Useful for quick smoke-runs.

    Returns
    -------
    pd.DataFrame
        Long-format: date, ticker, open, high, low, close, volume, delivery_pct.
        delivery_pct is 0.0 where not available (not all tickers have it).
    """
    from datastore.api.db import get_duckdb_connection

    db_path = db_path or DUCKDB_PATH
    with get_duckdb_connection(db_path) as conn:
        cutoff = conn.execute(
            "SELECT MAX(date) - INTERVAL (?) DAY FROM ohlcv_adjusted", [lookback_days]
        ).fetchone()[0]

        # Exclude benchmark ETF tickers — they are market-proxy series, not tradeable stocks
        benchmark_syms = list(BENCHMARK_TICKERS.values())
        placeholders = ", ".join(f"'{s}'" for s in benchmark_syms)

        df = conn.execute(
            f"""
            SELECT date, ticker, open, high, low, close, volume,
                   COALESCE(delivery_pct, 0.0) AS delivery_pct
            FROM ohlcv_adjusted
            WHERE date >= ?
              AND ticker NOT IN ({placeholders})
            ORDER BY ticker, date
            """,
            [cutoff],
        ).df()

    df["date"] = pd.to_datetime(df["date"])

    # Drop tickers with insufficient history
    counts = df.groupby("ticker")["date"].count()
    eligible = counts[counts >= _MIN_HISTORY_DAYS].index
    df = df[df["ticker"].isin(eligible)].reset_index(drop=True)

    if max_tickers is not None and df["ticker"].nunique() > max_tickers:
        top = (
            df.groupby("ticker")["date"].count()
            .nlargest(max_tickers)
            .index
        )
        df = df[df["ticker"].isin(top)].reset_index(drop=True)

    logger.info(
        "Loaded %d rows for %d tickers from ohlcv_adjusted (lookback=%d days, cutoff=%s)",
        len(df), df["ticker"].nunique(), lookback_days, cutoff,
    )
    return df


def load_benchmark_from_db(
    dates: pd.DatetimeIndex,
    db_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Load real NIFTY 50/100/500 benchmark close prices from ohlcv_adjusted.

    Returns a DataFrame with columns: date, nifty50_close, nifty100_close,
    nifty500_close — aligned to `dates`. Missing dates are forward-filled
    then back-filled so every date in `dates` has a value.

    Raises
    ------
    RuntimeError
        If any benchmark ticker has no rows in the DB for the requested
        date range — there is no synthetic/flat-price fallback.
    """
    from datastore.api.db import get_duckdb_connection

    db_path = db_path or DUCKDB_PATH
    ticker_to_col = {v: f"{k}_close" for k, v in BENCHMARK_TICKERS.items()}
    sym_list = list(BENCHMARK_TICKERS.values())
    placeholders = ", ".join(f"'{s}'" for s in sym_list)

    date_min = dates.min()
    date_max = dates.max()

    with get_duckdb_connection(db_path) as conn:
        raw = conn.execute(
            f"""
            SELECT date, ticker, close
            FROM ohlcv_adjusted
            WHERE ticker IN ({placeholders})
              AND date BETWEEN ? AND ?
            ORDER BY ticker, date
            """,
            [date_min, date_max],
        ).df()

    raw["date"] = pd.to_datetime(raw["date"])
    frame = pd.DataFrame({"date": dates})

    for sym, col in ticker_to_col.items():
        sub = raw[raw["ticker"] == sym][["date", "close"]].rename(columns={"close": col})
        frame = frame.merge(sub, on="date", how="left")
        if col not in frame.columns or frame[col].isna().all():
            raise RuntimeError(
                f"Benchmark ticker {sym} has no rows in ohlcv_adjusted for "
                f"{date_min.date()}..{date_max.date()}. There is no flat-price/synthetic "
                "fallback — backfill this ticker via ingestion/backfill_runner.py. "
                "See BuildLog.md 'Real data sourcing — Benchmarks'."
            )
        frame[col] = frame[col].ffill().bfill()

    return frame


def _build_training_dataset(
    ohlcv: pd.DataFrame,
    horizon_days: int,
    profit_multiplier: float,
    stop_multiplier: float,
    benchmark: pd.DataFrame,
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

    Parameters
    ----------
    benchmark : pd.DataFrame
        Real benchmark DataFrame (date, nifty50_close, nifty100_close,
        nifty500_close) from load_benchmark_from_db().
    """
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
    optuna_trials: int = 5,
    save: bool = True,
    seed: int = 42,
    db_path: Optional[Path] = None,
    lookback_days: int = 1260,
) -> Dict:
    """
    Run the full P1.5 training sequence: HMM (market-wide) -> P&D ->
    Signal5D -> Signal21D -> MetaLabeler -> Conformal.

    Trains exclusively on real ohlcv_adjusted data. There is no synthetic
    mode — see BuildLog.md "Real data sourcing" if the database is not
    yet sufficiently populated.

    Parameters
    ----------
    optuna_trials : int
        Per-model Optuna trial count. Defaults to 5 for a quick run;
        pass 100 for a production-grade fit.
    save : bool
        If True (default), persist each model to datastore/models/ and
        write registry.json.
    seed : int
        Base RNG seed for all model fits.
    db_path : Path, optional
        DuckDB path. Defaults to config.settings.DUCKDB_PATH.
    lookback_days : int
        How many calendar days of OHLCV history to load from the DB.
        Default 1260 (~5 years).

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
    RuntimeError
        If ohlcv_adjusted has no data — there is no synthetic fallback.
    """
    run_date = now_ist()

    logger.info("P1.5 training: loading ohlcv_adjusted (lookback=%d days)", lookback_days)
    ohlcv = load_ohlcv_from_db(db_path=db_path, lookback_days=lookback_days)
    if ohlcv.empty:
        raise RuntimeError(
            "No OHLCV data found in the database. There is no synthetic-data fallback — "
            "run ingestion/backfill_runner.py first. See BuildLog.md 'Real data sourcing'."
        )
    dates = pd.DatetimeIndex(sorted(ohlcv["date"].unique()))
    benchmark = load_benchmark_from_db(dates=dates, db_path=db_path)
    logger.info("P1.5 training: loaded %d tickers, %d dates", ohlcv["ticker"].nunique(), len(dates))

    registry: Dict = {}

    # ===== 1. HMM (market-wide, Nifty-proxy) =====
    from systems.ml_signal_engine.models.hmm.regime_detector import HMMRegimeDetector, compute_hmm_observables

    # Use real Nifty 50 ETF as the market proxy for HMM
    nifty_proxy = benchmark[["date", "nifty50_close"]].rename(columns={"nifty50_close": "close"})
    nifty_proxy["open"] = nifty_proxy["close"]
    nifty_proxy["high"] = nifty_proxy["close"]
    nifty_proxy["low"] = nifty_proxy["close"]
    nifty_proxy["volume"] = 1e6
    nifty_proxy["ticker"] = "NIFTYBEES"
    market_proxy = nifty_proxy.sort_values("date")

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
    pnd_X, pnd_y = load_pnd_training_data_from_db(db_path=db_path)
    pnd_model = PnDDetector(random_state=seed)
    pnd_model.train(pnd_X, pnd_y)
    if save:
        _save_model(pnd_model, "pnd_detector", run_date, registry)

    # ===== 3 & 4. Signal5D, Signal21D =====
    validator = WalkForwardValidator(n_folds=2)
    signal_models = {}
    for horizon, cls, name in ((5, Signal5DModel, "signal_5d"), (21, Signal21DModel, "signal_21d")):
        combined = _build_training_dataset(
            ohlcv, horizon, profit_multiplier=2.0, stop_multiplier=1.0, benchmark=benchmark
        )

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
        logger.warning("Too few Act-labeled rows to train MetaLabeler — skipped")

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
    """Print SPEC-BT-001 integrity check results against the training data."""
    _, val_df_5d = signal_models["signal_5d"]
    dates = ohlcv[["date"]].drop_duplicates()
    train_dates = dates[dates["date"] < val_df_5d["date"].min()] if "date" in val_df_5d.columns else dates.iloc[:1]
    test_dates = dates[dates["date"] >= dates["date"].quantile(0.8)]

    costs = IndianTransactionCosts()
    checker = BacktestIntegrityChecker(
        folds=[(train_dates, test_dates)] if len(train_dates) and len(test_dates) else None,
        feature_df=ohlcv[["date"]],
        ohlcv_df=pd.DataFrame({"adj_factor": [1.0]}),
        universe_tickers=set(ohlcv["ticker"].unique()),
        historical_tickers=set(ohlcv["ticker"].unique()),
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
        description="P1.5 walk-forward training (HMM->P&D->Signal5D->Signal21D->MetaLabeler->Conformal)"
    )
    parser.add_argument(
        "--folds", type=int, default=2, help="Walk-forward folds for signal models"
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=1260,
        help="Calendar days of OHLCV history to load from DB. Default: 1260 (~5y).",
    )
    parser.add_argument(
        "--trials", type=int, default=5, help="Optuna trials per model. Default: 5 (use 100 for production fit)."
    )
    args = parser.parse_args()

    result = train_all_phase1(optuna_trials=args.trials, lookback_days=args.lookback_days)
    print(f"\nModels saved: {list(result['registry'].keys())}")


if __name__ == "__main__":
    main()
