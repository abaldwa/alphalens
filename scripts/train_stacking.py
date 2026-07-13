"""
scripts/train_stacking.py

Phase: 3.2 (M-13 Stacking Ensemble — OOF training)
Specs: SPEC-MODEL-003, SPEC-MODEL-005, SPEC-MODEL-013, SPEC-SOLID-003
Owner: ml_signal_engine / deep
Consumers: operator CLI (`python -m scripts.train_stacking`)

Builds genuine out-of-fold (OOF) predictions from all 5 M-13 base models
(signal_5d, signal_21d, signal_63d, tft, bilstm) and fits
StackingMetaLearner.fit_meta() on them — the training path BuildLog.md's
"M-13 Stacking OOF Infrastructure" scope entry (2026-07-01) describes as
not existing anywhere in the codebase before this script.

Two feature-space regimes are bridged here:
  - The 3 signal models run through BacktestEngine.run_full_backtest(
    collect_oof=True), which now accumulates real per-row (date, ticker,
    y_true, proba_sell/hold/buy) rows across its walk-forward test folds
    (see backtest/engine.py).
  - TFT (M-11) and BiLSTM (M-12) predict from the 297-feature daily
    parquets (datastore/features/daily/*.parquet) using a 63-day (SEQ_LEN)
    rolling lookback window built directly from real parquet rows — no
    synthetic sequence generation, ever (CLAUDE.md Absolute Rule 6).

Common target: the meta-learner is trained against signal_21d's OOF label
(triple-barrier direction on the 21-day horizon) since that is the same
horizon TFT/BiLSTM were trained on (train_deep_models.py --horizon 21).
signal_5d/signal_63d contribute their own OOF probabilities as additional
base-model opinions on that same (date, ticker) row, not their own labels.

OOF window: restricted to --from-date (default 2024-01-01) onward, since
TFT/BiLSTM training data only covers the recent ~600 daily parquet files
(~2024-2026) — see tft_model.py's _FULL_RECENT_FILES. Rows where a
(date, ticker) pair isn't present in every one of the 5 models' OOF output
are dropped (inner join) rather than filled — the meta-learner only
trains where every base model has real OOF coverage.

Raises
------
FileNotFoundError
    If datastore/features/daily/ has no parquet files, or no TFT/BiLSTM
    checkpoint files are found in --model-dir.
ValueError
    If the inner-joined OOF set is empty (no overlapping (date, ticker)
    rows across all 5 models in the requested window).

A40 (2026-07-10) — root cause of the 2026-07-02 09:10:58 silent death
------------------------------------------------------------------------
The one real run of this script (logs/train_stacking.log) got through
signal_5d/21d/63d's BacktestEngine OOF collection (~70 min total) and
began scoring TFT, loading all 3 v20260701 fold checkpoints — then the
log simply stops, no traceback, no exit message. `journalctl -k` on this
same host shows `systemd-oomd` killing other AlphaLens processes on sight
of memory pressure (>50% cgroup usage for >20s, e.g. the scheduler
service on 2026-07-10) with a hard SIGKILL and zero cooperation from the
killed process — which explains the log's silence exactly: a SIGKILL
gives the Python process no chance to log a traceback or run an atexit
handler. The 2026-07-02 dmesg/journal window itself has since rotated
out of /var/log, so this is strong circumstantial evidence (same host,
same failure signature, systemd-oomd demonstrably active and this
aggressive), not a smoking-gun log line — but it matches every known
fact and is the same class of failure as the two dated, confirmed
2026-07-07/07-09 OOM incidents in retrain_phase2.py's module docstring.

Decision (2026-07-10): NOT wired into the daily/overnight pipeline this
session. Scoring 5 base models (3 already-heavy BacktestEngine OOF
passes + 2 deep-model forward passes over 297-feature sequences) in one
unbounded process on this ~15GB box is the same "everything in one
process" shape that OOM-killed retrain_phase2.py twice — StackingEnsemble
needs the same per-model subprocess isolation (see ML21's fix in
retrain_phase2.py/pipeline_scheduler.py) and a bounded --max-tickers
default before it's safe to run unattended, and even then the underlying
ensemble is only as trustworthy as its weakest input (A42: TFT/BiLSTM's
real per-category feature usage is still unverified). `main()` below now
writes an explicit `<output-dir>/train_stacking.status.json` marker
before/after the run specifically so a future silent-death postmortem
can distinguish "never started," "OOM-killed mid-run" (STARTED, no
COMPLETED/FAILED), and "completed"/"failed with a real traceback"
without needing to re-derive this from log timestamps.
"""

import argparse
import glob
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from backtest.engine import BacktestEngine
from backtest.run_phase1_backtest import (
    _fetch_historical_tickers,
    _fetch_real_benchmark,
    _fetch_real_universe,
    _real_sector_map,
)
from config.universe import get_tickers
from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector, load_pnd_training_data_from_db
from systems.ml_signal_engine.models.deep.stacking import DEFAULT_BASE_MODELS, StackingMetaLearner
from systems.ml_signal_engine.models.deep.tft_model import SEQ_LEN, TFTSignalModel
from systems.ml_signal_engine.models.deep.bilstm_model import BiLSTMSignalModel
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel
from systems.ml_signal_engine.models.signal.signal_21d import Signal21DModel
from systems.ml_signal_engine.models.signal.signal_63d import Signal63DModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FEATURES_DAILY_DIR = Path("datastore/features/daily")
MODELS_DIR = Path("datastore/models")

# horizon_days / profit_multiplier / stop_multiplier per signal model — mirrors
# run_phase3_backtest.py's _SIGNAL21D_* constants and signal_5d.py's own docstring
# recommendation (there is no single canonical value shared across the 3 models).
_SIGNAL_SPECS: Dict[str, Tuple[type, int, float, float]] = {
    "signal_5d": (Signal5DModel, 5, 1.5, 1.5),
    "signal_21d": (Signal21DModel, 21, 3.0, 3.0),
    "signal_63d": (Signal63DModel, 63, 5.0, 5.0),
}
_TARGET_MODEL = "signal_21d"  # OOF label + TFT/BiLSTM horizon both use this


class _NoOpExitModel:
    """
    Stand-in for ExitSignalModel used only for OOF collection.

    BacktestEngine.run_full_backtest() unconditionally simulates a
    portfolio (for FoldResult's cagr/sharpe/etc) even when collect_oof=True
    is only after the entry-side signal_model.predict_proba() rows. Real
    ExitSignalModel.train_full() requires >= 200 real closed paper-trading
    positions (load_exit_training_data_from_db(), exit_signal.py:408) —
    there are currently 0 (paper trading hasn't been running long enough;
    see BuildLog.md "Real data sourcing — Exit Signal"). Since stacking's
    OOF collection only reads oof_df (never the simulated portfolio's
    cagr/sharpe), a real exit model isn't needed for correctness here —
    this always reports exit_urgency=0 ("never force an exit"), which
    keeps _apply_exits() well-defined without fabricating a trained model
    or requiring data that doesn't exist yet. Never used outside this
    script; run_phase1/2/3_backtest.py still require and train a real
    ExitSignalModel.
    """

    def predict_full(self, X: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            {"exit_urgency": 0.0, "exit_type": "hold", "exit_survival_5d": 1.0,
             "exit_survival_21d": 1.0, "exit_survival_63d": 1.0},
            index=X.index,
        )


def _build_pnd_and_noop_exit(seed: int) -> Tuple[PnDDetector, _NoOpExitModel]:
    """Real PnDDetector (as run_phase3_backtest.py's _build_pnd_and_exit does) + _NoOpExitModel."""
    pnd_X, pnd_y = load_pnd_training_data_from_db()
    pnd = PnDDetector(random_state=seed)
    pnd.train(pnd_X, pnd_y)
    return pnd, _NoOpExitModel()


def _run_signal_oof(
    model_key: str, ohlcv: pd.DataFrame, sector_map: Dict[str, str], benchmark: pd.DataFrame,
    universe_tickers: set, historical_tickers: set, pnd_detector, exit_model,
    from_date: str, n_folds: int, optuna_trials: int, seed: int,
) -> pd.DataFrame:
    """Run one signal model's walk-forward backtest with collect_oof=True. Returns oof_df (may be empty)."""
    signal_cls, horizon_days, profit_mult, stop_mult = _SIGNAL_SPECS[model_key]
    engine = BacktestEngine(
        ohlcv=ohlcv, pnd_detector=pnd_detector, exit_model=exit_model,
        signal_model_cls=signal_cls, sector_map=sector_map,
        horizon_days=horizon_days, profit_multiplier=profit_mult, stop_multiplier=stop_mult,
        optuna_trials=optuna_trials, random_state=seed, n_folds=n_folds,
        benchmark=benchmark, universe_tickers=universe_tickers, historical_tickers=historical_tickers,
    )
    results = engine.run_full_backtest(model_key, from_date=from_date, folds=n_folds, collect_oof=True)
    if results.oof_df is None or results.oof_df.empty:
        logger.warning("%s: no OOF rows produced in the requested window", model_key)
        return pd.DataFrame(columns=["date", "ticker", "y_true", "proba_sell", "proba_hold", "proba_buy"])
    return results.oof_df


def _latest_checkpoint_version(model_prefix: str, horizon_days: int, model_dir: Path) -> str:
    """
    Find the most recent {model_prefix}_signal_{horizon}d_v{YYYYMMDD}_fold*.pt version
    in model_dir (e.g. 'tft', horizon=21 -> 'v20260701' if that's the latest present).

    Raises
    ------
    FileNotFoundError
        If no matching checkpoint exists.
    """
    pattern = str(model_dir / f"{model_prefix}_signal_{horizon_days}d_v*_fold*.pt")
    matches = glob.glob(pattern)
    if not matches:
        raise FileNotFoundError(
            f"No {model_prefix} checkpoints matching {pattern}. Train them first via "
            "`python -m systems.ml_signal_engine.inference.train_deep_models --model "
            f"{model_prefix} --folds 3`."
        )
    versions = sorted({re.search(r"_v(\d{8})_fold", m).group(1) for m in matches}, reverse=True)
    return versions[0]


def _fold_checkpoints(model_prefix: str, horizon_days: int, version: str, model_dir: Path) -> List[Path]:
    pattern = str(model_dir / f"{model_prefix}_signal_{horizon_days}d_v{version}_fold*.pt")
    paths = sorted(
        (Path(p) for p in glob.glob(pattern)),
        key=lambda p: int(re.search(r"_fold(\d+)\.pt$", p.name).group(1)),
    )
    return [Path(str(p)[:-3]) for p in paths]  # strip ".pt" — model.load() appends it back


def _feature_cols_from_schema(sample_parquet: Path) -> List[str]:
    """Same schema-derivation rule as tft_model.schedule_overnight_training — reproducible
    because every daily parquet is written by the same features/matrix_builder.py schema."""
    sample = pd.read_parquet(sample_parquet)
    duckdb_internal = {"__fragment_index", "__batch_index", "__last_in_fragment", "__filename"}
    id_cols = {"date", "ticker"}
    return [c for c in sample.columns if c not in id_cols and c not in duckdb_internal]


def _build_deep_oof(
    model_prefix: str, model_cls: type, target_keys: pd.DataFrame, model_dir: Path,
) -> pd.DataFrame:
    """
    Score TFT or BiLSTM on each (date, ticker) row in target_keys, using a real
    SEQ_LEN=63-day rolling lookback window sliced directly from
    datastore/features/daily/*.parquet — the pre-existing per-fold checkpoint
    whose validation window covers (or most closely precedes) each date is used.

    Parameters
    ----------
    model_prefix : 'tft' or 'bilstm'
    target_keys : DataFrame with columns 'date', 'ticker' — the rows to score.

    Returns
    -------
    pd.DataFrame
        Columns 'date', 'ticker', 'proba_sell', 'proba_hold', 'proba_buy' — one row
        per target_keys row that had a full SEQ_LEN lookback window available.
        Rows with insufficient history are silently dropped (not fabricated).
    """
    if not FEATURES_DAILY_DIR.exists():
        raise FileNotFoundError(
            f"{FEATURES_DAILY_DIR} does not exist. Run features/matrix_builder.py first."
        )
    all_files = sorted(FEATURES_DAILY_DIR.glob("*.parquet"))
    if not all_files:
        raise FileNotFoundError(f"No parquet files in {FEATURES_DAILY_DIR}.")

    horizon_days = 21  # TFT/BiLSTM were trained with --horizon 21 (train_deep_models.py default)
    feature_cols = _feature_cols_from_schema(all_files[-1])
    version = _latest_checkpoint_version(model_prefix, horizon_days, model_dir)
    checkpoints = _fold_checkpoints(model_prefix, horizon_days, version, model_dir)
    if not checkpoints:
        raise FileNotFoundError(f"No {model_prefix} v{version} fold checkpoints under {model_dir}")

    # File-date index for every daily parquet — used both to slice per-ticker
    # windows and to decide which fold checkpoint "would have been trained"
    # before a given target date (its validation-fold end date <= target date).
    file_dates = pd.to_datetime([f.stem for f in all_files])
    fold_boundaries = _reconstruct_fold_boundaries(len(all_files), len(checkpoints))

    models: List[object] = []
    for ckpt in checkpoints:
        model = model_cls()
        model.load(str(ckpt))
        models.append(model)

    target_keys = target_keys.copy()
    target_keys["date"] = pd.to_datetime(target_keys["date"])
    min_date, max_date = target_keys["date"].min(), target_keys["date"].max()

    # Only load the parquet files that could possibly be needed: SEQ_LEN days
    # before the earliest target date through the latest target date.
    lo_idx = max(0, int(np.searchsorted(file_dates, min_date)) - SEQ_LEN)
    hi_idx = int(np.searchsorted(file_dates, max_date, side="right"))
    needed_files = all_files[lo_idx:hi_idx]
    needed_dates = file_dates[lo_idx:hi_idx]

    from systems.ml_signal_engine.models.deep.tft_model import _load_parquets_float32

    panel = _load_parquets_float32(needed_files, feature_cols)
    panel_by_ticker = {t: g.reset_index(drop=True) for t, g in panel.groupby("ticker", sort=False)}

    out_rows = []
    X_by_fold: Dict[int, List[np.ndarray]] = {i: [] for i in range(len(models))}
    key_by_fold: Dict[int, List[Tuple]] = {i: [] for i in range(len(models))}

    for date, ticker in zip(target_keys["date"], target_keys["ticker"]):
        grp = panel_by_ticker.get(ticker)
        if grp is None:
            continue
        idx_arr = grp.index[grp["date"] == date]
        if len(idx_arr) == 0:
            continue
        i = int(idx_arr[0])
        if i < SEQ_LEN:
            continue
        window = grp.loc[i - SEQ_LEN: i - 1, feature_cols].to_numpy(dtype=np.float32)
        window = np.nan_to_num(window, nan=0.0, posinf=0.0, neginf=0.0)
        fold_idx = _select_fold(date, needed_dates, fold_boundaries)
        X_by_fold[fold_idx].append(window)
        key_by_fold[fold_idx].append((date, ticker))

    for fold_idx, model in enumerate(models):
        if not X_by_fold[fold_idx]:
            continue
        X = np.stack(X_by_fold[fold_idx])
        proba = model.predict_proba(X)
        for (date, ticker), row in zip(key_by_fold[fold_idx], proba):
            out_rows.append(
                {"date": date, "ticker": ticker, "proba_sell": row[0], "proba_hold": row[1], "proba_buy": row[2]}
            )

    return pd.DataFrame(out_rows, columns=["date", "ticker", "proba_sell", "proba_hold", "proba_buy"])


def _reconstruct_fold_boundaries(n_files: int, n_folds: int) -> List[int]:
    """Mirrors schedule_overnight_training's train_end index per fold (approximate — the
    exact all_files slice used at training time isn't persisted, but the deterministic
    fold_size formula is reproducible from n_files/n_folds alone)."""
    from systems.ml_signal_engine.models.deep.tft_model import _MIN_FOLD_FILES
    fold_size = max(_MIN_FOLD_FILES, n_files // (n_folds + 1))
    return [min((fold + 1) * fold_size, n_files) for fold in range(n_folds)]


def _select_fold(date, needed_dates: pd.DatetimeIndex, fold_boundaries: List[int]) -> int:
    """Pick the latest fold whose reconstructed train_end date is <= date; else fold 0."""
    pos = int(np.searchsorted(needed_dates, date, side="right"))
    for fold_idx in range(len(fold_boundaries) - 1, -1, -1):
        if fold_boundaries[fold_idx] <= pos:
            return fold_idx
    return 0


def train_stacking(
    from_date: str = "2024-01-01", n_folds: int = 3, optuna_trials: int = 5, seed: int = 42,
    max_real_tickers: Optional[int] = None, min_history_days: int = 252,
    model_dir: str = "datastore/models",
) -> StackingMetaLearner:
    """
    Build real 5-model OOF predictions and fit StackingMetaLearner on them.

    Returns
    -------
    StackingMetaLearner
        Fitted meta-learner, saved to {model_dir}/stacking_meta_v{version}.

    Raises
    ------
    ValueError
        If the inner-joined OOF set across all 5 models is empty.
    """
    model_dir_path = Path(model_dir)
    logger.info("Fetching real universe/benchmark/sector data...")
    ohlcv = _fetch_real_universe(max_real_tickers, min_history_days)
    sector_map = _real_sector_map()
    benchmark = _fetch_real_benchmark()
    universe_tickers = set(get_tickers())
    historical_tickers = _fetch_historical_tickers()
    pnd_detector, exit_model = _build_pnd_and_noop_exit(seed)

    signal_oof: Dict[str, pd.DataFrame] = {}
    for model_key in ("signal_5d", "signal_21d", "signal_63d"):
        logger.info("Running BacktestEngine OOF collection for %s...", model_key)
        signal_oof[model_key] = _run_signal_oof(
            model_key, ohlcv, sector_map, benchmark, universe_tickers, historical_tickers,
            pnd_detector, exit_model, from_date, n_folds, optuna_trials, seed,
        )

    target_df = signal_oof[_TARGET_MODEL][["date", "ticker", "y_true"]].drop_duplicates(["date", "ticker"])
    if target_df.empty:
        raise ValueError(f"{_TARGET_MODEL} produced no OOF rows in the requested window — cannot train stacking.")

    merged = target_df
    for model_key in ("signal_5d", "signal_63d"):
        cols = signal_oof[model_key][["date", "ticker", "proba_sell", "proba_hold", "proba_buy"]]
        cols = cols.rename(columns={c: f"{model_key}_{c}" for c in ("proba_sell", "proba_hold", "proba_buy")})
        merged = merged.merge(cols, on=["date", "ticker"], how="inner")
    target_cols = signal_oof[_TARGET_MODEL][["date", "ticker", "proba_sell", "proba_hold", "proba_buy"]]
    target_cols = target_cols.rename(columns={c: f"{_TARGET_MODEL}_{c}" for c in ("proba_sell", "proba_hold", "proba_buy")})
    merged = merged.merge(target_cols, on=["date", "ticker"], how="inner")

    logger.info("Scoring TFT (M-11) on the aligned OOF (date, ticker) rows...")
    tft_oof = _build_deep_oof("tft", TFTSignalModel, merged[["date", "ticker"]], model_dir_path)
    logger.info("Scoring BiLSTM (M-12) on the aligned OOF (date, ticker) rows...")
    bilstm_oof = _build_deep_oof("bilstm", BiLSTMSignalModel, merged[["date", "ticker"]], model_dir_path)

    merged = merged.merge(
        tft_oof.rename(columns={"proba_sell": "tft_proba_sell", "proba_hold": "tft_proba_hold", "proba_buy": "tft_proba_buy"}),
        on=["date", "ticker"], how="inner",
    )
    merged = merged.merge(
        bilstm_oof.rename(columns={"proba_sell": "bilstm_proba_sell", "proba_hold": "bilstm_proba_hold", "proba_buy": "bilstm_proba_buy"}),
        on=["date", "ticker"], how="inner",
    )

    if merged.empty:
        raise ValueError(
            "No (date, ticker) rows have OOF coverage from all 5 base models in the requested "
            "window — cannot train stacking. Widen --from-date or re-run TFT/BiLSTM training."
        )

    logger.info("Aligned OOF rows across all 5 base models: %d", len(merged))

    oof_predictions = {
        "signal_5d": merged[["signal_5d_proba_sell", "signal_5d_proba_hold", "signal_5d_proba_buy"]].to_numpy(),
        "signal_21d": merged[[f"{_TARGET_MODEL}_proba_sell", f"{_TARGET_MODEL}_proba_hold", f"{_TARGET_MODEL}_proba_buy"]].to_numpy(),
        "signal_63d": merged[["signal_63d_proba_sell", "signal_63d_proba_hold", "signal_63d_proba_buy"]].to_numpy(),
        "tft": merged[["tft_proba_sell", "tft_proba_hold", "tft_proba_buy"]].to_numpy(),
        "bilstm": merged[["bilstm_proba_sell", "bilstm_proba_hold", "bilstm_proba_buy"]].to_numpy(),
    }
    y_oof = merged["y_true"].to_numpy()

    assert list(oof_predictions.keys()) == DEFAULT_BASE_MODELS

    meta = StackingMetaLearner()
    meta.fit_meta(oof_predictions, y_oof)

    out_path = model_dir_path / f"stacking_meta_v{meta._version}"
    meta.save(str(out_path))
    logger.info("StackingMetaLearner saved to %s. training_samples=%d weights=%s",
                out_path, meta._training_samples, meta.weights)
    return meta


def _write_status_marker(output_dir: str, status: str, detail: str = "") -> None:
    """A40 (2026-07-10): STARTED/COMPLETED/FAILED marker so a future silent
    death (SIGKILL from systemd-oomd — see module docstring) leaves behind
    proof of how far the run got, instead of an ambiguous stopped log."""
    import json as _json

    from config.timezone import now_ist

    path = Path(output_dir) / "train_stacking.status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json.dumps({"status": status, "detail": detail, "at": now_ist().isoformat()}, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Train M-13 StackingMetaLearner on real 5-model OOF predictions")
    parser.add_argument("--from-date", default="2024-01-01")
    parser.add_argument("--n-folds", type=int, default=3)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--max-tickers", type=int, default=800,
        help="Cap universe size (default 800, matching retrain_phase2.py's DEFAULT_MAX_TICKERS — see A40/ML21 "
             "OOM history). Pass --full-universe to override.",
    )
    parser.add_argument("--full-universe", action="store_true", help="Override --max-tickers, use the full universe.")
    parser.add_argument("--min-history", type=int, default=252)
    parser.add_argument("--output-dir", default="datastore/models")
    args = parser.parse_args()

    max_tickers = None if args.full_universe else args.max_tickers
    _write_status_marker(args.output_dir, "STARTED", f"max_tickers={max_tickers}")
    try:
        train_stacking(
            from_date=args.from_date, n_folds=args.n_folds, optuna_trials=args.trials, seed=args.seed,
            max_real_tickers=max_tickers, min_history_days=args.min_history, model_dir=args.output_dir,
        )
    except Exception as exc:
        _write_status_marker(args.output_dir, "FAILED", str(exc))
        raise
    _write_status_marker(args.output_dir, "COMPLETED")


if __name__ == "__main__":
    main()
