"""
systems/ml_signal_engine/inference/retrain_phase2.py

Phase: 2.3 (F&O Features + Signal63D + Full Phase 2 Feature Matrix)
Specs: SPEC-MODEL-001 through SPEC-MODEL-004, SPEC-MODEL-008
Owner: ml_signal_engine / inference
Consumers: operator CLI (`python3 -m systems.ml_signal_engine.inference.retrain_phase2`)

Retrains Signal5D and Signal21D with the expanded Phase 2 feature set
(fundamental + governance + MF-holdings + corporate-action + F&O, on top
of the Phase 1 70 technical features) and trains Signal63D (M-03's 63d
half — out of scope until now; see signal_63d.py's module docstring:
"63d model only trains after Phase 2 fundamentals are flowing"). Compares
each horizon's Phase 1-only-feature Sharpe against its Phase 2-feature
Sharpe and reports whether Phase 2 improved or stayed neutral.

Both halves of the feature set are real: the technical half is computed
from real OHLCV loaded via train_all_phase1.load_ohlcv_from_db()
(ohlcv_adjusted), run through the REAL features.technical.
compute_technical_features() and REAL TripleBarrierLabeler. The Phase 2
feature half (fundamental/governance/MF-holdings/corp-action/F&O) is
computed via the REAL panel functions wired into features/matrix_builder.py,
called against the REAL running DataStore API for the same real tickers.
There is no synthetic-data mode — if ohlcv_adjusted doesn't have enough
history, this raises rather than falling back to a generated universe.

Sharpe here is a direct strategy-return Sharpe (predicted direction x
realized forward return on the validation fold), not a full
backtest.engine portfolio simulation (no transaction costs/position
sizing) — appropriate for a feature-set A/B comparison at the model-
training stage; backtest/run_phase1_backtest.py remains the heavier,
cost-aware portfolio-level backtest. Each row's realized return spans
`horizon_days` trading days (overlapping windows, not i.i.d. daily
returns), so the annualization factor is sqrt(252 / horizon_days) applied
to the per-row return series — a standard, documented approximation for
non-overlapping-equivalent annualization, not a literal daily Sharpe.
"""

import argparse
import gc
import logging
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from config.timezone import now_ist
from datastore.client import DataStoreClient
from features.corporate_action_features import CORPORATE_ACTION_FEATURES, compute_corporate_action_features_panel
from features.fno_features import FNO_FEATURES, compute_fno_features_panel
from features.fundamental import FUNDAMENTAL_FEATURES, compute_fundamental_features_panel
from features.governance import GOVERNANCE_FEATURES, compute_governance_features_panel
from features.mf_holdings import MF_HOLDINGS_FEATURES, compute_mf_holdings_features_panel
from features.technical import CORE_TECHNICAL_FEATURES
from systems.ml_signal_engine.inference.train_all_phase1 import (
    _build_training_dataset,
    _save_model,
    load_benchmark_from_db,
    load_ohlcv_from_db,
)
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel
from systems.ml_signal_engine.models.signal.signal_21d import Signal21DModel
from systems.ml_signal_engine.models.signal.signal_63d import Signal63DModel
from systems.ml_signal_engine.training.walk_forward import WalkForwardValidator

logger = logging.getLogger(__name__)

TRADING_DAYS_PER_YEAR = 252

# 2026-07-07 OOM incident: a single-shot, whole-universe (~2317 tickers x
# ~1260-day lookback x 297 float64 feature columns, ~6-7GB) call to
# compute_technical_features(), repeated once per horizon in HORIZON_CONFIGS
# with nothing freed in between, peaked at 9.4GB RSS and got OOM-killed on
# this ~15GB box. CORE_TECHNICAL_FEATURES/PHASE2_FEATURES are all per-ticker
# (no cross-sectional rank/z-score across the universe — those live in
# features/multibagger.py, not used here), so the universe can be processed
# in ticker batches and concatenated without changing any feature value.
DEFAULT_TICKER_CHUNK_SIZE = 150

# 2026-07-09 OOM incident: even with DEFAULT_TICKER_CHUNK_SIZE bounding peak
# memory during panel/feature *building*, the full ~2300-ticker universe's
# assembled training matrix (post-SMOTETomek oversampling, feeding 3
# Optuna-tuned stacking-ensemble models per horizon) still grew unbounded and
# got OOM-killed twice on this ~15GB box. Capping the universe itself (not
# just the build-time chunk size) is what actually bounded memory — a
# max_tickers=800 run completed cleanly end-to-end. Full-universe (None) is
# still selectable via --max-tickers, but is no longer the unattended
# scheduler default (see schedule_model_training in pipeline_scheduler.py,
# which invokes this module's main() with no args).
DEFAULT_MAX_TICKERS = 800


def _ticker_chunks(tickers: List[str], chunk_size: int) -> List[List[str]]:
    return [tickers[i : i + chunk_size] for i in range(0, len(tickers), chunk_size)]


def _downcast_floats(df: pd.DataFrame) -> pd.DataFrame:
    """float64 -> float32 for feature columns, roughly halving the frame's memory footprint."""
    float_cols = df.select_dtypes(include=["float64"]).columns
    if len(float_cols):
        df[float_cols] = df[float_cols].astype(np.float32)
    return df

PHASE2_NEW_FEATURES = (
    FUNDAMENTAL_FEATURES + GOVERNANCE_FEATURES + MF_HOLDINGS_FEATURES + CORPORATE_ACTION_FEATURES + FNO_FEATURES
)
PHASE2_FEATURES = CORE_TECHNICAL_FEATURES + PHASE2_NEW_FEATURES

# 02_models.md: "Barrier widths: 21d = 3x ATR, 63d = 5x ATR" — signal_5d.py's
# own doc kept 5d at the TripleBarrierLabeler default (2.0/1.0); same
# per-horizon multipliers used here as train_all_phase1.py used for 5d/21d,
# extended to 63d per signal_63d.py's documented "5x ATR" mapping.
HORIZON_CONFIGS = (
    (5, Signal5DModel, "signal_5d", 2.0, 1.0),
    (21, Signal21DModel, "signal_21d", 3.0, 3.0),
    (63, Signal63DModel, "signal_63d", 5.0, 5.0),
)

# ML21 (2026-07-10): signal_63d's label distribution is the most skewed of
# the 3 horizons (49.5% buy / 42.2% hold / 8.3% sell in the 2026-07-09
# incident run) so its SMOTETomek oversample grows the most before Optuna
# HPO even starts. Fewer trials for 63d specifically shaves the repeated-fit
# multiplier without touching 5d/21d's tuning quality.
OPTUNA_TRIALS_BY_HORIZON = {5: 5, 21: 5, 63: 3}


def _compute_phase2_panel(
    client: DataStoreClient, tickers: list, as_of: pd.Timestamp, chunk_size: int = DEFAULT_TICKER_CHUNK_SIZE
) -> pd.DataFrame:
    """One row per ticker, columns = ['ticker'] + PHASE2_NEW_FEATURES — see module docstring on why
    every value is honestly NaN for this script's synthetic tickers.

    Processed in ticker batches (see DEFAULT_TICKER_CHUNK_SIZE) — each panel-compute call and its
    intermediate merges only ever hold one batch's worth of rows, not the full ~2300-ticker universe.
    """
    panels = []
    for batch in _ticker_chunks(tickers, chunk_size):
        sector_map = {t: "UNKNOWN" for t in batch}
        tier_map = {t: 5 for t in batch}

        fundamental = compute_fundamental_features_panel(client, batch, as_of, sector_map)
        governance = compute_governance_features_panel(client, batch, as_of)
        mf_holdings = compute_mf_holdings_features_panel(batch, as_of, tier_map=tier_map)
        corp_action = compute_corporate_action_features_panel(client, batch, as_of)
        fno = compute_fno_features_panel(client, batch, as_of)

        panel = pd.DataFrame({"ticker": batch})
        for part in (fundamental, governance, mf_holdings, corp_action, fno):
            panel = panel.merge(part, on="ticker", how="left")
        panels.append(_downcast_floats(panel[["ticker"] + PHASE2_NEW_FEATURES]))
        del fundamental, governance, mf_holdings, corp_action, fno, panel

    result = pd.concat(panels, ignore_index=True)
    del panels
    gc.collect()
    return result


def _build_training_dataset_chunked(
    ohlcv: pd.DataFrame,
    tickers: list,
    horizon_days: int,
    profit_multiplier: float,
    stop_multiplier: float,
    benchmark: pd.DataFrame,
    chunk_size: int = DEFAULT_TICKER_CHUNK_SIZE,
) -> pd.DataFrame:
    """Ticker-batched version of _build_training_dataset — computes technical features/labels
    one batch of tickers at a time so compute_technical_features() never has to materialize a
    ~7GB whole-universe feature matrix in one call (2026-07-07 OOM incident). Feature values are
    identical to the unchunked path since CORE_TECHNICAL_FEATURES has no cross-sectional
    (cross-ticker) features — those live in features/multibagger.py, unused here."""
    parts = []
    for batch in _ticker_chunks(tickers, chunk_size):
        ohlcv_batch = ohlcv[ohlcv["ticker"].isin(batch)]
        part = _build_training_dataset(
            ohlcv_batch, horizon_days, profit_multiplier=profit_multiplier, stop_multiplier=stop_multiplier,
            benchmark=benchmark,
        )
        parts.append(_downcast_floats(part))
        del ohlcv_batch, part
        gc.collect()

    combined = pd.concat(parts, ignore_index=True)
    del parts
    gc.collect()
    return combined


def _strategy_sharpe(direction: pd.Series, realized_return: pd.Series, horizon_days: int) -> float:
    """
    Direct strategy-return Sharpe: position (from predicted direction in
    {-1, 0, 1}) x realized forward return, annualized by
    sqrt(252 / horizon_days) — see module docstring's documented
    overlapping-window approximation.
    """
    strategy_returns = (direction.to_numpy() * realized_return.to_numpy()).astype(np.float64)
    strategy_returns = strategy_returns[np.isfinite(strategy_returns)]
    if len(strategy_returns) < 2 or np.std(strategy_returns) == 0:
        return 0.0
    periods_per_year = TRADING_DAYS_PER_YEAR / horizon_days
    return float(np.mean(strategy_returns) / np.std(strategy_returns) * np.sqrt(periods_per_year))


def _train_and_evaluate(
    cls, feature_cols: list, train_df: pd.DataFrame, val_df: pd.DataFrame, horizon_days: int,
    optuna_trials: int, seed: int, max_sampling_ratio: float = None,
) -> Tuple[object, float, Dict]:
    """Train one signal model on `feature_cols`, return (model, validation Sharpe, train_full diagnostics)."""
    model = cls(optuna_trials=optuna_trials, random_state=seed, max_sampling_ratio=max_sampling_ratio)
    diag = model.train_full(
        train_df[feature_cols], train_df["_label"],
        val_df[feature_cols], val_df["_label"],
        returns_train=train_df["_return"], returns_val=val_df["_return"],
    )
    direction = model.predict(val_df[feature_cols])
    sharpe = _strategy_sharpe(direction, val_df["_return"], horizon_days)
    return model, sharpe, diag


def retrain_phase2(
    lookback_days: int = 1260,
    max_tickers: int = None,
    optuna_trials: int = None,
    save: bool = True,
    seed: int = 42,
    client: DataStoreClient = None,
    ticker_chunk_size: int = DEFAULT_TICKER_CHUNK_SIZE,
    only_horizon: int = None,
    max_sampling_ratio: float = None,
    use_training_universe: bool = True,
) -> Dict:
    """
    Retrain Signal5D/Signal21D and train Signal63D with the full Phase 2
    feature set, comparing each horizon's Phase 1-only vs Phase 2 Sharpe.

    Parameters
    ----------
    lookback_days : int
        Real OHLCV history window (ohlcv_adjusted), passed to
        train_all_phase1.load_ohlcv_from_db().
    max_tickers : int, optional
        Cap universe size for a quick run. Ignored when use_training_universe
        is True (default) — the curated list's own size (ADTV-floor-bound,
        currently 530 tickers) governs instead. Only takes effect when
        use_training_universe=False, as a fallback/debug path.
    use_training_universe : bool
        ML24 (2026-07-11): if True (default), train on
        config.training_universe.load_current_training_universe() — the
        versioned, ADTV-ranked (>= Rs 40cr/day) ticker list — instead of
        max_tickers' arbitrary row-count-based selection. This replaced the
        original max_tickers=800 approach after it was found to select
        tickers by data completeness (most rows in the lookback window),
        not anything economically meaningful; see FeatureBacklog.md ML24.
    optuna_trials : int, optional
        Per-model, per-feature-set Optuna trial count. If None (default),
        uses OPTUNA_TRIALS_BY_HORIZON (fewer trials for signal_63d, whose
        label distribution is the most skewed — see ML21). Pass an int to
        force the same trial count for every horizon (legacy behavior).
    only_horizon : int, optional
        Restrict this run to a single horizon (5/21/63). Used by main()'s
        --subprocess-per-horizon mode (ML21) to run each horizon as an
        isolated OS process so SMOTETomek's oversampled matrix for one
        horizon can never accumulate against another's in the same
        process's RSS.
    save : bool
        If True (default), persist each Phase 2 model to
        datastore/models/<name>/ + registry.json, under the SAME names
        Phase 1 used (signal_5d, signal_21d) plus the new signal_63d —
        this *is* the retrain, the registry now points at the
        Phase-2-feature-trained artifact.
    seed : int
    client : DataStoreClient, optional
        Injected for testability; defaults to a real DataStoreClient
        hitting config.settings.DATASTORE_API_BASE_URL.
    ticker_chunk_size : int
        Universe is processed in batches of this many tickers when building
        the phase2 panel and the per-horizon technical feature/label matrix,
        bounding peak memory instead of materializing all ~2300 tickers'
        feature columns at once (2026-07-07 OOM incident: this script alone
        peaked at 9.4GB RSS on a ~15GB box and got kernel-OOM-killed).
    max_sampling_ratio : float, optional
        ML21 (2026-07-10)/ML24 (2026-07-11) — caps SMOTETomek's minority
        oversampling: e.g. 0.5 resamples a rare class to at most half the
        majority class's count instead of unbounded 1:1 parity. None
        (default) preserves the existing unbounded 'auto' behavior. Added
        specifically for signal_63d, whose true pre-resample distribution
        (buy 49.5%, hold 42.2%, sell 8.3% per the 2026-07-09 retrain log)
        is inverted by unbounded SMOTETomek: the rare 'sell' class gets
        synthetically amplified ~4.2x while the natural 'buy' majority is
        undersampled, producing a worst-in-class buy-F1 of 0.28 and a
        buy_prob output that no longer tracks q50_return's sign
        (ML24 diagnostic, scripts/diagnose_signal_quantile_agreement.py).

    Returns
    -------
    dict
        {"registry": {...}, "comparison": {horizon_name: {"phase1_sharpe":
        ..., "phase2_sharpe": ..., "improved_or_neutral": bool}}}

    Spec References
    ----------------
    SPEC-MODEL-001 through SPEC-MODEL-004 (see base_signal_model.py).
    SPEC-MODEL-008: this script IS the retrain protocol's "train" step;
    the "when new quarterly fundamentals are announced" trigger is a
    scheduling concern outside this function (see signal_63d.py docstring).

    Raises
    ------
    RuntimeError
        If ohlcv_adjusted has no real OHLCV in the requested window —
        there is no synthetic-data fallback; run
        ingestion/backfill_runner.py first.
    """
    client = client or DataStoreClient()
    run_date = now_ist()
    logger.info(
        f"P2.3 retrain starting: real OHLCV (lookback={lookback_days} days), "
        f"Phase1={len(CORE_TECHNICAL_FEATURES)} features, Phase2={len(PHASE2_FEATURES)} features"
    )

    if use_training_universe:
        from config.training_universe import load_current_training_universe

        curated_tickers = load_current_training_universe()
        logger.info(f"Training on curated ADTV-ranked universe: {len(curated_tickers)} tickers")
        ohlcv = load_ohlcv_from_db(lookback_days=lookback_days, tickers=curated_tickers)
    else:
        ohlcv = load_ohlcv_from_db(lookback_days=lookback_days, max_tickers=max_tickers)
    if ohlcv.empty:
        raise RuntimeError(
            "No OHLCV data found in ohlcv_adjusted. There is no synthetic-data fallback — "
            "run ingestion/backfill_runner.py first. See BuildLog.md 'Real data sourcing'."
        )
    tickers = sorted(ohlcv["ticker"].unique())
    as_of = ohlcv["date"].max()
    phase2_panel = _compute_phase2_panel(client, tickers, as_of, chunk_size=ticker_chunk_size)
    benchmark = load_benchmark_from_db(dates=pd.DatetimeIndex(ohlcv["date"].unique()))

    validator = WalkForwardValidator(n_folds=2)
    registry: Dict = {}
    comparison: Dict[str, Dict] = {}

    horizon_configs = HORIZON_CONFIGS
    if only_horizon is not None:
        horizon_configs = tuple(h for h in HORIZON_CONFIGS if h[0] == only_horizon)
        if not horizon_configs:
            raise ValueError(f"only_horizon={only_horizon} is not one of {[h[0] for h in HORIZON_CONFIGS]}")

    for horizon_days, cls, name, profit_mult, stop_mult in horizon_configs:
        trials = optuna_trials if optuna_trials is not None else OPTUNA_TRIALS_BY_HORIZON[horizon_days]
        combined = _build_training_dataset_chunked(
            ohlcv, tickers, horizon_days, profit_mult, stop_mult, benchmark, chunk_size=ticker_chunk_size
        )
        combined = combined.merge(phase2_panel, on="ticker", how="left")
        combined = _downcast_floats(combined)

        n_folds_data = combined["date"].dt.year.nunique() - 1
        if n_folds_data < 1:
            train_df, val_df = validator.get_train_validation_split(combined, val_fraction=0.3)
        else:
            folds = validator.split_data(combined, n_folds=min(2, n_folds_data))
            train_fold, _test_fold = folds[0]
            train_df, val_df = validator.get_train_validation_split(train_fold, val_fraction=0.2)

        _, phase1_sharpe, _ = _train_and_evaluate(
            cls, CORE_TECHNICAL_FEATURES, train_df, val_df, horizon_days, trials, seed,
            max_sampling_ratio=max_sampling_ratio,
        )
        model, phase2_sharpe, diag = _train_and_evaluate(
            cls, PHASE2_FEATURES, train_df, val_df, horizon_days, trials, seed,
            max_sampling_ratio=max_sampling_ratio,
        )

        improved_or_neutral = phase2_sharpe >= phase1_sharpe - 1e-9
        comparison[name] = {
            "phase1_sharpe": phase1_sharpe, "phase2_sharpe": phase2_sharpe,
            "improved_or_neutral": improved_or_neutral,
        }
        log_fn = logger.info if improved_or_neutral else logger.warning
        log_fn(
            f"{name}: Phase1 Sharpe={phase1_sharpe:.3f}, Phase2 Sharpe={phase2_sharpe:.3f} "
            f"({'improved/neutral' if improved_or_neutral else 'REGRESSED'})"
        )

        if save:
            _save_model(
                model, name, run_date, registry,
                metadata_extra={"diagnostics": diag, "comparison": comparison[name]},
            )

        # Each horizon builds its own ~multi-GB combined/train/val frames (see
        # DEFAULT_TICKER_CHUNK_SIZE note above) — drop them before the next
        # horizon's iteration instead of letting 3 horizons' worth accumulate
        # as live references for the rest of the function.
        del combined, train_df, val_df, model, diag
        gc.collect()

    if save:
        import json

        from config.settings import MODELS_DIR

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

    return {"registry": registry, "comparison": comparison}


def _json_safe(meta: Dict) -> Dict:
    return {k: (v if not isinstance(v, pd.Timestamp) else v.isoformat()) for k, v in meta.items()}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="P2.3 retrain: Signal5D/21D with Phase 2 features + new Signal63D, Phase1-vs-Phase2 Sharpe"
    )
    parser.add_argument("--quick", action="store_true", help="Cap universe size + fewer Optuna trials")
    parser.add_argument(
        "--chunk-size", type=int, default=DEFAULT_TICKER_CHUNK_SIZE,
        help=f"Tickers processed per batch during panel/feature building (default {DEFAULT_TICKER_CHUNK_SIZE}); "
             "lower this on memory-constrained boxes (see 2026-07-07 OOM incident note on retrain_phase2()).",
    )
    parser.add_argument(
        "--max-tickers", type=int, default=DEFAULT_MAX_TICKERS,
        help=f"Cap universe size fed into training (default {DEFAULT_MAX_TICKERS}); pass --full-universe to "
             "train on all tickers instead (see 2026-07-09 OOM incident note above DEFAULT_MAX_TICKERS).",
    )
    parser.add_argument(
        "--full-universe", action="store_true",
        help="Override --max-tickers and train on the entire universe (higher OOM risk on memory-constrained boxes). "
             "Also disables --use-training-universe (mutually exclusive with the curated list).",
    )
    parser.add_argument(
        "--no-training-universe", action="store_true",
        help="ML24 (2026-07-11): opt OUT of the curated ADTV-ranked training universe (default ON) and fall back "
             "to --max-tickers' row-count-based selection instead. Use only for debugging/comparison runs.",
    )
    parser.add_argument(
        "--horizon", type=int, choices=[5, 21, 63], default=None,
        help="Internal: run only this horizon in-process (used by --subprocess-per-horizon's child invocations).",
    )
    parser.add_argument(
        "--max-sampling-ratio", type=float, default=None,
        help="ML21/ML24: cap SMOTETomek's minority-oversampling ratio (e.g. 0.5). None (default) preserves "
             "unbounded 'auto' behavior. See retrain_phase2()'s docstring for the signal_63d rationale.",
    )
    parser.add_argument(
        "--optuna-trials", type=int, default=None,
        help="ML24 (2026-07-11): override OPTUNA_TRIALS_BY_HORIZON's per-horizon default "
             "(signal_63d defaults to just 3, a likely source of Sharpe instability across reruns).",
    )
    parser.add_argument(
        "--subprocess-per-horizon", action="store_true",
        help="ML21 (2026-07-10): run signal_5d/21d/63d each as a separate OS subprocess instead of one Python "
             "loop, so SMOTETomek's oversampled matrix + Optuna/stacking-ensemble refit for one horizon cannot "
             "accumulate against another's in the same process's RSS — the OS reclaims all memory when each "
             "child exits, regardless of any lingering Python references. Recommended for unattended/scheduler "
             "runs on memory-constrained boxes; see FeatureBacklog.md ML21.",
    )
    args = parser.parse_args()

    use_training_universe = not (args.no_training_universe or args.full_universe or args.quick)

    if args.quick:
        max_tickers, trials = 15, 3
    elif args.full_universe:
        max_tickers, trials = None, None
    else:
        max_tickers, trials = args.max_tickers, None

    if args.optuna_trials is not None:
        trials = args.optuna_trials

    if args.subprocess_per_horizon:
        import subprocess
        import sys

        base_cmd = [sys.executable, "-m", "systems.ml_signal_engine.inference.retrain_phase2",
                    "--chunk-size", str(args.chunk_size)]
        if args.max_sampling_ratio is not None:
            base_cmd += ["--max-sampling-ratio", str(args.max_sampling_ratio)]
        if not use_training_universe:
            base_cmd.append("--no-training-universe")
        if args.quick:
            base_cmd.append("--quick")
        elif args.full_universe:
            base_cmd.append("--full-universe")
        else:
            base_cmd += ["--max-tickers", str(args.max_tickers)]

        for horizon_days, _cls, name, _pm, _sm in HORIZON_CONFIGS:
            cmd = base_cmd + ["--horizon", str(horizon_days)]
            logging.info(f"Launching isolated subprocess for {name} (horizon={horizon_days}d): {' '.join(cmd)}")
            proc = subprocess.run(cmd)
            if proc.returncode != 0:
                logging.error(f"{name} subprocess exited with code {proc.returncode} — see its own output above.")
                sys.exit(proc.returncode)
        print("\nAll horizons trained via isolated subprocesses. See each subprocess's own stdout above for results.")
        return

    result = retrain_phase2(
        max_tickers=max_tickers, optuna_trials=trials, ticker_chunk_size=args.chunk_size,
        only_horizon=args.horizon, max_sampling_ratio=args.max_sampling_ratio,
        use_training_universe=use_training_universe,
    )
    print(f"\nModels saved: {list(result['registry'].keys())}")
    print("\n=== Phase 1 vs Phase 2 Sharpe ===")
    for name, comp in result["comparison"].items():
        status = "PASS" if comp["improved_or_neutral"] else "REGRESSED"
        print(f"  {status}  {name}: Phase1={comp['phase1_sharpe']:.3f}  Phase2={comp['phase2_sharpe']:.3f}")


if __name__ == "__main__":
    main()
