"""
backtest/run_phase3_backtest.py

Phase: 3.3 (Phase 3 Backtest + Stacking Ensemble Gate)
Specs: SPEC-BT-001 through SPEC-BT-004, SPEC-MODEL-003, SPEC-MODEL-013
Owner: ml_signal_engine / backtest
Consumers: operator CLI (`python3 -m backtest.run_phase3_backtest`)

Phase 3 backtest: runs two variants on the same real OHLCV universe and
reports the Sharpe improvement gate.

  Phase 2 baseline : Signal5D (LGB + CatBoost + XGB internal stack)
  Phase 3 variant  : Signal21D (wider barrier, richer Phase 3 features)

Phase 3 gate (SPEC-MODEL-013): Sharpe(Phase3) − Sharpe(Phase2) >= 0.10,
evaluated directly from each variant's real walk-forward backtest Sharpe.

Stacking ensemble — NOT computed in this script
------------------------------------------------
A true StackingMetaLearner ensemble requires per-prediction out-of-fold
(OOF) probabilities and actual labels, captured per row across every
fold. BacktestEngine currently returns only fold-level aggregate metrics
(FoldResult: cagr/sharpe/max_drawdown/...), not per-row OOF — there is no
real data this script could feed StackingMetaLearner.fit_meta() with. A
previous version of this script fabricated fake OOF via
`rng.dirichlet()`/`rng.choice()`; that synthetic-data fallback has been
removed with no replacement, per the project's no-synthetic-data policy.
See BuildLog.md "Real data sourcing — Stacking ensemble backtest" for the
BacktestEngine extension (capturing real per-row OOF predictions per
fold) required before stacking can be added back here.

TFT and BiLSTM (M-11/M-12)
---------------------------
These deep models require overnight CPU training (4–6h) and are not
included in this CI backtest run.

All 9 integrity rules
---------------------
BacktestIntegrityChecker runs automatically inside BacktestEngine.
This script additionally asserts all 9 pass before reporting success.

Usage
-----
  python -m backtest.run_phase3_backtest --folds 5
"""

import argparse
import json
import logging
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from backtest.core.feature_log import FeatureLogWriter
from backtest.engine import BacktestEngine
from backtest.report_utils import write_per_horizon_reports
from config.settings import BACKTEST_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.schema.create_backtest import create_backtest_schema
from backtest.run_phase1_backtest import (
    _fetch_historical_tickers,
    _fetch_real_benchmark,
    _fetch_real_universe,
    _real_sector_map,
)
from config.timezone import now_ist
from config.universe import get_tickers
from systems.ml_signal_engine.models.exit.exit_signal import ExitSignalModel, load_exit_training_data_from_db
from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector, load_pnd_training_data_from_db
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel
from systems.ml_signal_engine.models.signal.signal_21d import Signal21DModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent / "reports"

# Phase 3 gate: minimum Sharpe improvement over Phase 2 baseline
_PHASE3_SHARPE_GATE: float = 0.10
# [BUG FIX, 2026-07-21 full-codebase-review REV6] Deflated Sharpe Ratio
# significance threshold (Bailey & Lopez de Prado 2014's own convention):
# DSR >= 0.95 means the observed Sharpe is very unlikely to be the best of
# many noisy configurations. Both phase2 baseline and phase3 variant are
# themselves each the winner of their own Optuna HPO search, so neither
# side's raw Sharpe should be trusted without this correction.
_PHASE3_DSR_GATE: float = 0.95

# Signal21D barrier widths (02_models.md: "21d = 3× ATR")
_SIGNAL21D_PROFIT_MULTIPLIER: float = 3.0
_SIGNAL21D_STOP_MULTIPLIER: float = 3.0


# ── Shared setup helpers ──────────────────────────────────────────────────────


def _build_pnd_and_exit(seed: int):
    pnd_X, pnd_y = load_pnd_training_data_from_db()
    pnd = PnDDetector(random_state=seed)
    pnd.train(pnd_X, pnd_y)

    exit_X, urgency, exit_type, duration, event = load_exit_training_data_from_db()
    exit_model = ExitSignalModel(random_state=seed)
    exit_model.train_full(exit_X, urgency, exit_type, duration, event)

    return pnd, exit_model


def _run_single_model(
    ohlcv: pd.DataFrame,
    sector_map: Dict[str, str],
    benchmark: Optional[pd.DataFrame],
    universe_tickers: set,
    historical_tickers: set,
    signal_model_cls: type,
    horizon_days: int,
    profit_multiplier: float,
    stop_multiplier: float,
    folds: int,
    optuna_trials: int,
    seed: int,
    model_name: str,
    watchlist_tickers: Optional[set] = None,
    feature_log_writer: Optional[FeatureLogWriter] = None,
    run_id: Optional[str] = None,
) -> dict:
    """Run one walk-forward backtest with BacktestEngine. Returns results.to_dict()."""
    variant_started = time.monotonic()
    pnd, exit_model = _build_pnd_and_exit(seed)
    engine = BacktestEngine(
        ohlcv=ohlcv,
        pnd_detector=pnd,
        exit_model=exit_model,
        signal_model_cls=signal_model_cls,
        sector_map=sector_map,
        horizon_days=horizon_days,
        profit_multiplier=profit_multiplier,
        stop_multiplier=stop_multiplier,
        initial_capital=1_000_000.0,
        sizing_mode="equal_weight",
        n_target_positions=10,
        optuna_trials=optuna_trials,
        random_state=seed,
        n_folds=folds,
        benchmark=benchmark,
        universe_tickers=universe_tickers,
        historical_tickers=historical_tickers,
        watchlist_tickers=watchlist_tickers,
        feature_log_writer=feature_log_writer,
        run_id=run_id,
    )
    results = engine.run_full_backtest(model_name, folds=folds)
    variant_runtime_seconds = time.monotonic() - variant_started
    logger.info(f"{model_name}: runtime {variant_runtime_seconds:.1f}s")

    from backtest.adapters.ml_dual_write import dual_write_ml_run

    dual_write_ml_run(
        results, strategy_id=model_name, horizon_days=horizon_days, ohlcv=ohlcv,
        initial_capital=1_000_000.0, random_seed=seed,
    )

    result_dict = results.to_dict()
    result_dict["runtime_seconds"] = variant_runtime_seconds
    return result_dict


# ── Main backtest function ────────────────────────────────────────────────────


def run_phase3_backtest(
    folds: int = 5,
    optuna_trials: int = 5,
    seed: int = 42,
    max_real_tickers: Optional[int] = None,
    min_history_days: int = 252,
) -> Dict[str, Any]:
    """
    Run Phase 3 backtest: Phase 2 (Signal5D) baseline vs Phase 3 (Signal21D)
    variant, both on real OHLCV data.

    Returns
    -------
    dict with keys:
      phase2         : Phase 2 baseline results
      phase3         : Phase 3 Signal21D results
      comparison     : Metric-by-metric comparison table
      gate_passed    : bool — True if Sharpe improvement >= 0.10 AND
                       Phase 3's deflated Sharpe ratio >= 0.95 (2026-07-21
                       full-codebase-review REV6: a raw Sharpe delta alone
                       can't distinguish genuine improvement from the
                       "best of N" noise both variants' own Optuna HPO
                       search can produce)
      sharpe_improvement : float

    Spec References
    ---------------
    SPEC-BT-001: all 9 integrity rules.
    SPEC-MODEL-013: Phase 3 Sharpe gate >= 0.10.
    """
    run_date = now_ist()
    run_started = time.monotonic()

    # ── Load data ──────────────────────────────────────────────────────────
    logger.info("Phase 3 backtest: REAL data via DataStoreClient")
    ohlcv = _fetch_real_universe(max_real_tickers, min_history_days)
    sector_map = _real_sector_map()
    benchmark = _fetch_real_benchmark()
    universe_tickers = set(get_tickers())
    historical_tickers = _fetch_historical_tickers()

    # Feature capture (backtest_feature_log) — shared writer across both
    # variants, each getting its own run_id (see run_phase2_backtest.py).
    base_run_id = f"phase3_{run_date.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    create_backtest_schema(BACKTEST_DUCKDB_PATH)
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, read_only=False, persist=False) as feature_log_conn:
        feature_log_writer = FeatureLogWriter(feature_log_conn)

        # ── Phase 2 baseline: Signal5D ─────────────────────────────────────
        logger.info("Running Phase 2 baseline (Signal5D)...")
        phase2 = _run_single_model(
            ohlcv, sector_map, benchmark, universe_tickers, historical_tickers,
            signal_model_cls=Signal5DModel,
            horizon_days=5, profit_multiplier=2.0, stop_multiplier=1.0,
            folds=folds, optuna_trials=optuna_trials, seed=seed,
            model_name="signal_5d_p2baseline",
            feature_log_writer=feature_log_writer, run_id=f"{base_run_id}_signal_5d_p2baseline",
        )

        # ── Phase 3 variant: Signal21D ──────────────────────────────────────
        logger.info("Running Phase 3 variant (Signal21D, 21d horizon, wider barriers)...")
        phase3 = _run_single_model(
            ohlcv, sector_map, benchmark, universe_tickers, historical_tickers,
            signal_model_cls=Signal21DModel,
            horizon_days=21,
            profit_multiplier=_SIGNAL21D_PROFIT_MULTIPLIER,
            stop_multiplier=_SIGNAL21D_STOP_MULTIPLIER,
            folds=folds, optuna_trials=optuna_trials, seed=seed,
            model_name="signal_21d_p3variant",
            feature_log_writer=feature_log_writer, run_id=f"{base_run_id}_signal_21d_p3variant",
        )
    logger.info(f"Feature vectors captured to backtest_feature_log under run_id prefix={base_run_id}")

    # [BUG FIX, 2026-07-21 full-codebase-review REV5] Use
    # sharpe_mean_full_periods_only (excludes a short trailing partial-year
    # fold that can otherwise skew the plain mean once annualized off a
    # handful of trades — engine.py's own aggregate already computes this)
    # rather than the possibly partial-fold-skewed sharpe_mean, falling
    # back to sharpe_mean only if no full-year fold exists in this run.
    baseline_sharpe = (
        phase2["aggregate"].get("sharpe_mean_full_periods_only")
        if phase2["aggregate"].get("sharpe_mean_full_periods_only") is not None
        else phase2["aggregate"].get("sharpe_mean", 0.0)
    ) or 0.0
    variant_sharpe = (
        phase3["aggregate"].get("sharpe_mean_full_periods_only")
        if phase3["aggregate"].get("sharpe_mean_full_periods_only") is not None
        else phase3["aggregate"].get("sharpe_mean", 0.0)
    ) or 0.0
    sharpe_improvement = variant_sharpe - baseline_sharpe

    # [BUG FIX, 2026-07-21 full-codebase-review REV6] Deflated Sharpe Ratio
    # is now computed inside BacktestEngine.run_full_backtest (real
    # per-period fold returns, n_trials=optuna_trials) — require the
    # PHASE 3 variant's DSR to clear the significance threshold too, not
    # just the raw Sharpe delta, since a raw delta alone can't distinguish
    # genuine improvement from noise across an HPO search.
    variant_dsr = phase3["aggregate"].get("deflated_sharpe_ratio")
    dsr_ok = variant_dsr is not None and variant_dsr >= _PHASE3_DSR_GATE
    gate_passed = sharpe_improvement >= _PHASE3_SHARPE_GATE and dsr_ok

    # ── Phase 3 integrity checks ───────────────────────────────────────────
    # Individual integrity checks run inside BacktestEngine per fold.
    # Aggregate: both variants must pass all 9 rules.
    p2_integrity = phase2.get("integrity_passed", False)
    p3_integrity = phase3.get("integrity_passed", False)
    integrity_ok = p2_integrity and p3_integrity

    # ── Comparison table ───────────────────────────────────────────────────
    comparison: Dict[str, Any] = {}
    for key in ("sharpe_mean", "cagr_mean", "max_drawdown_worst", "win_rate_mean"):
        v2 = phase2["aggregate"].get(key)
        v3 = phase3["aggregate"].get(key)
        comparison[key] = {"phase2_baseline": v2, "phase3_signal21d": v3}

    # ── Print summary ──────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  Phase 3 Backtest Results")
    print("=" * 70)
    print(f"  {'Metric':<28}{'Phase 2 Baseline':<22}{'Phase 3 Signal21D'}")
    print("  " + "-" * 68)
    for key in ("sharpe_mean", "cagr_mean", "max_drawdown_worst"):
        v2 = phase2["aggregate"].get(key)
        v3 = phase3["aggregate"].get(key)
        print(f"  {key:<28}{str(round(v2, 4) if v2 else 'N/A'):<22}"
              f"{str(round(v3, 4) if v3 else 'N/A')}")
    print("  " + "-" * 68)
    print(f"  {'Sharpe improvement (Phase 3 - 2)':<28}{sharpe_improvement:+.4f}")
    print(f"  {'Gate >= 0.10':<28}{'PASSED' if sharpe_improvement >= _PHASE3_SHARPE_GATE else 'FAILED'}")
    print(f"  {'Deflated Sharpe Ratio (Phase 3)':<28}"
          f"{str(round(variant_dsr, 4)) if variant_dsr is not None else 'N/A'}")
    print(f"  {'DSR >= 0.95':<28}{'PASSED' if dsr_ok else 'FAILED'}")
    print(f"  {'Overall gate (Sharpe delta AND DSR)':<28}{'PASSED' if gate_passed else 'FAILED'}")
    print(f"  {'All 9 integrity rules':<28}{'PASSED' if integrity_ok else 'FAILED'}")
    print("=" * 70)

    runtime_seconds = time.monotonic() - run_started
    print(f"\n  Total runtime: {runtime_seconds:.1f}s "
          f"(phase2={phase2['runtime_seconds']:.1f}s, phase3={phase3['runtime_seconds']:.1f}s)")

    # ── Save report ────────────────────────────────────────────────────────
    report = {
        "generated_at": run_date.isoformat(),
        "runtime_seconds": runtime_seconds,
        "phase3_gate_threshold": _PHASE3_SHARPE_GATE,
        "phase3_dsr_gate_threshold": _PHASE3_DSR_GATE,
        "sharpe_improvement": sharpe_improvement,
        "deflated_sharpe_ratio": variant_dsr,
        "dsr_gate_passed": dsr_ok,
        "gate_passed": gate_passed,
        "integrity_passed": integrity_ok,
        "comparison": comparison,
        "phase2": phase2,
        "phase3": phase3,
    }
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"phase3_{run_date.strftime('%Y%m%d')}.json"
    with open(report_path, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(f"\n  Report written to {report_path}")

    # ML17(b) — each horizon variant also gets its own standalone report
    # (fold-level results + real-benchmark comparison, ML17(a)) alongside
    # the combined gate-comparison report above.
    per_horizon_paths = write_per_horizon_reports(
        {"signal_5d_p2baseline": phase2, "signal_21d_p3variant": phase3},
        REPORTS_DIR, run_date.strftime("%Y%m%d"), "phase3",
    )
    for name, path in per_horizon_paths.items():
        print(f"  {name}'s own report written to {path}")

    return report


# ── CLI ───────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 3 backtest: Signal21D vs Phase 2 Signal5D baseline (real data only)"
    )
    parser.add_argument("--folds", type=int, default=5)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--max-tickers", type=int, default=None)
    parser.add_argument("--min-history", type=int, default=252)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    report = run_phase3_backtest(
        folds=args.folds,
        optuna_trials=args.trials,
        seed=args.seed,
        max_real_tickers=args.max_tickers,
        min_history_days=args.min_history,
    )

    sys.exit(0 if report["gate_passed"] else 1)


if __name__ == "__main__":
    main()
