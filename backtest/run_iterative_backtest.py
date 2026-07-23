"""
backtest/run_iterative_backtest.py

Owner: Platform / Backtest
Consumers: operator CLI (`python3 -m backtest.run_iterative_backtest`),
datastore/api/routers/backtest_runs.py (trigger endpoint, background)

CLI entry point for backtest/iterative_retrain.py's RetrainLoop — same
real-data-fetch pattern as run_phase1_backtest.py (reuses its helpers
rather than duplicating them). Trains a fresh P&D detector and exit
model once (same as every other phase script — these are NOT what this
loop iterates on), then hands control to RetrainLoop, which repeatedly
retrains the MetaLabeler entry-filter over a small fixed hyperparameter
grid, promotes only DSR-cleared, non-noise-fitting (random-feature-test)
improvements, and evaluates the winner exactly once on an untouched
holdout fiscal year.
"""

import argparse
import json
import logging
import time
from pathlib import Path
from typing import Optional

from backtest.batch_common import exclusive_backtest_lock
from backtest.core.feature_log import FeatureLogWriter
from backtest.iterative_retrain import (
    DEFAULT_MAX_ITERATIONS,
    DEFAULT_MAX_RANDOM_FEATURE_ACCURACY,
    DEFAULT_MIN_DSR_THRESHOLD,
    DEFAULT_PLATEAU_PATIENCE,
    RetrainLoop,
)
from backtest.run_phase1_backtest import (
    _fetch_historical_tickers,
    _fetch_real_benchmark,
    _fetch_real_benchmark_index,
    _fetch_real_universe,
    _real_sector_map,
)
from config.settings import BACKTEST_DUCKDB_PATH
from config.timezone import now_ist
from config.universe import get_tickers
from datastore.api.db import get_duckdb_connection
from datastore.schema.create_backtest import create_backtest_schema
from systems.ml_signal_engine.models.exit.exit_signal import ExitSignalModel, load_exit_training_data_from_db
from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector, load_pnd_training_data_from_db
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent / "reports"


def run_iterative_backtest(
    horizon_days: int = 5, seed: int = 42, max_real_tickers: Optional[int] = None,
    min_history_days: int = 252, max_iterations: int = DEFAULT_MAX_ITERATIONS,
    plateau_patience: int = DEFAULT_PLATEAU_PATIENCE, min_dsr_threshold: float = DEFAULT_MIN_DSR_THRESHOLD,
    max_random_feature_accuracy: float = DEFAULT_MAX_RANDOM_FEATURE_ACCURACY,
    folds: int = 4, report_suffix: Optional[str] = None,
) -> dict:
    run_date = now_ist()
    run_started = time.monotonic()

    logger.info("Iterative retrain starting: REAL data (config.universe.get_tickers() via DataStoreClient)")
    # Held across the entire real-work window — same system-wide
    # sequential-execution requirement as run_orchestrator_backtest.py;
    # see batch_common.exclusive_backtest_lock's docstring.
    with exclusive_backtest_lock(label="iterative_retrain"):
        ohlcv = _fetch_real_universe(max_real_tickers, min_history_days)
        sector_map = _real_sector_map()
        benchmark = _fetch_real_benchmark()
        benchmark_index = _fetch_real_benchmark_index()
        universe_tickers = set(get_tickers())
        historical_tickers = _fetch_historical_tickers()

        pnd_X, pnd_y = load_pnd_training_data_from_db()
        pnd_detector = PnDDetector(random_state=seed)
        pnd_detector.train(pnd_X, pnd_y)

        exit_X, urgency, exit_type, duration, event = load_exit_training_data_from_db()
        exit_model = ExitSignalModel(random_state=seed)
        exit_model.train_full(exit_X, urgency, exit_type, duration, event)

        engine_kwargs = dict(
            ohlcv=ohlcv, pnd_detector=pnd_detector, exit_model=exit_model, signal_model_cls=Signal5DModel,
            sector_map=sector_map, horizon_days=horizon_days, initial_capital=1_000_000.0, sizing_mode="equal_weight",
            n_target_positions=10, optuna_trials=5, random_state=seed, n_folds=folds,
            benchmark=benchmark, universe_tickers=universe_tickers, historical_tickers=historical_tickers,
            benchmark_index=benchmark_index,
        )

        create_backtest_schema(BACKTEST_DUCKDB_PATH)
        with get_duckdb_connection(BACKTEST_DUCKDB_PATH, read_only=False, persist=False) as conn:
            feature_log_writer = FeatureLogWriter(conn)
            loop = RetrainLoop(
                engine_kwargs=engine_kwargs, strategy_id="signal_5d_metalabeler_retrain",
                feature_log_writer=feature_log_writer, conn=conn, max_iterations=max_iterations,
                plateau_patience=plateau_patience, min_dsr_threshold=min_dsr_threshold,
                max_random_feature_accuracy=max_random_feature_accuracy, folds=folds,
            )
            result = loop.run(combined_ohlcv_max_date=ohlcv["date"].max())
            feature_log_writer.flush()
            conn.commit()

    runtime_seconds = time.monotonic() - run_started

    print("\n=== Holdout Selection (Explainability) ===")
    print(f"  {result.holdout_selection.explain()}")
    print(f"  Rows excluded entirely (too-recent-to-resolve buffer): {result.excluded_buffer_rows}")

    print("\n=== Iterations ===")
    for it in result.iterations:
        status = "PROMOTED" if it.promoted else f"rejected ({it.rejection_reason})"
        rfa_str = f" rfa={it.random_feature_accuracy:.3f}" if it.random_feature_accuracy is not None else ""
        print(
            f"  [{it.iteration}] sharpe={it.sharpe_mean:.3f} win_rate={it.win_rate_mean:.2%} "
            f"dsr={it.dsr:.3f}{rfa_str} runtime={it.runtime_seconds:.1f}s -> {status}"
        )
        if it.dropped_candidates:
            dropped_str = ", ".join(f"{k}={v}" for k, v in sorted(it.dropped_candidates.items()))
            print(f"       dropped candidates: {dropped_str}")

    print(f"\n  Stopped: {result.stopped_reason} after {len(result.iterations)} iteration(s)")

    if result.holdout_results is not None:
        print("\n=== Holdout Evaluation (one-shot, never seen during tuning) ===")
        agg = result.holdout_results.aggregate
        print(f"  Sharpe={agg.get('sharpe_mean')} CAGR={agg.get('cagr_mean')} WinRate={agg.get('win_rate_mean')}")
        print(f"  Holdout runtime: {result.holdout_runtime_seconds:.1f}s")
    else:
        print("\n  No iteration was promoted — no holdout evaluation was run.")

    print(f"\n  Total runtime: {runtime_seconds:.1f}s")

    report = {
        "generated_at": run_date.isoformat(),
        "loop_run_id": result.loop_run_id,
        "runtime_seconds": runtime_seconds,
        "stopped_reason": result.stopped_reason,
        "holdout_selection": {
            "holdout_start": str(result.holdout_selection.holdout_start.date()),
            "holdout_end": str(result.holdout_selection.holdout_end.date()),
            "skipped_fiscal_years": result.holdout_selection.skipped_fiscal_years,
            "explanation": result.holdout_selection.explain(),
        },
        "excluded_buffer_rows": result.excluded_buffer_rows,
        "iterations": [
            {
                "iteration": it.iteration, "run_id": it.run_id, "hyperparams": it.hyperparams,
                "sharpe_mean": it.sharpe_mean, "win_rate_mean": it.win_rate_mean, "dsr": it.dsr,
                "random_feature_accuracy": it.random_feature_accuracy,
                "promoted": it.promoted, "rejection_reason": it.rejection_reason,
                "runtime_seconds": it.runtime_seconds, "dropped_candidates": it.dropped_candidates,
            }
            for it in result.iterations
        ],
        "best_iteration_index": result.best_iteration_index,
        "best_hyperparams": result.best_hyperparams,
        "holdout_run_id": result.holdout_run_id,
        "holdout_runtime_seconds": result.holdout_runtime_seconds,
        "holdout_aggregate": result.holdout_results.aggregate if result.holdout_results else None,
    }
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    # report_suffix lets a caller (e.g. the trigger API endpoint,
    # datastore/api/routers/backtest_runs.py) name the report file
    # deterministically (its own job_id) rather than polling for
    # whatever timestamp this process happens to pick.
    suffix = report_suffix or run_date.strftime("%Y%m%d_%H%M%S")
    report_path = REPORTS_DIR / f"iterative_retrain_{suffix}.json"
    with open(report_path, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(f"\nReport written to {report_path}")

    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Iteratively retrain the MetaLabeler entry-filter and evaluate once on an untouched holdout fiscal year"
    )
    parser.add_argument("--horizon-days", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-real-tickers", type=int, default=None)
    parser.add_argument("--min-history-days", type=int, default=252)
    parser.add_argument("--max-iterations", type=int, default=DEFAULT_MAX_ITERATIONS)
    parser.add_argument("--plateau-patience", type=int, default=DEFAULT_PLATEAU_PATIENCE)
    parser.add_argument("--min-dsr-threshold", type=float, default=DEFAULT_MIN_DSR_THRESHOLD)
    parser.add_argument("--max-random-feature-accuracy", type=float, default=DEFAULT_MAX_RANDOM_FEATURE_ACCURACY)
    parser.add_argument("--folds", type=int, default=4)
    parser.add_argument("--report-suffix", type=str, default=None, help="Name the report file deterministically (e.g. a job_id)")
    args = parser.parse_args()

    run_iterative_backtest(
        horizon_days=args.horizon_days, seed=args.seed, max_real_tickers=args.max_real_tickers,
        min_history_days=args.min_history_days, max_iterations=args.max_iterations,
        plateau_patience=args.plateau_patience, min_dsr_threshold=args.min_dsr_threshold,
        max_random_feature_accuracy=args.max_random_feature_accuracy, folds=args.folds,
        report_suffix=args.report_suffix,
    )


if __name__ == "__main__":
    main()
