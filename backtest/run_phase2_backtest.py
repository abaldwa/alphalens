"""
backtest/run_phase2_backtest.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-BT-001 through SPEC-BT-004, SPEC-MODEL-001, SPEC-MODEL-006, SPEC-UI-003
Owner: ml_signal_engine / backtest
Consumers: operator CLI (`python3 -m backtest.run_phase2_backtest`)

Phase 2 backtest: Signal63D entries (instead of Phase 1's Signal5D) with
a multibagger watchlist entry filter — same MetaLabeler/P&D pre-filter/
ExitSignalModel/PortfolioSimulator pipeline as Phase 1
(backtest/run_phase1_backtest.py), via BacktestEngine's new (P2.6)
`watchlist_tickers` constructor parameter. Runs BOTH a Phase 1 baseline
(Signal5D, no watchlist filter) and the Phase 2 variant on the SAME real
OHLCV data, and reports the Sharpe/CAGR/MaxDD comparison the build prompt
asks for.

Always uses real data (DataStoreClient, SPEC-DS-002) — there is no
synthetic-data mode anywhere in this script.

[AS BUILT, P2.6] "Multibagger watchlist filter" here is a STATIC snapshot,
not a literal weekly-refreshing watchlist inside the walk-forward loop:
systems/ml_signal_engine/inference/score_multibagger.py scores the real
universe ONCE (at the backtest's as-of date) via
MultibaggerModel.generate_weekly_watchlist()'s own top-20/probability>0.30
filter, and that fixed ticker set is passed to BacktestEngine as
`watchlist_tickers` for the entire walk-forward run. A true week-by-week
refreshing watchlist would mean re-scoring multibagger probability at
every Monday inside every fold using only that day's PIT-available
history — a substantially larger undertaking (the model would need to be
re-trained or at least re-scored per fold-week, mirroring
WalkForwardValidator's per-fold retrain loop) out of this prompt's scope;
documented here as a real, intentional simplification, not a hidden gap.
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from backtest.engine import BacktestEngine
from backtest.report_utils import write_per_horizon_reports
from backtest.run_phase1_backtest import (
    _fetch_historical_tickers,
    _fetch_real_benchmark,
    _fetch_real_universe,
    _real_sector_map,
)
from config.timezone import now_ist
from config.universe import get_tickers
from datastore.client import DataStoreClient
from features.multibagger import MULTIBAGGER_FEATURES, compute_multibagger_features
from systems.ml_signal_engine.inference.score_multibagger import _fetch_benchmark_wide
from systems.ml_signal_engine.models.exit.exit_signal import ExitSignalModel, load_exit_training_data_from_db
from systems.ml_signal_engine.models.multibagger.multibagger_model import (
    MultibaggerModel,
    generate_weekly_watchlist,
    load_multibagger_training_data_from_db,
)
from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector, load_pnd_training_data_from_db
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel
from systems.ml_signal_engine.models.signal.signal_63d import Signal63DModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent / "reports"
# 02_models.md / retrain_phase2.py's documented "63d = 5x ATR" barrier mapping.
SIGNAL_63D_PROFIT_MULTIPLIER = 5.0
SIGNAL_63D_STOP_MULTIPLIER = 5.0


def _build_multibagger_watchlist(
    ohlcv: pd.DataFrame, sector_map: Dict[str, str], client: DataStoreClient
) -> set:
    """
    Score the real universe's multibagger probability ONCE (module
    docstring's documented static-snapshot simplification) and return the
    top-20/probability>0.30 ticker set via MultibaggerModel's own
    generate_weekly_watchlist() — same filter SPEC-UI-003's dashboard
    watchlist uses (datastore/api/routers/watchlist.py).
    """
    X_train, y_train, duration, event, groups, _pnd = load_multibagger_training_data_from_db()
    model = MultibaggerModel()
    model.train_full(X_train, y_train, duration, event, groups=groups)

    as_of = pd.Timestamp(ohlcv["date"].max())
    benchmark_wide = _fetch_benchmark_wide(client, as_of)
    features = compute_multibagger_features(ohlcv, benchmark_wide, sector_map)
    latest = features.sort_values("date").groupby("ticker").tail(1).set_index("ticker")

    scored = model.predict_full(latest[MULTIBAGGER_FEATURES])
    watchlist = generate_weekly_watchlist(scored, is_monday=True)
    tickers = set(watchlist.index) if watchlist is not None else set()
    logger.info(f"multibagger watchlist: {len(tickers)} tickers (top-20/prob>0.30)")
    return tickers


def _run_variant(
    ohlcv: pd.DataFrame, sector_map: Dict[str, str], benchmark: Optional[pd.DataFrame],
    universe_tickers: set, historical_tickers: set, signal_model_cls: type, horizon_days: int,
    profit_multiplier: float, stop_multiplier: float, watchlist_tickers: Optional[set],
    folds: int, optuna_trials: int, seed: int, model_name: str,
) -> dict:
    pnd_X, pnd_y = load_pnd_training_data_from_db()
    pnd_detector = PnDDetector(random_state=seed)
    pnd_detector.train(pnd_X, pnd_y)

    exit_X, urgency, exit_type, duration, event = load_exit_training_data_from_db()
    exit_model = ExitSignalModel(random_state=seed)
    exit_model.train_full(exit_X, urgency, exit_type, duration, event)

    engine = BacktestEngine(
        ohlcv=ohlcv, pnd_detector=pnd_detector, exit_model=exit_model, signal_model_cls=signal_model_cls,
        sector_map=sector_map, horizon_days=horizon_days, profit_multiplier=profit_multiplier,
        stop_multiplier=stop_multiplier, initial_capital=1_000_000.0, sizing_mode="equal_weight",
        n_target_positions=10, optuna_trials=optuna_trials, random_state=seed, n_folds=folds,
        benchmark=benchmark, universe_tickers=universe_tickers, historical_tickers=historical_tickers,
        watchlist_tickers=watchlist_tickers,
    )
    results = engine.run_full_backtest(model_name, folds=folds)

    from backtest.adapters.ml_dual_write import dual_write_ml_run

    dual_write_ml_run(
        results, strategy_id=model_name, horizon_days=horizon_days, ohlcv=ohlcv,
        initial_capital=1_000_000.0, random_seed=seed,
    )

    return results.to_dict()


def run_phase2_backtest(
    folds: int = 5, optuna_trials: int = 5, seed: int = 42,
    max_real_tickers: Optional[int] = None, min_history_days: int = 252,
) -> Dict[str, dict]:
    run_date = now_ist()
    client = DataStoreClient()

    logger.info("Phase 2 backtest starting: REAL data (config.universe.get_tickers() via DataStoreClient)")
    ohlcv = _fetch_real_universe(max_real_tickers, min_history_days)
    sector_map = _real_sector_map()
    benchmark = _fetch_real_benchmark()
    universe_tickers = set(get_tickers())
    historical_tickers = _fetch_historical_tickers()

    watchlist_tickers = _build_multibagger_watchlist(ohlcv, sector_map, client)

    logger.info("Running Phase 1 baseline (Signal5D, no watchlist filter)...")
    phase1 = _run_variant(
        ohlcv, sector_map, benchmark, universe_tickers, historical_tickers,
        signal_model_cls=Signal5DModel, horizon_days=5, profit_multiplier=2.0, stop_multiplier=1.0,
        watchlist_tickers=None, folds=folds, optuna_trials=optuna_trials, seed=seed, model_name="signal_5d",
    )

    logger.info("Running Phase 2 variant (Signal63D + multibagger watchlist filter)...")
    phase2 = _run_variant(
        ohlcv, sector_map, benchmark, universe_tickers, historical_tickers,
        signal_model_cls=Signal63DModel, horizon_days=63,
        profit_multiplier=SIGNAL_63D_PROFIT_MULTIPLIER, stop_multiplier=SIGNAL_63D_STOP_MULTIPLIER,
        watchlist_tickers=watchlist_tickers, folds=folds, optuna_trials=optuna_trials, seed=seed,
        model_name="signal_63d_watchlist",
    )

    print("\n=== Phase 1 vs Phase 2 Comparison ===")
    print(f"{'Metric':<20}{'Phase 1 (Signal5D)':<25}{'Phase 2 (Signal63D+Watchlist)':<25}")
    for key in ("sharpe_mean", "cagr_mean", "max_drawdown_worst"):
        v1 = phase1["aggregate"].get(key)
        v2 = phase2["aggregate"].get(key)
        print(f"{key:<20}{str(v1):<25}{str(v2):<25}")

    report = {
        "generated_at": run_date.isoformat(),
        "watchlist_size": len(watchlist_tickers),
        "phase1": phase1,
        "phase2": phase2,
    }
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"phase2_{run_date.strftime('%Y%m%d')}.json"
    with open(report_path, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(f"\nReport written to {report_path}")

    # ML17(b) — each horizon variant also gets its own standalone report
    # (fold-level results + real-benchmark comparison, ML17(a)) alongside
    # the combined comparison report above.
    per_horizon_paths = write_per_horizon_reports(
        {"signal_5d": phase1, "signal_63d_watchlist": phase2},
        REPORTS_DIR, run_date.strftime("%Y%m%d"), "phase2",
    )
    for name, path in per_horizon_paths.items():
        print(f"  {name}'s own report written to {path}")

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Phase 2 backtest (Signal63D + multibagger watchlist filter)")
    parser.add_argument("--folds", type=int, default=5)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--quick", action="store_true", help="Small/fast run for smoke-testing")
    parser.add_argument("--max-real-tickers", type=int, default=None, help="Cap universe size")
    parser.add_argument(
        "--min-history-days", type=int, default=252, help="Exclude tickers with fewer real OHLCV rows than this"
    )
    args = parser.parse_args()

    folds = 2 if args.quick else args.folds
    trials = 2 if args.quick else args.trials
    run_phase2_backtest(
        folds=folds, optuna_trials=trials, max_real_tickers=args.max_real_tickers,
        min_history_days=args.min_history_days,
    )


if __name__ == "__main__":
    main()
