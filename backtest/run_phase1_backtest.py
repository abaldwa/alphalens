"""
backtest/run_phase1_backtest.py

Phase: 1.6 (Exit Signal + First Backtest); real-data wiring added post-P1.7
Specs: SPEC-BT-001 through SPEC-BT-004, SPEC-MODEL-002, SPEC-MODEL-006, SPEC-MODEL-007
Owner: ml_signal_engine / backtest
Consumers: operator CLI (`python3 -m backtest.run_phase1_backtest`)

Phase 1 backtest: Signal5D + MetaLabeler + P&D pre-filter entries,
ExitSignalModel-driven exits, equal-weight sizing, run through
BacktestEngine's walk-forward harness (P1.6).

[AS BUILT] Two data sources, selected by --real-data:
- Default (synthetic): same documented "synthetic price series, real
  model/feature/backtest code" pattern as systems/ml_signal_engine/
  inference/train_all_phase1.py (P1.5) — kept as the default so existing
  callers (the 🔒 PHASE 1 GATE CHECK's --check-only, fast smoke tests)
  keep working unchanged (SOLID-002).
- --real-data: fetches real OHLCV via DataStoreClient (SPEC-DS-002 — never
  a direct DuckDB query from this consumer-layer script) for
  config.universe.get_tickers()'s curated universe, filtered to tickers
  with enough history to be useful; real sector mapping from config.
  universe.load_universe(); attempts a real benchmark (NIFTYBEES etc.),
  falling back to the same synthetic benchmark generator if the real
  series is too sparse to be useful (a real, currently-true gap — see
  BuildLog.md "First real production pipeline run" on why the benchmark
  tickers themselves aren't backfilled yet). PnDDetector/ExitSignalModel
  remain trained on their own synthetic archives regardless of
  --real-data — no real P&D-confirmed or exit-outcome archive exists yet
  to train them on (same Phase 1 gap as every other model in this
  project); --real-data is about the price/feature data the *signal*
  model walk-forward-trains and is evaluated against, not a claim that
  every model here is now real-data-trained end to end.

Prints BacktestIntegrityChecker results and per-fold/aggregate metrics,
then writes backtest/reports/phase1_YYYYMMDD.json.
"""

import argparse
import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from backtest.engine import BacktestEngine
from config.timezone import now_ist
from config.universe import get_tickers, load_universe
from datastore.client import DataStoreClient
from features.technical import BENCHMARK_TICKERS
from systems.ml_signal_engine.inference.train_all_phase1 import _generate_synthetic_universe
from systems.ml_signal_engine.models.exit.exit_signal import ExitSignalModel
from systems.ml_signal_engine.models.exit.exit_signal import generate_synthetic_training_data as exit_synthetic_data
from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector
from systems.ml_signal_engine.models.pnd.pnd_detector import generate_synthetic_training_data as pnd_synthetic_data
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent / "reports"
# No real sector/industry mapping ingested yet — a deterministic round-robin
# placeholder is enough to exercise PortfolioSimulator's MAX_SECTOR_PCT gate;
# same "documented synthetic stand-in" pattern as every other Phase 1 gap.
SYNTHETIC_SECTORS = ["IT", "BANKING", "PHARMA", "FMCG", "AUTO", "ENERGY", "METALS", "REALTY"]
# A real benchmark series shorter than this can't populate Category 7
# (relative-strength) features at all — not worth using over the synthetic
# fallback. Matches the 252-day (1 trading year) bar used elsewhere in this
# project for "enough history to be useful."
MIN_BENCHMARK_ROWS = 252
REAL_DATA_LOOKBACK_YEARS = 5


def _build_sector_map(tickers) -> Dict[str, str]:
    return {t: SYNTHETIC_SECTORS[i % len(SYNTHETIC_SECTORS)] for i, t in enumerate(sorted(tickers))}


def _real_sector_map() -> Dict[str, str]:
    """Real ticker -> sector mapping (loaded via config.universe, not a direct DB query — SPEC-DS-002)."""
    universe = load_universe()
    return dict(zip(universe["ticker"], universe["sector"]))


def _fetch_real_universe(
    max_tickers: Optional[int], min_history_days: int, api_base_url: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch real OHLCV for config.universe.get_tickers()'s curated universe,
    via DataStoreClient (SPEC-DS-002), filtered to tickers with at least
    min_history_days rows.

    Returns
    -------
    pd.DataFrame
        Long-format: date, ticker, open, high, low, close, volume.

    Raises
    ------
    ValueError
        If no ticker meets min_history_days.
    """
    client = DataStoreClient(base_url=api_base_url) if api_base_url else DataStoreClient()
    tickers = get_tickers()
    if max_tickers:
        tickers = tickers[:max_tickers]

    to_dt = now_ist()
    from_dt = to_dt - timedelta(days=365 * REAL_DATA_LOOKBACK_YEARS)

    frames = []
    for ticker in tickers:
        rows = client.get_ohlcv(ticker, from_dt, to_dt)
        if len(rows) >= min_history_days:
            df = pd.DataFrame(rows)[["date", "ticker", "open", "high", "low", "close", "volume", "adj_factor"]]
            frames.append(df)

    if not frames:
        raise ValueError(f"no ticker in the universe has >= {min_history_days} rows of real OHLCV history")

    ohlcv = pd.concat(frames, ignore_index=True)
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    n_used = ohlcv["ticker"].nunique()
    logger.info(f"real data: {n_used}/{len(tickers)} universe tickers had >= {min_history_days} rows")
    return ohlcv


def _fetch_real_benchmark(api_base_url: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    Attempt to fetch a real benchmark (NIFTYBEES/NIF100BEES/MONIFTY500) via
    DataStoreClient. Returns None (caller falls back to synthetic) if every
    series is shorter than MIN_BENCHMARK_ROWS — a real, currently-true gap:
    BENCHMARK_TICKERS are never in scope for ingestion.backfill_runner's
    universe loop, only config.universe.get_tickers()'s investable universe.
    """
    client = DataStoreClient(base_url=api_base_url) if api_base_url else DataStoreClient()
    to_dt = now_ist()
    from_dt = to_dt - timedelta(days=365 * REAL_DATA_LOOKBACK_YEARS)

    series = {}
    for name, ticker in BENCHMARK_TICKERS.items():
        rows = client.get_ohlcv(ticker, from_dt, to_dt)
        if len(rows) >= MIN_BENCHMARK_ROWS:
            df = pd.DataFrame(rows)[["date", "close"]].rename(columns={"close": f"{name}_close"})
            df["date"] = pd.to_datetime(df["date"])
            series[name] = df

    if not series:
        logger.warning(
            f"real benchmark unavailable (all of {list(BENCHMARK_TICKERS.values())} have "
            f"< {MIN_BENCHMARK_ROWS} rows) — falling back to the synthetic benchmark generator"
        )
        return None

    benchmark = None
    for df in series.values():
        benchmark = df if benchmark is None else benchmark.merge(df, on="date", how="outer")
    return benchmark.sort_values("date").reset_index(drop=True)


def _fetch_historical_tickers(api_base_url: Optional[str] = None) -> set:
    """Every ticker ohlcv_adjusted has ever seen (GET /api/v1/ohlcv/_meta/tickers) — for check_04_survivorship."""
    client = DataStoreClient(base_url=api_base_url) if api_base_url else DataStoreClient()
    response = client.get_universe_tickers()
    return set(response["tickers"])


def run_phase1_backtest(
    n_tickers: int = 40, n_days: int = 400, folds: int = 5, optuna_trials: int = 5,
    n_target_positions: int = 10, seed: int = 42, check_only: bool = False,
    use_real_data: bool = False, max_real_tickers: Optional[int] = None,
    min_history_days: int = 252, api_base_url: Optional[str] = None,
) -> dict:
    run_date = now_ist()

    if use_real_data:
        logger.info("Phase 1 backtest starting: REAL data (config.universe.get_tickers() via DataStoreClient)")
        ohlcv = _fetch_real_universe(max_real_tickers, min_history_days, api_base_url)
        sector_map = _real_sector_map()
        benchmark = _fetch_real_benchmark(api_base_url)
        universe_tickers = set(get_tickers())
        historical_tickers = _fetch_historical_tickers(api_base_url)
        engine_kwargs = {
            "benchmark": benchmark, "universe_tickers": universe_tickers, "historical_tickers": historical_tickers,
        }
    else:
        logger.info(f"Phase 1 backtest starting: {n_tickers} synthetic tickers x {n_days} days, seed={seed}")
        ohlcv = _generate_synthetic_universe(n_tickers, n_days, seed=seed)
        sector_map = _build_sector_map(ohlcv["ticker"].unique())
        engine_kwargs = {}

    pnd_X, pnd_y = pnd_synthetic_data(n_positive=30, n_negative=970, n_days=90, seed=seed)
    pnd_detector = PnDDetector(random_state=seed)
    pnd_detector.train(pnd_X, pnd_y)
    logger.info("P&D pre-filter trained (synthetic archive)")

    exit_X, urgency, exit_type, duration, event = exit_synthetic_data(n=800, seed=seed)
    exit_model = ExitSignalModel(random_state=seed)
    exit_diag = exit_model.train_full(exit_X, urgency, exit_type, duration, event)
    logger.info(f"Exit signal model trained: {exit_diag}")

    engine = BacktestEngine(
        ohlcv=ohlcv, pnd_detector=pnd_detector, exit_model=exit_model, signal_model_cls=Signal5DModel,
        sector_map=sector_map, initial_capital=1_000_000.0, sizing_mode="equal_weight",
        n_target_positions=n_target_positions, optuna_trials=optuna_trials, random_state=seed, n_folds=folds,
        **engine_kwargs,
    )
    logger.info(f"Feature/label dataset built: {engine._combined.shape}")

    results = engine.run_full_backtest("signal_5d", folds=folds)

    print("\n=== Backtest Integrity Checks ===")
    print(f"  PASSED: {results.integrity_passed}")
    for failure in results.integrity_detail.get("critical_failures", []):
        print(f"  CRITICAL: {failure}")

    if check_only:
        # Gate-check mode (🔒 PHASE 1 GATE CHECK item 2): report integrity
        # results only, skip the full fold/aggregate metrics printout and
        # the JSON report write — this is a fast pass/fail check, not a
        # full backtest run.
        return results.to_dict()

    print("\n=== Per-Fold Metrics ===")
    for f in results.fold_results:
        print(
            f"  Fold {f.fold_index}: train [{f.train_start.date()} -> {f.train_end.date()}] "
            f"test [{f.test_start.date()} -> {f.test_end.date()}] "
            f"CAGR={f.cagr:.2%} Sharpe={f.sharpe:.2f} MaxDD={f.max_drawdown:.2%} "
            f"WinRate={f.win_rate:.2%} ProfitFactor={f.profit_factor:.2f} Trades={f.n_trades}"
        )

    print("\n=== Aggregate Metrics ===")
    for k, v in results.aggregate.items():
        print(f"  {k}: {v}")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"phase1_{run_date.strftime('%Y%m%d')}.json"
    with open(report_path, "w") as fh:
        json.dump(results.to_dict(), fh, indent=2, default=str)
    print(f"\nReport written to {report_path}")

    return results.to_dict()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the first Phase 1 backtest (Signal5D + MetaLabeler + P&D + Exit)")
    parser.add_argument("--tickers", type=int, default=40, help="Synthetic mode only")
    parser.add_argument("--days", type=int, default=400, help="Synthetic mode only")
    parser.add_argument("--folds", type=int, default=5)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--quick", action="store_true", help="Small/fast synthetic run for smoke-testing")
    parser.add_argument(
        "--check-only", action="store_true",
        help="Gate-check mode: run BacktestIntegrityChecker only (implies --quick), print PASS/FAIL, exit 1 on failure",
    )
    parser.add_argument(
        "--real-data", action="store_true",
        help="Use real OHLCV/sector/benchmark data (DataStoreClient) instead of the synthetic universe",
    )
    parser.add_argument("--max-real-tickers", type=int, default=None, help="--real-data only: cap universe size")
    parser.add_argument(
        "--min-history-days", type=int, default=252,
        help="--real-data only: exclude tickers with fewer real OHLCV rows than this",
    )
    args = parser.parse_args()

    quick = args.quick or args.check_only
    n_tickers, n_days, trials = (15, 200, 2) if quick else (args.tickers, args.days, args.trials)
    folds = 2 if args.check_only else args.folds
    results = run_phase1_backtest(
        n_tickers=n_tickers, n_days=n_days, folds=folds, optuna_trials=trials, check_only=args.check_only,
        use_real_data=args.real_data, max_real_tickers=args.max_real_tickers,
        min_history_days=args.min_history_days,
    )

    if args.check_only and not results["integrity_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
