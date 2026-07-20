"""
backtest/run_phase1_backtest.py

Phase: 1.6 (Exit Signal + First Backtest); real-data wiring added post-P1.7
Specs: SPEC-BT-001 through SPEC-BT-004, SPEC-MODEL-002, SPEC-MODEL-006, SPEC-MODEL-007
Owner: ml_signal_engine / backtest
Consumers: operator CLI (`python3 -m backtest.run_phase1_backtest`)

Phase 1 backtest: Signal5D + MetaLabeler + P&D pre-filter entries,
ExitSignalModel-driven exits, equal-weight sizing, run through
BacktestEngine's walk-forward harness (P1.6).

Real OHLCV is fetched via DataStoreClient (SPEC-DS-002 — never a direct
DuckDB query from this consumer-layer script) for config.universe.
get_tickers()'s curated universe, filtered to tickers with enough history
to be useful; real sector mapping from config.universe.load_universe();
a real benchmark (NIFTYBEES etc.) is required — there is no synthetic
fallback for OHLCV, sector mapping, or benchmark data.

PnDDetector trains on load_pnd_training_data_from_db() (real OHLCV,
KNOWN_PND_TICKERS labels). ExitSignalModel trains on
load_exit_training_data_from_db() (real closed paper-trading positions) —
this will raise until enough real closed positions exist; see
BuildLog.md "Real data sourcing — Exit Signal".

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
from systems.ml_signal_engine.models.exit.exit_signal import ExitSignalModel, load_exit_training_data_from_db
from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector, load_pnd_training_data_from_db
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent / "reports"
# A real benchmark series shorter than this can't populate Category 7
# (relative-strength) features at all. Matches the 252-day (1 trading
# year) bar used elsewhere in this project for "enough history to be useful."
MIN_BENCHMARK_ROWS = 252
REAL_DATA_LOOKBACK_YEARS = 5


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


def _fetch_real_benchmark(api_base_url: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch a real benchmark (NIFTYBEES/NIF100BEES/MONIFTY500) via
    DataStoreClient.

    Raises
    ------
    RuntimeError
        If every benchmark ticker has fewer than MIN_BENCHMARK_ROWS real
        rows. There is no synthetic-benchmark fallback — backfill
        BENCHMARK_TICKERS via ingestion/backfill_runner.py. See
        BuildLog.md "Real data sourcing — Benchmarks".
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
        raise RuntimeError(
            f"Real benchmark unavailable: all of {list(BENCHMARK_TICKERS.values())} have "
            f"< {MIN_BENCHMARK_ROWS} rows in ohlcv_adjusted. There is no synthetic-benchmark "
            "fallback. Backfill these tickers via ingestion/backfill_runner.py. See "
            "BuildLog.md 'Real data sourcing — Benchmarks'."
        )

    benchmark = None
    for df in series.values():
        benchmark = df if benchmark is None else benchmark.merge(df, on="date", how="outer")
    return benchmark.sort_values("date").reset_index(drop=True)


def _fetch_real_benchmark_index(api_base_url: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    ML17a: real Nifty 500 index level (index_ohlcv table,
    ingestion/scrapers/nse_indices.py — NSE's own published index, not an
    ETF-price proxy) for BacktestEngine's buy-and-hold benchmark equity
    curve (benchmark_cagr/benchmark_sharpe/excess_return per fold).

    Returns
    -------
    pd.DataFrame or None
        Columns: date, close. None if index_ohlcv has no "Nifty 500" rows
        yet (e.g. before ML12 step 3's daily download_index_ohlcv step /
        scripts/backfill_index_ohlcv.py have populated real history) — no
        synthetic fallback; BacktestEngine.run_full_backtest() simply
        leaves every fold's benchmark_* fields as None in that case (see
        compute_fold_metrics's docstring).
    """
    client = DataStoreClient(base_url=api_base_url) if api_base_url else DataStoreClient()
    to_dt = now_ist()
    from_dt = to_dt - timedelta(days=365 * REAL_DATA_LOOKBACK_YEARS)
    df = client.get_index_ohlcv("Nifty 500", from_dt, to_dt)
    if df.empty:
        logger.warning(
            "No real Nifty 500 index_ohlcv history available — ML17a benchmark_cagr/"
            "benchmark_sharpe/excess_return will be None for every fold. Run "
            "scripts/backfill_index_ohlcv.py to backfill."
        )
        return None
    return df[["date", "close"]].sort_values("date").reset_index(drop=True)


def _fetch_historical_tickers(api_base_url: Optional[str] = None) -> set:
    """Every ticker ohlcv_adjusted has ever seen (GET /api/v1/ohlcv/_meta/tickers) — for check_04_survivorship."""
    client = DataStoreClient(base_url=api_base_url) if api_base_url else DataStoreClient()
    response = client.get_universe_tickers()
    return set(response["tickers"])


def run_phase1_backtest(
    folds: int = 5, optuna_trials: int = 5,
    n_target_positions: int = 10, seed: int = 42, check_only: bool = False,
    max_real_tickers: Optional[int] = None,
    min_history_days: int = 252, api_base_url: Optional[str] = None,
) -> dict:
    run_date = now_ist()

    logger.info("Phase 1 backtest starting: REAL data (config.universe.get_tickers() via DataStoreClient)")
    ohlcv = _fetch_real_universe(max_real_tickers, min_history_days, api_base_url)
    sector_map = _real_sector_map()
    benchmark = _fetch_real_benchmark(api_base_url)
    benchmark_index = _fetch_real_benchmark_index(api_base_url)
    universe_tickers = set(get_tickers())
    historical_tickers = _fetch_historical_tickers(api_base_url)
    engine_kwargs = {
        "benchmark": benchmark, "universe_tickers": universe_tickers, "historical_tickers": historical_tickers,
        "benchmark_index": benchmark_index,
    }

    pnd_X, pnd_y = load_pnd_training_data_from_db()
    pnd_detector = PnDDetector(random_state=seed)
    pnd_detector.train(pnd_X, pnd_y)
    logger.info("P&D pre-filter trained (real ohlcv_adjusted + KNOWN_PND_TICKERS)")

    exit_X, urgency, exit_type, duration, event = load_exit_training_data_from_db()
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
        bm_str = (
            f" | Benchmark CAGR={f.benchmark_cagr:.2%} Sharpe={f.benchmark_sharpe:.2f} "
            f"Excess={f.excess_return:.2%}"
            if f.benchmark_cagr is not None
            else " | Benchmark: n/a (no real index_ohlcv coverage for this fold)"
        )
        print(
            f"  Fold {f.fold_index}: train [{f.train_start.date()} -> {f.train_end.date()}] "
            f"test [{f.test_start.date()} -> {f.test_end.date()}] "
            f"CAGR={f.cagr:.2%} Sharpe={f.sharpe:.2f} MaxDD={f.max_drawdown:.2%} "
            f"WinRate={f.win_rate:.2%} ProfitFactor={f.profit_factor:.2f} Trades={f.n_trades}"
            f"{bm_str}"
        )

    print("\n=== Aggregate Metrics ===")
    for k, v in results.aggregate.items():
        print(f"  {k}: {v}")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"phase1_{run_date.strftime('%Y%m%d')}.json"
    with open(report_path, "w") as fh:
        json.dump(results.to_dict(), fh, indent=2, default=str)
    print(f"\nReport written to {report_path}")

    from backtest.adapters.ml_dual_write import dual_write_ml_run

    dual_write_ml_run(
        results, strategy_id="signal_5d", horizon_days=5, ohlcv=ohlcv,
        initial_capital=1_000_000.0, random_seed=seed,
    )

    return results.to_dict()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the first Phase 1 backtest (Signal5D + MetaLabeler + P&D + Exit)")
    parser.add_argument("--folds", type=int, default=5)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument(
        "--check-only", action="store_true",
        help="Gate-check mode: run BacktestIntegrityChecker only, print PASS/FAIL, exit 1 on failure",
    )
    parser.add_argument("--max-real-tickers", type=int, default=None, help="Cap universe size")
    parser.add_argument(
        "--min-history-days", type=int, default=252,
        help="Exclude tickers with fewer real OHLCV rows than this",
    )
    args = parser.parse_args()

    folds = 2 if args.check_only else args.folds
    results = run_phase1_backtest(
        folds=folds, optuna_trials=args.trials, check_only=args.check_only,
        max_real_tickers=args.max_real_tickers, min_history_days=args.min_history_days,
    )

    if args.check_only and not results["integrity_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
