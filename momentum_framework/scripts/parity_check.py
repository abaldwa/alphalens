"""
momentum_framework/scripts/parity_check.py

Trade-by-trade parity checker: runs the SAME config through the legacy
engine (backtest/run_orchestrator_backtest.py) and the framework's
native engine (backtesting/orchestrator.py::run_native()), then diffs
their trade logs. This is THE gate docs/MIGRATION.md has flagged since
the framework's first day — nothing before this script has ever actually
compared the two engines' output.

SAFETY: legacy runs are isolated via BACKTEST_DUCKDB_PATH (the project's
own documented mechanism, see run_orchestrator_backtest.py) pointed at a
throwaway file — this NEVER writes to the main backtest store or the
production DB. See CLAUDE.md's DuckDB concurrency section for why this
matters (single-writer; the main store must stay available for real work).

WHAT "PARITY" MEANS HERE: the two engines are NOT expected to produce
identical share counts or capital figures — the native engine
(backtesting/portfolio.py) deliberately omits costs, tax, and slippage
that the legacy engine models (see that file's own docstring). Parity
here means SIGNAL agreement: on each rebalance date, did both engines
decide to buy/sell the SAME tickers? That is the property a strategy's
rebalance() logic actually controls; execution-level dollar figures are
a different, later concern (see docs/MIGRATION.md's cutover criteria).

Run: PYTHONPATH=. python3 momentum_framework/scripts/parity_check.py
"""

import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

PARITY_DB_DIR = Path(__file__).resolve().parent.parent / "cache" / "parity_check_dbs"


def run_legacy(job_spec: Dict[str, Any], isolated_db_path: Path) -> Dict[str, Any]:
    """
    Invokes backtest/run_orchestrator_backtest.py's callable directly
    (in-process import, not a subprocess — faster and lets us catch
    exceptions cleanly).

    ISOLATION (fixed 2026-09-04 after a real leak — see
    project_parity_check_first_result_divergence memory): TWO separate
    mechanisms must both be redirected, not one:
      1. backtest/run_orchestrator_backtest.py reads env var
         BACKTEST_DUCKDB_PATH at import time — covers backtest_runs,
         trade_book.
      2. strategies/db.py (the strategy_signals ledger) resolves its
         path through config/settings.py::BACKTEST_DUCKDB_PATH, which is
         a MODULE-LEVEL Path computed from env var
         ALPHALENS_BACKTEST_DUCKDB_PATH (a DIFFERENT name) at config
         module import time — setting the env var alone does nothing if
         config.settings was already imported (near-certain, as a core
         settings module). Directly monkeypatched below as well as
         env-var-set, so it's redirected regardless of import order.
    A first run of this script (2026-09-04) leaked 19 rows into the real
    strategy_signals table via gap #2 alone — cleaned up by hand at the
    time; this fix is what prevents a repeat.
    """
    from datetime import date as date_type

    import config.settings as settings_module

    env_backup = os.environ.get("BACKTEST_DUCKDB_PATH")
    env_backup_alphalens = os.environ.get("ALPHALENS_BACKTEST_DUCKDB_PATH")
    settings_backup = settings_module.BACKTEST_DUCKDB_PATH

    os.environ["BACKTEST_DUCKDB_PATH"] = str(isolated_db_path)
    os.environ["ALPHALENS_BACKTEST_DUCKDB_PATH"] = str(isolated_db_path)
    settings_module.BACKTEST_DUCKDB_PATH = isolated_db_path
    try:
        # Imported here, not at module top, so BACKTEST_DUCKDB_PATH is set
        # BEFORE run_orchestrator_backtest.py's module-level code reads it
        # (see that file's line ~109: read once at import time).
        for mod_name in list(sys.modules):
            if mod_name.startswith("backtest.run_orchestrator_backtest"):
                del sys.modules[mod_name]
        from backtest.run_orchestrator_backtest import run_orchestrator_backtest

        report = run_orchestrator_backtest(
            channel="momentum",
            start_date=date_type.fromisoformat(job_spec["start_date"]),
            end_date=date_type.fromisoformat(job_spec["end_date"]),
            capital_mode="lump",
            initial_capital=job_spec["initial_capital"],
            top_n=job_spec["top_n"],
            lookback_months=job_spec["lookback_months"],
            rank_method=job_spec["rank_method"],
            skip_months=job_spec.get("skip_months", 0),
            crash_regime_enabled=job_spec.get("crash_regime_enabled", False),
            rank_band_id=job_spec["rank_band_id"],
            rebalance_cadence_days=job_spec["rebalance_cadence_days"],
            exit_policy_variant=job_spec.get("exit_variant", "unconstrained"),
            weight_method=job_spec.get("weight_method"),
            vol_scaling_mode=job_spec.get("vol_scaling_mode"),
            vol_target_enabled=job_spec.get("vol_target_enabled", False),
            strategy_family="R",
            report_suffix=f"parity_check_{job_spec['strategy_family']}",
        )
        return report
    finally:
        if env_backup is not None:
            os.environ["BACKTEST_DUCKDB_PATH"] = env_backup
        else:
            os.environ.pop("BACKTEST_DUCKDB_PATH", None)
        if env_backup_alphalens is not None:
            os.environ["ALPHALENS_BACKTEST_DUCKDB_PATH"] = env_backup_alphalens
        else:
            os.environ.pop("ALPHALENS_BACKTEST_DUCKDB_PATH", None)
        settings_module.BACKTEST_DUCKDB_PATH = settings_backup


def load_legacy_trades(report: Dict[str, Any]) -> pd.DataFrame:
    """
    Legacy backtest/export_trade_book.py's format is ONE ROW PER ROUND-
    TRIP position (columns: ticker, qty, buy_date, buy_price, sale_date,
    sale_price, ...) — verified 2026-09-04 against a real run, not
    assumed. Exploded here into one row per (date, ticker, action) event
    — a "buy" event at buy_date and a "sell" event at sale_date per
    round trip — to match native_trades_to_df()'s per-event shape.
    """
    trade_log_path = report.get("trade_log_path")
    if not trade_log_path or not Path(trade_log_path).exists():
        return pd.DataFrame(columns=["date", "ticker", "action"])
    df = pd.read_csv(trade_log_path)

    if {"ticker", "buy_date", "sale_date"}.issubset(df.columns):
        buys = df[["buy_date", "ticker"]].rename(columns={"buy_date": "date"})
        buys["action"] = "buy"
        sells = df[["sale_date", "ticker"]].dropna(subset=["sale_date"]).rename(columns={"sale_date": "date"})
        sells["action"] = "sell"
        return pd.concat([buys, sells], ignore_index=True)[["date", "ticker", "action"]]

    date_col = next((c for c in ["date", "trade_date", "signal_date"] if c in df.columns), None)
    action_col = next((c for c in ["action", "side", "trade_type"] if c in df.columns), None)
    if date_col is None or "ticker" not in df.columns or action_col is None:
        raise ValueError(f"Unrecognized legacy trade log columns: {list(df.columns)}")
    return df.rename(columns={date_col: "date", action_col: "action"})[["date", "ticker", "action"]]


def native_trades_to_df(trades: List[Dict[str, Any]]) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame(columns=["date", "ticker", "action"])
    df = pd.DataFrame(trades)
    return df[["date", "ticker", "action"]]


def compare_trade_logs(legacy_df: pd.DataFrame, native_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Signal-level comparison: does each engine agree on WHICH tickers were
    bought/sold on WHICH dates? Not share counts or prices — see module
    docstring for why those are out of scope for this comparison.
    """
    def _key_set(df: pd.DataFrame, action_filter: str) -> Set[Tuple[str, str]]:
        mask = df["action"].str.lower().str.contains(action_filter, na=False)
        subset: pd.DataFrame = df.loc[mask]
        # pandas-stubs mis-infers .astype(str).tolist()'s element type here
        # (reports list[bool] despite the runtime values being strings,
        # confirmed via this script's own real output all session) —
        # suppressed at the zip() call, not the actual data.
        dates = subset["date"].astype(str).tolist()
        tickers = subset["ticker"].astype(str).tolist()
        return set(zip(dates, tickers))  # type: ignore[arg-type]

    legacy_buys = _key_set(legacy_df, "buy")
    native_buys = _key_set(native_df, "buy")
    legacy_sells = _key_set(legacy_df, "sell")
    native_sells = _key_set(native_df, "sell")

    buy_agreement = legacy_buys & native_buys
    buy_only_legacy = legacy_buys - native_buys
    buy_only_native = native_buys - legacy_buys

    total_legacy = len(legacy_buys) or 1
    return {
        "legacy_buy_count": len(legacy_buys),
        "native_buy_count": len(native_buys),
        "buy_agreement_count": len(buy_agreement),
        "buy_agreement_pct": len(buy_agreement) / total_legacy * 100,
        "buy_only_legacy_sample": sorted(buy_only_legacy)[:10],
        "buy_only_native_sample": sorted(buy_only_native)[:10],
        "legacy_sell_count": len(legacy_sells),
        "native_sell_count": len(native_sells),
    }


def check_parity(
    strategy_factory, start_date: str, end_date: str, prod_conn: Any,
    initial_capital: float = 1_000_000.0, verbose: bool = True,
) -> Dict[str, Any]:
    """
    Runs both engines for one config, returns the comparison dict.
    `strategy_factory` — zero-arg callable returning a fresh strategy
    instance (see project_windowed_backtest_analysis memory for why this
    matters: never reuse a StrategyAdapter across runs).

    The legacy job_spec is derived from BacktestOrchestrator.
    build_legacy_job_spec() (via a throwaway strategy instance), NOT
    hand-built here — that translation is already tested and used by
    every other caller; hand-rolling it per call site is exactly the bug
    class metrics/nomenclature.py's module docstring warns about (ad hoc
    duplicated identity/config construction silently drifting out of
    sync with what a strategy actually does). Needed once this script
    scaled from one hardcoded R01 config to covering all 13 strategies.
    """
    from momentum_framework.backtesting.orchestrator import BacktestConfig, BacktestOrchestrator

    config = BacktestConfig(start_date=start_date, end_date=end_date, initial_capital=initial_capital)
    job_spec = BacktestOrchestrator(strategy_factory(), config).build_legacy_job_spec()

    PARITY_DB_DIR.mkdir(parents=True, exist_ok=True)
    # Reserve a unique filename, then immediately free the path — DuckDB
    # must create the file itself (an empty pre-created file is not a
    # valid DuckDB database and duckdb.connect() rejects it outright).
    with tempfile.NamedTemporaryFile(dir=PARITY_DB_DIR, suffix=".duckdb", delete=False) as tmp:
        isolated_db_path = Path(tmp.name)
    isolated_db_path.unlink()

    try:
        if verbose:
            print(f"Running legacy engine (isolated DB: {isolated_db_path})...")
        legacy_report = run_legacy(job_spec, isolated_db_path)
        legacy_trades = load_legacy_trades(legacy_report)
        if verbose:
            print(f"  Legacy: {len(legacy_trades)} trade rows, Sharpe={legacy_report.get('metrics', {}).get('sharpe_ratio')}")

        if verbose:
            print("Running native engine...")
        native_strategy = strategy_factory()  # fresh instance — never reuse across engines/runs
        native_result = BacktestOrchestrator(native_strategy, config).run_native(prod_conn)
        native_trades = native_trades_to_df(native_result.trades or [])
        if verbose:
            print(f"  Native: {len(native_trades)} trade rows, Sharpe={native_result.sharpe()}")

        comparison = compare_trade_logs(legacy_trades, native_trades)
        comparison["legacy_metrics"] = legacy_report.get("metrics", {})
        comparison["native_metrics"] = native_result.metrics
        comparison["native_result"] = native_result
        return comparison
    finally:
        isolated_db_path.unlink(missing_ok=True)


if __name__ == "__main__":
    import duckdb as duckdb_mod
    from momentum_framework.strategies.r01_trailing_momentum import R01TrailingMomentum

    prod_conn = duckdb_mod.connect(
        "/home/amit/projects/AlphaLens/datastore/normalised/alphalens.duckdb", read_only=True
    )

    result = check_parity(
        strategy_factory=lambda: R01TrailingMomentum(
            band_id=2, top_n=5, lookback_months=3, rebalance_cadence_days=21),
        start_date="2023-01-01", end_date="2023-06-30", prod_conn=prod_conn,
    )

    print("\n=== Parity Report: M02, R01, 2023 H1 ===")
    for k, v in result.items():
        if k == "native_result":
            continue  # the BacktestResult object — programmatic access only, not printable here
        print(f"  {k}: {v}")
