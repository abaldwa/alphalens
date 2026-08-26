#!/usr/bin/env python3
"""
backtest/jobs/generate_signals.py

Generate signals for a band/rebalance_cadence combination using the unified
signal generation pipeline. Signals are written to strategy_signals table and
tracked in signal_generation_ledger for reuse across backtest variants.

This is the single point of signal generation: backtest, paper trading, and
live trading all read from strategy_signals using this same generation logic.

Owner: Platform / Backtest
Consumers: run_orchestrator_backtest.py (skip_signal_generation=true path)
Date: 2026-08-26 (signal-first optimization)
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Any


from backtest.adapters.momentum_adapter import MomentumAdapter
from config.settings import BACKTEST_DUCKDB_PATH, DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from features.momentum_signal import load_price_panel
from strategies.signals import write_signals

logger = logging.getLogger(__name__)


def generate_band_signals(
    band_id: int,
    rank_start: int,
    rank_end: int,
    rank_method: str,
    top_n: int,
    rebalance_cadence_days: int,
    lookback_months: int,
    backtest_start_date: str,
    backtest_end_date: str,
    vol_scaling_mode: Optional[str] = None,
    vol_scaling_lookback_days: int = 126,
    vol_scaling_leverage_cap: Optional[float] = None,
    run_id: str = "",  # Empty for pre-gen signals (not tied to a backtest run)
) -> Dict[str, Any]:
    """
    Generate momentum signals at rebalance dates only (sparse evaluation).

    Returns:
        {
            "signal_count": int,
            "signal_dates": [date, ...],
            "error": str or None,
        }
    """

    start_dt = datetime.fromisoformat(backtest_start_date)
    end_dt = datetime.fromisoformat(backtest_end_date)
    strategy_key = (
        f"{rank_method}_M{band_id}_top{top_n}_{rebalance_cadence_days}d"
        f"{'_' + vol_scaling_mode if vol_scaling_mode else ''}"
    )

    logger.info(
        f"Generating signals: {strategy_key}, "
        f"band={band_id} ({rank_start}–{rank_end}), "
        f"cadence={rebalance_cadence_days}d, lookback={lookback_months}mo"
    )

    try:
        # Load price panel for full Nifty 800 universe
        with get_duckdb_connection(DUCKDB_PATH, read_only=True) as normalised_conn:
            # Get all tickers in the Nifty 800 universe (top 800 by ADTV, or all available)
            tickers_result = normalised_conn.execute(
                "SELECT DISTINCT ticker FROM ohlcv_adjusted ORDER BY ticker"
            ).fetchall()
            tickers = [row[0] for row in tickers_result]

            if not tickers:
                return {"signal_count": 0, "signal_dates": [], "error": "No tickers found in ohlcv_adjusted"}

            logger.info(f"Loading price panel for {len(tickers)} tickers...")
            price_panel = load_price_panel(
                normalised_conn,
                tickers,
                start_date=start_dt.date().isoformat(),
                end_date=end_dt.date().isoformat(),
            )

        if price_panel.empty:
            return {"signal_count": 0, "signal_dates": [], "error": "No price data loaded"}

        # Initialize momentum adapter (this does the heavy lifting)
        adapter = MomentumAdapter(
            price_panel=price_panel,
            top_n=top_n,
            lookback_months=lookback_months,
            rank_start=rank_start,
            rank_method=rank_method,
            vol_scaling_mode=vol_scaling_mode,
            vol_scaling_lookback_days=vol_scaling_lookback_days,
            vol_scaling_leverage_cap=vol_scaling_leverage_cap,
        )

        # Generate rebalance dates (sparse: at cadence intervals only)
        current_date = start_dt.date()
        rebalance_dates = []
        while current_date <= end_dt.date():
            rebalance_dates.append(current_date)
            current_date += timedelta(days=rebalance_cadence_days)

        logger.info(f"Rebalance dates: {len(rebalance_dates)} dates")

        # Generate signals at each rebalance date (sparse, not daily)
        all_signals = []
        for rebalance_date in rebalance_dates:
            horizon_bucket = None  # Not used for momentum

            # Get signals from adapter for this single date
            signals = adapter.generate_signals(
                rebalance_date,
                price_panel.columns.tolist(),  # universe
                horizon_bucket,
            )

            if signals:
                all_signals.extend(signals)
                logger.debug(f"  {rebalance_date}: {len(signals)} signals")

        logger.info(f"Total signals generated: {len(all_signals)}")

        # Write to strategy_signals table
        if all_signals:
            write_signals(
                all_signals,
                strategy_key=strategy_key,
                strategy_version=1,
                source="backtest",
                run_id=run_id or "",
                allow_hold=False,
                db_path=BACKTEST_DUCKDB_PATH,
            )

        return {
            "signal_count": len(all_signals),
            "signal_dates": rebalance_dates,
            "error": None,
        }

    except Exception as e:
        error_msg = f"Signal generation failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "signal_count": 0,
            "signal_dates": [],
            "error": error_msg,
        }


def update_ledger(
    strategy_key: str,
    band_id: int,
    rebalance_cadence_days: int,
    rank_method: str,
    top_n: int,
    vol_scaling_mode: Optional[str],
    backtest_start_date: str,
    backtest_end_date: str,
    lookback_months: int,
    result: Dict[str, Any],
) -> None:
    """Update signal_generation_ledger to track this completion."""

    with get_duckdb_connection(BACKTEST_DUCKDB_PATH) as conn:
        now = datetime.now()
        status = "completed" if result["error"] is None else "failed"

        conn.execute(
            """
            INSERT INTO signal_generation_ledger (
                strategy_key, band_id, rebalance_cadence_days,
                rank_method, top_n, vol_scaling_mode,
                backtest_start_date, backtest_end_date, lookback_months,
                signal_count, signal_dates_json,
                status, error_message,
                generated_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (strategy_key, band_id, rebalance_cadence_days) DO UPDATE SET
                signal_count = EXCLUDED.signal_count,
                signal_dates_json = EXCLUDED.signal_dates_json,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                generated_at = EXCLUDED.generated_at,
                completed_at = EXCLUDED.completed_at
            """,
            [
                strategy_key,
                band_id,
                rebalance_cadence_days,
                rank_method,
                top_n,
                vol_scaling_mode,
                backtest_start_date,
                backtest_end_date,
                lookback_months,
                result["signal_count"],
                json.dumps([d.isoformat() for d in result["signal_dates"]]),
                status,
                result["error"],
                now,
                now if status == "completed" else None,
            ],
        )

        logger.info(
            f"Ledger updated: {strategy_key} "
            f"({result['signal_count']} signals, status={status})"
        )


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    # Example invocation (would come from queue job params):
    # python3 backtest/jobs/generate_signals.py \
    #   --band_id 12 --rank_start 551 --rank_end 800 \
    #   --rank_method jt_momentum --top_n 5 \
    #   --rebalance_cadence_days 21 --lookback_months 6 \
    #   --backtest_start_date 2009-01-01 --backtest_end_date 2026-06-30 \
    #   --vol_scaling_mode inverse_volatility

    if len(sys.argv) < 5:
        print(
            "Usage: python generate_signals.py "
            "--band_id INT --rank_start INT --rank_end INT "
            "--rank_method {jt_momentum|equal_weight} --top_n INT "
            "--rebalance_cadence_days INT --lookback_months INT "
            "--backtest_start_date YYYY-MM-DD --backtest_end_date YYYY-MM-DD "
            "[--vol_scaling_mode MODE] [--vol_scaling_lookback_days INT] "
            "[--vol_scaling_leverage_cap FLOAT] [--run_id STR]"
        )
        sys.exit(1)

    # Parse CLI args (simple approach for now; could use argparse)
    params: Dict[str, str] = {}
    for i, arg in enumerate(sys.argv[1:]):
        if arg.startswith("--"):
            key = arg[2:]
            val = sys.argv[i + 2] if i + 2 < len(sys.argv) else ""
            params[key] = val

    result = generate_band_signals(
        band_id=int(params.get("band_id", "0")),
        rank_start=int(params.get("rank_start", "0")),
        rank_end=int(params.get("rank_end", "0")),
        rank_method=params.get("rank_method", "equal_weight"),
        top_n=int(params.get("top_n", "5")),
        rebalance_cadence_days=int(params.get("rebalance_cadence_days", "21")),
        lookback_months=int(params.get("lookback_months", "6")),
        backtest_start_date=params.get("backtest_start_date", "2009-01-01"),
        backtest_end_date=params.get("backtest_end_date", "2026-06-30"),
        vol_scaling_mode=params.get("vol_scaling_mode"),
        vol_scaling_lookback_days=int(params.get("vol_scaling_lookback_days", "126")),
        vol_scaling_leverage_cap=float(params.get("vol_scaling_leverage_cap", "9999")) if params.get("vol_scaling_leverage_cap") else None,
        run_id=params.get("run_id", ""),
    )

    rank_method = params.get("rank_method", "equal_weight")
    band_id = params.get("band_id", "0")
    top_n = params.get("top_n", "5")
    cadence_days = params.get("rebalance_cadence_days", "21")
    vol_mode = params.get("vol_scaling_mode", "")

    strategy_key = (
        f"{rank_method}_M{band_id}_top{top_n}_{cadence_days}d"
        f"{'_' + vol_mode if vol_mode else ''}"
    )

    rank_method_val = params.get("rank_method", "equal_weight")
    update_ledger(
        strategy_key=strategy_key,
        band_id=int(params.get("band_id", "0")),
        rebalance_cadence_days=int(params.get("rebalance_cadence_days", "21")),
        rank_method=rank_method_val,
        top_n=int(params.get("top_n", "5")),
        vol_scaling_mode=params.get("vol_scaling_mode"),
        backtest_start_date=params.get("backtest_start_date", "2009-01-01"),
        backtest_end_date=params.get("backtest_end_date", "2026-06-30"),
        lookback_months=int(params.get("lookback_months", "6")),
        result=result,
    )

    if result["error"]:
        logger.error(f"FAILED: {result['error']}")
        sys.exit(1)
    else:
        logger.info(f"SUCCESS: {result['signal_count']} signals generated")
        sys.exit(0)
