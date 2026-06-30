#!/usr/bin/env python3
"""
Historical paper trading simulation using pre-trained Signal5D + MetaLabeler.

Loads models once, then runs day-by-day inference on existing feature parquets
without any model retraining. Tracks paper P&L using actual OHLCV prices from
the database. Outputs a JSON report and a CSV trade log.

Usage:
    python3 scripts/run_paper_trading_sim.py                         # 90 days from 2007-01-03
    python3 scripts/run_paper_trading_sim.py --from-date 2023-01-01  # specific start
    python3 scripts/run_paper_trading_sim.py --days 180              # longer window
    python3 scripts/run_paper_trading_sim.py --from-date 2007-01-03 --days 90
"""

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from features.technical import CORE_TECHNICAL_FEATURES
from systems.ml_signal_engine.models.signal.meta_labeler import MetaLabeler
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

DUCKDB_PATH = Path("datastore/normalised/alphalens.duckdb")
FEATURES_DAILY_DIR = Path("datastore/features/daily")
MODELS_DIR = Path("datastore/models")

SIGNAL_MODEL_NAME = "signal_5d"
META_MODEL_NAME = "meta_labeler"
HOLD_DAYS = 5
N_POSITIONS = 10
INITIAL_CAPITAL = 10_000_000  # 1 crore INR
ROUNDTRIP_COST_PCT = 0.005  # ~50 bps


def _get_trading_dates(from_date: date, n_days: int) -> List[date]:
    available = sorted(
        date.fromisoformat(p.stem) for p in FEATURES_DAILY_DIR.glob("*.parquet")
    )
    start_idx = next((i for i, d in enumerate(available) if d >= from_date), None)
    if start_idx is None:
        raise ValueError(f"No feature dates on or after {from_date}")
    chosen = available[start_idx : start_idx + n_days]
    log.info("Simulation window: %s → %s (%d trading days)", chosen[0], chosen[-1], len(chosen))
    return chosen


def _load_ohlcv_panel(dates: List[date], lookahead_days: int) -> pd.DataFrame:
    """Load OHLCV for all tickers from first date to last date + lookahead buffer."""
    start = dates[0]
    # Fetch up to lookahead_days extra calendar days after end date
    end_pd = pd.Timestamp(dates[-1]) + pd.offsets.BDay(lookahead_days + 5)
    end_str = end_pd.date().isoformat()
    log.info("Loading OHLCV panel from DuckDB: %s → %s …", start, end_str)
    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as conn:
        df = conn.execute(
            """
            SELECT date, ticker, open, close
            FROM ohlcv_adjusted
            WHERE date >= ? AND date <= ?
            ORDER BY ticker, date
            """,
            [str(start), end_str],
        ).df()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    log.info("OHLCV panel: %d rows, %d tickers", len(df), df["ticker"].nunique())
    return df


def _get_nth_business_day(ohlcv: pd.DataFrame, ticker: str, after_date: date, n: int) -> Optional[Tuple[date, float]]:
    """Return (exit_date, exit_close) n business days after after_date for ticker."""
    rows = ohlcv[(ohlcv["ticker"] == ticker) & (ohlcv["date"] > after_date)].sort_values("date")
    if len(rows) >= n:
        row = rows.iloc[n - 1]
        return row["date"], float(row["close"])
    return None


def _get_next_open(ohlcv: pd.DataFrame, ticker: str, after_date: date) -> Optional[Tuple[date, float]]:
    """Return (entry_date, next_open) for ticker on the first trading day after after_date."""
    rows = ohlcv[(ohlcv["ticker"] == ticker) & (ohlcv["date"] > after_date)].sort_values("date")
    if not rows.empty:
        row = rows.iloc[0]
        return row["date"], float(row["open"])
    return None


def run_simulation(
    from_date: date,
    n_days: int,
    hold_days: int,
    n_positions: int,
    output_dir: Path,
) -> Dict:
    trading_dates = _get_trading_dates(from_date, n_days)
    ohlcv = _load_ohlcv_panel(trading_dates, lookahead_days=hold_days + 5)
    ohlcv_by_ticker = {t: g.set_index("date") for t, g in ohlcv.groupby("ticker")}

    log.info("Loading pre-trained models …")
    signal_model = Signal5DModel()
    signal_model.load(str(MODELS_DIR / SIGNAL_MODEL_NAME / f"{SIGNAL_MODEL_NAME}_current.pkl"))
    meta_model = MetaLabeler()
    meta_model.load(str(MODELS_DIR / META_MODEL_NAME / f"{META_MODEL_NAME}_current.pkl"))
    log.info("Models loaded: %d signal features, MetaLabeler threshold=%.3f",
             len(signal_model._feature_names), meta_model._threshold)

    trades: List[Dict] = []
    daily_equity = [INITIAL_CAPITAL]
    skipped_dates = 0

    for i, signal_date in enumerate(trading_dates):
        parquet = FEATURES_DAILY_DIR / f"{signal_date}.parquet"
        if not parquet.exists():
            log.warning("Missing feature parquet for %s — skipping", signal_date)
            skipped_dates += 1
            daily_equity.append(daily_equity[-1])
            continue

        fm = pd.read_parquet(parquet)
        if "ticker" not in fm.columns:
            log.warning("Feature matrix for %s has no ticker column", signal_date)
            daily_equity.append(daily_equity[-1])
            continue

        fm = fm.set_index("ticker")
        feat_cols = [c for c in signal_model._feature_names if c in fm.columns]
        if not feat_cols:
            log.warning("No overlapping feature columns for %s", signal_date)
            daily_equity.append(daily_equity[-1])
            continue

        X = fm[feat_cols].copy()
        X = X.replace([np.inf, -np.inf], np.nan)

        try:
            proba = signal_model.predict_signals(X)
            direction = signal_model.predict(X)
            meta_out = meta_model.predict_full(X)
        except Exception as e:
            log.warning("Inference failed for %s: %s", signal_date, e)
            daily_equity.append(daily_equity[-1])
            continue

        # Merge and filter: direction==BUY (1) AND meta_label_act==True
        scored = pd.DataFrame({
            "buy_prob": proba["signal_buy_prob"],
            "direction": direction,
            "meta_act": meta_out["meta_label_act"],
            "meta_prob": meta_out["meta_label_prob"],
        }, index=X.index)
        buys = scored[(scored["direction"] == 1) & (scored["meta_act"])].sort_values(
            "buy_prob", ascending=False
        ).head(n_positions)

        day_pnl = 0.0
        day_trades = 0

        for ticker, row in buys.iterrows():
            entry_info = _get_next_open(ohlcv, ticker, signal_date)
            if entry_info is None:
                continue
            entry_date, entry_price = entry_info
            if entry_price <= 0:
                continue

            exit_info = _get_nth_business_day(ohlcv, ticker, entry_date, hold_days)
            if exit_info is None:
                continue
            exit_date, exit_price = exit_info

            # Position sizing: equal weight across n_positions
            position_value = INITIAL_CAPITAL / n_positions
            shares = int(position_value / entry_price)
            if shares == 0:
                continue

            gross_return_pct = (exit_price - entry_price) / entry_price
            net_return_pct = gross_return_pct - ROUNDTRIP_COST_PCT
            trade_pnl = shares * (exit_price - entry_price) - (
                position_value * ROUNDTRIP_COST_PCT
            )

            trades.append({
                "signal_date": str(signal_date),
                "ticker": ticker,
                "entry_date": str(entry_date),
                "entry_price": round(entry_price, 2),
                "exit_date": str(exit_date),
                "exit_price": round(exit_price, 2),
                "shares": shares,
                "gross_return_pct": round(gross_return_pct * 100, 3),
                "net_return_pct": round(net_return_pct * 100, 3),
                "pnl_inr": round(trade_pnl, 2),
                "buy_prob": round(float(row["buy_prob"]), 4),
                "meta_prob": round(float(row["meta_prob"]), 4),
            })
            day_pnl += trade_pnl
            day_trades += 1

        daily_equity.append(daily_equity[-1] + day_pnl)
        if (i + 1) % 10 == 0 or i == 0:
            log.info(
                "Day %d/%d (%s): %d buys, pnl=₹%.0f, equity=₹%.0f",
                i + 1, len(trading_dates), signal_date, day_trades, day_pnl, daily_equity[-1],
            )

    # ---- Metrics ----
    if not trades:
        log.warning("No trades generated — check if models need retraining or feature columns mismatch")
        return {"trades": 0, "sharpe": None, "cagr": None}

    df_trades = pd.DataFrame(trades)
    df_equity = pd.Series(daily_equity)

    daily_returns = df_equity.pct_change().dropna()
    sharpe = float(daily_returns.mean() / daily_returns.std() * np.sqrt(252)) if daily_returns.std() > 0 else 0.0

    total_days = len(trading_dates)
    years = total_days / 252
    final_equity = daily_equity[-1]
    cagr = float((final_equity / INITIAL_CAPITAL) ** (1 / years) - 1) if years > 0 else 0.0

    win_rate = float((df_trades["net_return_pct"] > 0).mean())
    avg_win = float(df_trades.loc[df_trades["net_return_pct"] > 0, "net_return_pct"].mean()) if win_rate > 0 else 0.0
    avg_loss = float(df_trades.loc[df_trades["net_return_pct"] <= 0, "net_return_pct"].mean()) if win_rate < 1 else 0.0
    total_pnl = float(df_trades["pnl_inr"].sum())

    report = {
        "simulation_type": "historical_paper_trading",
        "from_date": str(trading_dates[0]),
        "to_date": str(trading_dates[-1]),
        "trading_days": len(trading_dates),
        "skipped_dates": skipped_dates,
        "hold_days": hold_days,
        "n_positions": n_positions,
        "initial_capital_inr": INITIAL_CAPITAL,
        "final_equity_inr": round(final_equity, 2),
        "total_pnl_inr": round(total_pnl, 2),
        "total_trades": len(df_trades),
        "win_rate_pct": round(win_rate * 100, 2),
        "avg_win_pct": round(avg_win, 3),
        "avg_loss_pct": round(avg_loss, 3),
        "sharpe_ratio": round(sharpe, 4),
        "cagr_pct": round(cagr * 100, 2),
        "model": SIGNAL_MODEL_NAME + "_current",
        "meta_model": META_MODEL_NAME + "_current",
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"paper_trading_sim_{trading_dates[0]}_{trading_dates[-1]}.json"
    trades_path = output_dir / f"paper_trading_trades_{trading_dates[0]}_{trading_dates[-1]}.csv"

    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    df_trades.to_csv(trades_path, index=False)

    log.info("=" * 60)
    log.info("PAPER TRADING SIMULATION COMPLETE")
    log.info("  Period     : %s → %s (%d days)", trading_dates[0], trading_dates[-1], len(trading_dates))
    log.info("  Trades     : %d", len(df_trades))
    log.info("  Win rate   : %.1f%%", win_rate * 100)
    log.info("  Total P&L  : ₹%.0f", total_pnl)
    log.info("  Sharpe     : %.4f", sharpe)
    log.info("  CAGR       : %.2f%%", cagr * 100)
    log.info("  Report     : %s", report_path)
    log.info("  Trades CSV : %s", trades_path)
    log.info("=" * 60)

    return report


def main():
    parser = argparse.ArgumentParser(description="Historical paper trading simulation")
    parser.add_argument(
        "--from-date", default="2007-01-03",
        help="Start date (YYYY-MM-DD). Earliest available: 2007-01-03 (default)"
    )
    parser.add_argument("--days", type=int, default=90, help="Number of trading days to simulate (default: 90)")
    parser.add_argument("--hold-days", type=int, default=HOLD_DAYS, help="Days to hold each position (default: 5)")
    parser.add_argument("--n-positions", type=int, default=N_POSITIONS, help="Max concurrent positions (default: 10)")
    parser.add_argument("--output-dir", default="paper_trading/sim_reports")
    args = parser.parse_args()

    from_date = date.fromisoformat(args.from_date)
    earliest = date(2007, 1, 3)
    if from_date < earliest:
        log.warning("Feature store starts %s — adjusting from-date from %s", earliest, from_date)
        from_date = earliest

    run_simulation(
        from_date=from_date,
        n_days=args.days,
        hold_days=args.hold_days,
        n_positions=args.n_positions,
        output_dir=Path(args.output_dir),
    )


if __name__ == "__main__":
    main()
