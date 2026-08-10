"""
scripts/export_backtest_csv.py

Phase: 3.x (Backtest — reporting/diagnosis)
Owner: Platform / Backtest
Consumers: analyst CSV export, ad-hoc diagnosis

Exports a per-strategy summary CSV and a per-strategy monthly-returns CSV
from stored orchestrator backtest reports, for offline diagnosis of raw
CAGR relative to the Nifty 500.

WHY AN EQUITY CURVE IS RECONSTRUCTED HERE
-----------------------------------------
backtest/core/engine.py computes a real `portfolio.equity_curve` at runtime
but only persists `metrics.cash_position_series` — the curve itself is
dropped. Volatility, time-in-market, average exposure and monthly returns
all need equity, not cash, so this script rebuilds a daily equity series as

    equity(t) = cash(t) + SUM over positions open at t of qty * close(t)

using the run's own trade log (buy_date/sale_date/qty per position) and real
`ohlcv_adjusted` closes. This is a RECONSTRUCTION, not the engine's own
curve: intraday timing, partial fills and the engine's tax outflow are not
replayed, so the reconstructed final equity can differ slightly from the
run's reported final_capital. The script emits `EquityReconDriftPct` per row
so that error is visible rather than assumed away — treat a large drift as a
reason to distrust that row's Volatility/Exposure/monthly figures, not the
CAGR/Sharpe/drawdown ones (which come from the engine's own metrics).

R-MULTIPLES
-----------
AvgWin_R / AvgLoss_R are only meaningful when a per-trade risk unit exists.
"R" here is the run's configured stop-loss fraction; runs on the
`unconstrained` exit variant have NO stop, so R is undefined and those
columns are left blank rather than filled with a made-up denominator.
AvgWin_Pct / AvgLoss_Pct are always emitted and need no such assumption.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DUCKDB_PATH  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent.parent / "backtest" / "reports"
BENCHMARK_INDEX = "Nifty 500"
TRADING_DAYS = 252


def _load_reports(suffixes: List[str]) -> List[Dict[str, Any]]:
    out = []
    for suffix in suffixes:
        for path in sorted(glob.glob(str(REPORTS_DIR / f"orchestrator_{suffix}_job*.json"))):
            try:
                with open(path) as fh:
                    d = json.load(fh)
                d["_report_path"] = path
                d["_suffix"] = suffix
                out.append(d)
            except (OSError, ValueError) as exc:
                logger.warning("skipping unreadable report %s: %s", path, exc)
    return out


def _closes(conn, tickers: List[str], start: str, end: str) -> pd.DataFrame:
    """Wide date x ticker close panel from real ohlcv_adjusted rows."""
    if not tickers:
        return pd.DataFrame()
    ph = ",".join("?" * len(tickers))
    df = conn.execute(
        f"""SELECT date, ticker, close FROM ohlcv_adjusted
            WHERE ticker IN ({ph}) AND date BETWEEN ? AND ? AND close IS NOT NULL""",
        [*tickers, start, end],
    ).df()
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    return df.pivot_table(index="date", columns="ticker", values="close", aggfunc="last").sort_index()


def _reconstruct_equity(cash_series: pd.Series, trades: pd.DataFrame, closes: pd.DataFrame) -> pd.Series:
    """cash(t) + market value of every position open at t. Positions are held
    [buy_date, sale_date); a still-open position runs to the last date."""
    if cash_series.empty:
        return pd.Series(dtype=float)
    holdings = pd.DataFrame(0.0, index=cash_series.index, columns=list(closes.columns))
    for _, tr in trades.iterrows():
        t = tr["ticker"]
        if t not in holdings.columns:
            continue
        start = pd.Timestamp(tr["buy_date"])
        end = pd.Timestamp(tr["sale_date"]) if pd.notna(tr["sale_date"]) else cash_series.index[-1]
        mask = (holdings.index >= start) & (holdings.index < end)
        holdings.loc[mask, t] += float(tr["qty"] or 0)
    aligned = closes.reindex(holdings.index).ffill()
    market_value = (holdings * aligned).sum(axis=1, skipna=True)
    return cash_series + market_value


def _benchmark(conn, start: str, end: str) -> Optional[pd.Series]:
    df = conn.execute(
        "SELECT date, close FROM index_ohlcv WHERE index_name = ? AND date BETWEEN ? AND ? "
        "AND close IS NOT NULL ORDER BY date",
        [BENCHMARK_INDEX, start, end],
    ).df()
    if len(df) < 2:
        return None
    return pd.Series(df["close"].values, index=pd.to_datetime(df["date"]), dtype=float)


def _safe_div(a, b):
    return a / b if (b not in (None, 0) and pd.notna(b)) else None


def _pct(v):
    return round(100.0 * v, 4) if v is not None and pd.notna(v) else ""


def build_rows(reports: List[Dict[str, Any]], conn) -> tuple:
    summary_rows, monthly_rows = [], []
    for rep in reports:
        run, metrics = rep.get("run") or {}, rep.get("metrics") or {}
        # The orchestrator nests strategy parameters under run["config"];
        # reading run directly yielded top_n=None and collapsed every variant
        # of a template onto one StrategyID.
        cfg = {**run, **(run.get("config") or {})} if isinstance(run, dict) else {}
        template = cfg.get("template_name") or cfg.get("strategy_id") or "?"
        start, end = str(cfg.get("start_date") or "")[:10], str(cfg.get("end_date") or "")[:10]
        if not start or not end:
            continue
        exit_variant = rep.get("exit_policy_variant") or cfg.get("exit_variant") or ""
        top_n = cfg.get("top_n")
        # Every parameter that is swept must appear in the ID, or variants
        # collapse onto one row and "one row per strategy" silently becomes
        # "one row per several strategies".
        _parts = [template, f"top{top_n}", exit_variant or "na"]
        if cfg.get("downtrend_filter_pct"):
            _parts.append(f"dtf{float(cfg['downtrend_filter_pct']):g}")
        if cfg.get("bear_drawdown_pct"):
            _parts.append(f"bear{float(cfg['bear_drawdown_pct']):g}")
        elif cfg.get("disable_buys_in_regime"):
            _parts.append(f"noBuy{cfg['disable_buys_in_regime']}")
        if cfg.get("min_adtv_cr"):
            _parts.append(f"adtv{float(cfg['min_adtv_cr']):g}")
        strategy_id = "_".join(str(p) for p in _parts)

        # --- trades -------------------------------------------------------
        trades = pd.DataFrame()
        tl = rep.get("trade_log_path")
        if tl and os.path.exists(tl):
            try:
                trades = pd.read_csv(tl)
            except (OSError, ValueError) as exc:
                logger.warning("%s: unreadable trade log: %s", strategy_id, exc)
        n_trades = len(trades)
        wins = trades[trades["pnl_pct"] > 0]["pnl_pct"] if n_trades else pd.Series(dtype=float)
        losses = trades[trades["pnl_pct"] <= 0]["pnl_pct"] if n_trades else pd.Series(dtype=float)
        avg_win_pct = wins.mean() if len(wins) else None
        avg_loss_pct = losses.mean() if len(losses) else None

        # R-multiples only where a real risk unit exists (see module docstring).
        stop = cfg.get("exit_stop_pct") or cfg.get("stop_pct")
        r_unit = abs(float(stop)) if stop else None
        avg_win_r = _safe_div(avg_win_pct, r_unit) if (avg_win_pct is not None and r_unit) else None
        avg_loss_r = _safe_div(avg_loss_pct, r_unit) if (avg_loss_pct is not None and r_unit) else None

        # --- equity reconstruction ---------------------------------------
        cash_raw = metrics.get("cash_position_series") or []
        equity = pd.Series(dtype=float)
        recon_drift = None
        if cash_raw:
            cash = pd.Series(
                [float(p["cash"]) for p in cash_raw],
                index=pd.to_datetime([p["date"] for p in cash_raw]),
                dtype=float,
            ).sort_index()
            tickers = sorted(set(trades["ticker"].dropna())) if n_trades else []
            closes = _closes(conn, tickers, start, end)
            equity = _reconstruct_equity(cash, trades, closes) if not closes.empty else cash
            reported_final = metrics.get("final_capital")
            if reported_final and len(equity) and equity.iloc[-1]:
                recon_drift = equity.iloc[-1] / float(reported_final) - 1.0

        # Reconstruction is only trustworthy when the rebuilt final equity
        # lands near the engine's own final_capital. Trade logs carry CLOSED
        # trades, so a run still holding positions at the end reconstructs
        # low; rather than publish quietly-wrong Volatility/Exposure/monthly
        # numbers, those columns are left blank past this tolerance.
        MAX_RECON_DRIFT = 0.02
        recon_ok = recon_drift is not None and abs(recon_drift) <= MAX_RECON_DRIFT
        vol = time_in_mkt = avg_expo = None
        if len(equity) > 2 and recon_ok:
            rets = equity.pct_change().dropna()
            vol = float(rets.std() * np.sqrt(TRADING_DAYS)) if len(rets) > 1 else None
            invested = equity - pd.Series(
                [float(p["cash"]) for p in cash_raw],
                index=pd.to_datetime([p["date"] for p in cash_raw]),
                dtype=float,
            ).sort_index().reindex(equity.index).ffill()
            expo = (invested / equity).replace([np.inf, -np.inf], np.nan).dropna()
            if len(expo):
                time_in_mkt = float((expo > 0.01).mean())
                avg_expo = float(expo.mean())
            # monthly returns from the reconstructed curve
            for period, val in equity.resample("ME").last().pct_change().dropna().items():
                monthly_rows.append(
                    {"StrategyID": strategy_id, "Month": period.strftime("%Y-%m"),
                     "Return_Pct": round(100.0 * float(val), 4)}
                )

        # --- benchmark ----------------------------------------------------
        bench = _benchmark(conn, start, end)
        bench_total = bench_cagr = None
        if bench is not None:
            bench_total = float(bench.iloc[-1] / bench.iloc[0] - 1.0)
            yrs = (pd.Timestamp(end) - pd.Timestamp(start)).days / 365.25
            if yrs > 0:
                bench_cagr = float((bench.iloc[-1] / bench.iloc[0]) ** (1 / yrs) - 1.0)

        cagr = metrics.get("cagr")
        final_cap, contributed = metrics.get("final_capital"), metrics.get("total_contributed")
        total_ret = _safe_div(final_cap, contributed)
        total_ret = (total_ret - 1.0) if total_ret is not None else None

        regime_logic = "none"
        if cfg.get("bear_drawdown_pct"):
            regime_logic = f"pit_drawdown_{float(cfg['bear_drawdown_pct']):.0%}"
        elif cfg.get("disable_buys_in_regime"):
            regime_logic = f"segment_{cfg['disable_buys_in_regime']}"

        summary_rows.append({
            "StrategyID": strategy_id,
            "Template": template,
            "StartDate": start,
            "EndDate": end,
            "TotalReturnPct": _pct(total_ret),
            "CAGR_Pct": _pct(cagr),
            "BenchmarkTotalReturnPct": _pct(bench_total),
            "BenchmarkCAGR_Pct": _pct(bench_cagr),
            "ExcessCAGR_Pct": _pct(cagr - bench_cagr) if (cagr is not None and bench_cagr is not None) else "",
            "Volatility_Pct": _pct(vol),
            "MaxDD_Pct": _pct(metrics.get("max_drawdown")),
            "Sharpe": round(metrics["sharpe"], 4) if metrics.get("sharpe") is not None else "",
            "Sortino": round(metrics["sortino"], 4) if metrics.get("sortino") is not None else "",
            "TimeInMarket_Pct": _pct(time_in_mkt),
            "AvgExposure_Pct": _pct(avg_expo),
            "Turnover_Pct": _pct(metrics.get("turnover_ratio")),
            "NumTrades": n_trades or (metrics.get("n_trades") or ""),
            "WinRate_Pct": _pct(metrics.get("win_rate")),
            "AvgWin_Pct": _pct(avg_win_pct),
            "AvgLoss_Pct": _pct(avg_loss_pct),
            "AvgWin_R": round(avg_win_r, 4) if avg_win_r is not None else "",
            "AvgLoss_R": round(avg_loss_r, 4) if avg_loss_r is not None else "",
            "ProfitFactor": round(metrics["profit_factor"], 4) if metrics.get("profit_factor") is not None else "",
            "EngineType": f"orchestrator/{exit_variant}" if exit_variant else "orchestrator",
            "RegimeLogic": regime_logic,
            "UniverseTag": f"{cfg.get('universe_spec') or '?'}_top{cfg.get('max_tickers') or '?'}",
            "IntegrityPassed": rep.get("integrity_passed"),
            "EquityReconDriftPct": _pct(recon_drift),
            "ReportPath": rep.get("_report_path", ""),
        })
    return summary_rows, monthly_rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Export backtest summary + monthly-returns CSVs")
    ap.add_argument("--suffix", action="append", required=True,
                    help="report suffix to include (repeatable), e.g. --suffix T_winners2021")
    ap.add_argument("--out-dir", default=str(REPORTS_DIR))
    args = ap.parse_args()

    reports = _load_reports(args.suffix)
    if not reports:
        logger.error("no reports found for suffixes %s", args.suffix)
        return 1
    logger.info("loaded %d reports", len(reports))

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    summary, monthly = build_rows(reports, conn)
    if not summary:
        logger.error("no exportable rows built")
        return 1

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = "_".join(args.suffix)
    s_path, m_path = out_dir / f"strategies_{tag}.csv", out_dir / f"monthly_returns_{tag}.csv"

    with open(s_path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(summary[0].keys()))
        w.writeheader()
        w.writerows(summary)
    logger.info("wrote %s (%d strategies)", s_path, len(summary))

    if monthly:
        with open(m_path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["StrategyID", "Month", "Return_Pct"])
            w.writeheader()
            w.writerows(monthly)
        logger.info("wrote %s (%d strategy-months)", m_path, len(monthly))
    else:
        logger.warning("no monthly rows built — equity curves could not be reconstructed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
