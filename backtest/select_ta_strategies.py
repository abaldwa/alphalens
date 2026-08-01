"""
backtest/select_ta_strategies.py

Ranks all technical-analysis orchestrator backtest runs in backtest/reports/
and shortlists the top N by risk-adjusted, regime-stable, cost-aware
performance -- with correlation-based de-duplication and category coverage.

Usage:
    python backtest/select_ta_strategies.py [--top 10] [--min-trades 30]
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys

import numpy as np
import pandas as pd

REPORTS_DIR = os.path.join(os.path.dirname(__file__), "reports")

TEMPLATE_LABELS = {
    "A1": "BB Squeeze Breakout", "A2": "MACD Histogram Divergence",
    "A3": "Williams %R Mean Reversion", "A4": "RSI Oversold + Trend",
    "B1": "Weinstein Stage 2", "B2": "IBD Base Breakout", "B3": "Darvas Box",
    "B4": "AVWAP Support", "B5": "Livermore Pivot",
    "C1": "Time Series Momentum", "C2": "Cross-Sectional Momentum",
    "C3": "Dual Momentum", "C4": "CAN SLIM proxy", "C5": "52-Week High Proximity",
    "C6": "EMA Ribbon Alignment", "C7": "Post-Earnings Drift",
    "D1": "RSI-2 Mean Reversion", "D2": "Long-Horizon Contrarian",
    "D3": "MACD + RSI Divergence", "D4": "IBD Follow-Through Day",
    "E1": "Turtle Donchian", "E2": "Minervini SEPA", "E3": "Piotroski F proxy",
    "E4": "Sector Rotation", "E5": "Earnings Acceleration", "E6": "GARP Momentum",
    "E7": "Greenblatt Magic Formula proxy",
    "F1": "Low RSI Quality", "F2": "Momentum + Volume",
    "F3": "Dividend/Consistent Growth proxy", "F4": "Compounder proxy",
    "F5": "Cash Flow King proxy", "F6": "Turnaround proxy",
    "F7": "Promoter Confidence proxy", "F8": "PEG proxy",
    "S001": "EMA Crossover", "S002": "Supertrend Breakout",
    "S003": "RSI Mean Reversion", "S004": "52-Week High Breakout",
    "S005": "VWAP Reversal", "S006": "Ichimoku Cloud Breakout",
    "S008": "MACD Histogram",
}


def load_technical_runs(min_trades: int) -> pd.DataFrame:
    rows = []
    for path in glob.glob(os.path.join(REPORTS_DIR, "orchestrator_*.json")):
        try:
            with open(path) as fh:
                d = json.load(fh)
        except (json.JSONDecodeError, OSError):
            continue
        run = d.get("run", {})
        if run.get("channel") != "technical":
            continue
        metrics = d.get("metrics", {})
        n_trades = metrics.get("n_trades") or 0
        if n_trades < min_trades:
            continue
        config = run.get("config", {})
        template = config.get("template_name")
        if not template:
            continue

        regimes = d.get("regime_breakdown") or []
        regime_cagrs = [r["cagr"] for r in regimes if r.get("n_trades", 0) >= 5 and r.get("cagr") is not None]
        regime_sharpes = [r["sharpe"] for r in regimes if r.get("n_trades", 0) >= 5 and r.get("sharpe") is not None]
        frac_regimes_positive = (
            float(np.mean([c > 0 for c in regime_cagrs])) if regime_cagrs else np.nan
        )
        min_regime_sharpe = min(regime_sharpes) if regime_sharpes else np.nan

        rows.append({
            "run_id": run.get("run_id"),
            "strategy_id": run.get("strategy_id"),
            "template": template,
            "category": template[0],
            "label": TEMPLATE_LABELS.get(template, template),
            "horizon_bucket": run.get("horizon_bucket"),
            "exit_variant": config.get("exit_variant"),
            "cagr": metrics.get("cagr"),
            "sharpe": metrics.get("sharpe"),
            "sortino": metrics.get("sortino"),
            "calmar": metrics.get("calmar"),
            "max_drawdown": metrics.get("max_drawdown"),
            "win_rate": metrics.get("win_rate"),
            "profit_factor": metrics.get("profit_factor"),
            "turnover_ratio": metrics.get("turnover_ratio"),
            "n_trades": n_trades,
            "frac_regimes_positive": frac_regimes_positive,
            "min_regime_sharpe": min_regime_sharpe,
            "n_regimes": len(regime_cagrs),
            "trade_log_path": d.get("trade_log_path"),
            "created_at": run.get("created_at"),
        })
    return pd.DataFrame(rows)


def dedupe_latest(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the most recent run per (template, horizon_bucket, exit_variant)."""
    df = df.sort_values("created_at")
    key = ["template", "horizon_bucket", "exit_variant"]
    return df.drop_duplicates(subset=key, keep="last").reset_index(drop=True)


def score(df: pd.DataFrame) -> pd.DataFrame:
    """Composite rank score: reward regime-stable risk-adjusted return,
    penalize turnover (cost drag) and drawdown."""
    df = df.copy()

    def z(col):
        s = df[col]
        std = s.std(ddof=0)
        return (s - s.mean()) / std if std else s * 0

    df["z_sharpe"] = z("sharpe")
    df["z_min_regime_sharpe"] = z("min_regime_sharpe").fillna(df["z_sharpe"].min())
    df["z_calmar"] = z("calmar")
    df["z_turnover_penalty"] = -z("turnover_ratio")
    df["z_drawdown_penalty"] = z("max_drawdown")  # less negative dd = higher z

    df["composite_score"] = (
        0.30 * df["z_sharpe"]
        + 0.30 * df["z_min_regime_sharpe"]
        + 0.15 * df["z_calmar"]
        + 0.15 * df["z_drawdown_penalty"]
        + 0.10 * df["z_turnover_penalty"]
    )
    return df.sort_values("composite_score", ascending=False).reset_index(drop=True)


def load_equity_curve(trade_log_path: str) -> pd.Series | None:
    if not trade_log_path or not os.path.exists(trade_log_path):
        return None
    try:
        tl = pd.read_csv(trade_log_path)
    except Exception:
        return None
    date_col = next((c for c in ("exit_date", "date", "trade_date") if c in tl.columns), None)
    pnl_col = next((c for c in ("pnl", "realized_pnl", "net_pnl") if c in tl.columns), None)
    if not date_col or not pnl_col:
        return None
    tl[date_col] = pd.to_datetime(tl[date_col])
    daily = tl.groupby(date_col)[pnl_col].sum().sort_index()
    return daily


def correlation_dedupe(candidates: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """Greedily walk down the ranked list, dropping strategies whose daily
    PnL is highly correlated (>0.7) with one already selected."""
    curves = {}
    for _, row in candidates.iterrows():
        curve = load_equity_curve(row["trade_log_path"])
        if curve is not None and len(curve) >= 20:
            curves[row["run_id"]] = curve

    selected: list[str] = []
    for _, row in candidates.iterrows():
        rid = row["run_id"]
        if len(selected) >= top_n:
            break
        curve = curves.get(rid)
        if curve is None:
            selected.append(rid)  # no trade log to compare; keep by rank
            continue
        redundant = False
        for other_rid in selected:
            other = curves.get(other_rid)
            if other is None:
                continue
            aligned = pd.concat([curve, other], axis=1, join="inner")
            if len(aligned) < 20:
                continue
            corr = aligned.iloc[:, 0].corr(aligned.iloc[:, 1])
            if corr is not None and corr > 0.7:
                redundant = True
                break
        if not redundant:
            selected.append(rid)

    return candidates[candidates["run_id"].isin(selected)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--min-trades", type=int, default=30)
    ap.add_argument("--csv-out", default=os.path.join(REPORTS_DIR, "ta_strategy_shortlist.csv"))
    args = ap.parse_args()

    df = load_technical_runs(args.min_trades)
    if df.empty:
        print("No technical runs found meeting the min-trades threshold.", file=sys.stderr)
        sys.exit(1)

    df = dedupe_latest(df)
    ranked = score(df)

    pool = ranked.head(min(len(ranked), max(args.top * 4, 40)))
    shortlist = correlation_dedupe(pool, args.top)
    shortlist = ranked[ranked["run_id"].isin(shortlist["run_id"])]

    cols = [
        "template", "label", "category", "horizon_bucket", "exit_variant",
        "cagr", "sharpe", "min_regime_sharpe", "frac_regimes_positive",
        "calmar", "max_drawdown", "win_rate", "turnover_ratio", "n_trades",
        "composite_score",
    ]
    out = shortlist[cols].copy()
    out["cagr"] = (out["cagr"] * 100).round(2)
    out["max_drawdown"] = (out["max_drawdown"] * 100).round(2)
    out["win_rate"] = (out["win_rate"] * 100).round(1)
    out["frac_regimes_positive"] = (out["frac_regimes_positive"] * 100).round(0)
    out = out.round({"sharpe": 2, "min_regime_sharpe": 2, "calmar": 2,
                      "turnover_ratio": 1, "composite_score": 2})

    print(f"Scanned {len(df)} deduped technical runs -> shortlisting top {len(out)}\n")
    print(out.to_string(index=False))

    out.to_csv(args.csv_out, index=False)
    print(f"\nWritten to {args.csv_out}")

    cat_counts = out["category"].value_counts()
    print(f"\nCategory coverage: {dict(cat_counts)}")


if __name__ == "__main__":
    main()
