"""
backtest/diagnose_ta_signal_quality.py

Signal-quality diagnostic for TA screener templates, independent of
portfolio construction, costs, or exit rules. For each template, on a
sample of historical dates, evaluates which tickers the template's
conditions fire on and compares their forward N-day returns against the
rest of the universe. This isolates "does the entry signal have any raw
edge" from "did the portfolio-level backtest execute it well" -- the two
were conflated in the orchestrator CAGR numbers.

Usage:
    python backtest/diagnose_ta_signal_quality.py [--horizon 21] [--samples 40]
"""
from __future__ import annotations

import argparse
import glob
import os
import sys

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DUCKDB_PATH, FEATURES_DAILY_DIR  # noqa: E402
from systems.technical_analysis.screener.engine import ScreenerEngine  # noqa: E402
from systems.technical_analysis.screener.templates import TEMPLATES  # noqa: E402

REPORTS_DIR = os.path.join(os.path.dirname(__file__), "reports")


def sample_dates(n: int) -> list[str]:
    """Evenly spaced feature-Parquet dates across the available history."""
    files = sorted(glob.glob(os.path.join(str(FEATURES_DAILY_DIR), "*.parquet")))
    dates = [os.path.basename(f).replace(".parquet", "") for f in files]
    dates = [d for d in dates if d >= "2016-07-01" and d <= "2026-01-01"]
    if len(dates) <= n:
        return dates
    idx = np.linspace(0, len(dates) - 1, n).astype(int)
    return [dates[i] for i in idx]


def load_forward_returns(con: duckdb.DuckDBPyConnection, as_of: str, horizon: int) -> pd.Series:
    """close[t+horizon trading days] / close[t] - 1, per ticker, computed
    from the actual trading-day sequence (not calendar days)."""
    q = """
        WITH ranked AS (
            SELECT ticker, date, close,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) AS rn
            FROM ohlcv_adjusted
            WHERE date BETWEEN CAST(?::DATE - INTERVAL 5 DAY AS DATE)
                            AND CAST(?::DATE + INTERVAL 120 DAY AS DATE)
        ),
        anchor_rn AS (
            SELECT ticker, MAX(rn) AS anchor_rn
            FROM ranked
            WHERE date <= ?::DATE
            GROUP BY ticker
        ),
        anchor AS (
            SELECT r.ticker, r.rn AS anchor_rn, r.close AS anchor_close
            FROM ranked r
            JOIN anchor_rn ar ON ar.ticker = r.ticker AND ar.anchor_rn = r.rn
        )
        SELECT a.ticker, a.anchor_close, f.close AS fwd_close
        FROM anchor a
        JOIN ranked f ON f.ticker = a.ticker AND f.rn = a.anchor_rn + ?
    """
    df = con.execute(q, [as_of, as_of, as_of, horizon]).fetchdf()
    if df.empty:
        return pd.Series(dtype=float)
    df["fwd_return"] = df["fwd_close"] / df["anchor_close"] - 1.0
    return df.set_index("ticker")["fwd_return"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", type=int, default=21, help="forward trading days")
    ap.add_argument("--samples", type=int, default=40, help="number of sample dates")
    ap.add_argument("--csv-out", default=os.path.join(REPORTS_DIR, "ta_signal_quality.csv"))
    args = ap.parse_args()

    engine = ScreenerEngine()
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    dates = sample_dates(args.samples)
    print(f"Sampling {len(dates)} dates, forward horizon {args.horizon}d, "
          f"{len(TEMPLATES)} templates\n")

    rows = []
    fwd_cache: dict[str, pd.Series] = {}

    for template in TEMPLATES:
        fired_returns = []
        universe_returns = []
        n_fire_dates = 0

        for d in dates:
            df = engine._load_df(d)
            if df is None or "ticker" not in df.columns:
                continue
            available = frozenset(df.columns)
            mask = pd.Series(True, index=df.index)
            any_missing = False
            for cond in template.conditions:
                cmask, missing = engine._apply_single_condition(df, cond, available)
                mask &= cmask
                any_missing = any_missing or missing
            fired_tickers = df.loc[mask, "ticker"].tolist()
            if not fired_tickers:
                continue

            if d not in fwd_cache:
                fwd_cache[d] = load_forward_returns(con, d, args.horizon)
            fwd = fwd_cache[d]
            if fwd.empty:
                continue

            fired_fwd = fwd.reindex(fired_tickers).dropna()
            if fired_fwd.empty:
                continue
            fired_returns.extend(fired_fwd.tolist())
            universe_returns.extend(fwd.dropna().tolist())
            n_fire_dates += 1

        if not fired_returns or n_fire_dates < 3:
            rows.append({
                "template": template.name, "category": template.category,
                "label": template.description, "n_fire_dates": n_fire_dates,
                "n_fired_obs": len(fired_returns), "fired_median_fwd_return": np.nan,
                "universe_median_fwd_return": np.nan, "edge_pct": np.nan,
                "fired_win_rate": np.nan,
            })
            continue

        fired_med = float(np.median(fired_returns))
        universe_med = float(np.median(universe_returns))
        rows.append({
            "template": template.name,
            "category": template.category,
            "label": template.description,
            "n_fire_dates": n_fire_dates,
            "n_fired_obs": len(fired_returns),
            "fired_median_fwd_return": round(fired_med * 100, 2),
            "universe_median_fwd_return": round(universe_med * 100, 2),
            "edge_pct": round((fired_med - universe_med) * 100, 2),
            "fired_win_rate": round(float(np.mean(np.array(fired_returns) > 0)) * 100, 1),
        })
        print(f"  {template.name:6s} {template.description:35s} "
              f"fire_dates={n_fire_dates:3d} obs={len(fired_returns):5d} "
              f"edge={rows[-1]['edge_pct']:+.2f}pp")

    out = pd.DataFrame(rows).sort_values("edge_pct", ascending=False)
    out.to_csv(args.csv_out, index=False)

    print(f"\n{'='*90}")
    print(f"Ranked by raw signal edge (fired median fwd return - universe median), {args.horizon}d horizon")
    print(out.to_string(index=False))
    print(f"\nWritten to {args.csv_out}")

    has_edge = out[out["edge_pct"] > 0.5]
    print(f"\n{len(has_edge)}/{len(out)} templates show >0.5pp raw edge over the universe median.")
    print("Templates with no measurable raw edge are not worth refining via exit rules")
    print("or portfolio construction -- the entry signal itself has nothing to work with.")


if __name__ == "__main__":
    main()
