#!/usr/bin/env python3
"""
scripts/derive_exit_params_from_unconstrained.py

Extracts closed trades from the "unconstrained" Technical runs, replays OHLCV
between each trade's buy and sale date to recover the path it actually took
(MAE/MFE), and emits per-template stop / target / max-hold via
backtest/derive_exit_params.py.

Run this whenever the unconstrained control runs are refreshed; the output is
checked in so the parameters the backtests use are reviewable in a diff rather
than recomputed invisibly at run time.

    python3 scripts/derive_exit_params_from_unconstrained.py \
        --out backtest/config/derived_exit_params.json

DuckDB is single-writer and blocks even read_only connections while the
scheduler holds the file, so this opens short-lived read-only connections and
closes the backtest DB before touching the market DB rather than holding both.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from backtest.derive_exit_params import derive_params, params_to_frame

# Sampling exists because the full unconstrained set is ~679k trades and the
# path join is the expensive part. 0 means "no sampling" — prefer that for the
# checked-in artifact; the sample is for interactive iteration.
DEFAULT_SAMPLE_ROWS = 0


def load_trades(backtest_db: Path, market_db: Path, sample_rows: int) -> pd.DataFrame:
    sample_clause = f"using sample {sample_rows} rows" if sample_rows else ""
    con = duckdb.connect(str(backtest_db), read_only=True)
    try:
        trades = con.execute(
            f"""
            select t.template_name as template, t.ticker, t.buy_date, t.sale_date,
                   t.buy_price, t.pnl_pct, t.holding_days
            from backtest_trades t
            join backtest_runs r using(run_id)
            where r.exit_policy_variant = 'unconstrained'
              and r.channel = 'technical'
              and t.pnl_pct is not null
              and t.holding_days is not null
              and t.buy_price > 0
            {sample_clause}
            """
        ).fetchdf()
        window = con.execute(
            """
            select min(r.start_date), max(r.end_date), count(distinct r.run_id)
            from backtest_runs r
            where r.exit_policy_variant = 'unconstrained' and r.channel = 'technical'
            """
        ).fetchone()
    finally:
        con.close()

    if trades.empty:
        raise SystemExit(
            "No unconstrained Technical trades found. Barriers cannot be derived "
            "from any other variant: every other variant's trades are truncated by "
            "its own barriers, so deriving barriers from them is circular."
        )

    con = duckdb.connect(str(market_db), read_only=True)
    try:
        con.register("trades", trades)
        # MAE/MFE from the day AFTER entry through the exit day: an exit
        # barrier cannot fire on the bar that opened the position.
        path = con.execute(
            """
            select t.template, t.pnl_pct, t.holding_days,
                   min(o.low) / t.buy_price - 1.0 as mae,
                   max(o.high) / t.buy_price - 1.0 as mfe
            from trades t
            join ohlcv_adjusted o
              on o.ticker = t.ticker
             and o.date > t.buy_date
             and o.date <= t.sale_date
            group by t.template, t.pnl_pct, t.holding_days,
                     t.ticker, t.buy_date, t.buy_price
            """
        ).fetchdf()
    finally:
        con.close()

    # The GROUP BY above collapses identical trades, and most of the row count
    # IS duplicates: 538,985 of 679,492 unconstrained Technical trade rows are
    # exact repeats, because the same (ticker, buy_date, sale_date) trade is
    # produced by every run whose config differs only in a dimension that did
    # not change the trade (top_n, capital_mode). Collapsing them is correct
    # here — a trade that happens to appear under six configs is one piece of
    # evidence about how that screen's trades behave, not six — but it must be
    # deliberate, so it is asserted rather than left to a silent side effect.
    #
    # This is separately verified: a per-trade bar count over the market DB
    # found ZERO unconstrained Technical trades with no OHLCV bars between
    # entry and exit. If that ever stops being true the difference below stops
    # matching the duplicate count, and the check fires.
    unique_trades = trades.drop_duplicates(
        subset=["ticker", "buy_date", "sale_date", "template", "pnl_pct"]
    )
    if len(path) < len(unique_trades):
        raise SystemExit(
            f"{len(unique_trades) - len(path)} distinct trades have no OHLCV path "
            "between entry and exit. Barriers derived from the remainder would be "
            "biased toward whichever tickers happen to have coverage — fix the "
            "OHLCV gap rather than proceeding."
        )
    print(
        f"{len(trades)} trade rows -> {len(path)} distinct trade paths "
        f"({len(trades) - len(path)} duplicate rows across configs collapsed)"
    )
    path.attrs["window"] = window
    return path


def main() -> None:
    from config.settings import BACKTEST_DUCKDB_PATH, DUCKDB_PATH

    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=Path("backtest/config/derived_exit_params.json"))
    ap.add_argument("--sample-rows", type=int, default=DEFAULT_SAMPLE_ROWS)
    args = ap.parse_args()

    path = load_trades(BACKTEST_DUCKDB_PATH, DUCKDB_PATH, args.sample_rows)
    params = derive_params(path)
    frame = params_to_frame(params)

    print(frame.to_string(index=False))

    start, end, n_runs = path.attrs["window"]
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provenance": {
            "source_variant": "unconstrained",
            "channel": "technical",
            "n_runs": int(n_runs),
            "n_trades": int(len(path)),
            "window": [str(start), str(end)],
            "sampled_rows": args.sample_rows or None,
        },
        "params": {p.template: p.__dict__ for p in params},
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(f"\nwrote {args.out} ({len(params)} templates)")


if __name__ == "__main__":
    main()
