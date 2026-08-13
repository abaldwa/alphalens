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
from backtest.trade_filters import ADTV_LOOKBACK_SESSIONS, DEFAULT_TOP_N_BY_ADTV

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
        # Tradeability filters, applied here rather than assumed.
        #
        # IMPORTANT LIMITATION, stated rather than buried: these trades were
        # PRODUCED by runs that had none of these filters on. Removing the
        # untradeable ones after the fact is a large improvement over using
        # them, but it is not equivalent to re-running the unconstrained sweep
        # with the filters active — in a real re-run the capital those trades
        # consumed would have gone to different names, so the surviving trade
        # SET would differ, not just shrink. Barriers derived here are sound
        # enough to configure the re-run with; the re-run's own unconstrained
        # arm is what they should ultimately be re-derived from.
        path = con.execute(
            f"""
            with adtv as (
                select ticker, date,
                       avg(close * volume) over (
                           partition by ticker order by date
                           rows between {ADTV_LOOKBACK_SESSIONS} preceding and 1 preceding
                       ) as adtv_value
                from ohlcv_adjusted
            ),
            ranked as (
                select ticker, date,
                       row_number() over (partition by date order by adtv_value desc) as adtv_rank
                from adtv where adtv_value is not null
            ),
            eligible as (
                select t.*
                from trades t
                join ranked r on r.ticker = t.ticker and r.date = t.buy_date
                where r.adtv_rank <= {DEFAULT_TOP_N_BY_ADTV}
            ),
            -- A trade whose ENTRY or EXIT bar was circuit-locked was not
            -- fillable at that price; high == low on a day that traded.
            unlocked as (
                select e.* from eligible e
                where not exists (
                    select 1 from ohlcv_adjusted o
                    where o.ticker = e.ticker and o.high = o.low and o.volume > 0
                      and o.date in (e.buy_date, e.sale_date)
                )
            )
            select u.template, u.pnl_pct, u.holding_days,
                   min(o.low) / u.buy_price - 1.0 as mae,
                   max(o.high) / u.buy_price - 1.0 as mfe,
                   count(*) as n_bars,
                   date_diff('day', u.buy_date, u.sale_date) as calendar_days
            from unlocked u
            join ohlcv_adjusted o
              on o.ticker = u.ticker
             and o.date > u.buy_date
             and o.date <= u.sale_date
            group by u.template, u.pnl_pct, u.holding_days,
                     u.ticker, u.buy_date, u.buy_price, u.sale_date
            """
        ).fetchdf()

        # Blackout rule: a trade whose bar count is far below the trading days
        # its window spans was held through a period no barrier could act in.
        # ~0.68 bars per calendar day is the normal ratio (5 sessions / 7.35
        # calendar days); 0.5 leaves generous room for holidays before flagging.
        before_blackout = len(path)
        path = path[path["n_bars"] >= 0.5 * path["calendar_days"] * (5.0 / 7.0)]
        print(f"blackout filter dropped {before_blackout - len(path)} trades")
    finally:
        con.close()

    # Stage-by-stage accounting. Written this way after the first version's
    # guard misfired: it compared the final row count against the count of
    # DISTINCT input trades, which was correct before the tradeability filters
    # existed and wrong afterwards — every legitimately filtered trade looked
    # like missing OHLCV, and it reported 54,005 "trades with no price path"
    # that were simply illiquid, circuit-locked or blacked out. A filter drop
    # and a data hole must never share a counter.
    unique_trades = trades.drop_duplicates(
        subset=["ticker", "buy_date", "sale_date", "template", "pnl_pct"]
    )
    print(
        f"{len(trades)} trade rows -> {len(unique_trades)} distinct trades "
        f"({len(trades) - len(unique_trades)} duplicate rows across configs collapsed)"
    )
    print(
        f"after tradeability filters (PIT top-{DEFAULT_TOP_N_BY_ADTV} ADTV, circuit-locked "
        f"entry/exit bars, blackouts): {len(path)} trades "
        f"({100.0 * (1 - len(path) / max(len(unique_trades), 1)):.1f}% removed)"
    )
    if path.empty:
        raise SystemExit(
            "every trade was filtered out — check the filter thresholds before "
            "concluding anything from an empty set."
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
