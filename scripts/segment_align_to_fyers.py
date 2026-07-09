"""
For the 36 tickers whose single-jump alignment (align_remaining_to_fyers.py)
still left a residual mismatch, pull full Fyers history and do a
multi-breakpoint segmented alignment instead of one correction factor.

Rationale: a single multiplicative correction only works if there is exactly
one discontinuity between our data and Fyers. The 36 residual-mismatch
tickers likely have more than one (multiple corporate actions, or a
continuous dividend-convention drift) — a single factor can't close that.
This script finds ALL significant breakpoints in the our/Fyers close ratio,
segments the series, and rescales every segment to match the most recent
segment (anchored to today's real, presumably-correct price), working
backward through time.

Usage:
    python3 scripts/segment_align_to_fyers.py [--dry-run]
"""
import argparse
import sys

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, ".")
from config.settings import DUCKDB_PATH
from ingestion.scrapers.fyers_backfill import FYERSBackfill

JUMP_THRESHOLD = 0.05  # abs log-ratio day-over-day change to flag as a breakpoint


def find_breakpoints(log_ratio: pd.Series) -> list:
    diffs = log_ratio.diff().abs()
    return list(diffs[diffs >= JUMP_THRESHOLD].index)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--tickers", default=None, help="comma-separated subset")
    args = ap.parse_args()

    tickers = args.tickers.split(",") if args.tickers else [
        "SURANAT&P", "JINDWORLD", "BANCOINDIA", "TPLPLASTEH", "JYOTHYLAB",
        "HERITGFOOD", "IIFL", "NCC", "PETRONET", "HINDPETRO", "SRF",
        "CASTROLIND", "AARTIIND", "TORNTPHARM", "NAVINFLUOR", "REDINGTON",
        "MARICO", "MUTHOOTFIN", "TRIDENT", "SUPREMEIND", "GLAXO", "TVSMOTOR",
        "SARDAEN", "CRISIL", "IRB", "CONCOR", "GRAPHITE", "FSL", "SOUTHBANK",
        "SBIN", "ZENSARTECH", "ACC", "CHENNPETRO", "SIEMENS", "BAJAJ-AUTO",
        "CANBK",
    ]

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=args.dry_run)
    fy = FYERSBackfill()

    for ticker in tickers:
        ours = conn.execute(
            "SELECT date, close FROM ohlcv_adjusted WHERE ticker=? ORDER BY date", [ticker]
        ).fetchdf()
        if ours.empty:
            print(f"{ticker}: no our data, skip")
            continue
        our_min = pd.Timestamp(ours.date.min()).strftime("%Y-%m-%d")
        our_max = pd.Timestamp(ours.date.max()).strftime("%Y-%m-%d")

        try:
            hist = fy.download_history(ticker, our_min, our_max)
        except Exception as e:
            print(f"{ticker}: fyers error {e}, skip")
            continue
        if hist.empty:
            print(f"{ticker}: no fyers data, skip")
            continue

        hist["date"] = pd.to_datetime(hist["date"])
        ours["date"] = pd.to_datetime(ours["date"])
        merged = pd.merge(ours, hist[["date", "close"]], on="date", suffixes=("_ours", "_fyers"))
        merged = merged[merged["close_fyers"] > 0].sort_values("date").reset_index(drop=True)
        if len(merged) < 30:
            print(f"{ticker}: insufficient overlap ({len(merged)} rows), skip")
            continue

        merged["ratio"] = merged["close_ours"] / merged["close_fyers"]
        log_ratio = np.log(merged["ratio"])
        bp_idx = sorted(find_breakpoints(log_ratio))

        if not bp_idx:
            print(f"{ticker}: no breakpoints found (already consistent), skip")
            continue

        # merge breakpoints closer than the local window (flip-flop noise, not
        # real distinct actions)
        merged_bp = [bp_idx[0]]
        for bp in bp_idx[1:]:
            if bp - merged_bp[-1] < 20:
                continue
            merged_bp.append(bp)
        bp_idx = merged_bp

        if len(bp_idx) > 6:
            print(f"{ticker}: {len(bp_idx)} breakpoints even after merging — looks like "
                  f"noise, not real actions. Skipping for manual review.")
            continue

        print(f"{ticker}: {len(bp_idx)} breakpoint(s) found")

        # Process latest-first: each breakpoint's local "after" window reflects
        # whatever correction downstream (later) breakpoints already applied,
        # so working backward keeps every earlier segment consistent with today.
        LOCAL_WINDOW = 40  # rows on each side of the breakpoint

        def current_ratio(window_df):
            # re-read close from the DB fresh: any later (already-processed)
            # breakpoint's correction may have touched these same dates.
            dates = window_df["date"].tolist()
            cur = conn.execute(
                "SELECT date, close FROM ohlcv_adjusted WHERE ticker=? AND date = ANY(?::DATE[])",
                [ticker, [str(d.date()) for d in dates]],
            ).fetchdf()
            cur["date"] = pd.to_datetime(cur["date"])
            m = pd.merge(cur, window_df[["date", "close_fyers"]], on="date")
            return (m["close"] / m["close_fyers"]).median()

        for bp in reversed(bp_idx):
            before = merged.iloc[max(0, bp - LOCAL_WINDOW):bp]
            after = merged.iloc[bp:bp + LOCAL_WINDOW]
            if len(before) < 5 or len(after) < 5:
                print(f"  breakpoint at {merged.loc[bp, 'date'].date()}: insufficient local window, skip")
                continue

            ratio_after = current_ratio(after)
            ratio_before = current_ratio(before)
            factor = ratio_after / ratio_before
            bp_date = merged.loc[bp, "date"]

            print(f"  breakpoint at {bp_date.date()}: ratio_before={ratio_before:.4f} "
                  f"ratio_after={ratio_after:.4f} factor={factor:.4f}")

            if args.dry_run:
                continue
            conn.execute(
                """UPDATE ohlcv_adjusted
                   SET open=open*?, high=high*?, low=low*?, close=close*?,
                       adj_factor=adj_factor*?
                   WHERE ticker=? AND date < ?""",
                [factor, factor, factor, factor, factor, ticker, str(bp_date.date())],
            )

    conn.close()


if __name__ == "__main__":
    main()
