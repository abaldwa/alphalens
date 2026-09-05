"""
scripts/verify_fyers_repull_fixes_discontinuity.py

Part A dry-run (2026-09-05, user-directed investigation into the corp-action
price-discontinuity bug found via the momentum campaign's Run Analysis panel):
for each (ticker, ex_date) with a confirmed unadjusted price jump in our
stored ohlcv_adjusted, pull a FRESH window from Fyers' live history API and
check whether Fyers' own current series is continuous across that ex_date.

Per scripts/validate_corporate_actions_fyers.py's documented (2026-07-05)
finding, Fyers' history endpoint retroactively self-adjusts standard
SPLIT/BONUS/RIGHTS corporate actions over time -- our stored ohlcv_adjusted
rows were captured at original ingestion time, before that retroactive
adjustment happened, and were never re-synced. This script tests that
hypothesis directly, ticker by ticker, rather than assuming it.

READ-ONLY: makes Fyers API calls and reads ohlcv_adjusted; does NOT write
anything to the database. Output is a verdict CSV for manual review before
any actual data patch (same discipline as the 2026-07-05 RIGHTS fix and
scripts/detect_missing_split_reconstruction.py).

Usage:
    python3 scripts/verify_fyers_repull_fixes_discontinuity.py --input <csv> --out <csv>
"""
import argparse
import sys
import time
from datetime import timedelta
from typing import Any, Dict, List

import duckdb
import pandas as pd

sys.path.insert(0, ".")
from config.settings import DUCKDB_PATH  # noqa: E402
from ingestion.scrapers.fyers_backfill import FYERSBackfill  # noqa: E402

WINDOW_DAYS = 10
JUMP_THRESHOLD = 1.4  # same threshold used in the original discovery sweep


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input", required=True,
        help="CSV with ticker, ex_date, action_type, ratio, source columns "
             "(one row per confirmed price discontinuity to re-test against FYERS)",
    )
    ap.add_argument("--out", default="fyers_repull_verdicts.csv")
    ap.add_argument("--max-tickers", type=int, default=1000)
    args = ap.parse_args()

    candidates = pd.read_csv(args.input)
    candidates["ex_date"] = pd.to_datetime(candidates["ex_date"])
    distinct = candidates.drop_duplicates(subset=["ticker", "ex_date"]).reset_index(drop=True)

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    fy = FYERSBackfill()

    rows: List[Dict[str, Any]] = []
    for i, row in distinct.iterrows():
        if i >= args.max_tickers:
            break
        ticker, ex_date = row["ticker"], row["ex_date"]
        win_from = (ex_date - timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")
        win_to = (ex_date + timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")

        verdict = {
            "ticker": ticker, "ex_date": ex_date.date(), "action_type": row["action_type"],
            "ratio": row["ratio"], "stored_source": row["source"],
        }
        try:
            fresh = fy.download_history(ticker, win_from, win_to)
        except Exception as e:
            verdict["verdict"] = "fyers_error"
            verdict["note"] = str(e)[:200]
            rows.append(verdict)
            print(f"{ticker} {ex_date.date()}: FYERS ERROR — {e}")
            continue

        if fresh.empty or len(fresh) < 2:
            verdict["verdict"] = "no_fyers_data"
            rows.append(verdict)
            print(f"{ticker} {ex_date.date()}: no fresh Fyers data")
            continue

        fresh = fresh.sort_values("date")
        fresh["date"] = pd.to_datetime(fresh["date"])
        fresh["prev_close"] = fresh["close"].shift(1)
        fresh["jump_ratio"] = fresh["prev_close"] / fresh["close"]
        valid = fresh.dropna(subset=["jump_ratio"])
        if valid.empty:
            verdict["verdict"] = "insufficient_window"
            rows.append(verdict)
            continue

        # Jump AT the actual ex_date specifically -- not the biggest jump
        # anywhere in the +/-10 day window. Fyers' raw feed has occasional
        # unrelated one-day bad-print glitches nearby in time (confirmed:
        # FIEMIND 2024-02-22, BBL 2024-04-18) that are NOT corporate-action
        # discontinuities; scanning for the window's max jump picks those up
        # as false positives and reports a genuinely-fixed ticker as broken.
        on_or_after = valid[valid["date"] >= ex_date]
        if on_or_after.empty:
            verdict["verdict"] = "insufficient_window"
            rows.append(verdict)
            continue
        fresh_jump_ratio = float(on_or_after.iloc[0]["jump_ratio"])

        stored = conn.execute(
            "SELECT date, close FROM ohlcv_adjusted WHERE ticker=? AND date BETWEEN ? AND ? ORDER BY date",
            [ticker, win_from, win_to],
        ).fetchdf()

        if fresh_jump_ratio > JUMP_THRESHOLD or fresh_jump_ratio < 1 / JUMP_THRESHOLD:
            verdict["verdict"] = "STILL_BROKEN_needs_manual"
        else:
            verdict["verdict"] = "FIXED_BY_REPULL"
        verdict["fresh_jump_ratio"] = fresh_jump_ratio
        verdict["stored_row_count"] = len(stored)
        rows.append(verdict)
        print(f"{ticker} {ex_date.date()} [{row['action_type']}]: fresh_jump_ratio={fresh_jump_ratio:.3f} -> {verdict['verdict']}")

        time.sleep(0.3)  # gentle on Fyers' shared rate limit

    out_df = pd.DataFrame(rows)
    out_df.to_csv(args.out, index=False)
    print(f"\nWrote {len(out_df)} verdicts to {args.out}")
    if not out_df.empty:
        print(out_df["verdict"].value_counts())


if __name__ == "__main__":
    main()
