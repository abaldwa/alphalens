"""
Detect the ex-date and implied price factor for the 174 likely-missing-split
tickers flagged in FutureDevelopment.md #32 (followup_missing_splits_20260705.csv,
likely_missing_split=True), by comparing our full ohlcv_adjusted history against
Fyers' full history for the same ticker.

Analysis-only: writes a review CSV, does NOT touch corporate_actions or
ohlcv_adjusted. That write step is a separate, explicitly-approved follow-up
per ticker/batch, same discipline used for the 2026-07-05 RIGHTS fix.

Usage:
    python3 scripts/detect_missing_split_reconstruction.py --start 0 --count 20
"""
import argparse
import sys

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, ".")
from config.settings import DUCKDB_PATH
from ingestion.scrapers.fyers_backfill import FYERSBackfill

# common split/bonus multiplicative price factors -> (action_type, ratio) for that factor
CANDIDATE_FACTORS = {}
for split_ratio in [2, 3, 4, 5, 10, 20, 25, 50, 100]:
    CANDIDATE_FACTORS[round(1.0 / split_ratio, 6)] = ("SPLIT", split_ratio)
for bonus_ratio in [(1, 1), (1, 2), (2, 1), (3, 1), (1, 3), (3, 2), (4, 1), (5, 1), (5, 2)]:
    num, den = bonus_ratio
    r = num / den
    factor = round(1.0 / (1.0 + r), 6)
    CANDIDATE_FACTORS[factor] = ("BONUS", r)

TOLERANCE_PCT = 3.0


def classify_factor(implied_factor):
    best = None
    for cand_factor, (atype, ratio) in CANDIDATE_FACTORS.items():
        pct_diff = abs(implied_factor - cand_factor) / cand_factor * 100
        if best is None or pct_diff < best[0]:
            best = (pct_diff, atype, ratio, cand_factor)
    return best  # (pct_diff, action_type, ratio, matched_factor)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--count", type=int, default=20)
    ap.add_argument("--out", default="reconstruction_review_batch.csv")
    args = ap.parse_args()

    missing = pd.read_csv("followup_missing_splits_20260705.csv")
    missing = missing[missing.likely_missing_split].reset_index(drop=True)
    batch = missing.iloc[args.start : args.start + args.count]

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    fy = FYERSBackfill()

    rows = []
    for row in batch.itertuples():
        ticker = row.ticker
        existing_ca = conn.execute(
            "SELECT count(*) FROM corporate_actions WHERE ticker=?", [ticker]
        ).fetchone()[0]
        ours = conn.execute(
            "SELECT date, close FROM ohlcv_adjusted WHERE ticker=? ORDER BY date",
            [ticker],
        ).fetchdf()
        if ours.empty:
            rows.append({"ticker": ticker, "status": "no_our_data"})
            continue
        our_min = pd.Timestamp(ours.date.min()).strftime("%Y-%m-%d")
        our_max = pd.Timestamp(ours.date.max()).strftime("%Y-%m-%d")

        try:
            hist = fy.download_history(ticker, our_min, our_max)
        except Exception as e:
            rows.append({"ticker": ticker, "status": f"fyers_error: {e}"})
            continue

        if hist.empty:
            rows.append({"ticker": ticker, "status": "no_fyers_data"})
            continue

        hist["date"] = pd.to_datetime(hist["date"])
        ours["date"] = pd.to_datetime(ours["date"])
        merged = pd.merge(ours, hist[["date", "close"]], on="date", suffixes=("_ours", "_fyers"))
        merged = merged[merged["close_fyers"] > 0].sort_values("date").reset_index(drop=True)
        if len(merged) < 20:
            rows.append({"ticker": ticker, "status": "insufficient_overlap"})
            continue

        merged["ratio"] = merged["close_ours"] / merged["close_fyers"]
        # locate the single largest jump in log(ratio) day-over-day
        log_ratio = np.log(merged["ratio"])
        diffs = log_ratio.diff().abs()
        jump_idx = diffs.idxmax()
        if pd.isna(jump_idx) or jump_idx == 0:
            rows.append({"ticker": ticker, "status": "no_jump_found"})
            continue

        jump_date = merged.loc[jump_idx, "date"]
        before = merged.loc[: jump_idx - 1]
        after = merged.loc[jump_idx:]
        if len(before) < 5 or len(after) < 5:
            rows.append({"ticker": ticker, "status": "insufficient_window_around_jump", "candidate_date": str(jump_date.date())})
            continue

        ratio_before = before["ratio"].median()
        ratio_after = after["ratio"].median()
        implied_factor = ratio_after / ratio_before

        # sanity: confirm ratio is near-constant on both sides (single clean jump)
        cv_before = before["ratio"].std() / ratio_before if ratio_before else np.nan
        cv_after = after["ratio"].std() / ratio_after if ratio_after else np.nan

        pct_diff, atype, cand_ratio, matched_factor = classify_factor(implied_factor)
        confidence = "high" if pct_diff <= TOLERANCE_PCT and cv_before < 0.05 and cv_after < 0.05 else "low"

        rows.append({
            "ticker": ticker,
            "status": "ok",
            "existing_ca_rows": existing_ca,
            "candidate_ex_date": str(jump_date.date()),
            "ratio_before": ratio_before,
            "ratio_after": ratio_after,
            "implied_factor": implied_factor,
            "matched_action_type": atype,
            "matched_ratio": cand_ratio,
            "matched_factor": matched_factor,
            "match_pct_diff": pct_diff,
            "cv_before": cv_before,
            "cv_after": cv_after,
            "confidence": confidence,
            "n_before": len(before),
            "n_after": len(after),
        })
        print(f"{ticker}: jump@{jump_date.date()} implied_factor={implied_factor:.4f} "
              f"-> {atype} ratio={cand_ratio} (diff={pct_diff:.2f}%) conf={confidence}")

    conn.close()
    out_df = pd.DataFrame(rows)
    out_df.to_csv(args.out, index=False)
    print(f"\nWrote {len(out_df)} rows to {args.out}")
    if "confidence" in out_df.columns:
        print(out_df["confidence"].value_counts())


if __name__ == "__main__":
    main()
