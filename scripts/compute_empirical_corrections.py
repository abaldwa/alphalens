"""
scripts/compute_empirical_corrections.py

Part B prep (2026-09-05 corp-action price-discontinuity investigation): for
the 60 tickers confirmed STILL_BROKEN_needs_manual (a fresh FYERS pull
still shows the same raw discontinuity -- Fyers itself never adjusted
these, mostly genuine Demergers/Schemes-of-Arrangement with no disclosed
numeric ratio, plus a few compound SPLIT+BONUS events under-adjusted by
our own corporate_actions row), compute the EMPIRICAL backward-adjustment
factor directly from the observed price series -- the same principle as
the 2026-07-05 RIGHTS fix (scripts/validate_corporate_actions_fyers.py's
ratio_pre/ratio_post method), generalized to a case where FYERS' own data
can't be used as the "already adjusted" reference (it has the same jump).

METHOD: for each (ticker, ex_date), take the median close over a window of
trading days strictly BEFORE ex_date and strictly ON/AFTER ex_date (median,
not the single day-before/day-after close, to be robust to one noisy
print). price_factor = median_post / median_pre -- multiplying every
pre-ex_date raw price by this factor rescales history onto the same basis
as the post-event series, exactly what price_adjuster.py's SPLIT branch
does with a disclosed ratio (price_factor = 1/ratio), just derived
empirically here because no reliable disclosed ratio exists for a
Demerger/Scheme-of-Arrangement.

HONEST LIMITATION (documented, not hidden): for a genuine demerger, this
factor makes the SURVIVING entity's price series continuous for backtest
purposes, but it does NOT reconstruct "what a holder's total value really
was" -- that would require crediting the spun-off entity's shares
separately, which this project's Portfolio does not model (see the
2026-09-05 investigation's Category B finding). This script fixes price
CONTINUITY, not economic completeness -- flagged per-row via
`is_demerger_or_scheme` so a reviewer can judge whether M13_STAR-style
trades spanning one of these should be trusted at all, adjusted or not.

READ-ONLY: only reads ohlcv_adjusted (via a fresh FYERS pull, reusing the
already-fixed download_history segment fallback) and writes a review CSV.
Does NOT touch the database. Applying these factors is a deliberate
separate step (mirrors scripts/patch_confirmed_fyers_fixes.py's --apply
gate), pending human review of the low-confidence rows this script flags.

Usage:
    python3 scripts/compute_empirical_corrections.py --input <csv> --out <csv>
"""
import argparse
import sys
import time
from datetime import timedelta
from typing import Any, Dict, List

import pandas as pd

sys.path.insert(0, ".")
from ingestion.scrapers.fyers_backfill import FYERSBackfill  # noqa: E402

WINDOW_TRADING_DAYS = 10  # rows on each side of ex_date used for the median
MIN_SIDE_ROWS = 3  # fewer than this on either side -> low_confidence
# [2026-09-05] 8% flagged 55/58 tickers -- most of this universe is small/
# mid-cap Indian equity, where a 10-trading-day range of 10-20% either side
# of a real corporate action is routine, not a sign the median is unreliable.
# Widened so the flag actually discriminates noisy windows from normal
# volatility, rather than firing on almost everything.
HIGH_DISPERSION_PCT = 20.0  # spread within one side wider than this -> low_confidence
DEMERGER_KEYWORDS = ("demerger", "scheme of arrangement", "arrangement")


def _is_demerger_or_scheme(details: str) -> bool:
    d = (details or "").lower()
    return any(k in d for k in DEMERGER_KEYWORDS)


def _empirical_factor(
    fy: FYERSBackfill, ticker: str, ex_date: pd.Timestamp
) -> Dict[str, Any]:
    win_from = (ex_date - timedelta(days=WINDOW_TRADING_DAYS * 2)).strftime("%Y-%m-%d")
    win_to = (ex_date + timedelta(days=WINDOW_TRADING_DAYS * 2)).strftime("%Y-%m-%d")

    hist = fy.download_history(ticker, win_from, win_to)
    if hist.empty:
        return {"status": "no_fyers_data"}

    hist = hist.sort_values("date")
    hist["date"] = pd.to_datetime(hist["date"])

    pre = hist[hist["date"] < ex_date].tail(WINDOW_TRADING_DAYS)
    post = hist[hist["date"] >= ex_date].head(WINDOW_TRADING_DAYS)

    if len(pre) < MIN_SIDE_ROWS or len(post) < MIN_SIDE_ROWS:
        return {"status": "insufficient_window", "n_pre": len(pre), "n_post": len(post)}

    median_pre = float(pre["close"].median())
    median_post = float(post["close"].median())
    if median_pre <= 0 or median_post <= 0:
        return {"status": "invalid_prices"}

    price_factor = median_post / median_pre
    dispersion_pre = float((pre["close"].max() - pre["close"].min()) / median_pre * 100)
    dispersion_post = float((post["close"].max() - post["close"].min()) / median_post * 100)

    low_confidence = dispersion_pre > HIGH_DISPERSION_PCT or dispersion_post > HIGH_DISPERSION_PCT

    return {
        "status": "low_confidence" if low_confidence else "ok",
        "n_pre": len(pre), "n_post": len(post),
        "median_pre": median_pre, "median_post": median_post,
        "price_factor": price_factor, "implied_ratio": 1.0 / price_factor if price_factor else None,
        "dispersion_pre_pct": dispersion_pre, "dispersion_post_pct": dispersion_post,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="confirmed_needs_manual.csv (ticker, ex_date, action_type, ratio, details)")
    ap.add_argument("--out", default="empirical_corrections.csv")
    ap.add_argument("--max-tickers", type=int, default=1000)
    args = ap.parse_args()

    candidates = pd.read_csv(args.input)
    candidates["ex_date"] = pd.to_datetime(candidates["ex_date"])
    distinct = candidates.drop_duplicates(subset=["ticker", "ex_date"]).reset_index(drop=True)

    fy = FYERSBackfill()
    rows: List[Dict[str, Any]] = []

    for i, row in distinct.iterrows():
        if i >= args.max_tickers:
            break
        ticker, ex_date = row["ticker"], row["ex_date"]
        details = row.get("details", "") if "details" in row else ""

        try:
            result = _empirical_factor(fy, ticker, ex_date)
        except Exception as e:
            result = {"status": "fyers_error", "note": str(e)[:200]}

        result.update({
            "ticker": ticker, "ex_date": ex_date.date(),
            "action_type": row.get("action_type"), "disclosed_ratio": row.get("ratio"),
            "is_demerger_or_scheme": _is_demerger_or_scheme(str(details)),
        })
        rows.append(result)

        pf = result.get("price_factor")
        pf_str = f"{pf:.4f}" if pf is not None else "n/a"
        print(f"{ticker} {ex_date.date()} [{row.get('action_type')}]: price_factor={pf_str} -> {result['status']}")

        time.sleep(0.3)

    out_df = pd.DataFrame(rows)
    out_df.to_csv(args.out, index=False)
    print(f"\nWrote {len(out_df)} rows to {args.out}")
    if not out_df.empty:
        print(out_df["status"].value_counts())
        print(f"\nOf these, {int(out_df['is_demerger_or_scheme'].sum())} are genuine Demerger/Scheme-of-"
              f"Arrangement events -- a price_factor fixes CONTINUITY for these, not economic completeness "
              f"(see module docstring's Honest Limitation).")


if __name__ == "__main__":
    main()
