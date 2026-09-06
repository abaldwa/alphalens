"""
scripts/patch_confirmed_fyers_fixes.py

Part A apply step (2026-09-05 corp-action price-discontinuity investigation):
for the 22 tickers scripts/verify_fyers_repull_fixes_discontinuity.py
confirmed are FIXED_BY_REPULL (a fresh FYERS pull is continuous across the
ex_date; our stored ohlcv_adjusted copy is simply stale), re-pull each
ticker's FULL history and replace the stored rows with it.

Unlike ingestion/adjust/price_adjuster.py (which computes a multiplicative
factor from a corporate_actions ratio and applies it going backward), this
script does not compute a factor at all -- it substitutes the known-correct
FYERS values directly, because that vendor series is already the ground
truth for these tickers (the same premise price_adjuster.py's own "skip
source='fyers' rows" exclusion already relies on -- see that module's
adjust_for_corporate_actions docstring). adj_factor is set to 1.0 and
source to 'fyers' for every rewritten row, consistent with that premise.

SAFETY (per db-migration-check discipline):
  - Default mode is DRY RUN: prints a diff summary per ticker, writes
    nothing. Pass --apply to actually write.
  - Before writing, captures each changed row's PRE-PATCH values into
    ohlcv_ca_audit (raw_* columns, first-write-wins via ON CONFLICT DO
    NOTHING) so the broken data is never silently lost -- it stays
    inspectable in the audit table under a new 'pre_patch_' style entry
    is NOT created; instead the pre-patch row is preserved exactly as
    ohlcv_ca_audit already models "the value before this correction",
    the same semantics price_adjuster.py's own audit writes use.
  - Only ever touches rows for tickers in --input, and only within the
    date range covered by our stored ohlcv_adjusted history for that
    ticker (never invents a longer history than we already track).
  - No DELETE, no schema change. Idempotent: re-running after a successful
    apply re-fetches the same (now-correct) FYERS data and finds nothing
    left to change.

Usage:
    # Dry run (default) -- prints diffs, writes nothing
    python3 scripts/patch_confirmed_fyers_fixes.py --input <csv>

    # Actually apply, one ticker at a time for review
    python3 scripts/patch_confirmed_fyers_fixes.py --input <csv> --ticker DPSCLTD --apply

    # Apply the whole confirmed list (only after reviewing the dry run)
    python3 scripts/patch_confirmed_fyers_fixes.py --input <csv> --apply
"""
import argparse
import sys
import time
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

sys.path.insert(0, ".")
from config.settings import DUCKDB_PATH  # noqa: E402
from datastore.api.db import get_duckdb_connection  # noqa: E402
from ingestion.scrapers.fyers_backfill import FYERSBackfill  # noqa: E402

PRICE_TOLERANCE_PCT = 0.5  # rows within this tolerance aren't reported as a diff (rounding noise)


WINDOW_DAYS_BEFORE = 30
WINDOW_TOTAL_DAYS = 365


def _diff_ticker(
    conn: duckdb.DuckDBPyConnection, fy: FYERSBackfill, ticker: str, ex_date: str
) -> Optional[pd.DataFrame]:
    """
    Re-pull only a window around `ex_date` -- WINDOW_DAYS_BEFORE days
    before it through WINDOW_TOTAL_DAYS total (one single FYERS call, no
    chunking) -- rather than this ticker's full stored history. The
    FIXED_BY_REPULL verdict only tells us FYERS is continuous AT this
    specific ex_date; a full-history pull re-verifies years of data that
    was never in question, for 26 tickers, at real API-call cost, with no
    added confidence about the one date range that actually needs fixing.

    Returns a DataFrame of rows in that window that differ beyond
    tolerance -- date, stored_close, fresh_close, pct_diff. None if the
    ticker has no stored rows in the window or FYERS has nothing for it.
    """
    ex_ts = pd.Timestamp(ex_date)
    from_ts = ex_ts - pd.Timedelta(days=WINDOW_DAYS_BEFORE)
    # Clip to today -- a recent ex_date (e.g. CAPTRUST, 2025-10-10) pushes
    # from_ts + WINDOW_TOTAL_DAYS past the current date, which FYERS
    # rejects outright rather than just truncating the response itself.
    to_ts = min(from_ts + pd.Timedelta(days=WINDOW_TOTAL_DAYS), pd.Timestamp.now().normalize())
    from_date, to_date = from_ts.strftime("%Y-%m-%d"), to_ts.strftime("%Y-%m-%d")

    fresh = fy.download_history(ticker, from_date, to_date)
    if fresh.empty:
        print(f"{ticker}: FYERS returned no data for {from_date}..{to_date} -- skipping")
        return None

    stored = conn.execute(
        "SELECT date, open, high, low, close, volume, source, adj_factor "
        "FROM ohlcv_adjusted WHERE ticker = ? AND date BETWEEN ? AND ? ORDER BY date",
        [ticker, from_date, to_date],
    ).fetchdf()
    if stored.empty:
        print(f"{ticker}: no stored ohlcv_adjusted rows in {from_date}..{to_date} -- skipping")
        return None
    stored["date"] = pd.to_datetime(stored["date"])
    fresh["date"] = pd.to_datetime(fresh["date"])

    merged = pd.merge(
        stored, fresh, on="date", suffixes=("_stored", "_fresh"), how="inner"
    )
    merged["pct_diff"] = (
        (merged["close_fresh"] - merged["close_stored"]).abs() / merged["close_stored"] * 100
    )
    changed = merged[merged["pct_diff"] > PRICE_TOLERANCE_PCT].copy()
    changed.attrs["fresh_full"] = fresh
    changed.attrs["n_stored"] = len(stored)
    changed.attrs["n_fresh"] = len(fresh)
    return changed


def _apply_ticker(conn: duckdb.DuckDBPyConnection, ticker: str, changed: pd.DataFrame) -> None:
    """
    Write the fix for one ticker: preserve pre-patch values in
    ohlcv_ca_audit (raw_* = what was stored before this patch, first-write
    wins so a re-run never overwrites the true original), then overwrite
    ohlcv_adjusted with the FYERS values, adj_factor=1.0, source='fyers'.
    """
    stage = changed[["date", "open_stored", "high_stored", "low_stored", "close_stored",
                      "volume_stored"]].copy()
    stage.columns = ["date", "raw_open", "raw_high", "raw_low", "raw_close", "raw_volume"]
    stage["ticker_col"] = ticker

    conn.register("_patch_audit", stage)
    try:
        conn.execute(
            """
            INSERT INTO ohlcv_ca_audit
                (date, ticker, raw_open, raw_high, raw_low, raw_close,
                 raw_volume, adj_factor, vol_adj_factor)
            SELECT date, ticker_col, raw_open, raw_high, raw_low, raw_close,
                   CAST(raw_volume AS BIGINT), 1.0, 1.0
            FROM _patch_audit
            ON CONFLICT (date, ticker) DO NOTHING
            """
        )
    finally:
        conn.unregister("_patch_audit")

    update_stage = changed[["date", "open_fresh", "high_fresh", "low_fresh", "close_fresh",
                             "volume_fresh"]].copy()
    update_stage.columns = ["date", "new_open", "new_high", "new_low", "new_close", "new_volume"]
    update_stage["ticker_col"] = ticker

    conn.register("_patch_updates", update_stage)
    try:
        conn.execute(
            """
            UPDATE ohlcv_adjusted
            SET open = u.new_open, high = u.new_high, low = u.new_low,
                close = u.new_close, volume = CAST(u.new_volume AS BIGINT),
                adj_factor = 1.0, source = 'fyers'
            FROM _patch_updates u
            WHERE ohlcv_adjusted.ticker = u.ticker_col AND ohlcv_adjusted.date = u.date
            """
        )
    finally:
        conn.unregister("_patch_updates")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="confirmed_fixed_by_repull.csv (ticker, ex_date columns)")
    ap.add_argument("--ticker", default=None, help="Limit to one ticker (for reviewing before a bulk apply)")
    ap.add_argument("--apply", action="store_true", help="Actually write. Without this, dry-run only.")
    args = ap.parse_args()

    targets = pd.read_csv(args.input)[["ticker", "ex_date"]].drop_duplicates().sort_values("ticker")
    if args.ticker:
        targets = targets[targets["ticker"] == args.ticker]

    fy = FYERSBackfill()
    summary: List[Dict[str, Any]] = []

    # persist=False: this DB is shared with the ingestion scheduler and the
    # API server (SPEC-SCHED-013) -- never hold it open longer than this
    # script's own run, and retry-with-backoff on a transient lock instead
    # of failing outright (get_duckdb_connection's default budget).
    with get_duckdb_connection(DUCKDB_PATH, read_only=not args.apply, persist=False) as conn:
        for row in targets.itertuples():
            ticker, ex_date = str(row.ticker), str(row.ex_date)
            changed = _diff_ticker(conn, fy, ticker, ex_date)
            if changed is None:
                summary.append({"ticker": ticker, "status": "skipped_no_data"})
                continue

            n_stored = changed.attrs["n_stored"]
            if changed.empty:
                print(f"{ticker}: 0 / {n_stored} rows differ beyond {PRICE_TOLERANCE_PCT}% -- already correct")
                summary.append({"ticker": ticker, "status": "already_correct", "rows_changed": 0})
                continue

            worst = changed.loc[changed["pct_diff"].idxmax()]
            worst_pct_diff: float = changed["pct_diff"].max()
            print(
                f"{ticker}: {len(changed)} / {n_stored} rows would change "
                f"(worst: {worst['date'].date()} stored={worst['close_stored']:.2f} "
                f"-> fresh={worst['close_fresh']:.2f}, {worst_pct_diff:.1f}% diff)"
            )
            summary.append({
                "ticker": ticker, "status": "diff_found", "rows_changed": len(changed),
                "worst_date": str(worst["date"].date()), "worst_pct_diff": worst_pct_diff,
            })

            if args.apply:
                _apply_ticker(conn, ticker, changed)
                conn.commit()
                print(f"{ticker}: APPLIED — {len(changed)} rows patched, {len(changed)} audit rows preserved")

            time.sleep(0.3)

    out = pd.DataFrame(summary)
    print("\n" + ("APPLY" if args.apply else "DRY RUN") + " summary:")
    print(out.to_string())


if __name__ == "__main__":
    main()
