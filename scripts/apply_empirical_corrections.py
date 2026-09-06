"""
scripts/apply_empirical_corrections.py

Part B apply step (2026-09-05 corp-action price-discontinuity investigation):
applies the price_factor scripts/compute_empirical_corrections.py already
computed for the STILL_BROKEN tickers (no defined SPLIT/BONUS ratio, or
FYERS itself carries the same unadjusted jump) -- mostly genuine Demerger/
Scheme-of-Arrangement events. Unlike patch_confirmed_fyers_fixes.py, this
needs NO live FYERS call: the factor was already derived from a FYERS pull
during the investigation and is read straight from --input.

METHOD (same convention as ingestion/adjust/price_adjuster.py's SPLIT
branch, just with an empirically-derived factor instead of a disclosed
ratio): every row strictly BEFORE ex_date gets open/high/low/close
multiplied by price_factor, rescaling history onto the same basis as the
already-correct post-event series. Only rows before ex_date change --
the post-event series (already the "today's basis" this backward-adjusts
history onto) is left untouched.

HONEST LIMITATION, repeated from compute_empirical_corrections.py: for a
genuine Demerger/Scheme of Arrangement (flagged via is_demerger_or_scheme),
this fixes price CONTINUITY for backtesting, not economic completeness --
a real holder would also receive shares in the spun-off entity, which this
project's Portfolio does not model. Applying this factor makes a strategy
stop reporting a phantom -87% loss or +270% gain on such a trade; it does
not make the trade's economic story correct in an absolute sense.

SAFETY (same discipline as patch_confirmed_fyers_fixes.py):
  - Default mode is DRY RUN: prints what would change, writes nothing.
    --apply required to write.
  - Only rows with status == --min-status ('ok' by default; pass
    'low_confidence' explicitly to widen the run) are touched --
    low-confidence rows require a human decision, not a blanket apply.
  - Pre-patch values captured into ohlcv_ca_audit (raw_* columns,
    first-write-wins) before any overwrite.
  - adj_factor is MULTIPLIED (not overwritten) by price_factor for the
    touched rows, and source is left as-is -- unlike
    patch_confirmed_fyers_fixes.py's full substitution, this is a
    backward-adjustment on top of whatever was already there, exactly
    the same operation ingestion/adjust/price_adjuster.py performs for a
    disclosed-ratio SPLIT/BONUS.
  - Idempotent: re-applying to a ticker whose adj_factor already reflects
    this correction is a no-op (checked via the SAME tolerance the
    diff step uses, not by re-multiplying blindly).

Usage:
    # Dry run (default) -- prints what would change, writes nothing
    python3 scripts/apply_empirical_corrections.py --input <csv>

    # Apply only the 'ok' (high-confidence) rows
    python3 scripts/apply_empirical_corrections.py --input <csv> --apply

    # Review one ticker first
    python3 scripts/apply_empirical_corrections.py --input <csv> --ticker ADANIENT --apply
"""
import argparse
import sys
from typing import Any, Dict, List

import pandas as pd

sys.path.insert(0, ".")
from config.settings import DUCKDB_PATH  # noqa: E402
from datastore.api.db import get_duckdb_connection  # noqa: E402

ADJ_FACTOR_TOLERANCE = 1e-6


def _apply_one(conn: Any, ticker: str, ex_date: str, price_factor: float) -> Dict[str, Any]:
    """Backward-adjust every row for `ticker` strictly before `ex_date` by
    `price_factor`. Returns a summary dict; does not write unless the
    caller is holding a writable connection (dry-run callers pass a
    read_only conn and this function only ever SELECTs)."""
    rows = conn.execute(
        """
        SELECT date, open, high, low, close, volume, adj_factor
        FROM ohlcv_adjusted
        WHERE ticker = ? AND date < ?
        ORDER BY date
        """,
        [ticker, ex_date],
    ).fetchdf()

    if rows.empty:
        return {"ticker": ticker, "status": "no_rows_before_ex_date", "n_rows": 0}

    # Idempotency: if adj_factor already reflects this exact correction
    # (median of the rows' own factor, since a prior partial run or a
    # pre-existing SPLIT/BONUS factor could already be layered in), a
    # re-apply would double-adjust -- skip instead.
    current_factor = float(rows["adj_factor"].fillna(1.0).median())
    if abs(current_factor - price_factor) < ADJ_FACTOR_TOLERANCE:
        return {"ticker": ticker, "status": "already_applied", "n_rows": len(rows)}

    return {"ticker": ticker, "status": "pending", "n_rows": len(rows), "rows": rows}


def _write_one(conn: Any, ticker: str, price_factor: float, rows: pd.DataFrame) -> None:
    stage = rows[["date", "open", "high", "low", "close", "volume"]].copy()
    stage["raw_open"] = stage["open"]
    stage["raw_high"] = stage["high"]
    stage["raw_low"] = stage["low"]
    stage["raw_close"] = stage["close"]
    stage["raw_volume"] = stage["volume"]
    stage["new_open"] = stage["open"] * price_factor
    stage["new_high"] = stage["high"] * price_factor
    stage["new_low"] = stage["low"] * price_factor
    stage["new_close"] = stage["close"] * price_factor
    stage["new_adj_factor"] = rows["adj_factor"].fillna(1.0) * price_factor
    stage["ticker_col"] = ticker

    conn.register(
        "_emp_audit",
        stage[["date", "ticker_col", "raw_open", "raw_high", "raw_low", "raw_close", "raw_volume"]],
    )
    try:
        conn.execute(
            """
            INSERT INTO ohlcv_ca_audit
                (date, ticker, raw_open, raw_high, raw_low, raw_close, raw_volume,
                 adj_factor, vol_adj_factor)
            SELECT date, ticker_col, raw_open, raw_high, raw_low, raw_close,
                   CAST(raw_volume AS BIGINT), 1.0, 1.0
            FROM _emp_audit
            ON CONFLICT (date, ticker) DO NOTHING
            """
        )
    finally:
        conn.unregister("_emp_audit")

    conn.register(
        "_emp_updates",
        stage[["date", "ticker_col", "new_open", "new_high", "new_low", "new_close", "new_adj_factor"]],
    )
    try:
        conn.execute(
            """
            UPDATE ohlcv_adjusted
            SET open = u.new_open, high = u.new_high, low = u.new_low,
                close = u.new_close, adj_factor = u.new_adj_factor
            FROM _emp_updates u
            WHERE ohlcv_adjusted.ticker = u.ticker_col AND ohlcv_adjusted.date = u.date
            """
        )
    finally:
        conn.unregister("_emp_updates")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="empirical_corrections CSV (ticker, ex_date, status, price_factor, ...)")
    ap.add_argument("--ticker", default=None, help="Limit to one ticker")
    ap.add_argument(
        "--min-status", default="ok", choices=["ok", "low_confidence"],
        help="'ok' (default) applies only high-confidence rows; 'low_confidence' widens to include those too -- do this only after human review.",
    )
    ap.add_argument("--apply", action="store_true", help="Actually write. Without this, dry-run only.")
    args = ap.parse_args()

    corrections = pd.read_csv(args.input)
    statuses = ["ok"] if args.min_status == "ok" else ["ok", "low_confidence"]
    corrections = corrections[corrections["status"].isin(statuses)]
    if args.ticker:
        corrections = corrections[corrections["ticker"] == args.ticker]
    # A ticker with more than one corporate action (e.g. STAR: 2013-12-19
    # and 2024-12-06) must be applied earliest-first -- each backward
    # adjustment reads ohlcv_adjusted's CURRENT stored values, so applying
    # out of order would compound the later factor into rows the earlier
    # correction hasn't touched yet, or vice versa. Sorting here removes
    # the input CSV's own row order as a source of correctness.
    corrections = corrections.sort_values(["ticker", "ex_date"])

    summary: List[Dict[str, Any]] = []
    with get_duckdb_connection(DUCKDB_PATH, read_only=not args.apply, persist=False) as conn:
        for row in corrections.itertuples():
            ticker, ex_date, price_factor = str(row.ticker), str(row.ex_date), float(row.price_factor)
            result = _apply_one(conn, ticker, ex_date, price_factor)
            demerger_note = " [Demerger/Scheme -- continuity fix only, not economic completeness]" if getattr(row, "is_demerger_or_scheme", False) else ""

            if result["status"] == "pending":
                print(f"{ticker} {ex_date}: {result['n_rows']} rows would be backward-adjusted by factor={price_factor:.4f}{demerger_note}")
                if args.apply:
                    _write_one(conn, ticker, price_factor, result["rows"])
                    conn.commit()
                    print(f"{ticker}: APPLIED")
                    result["status"] = "applied"
            else:
                print(f"{ticker} {ex_date}: {result['status']} ({result['n_rows']} rows)")

            summary.append({"ticker": ticker, "ex_date": ex_date, "price_factor": price_factor, **{k: v for k, v in result.items() if k != "rows"}})

    out = pd.DataFrame(summary)
    print("\n" + ("APPLY" if args.apply else "DRY RUN") + f" summary (min_status={args.min_status}):")
    print(out.to_string())


if __name__ == "__main__":
    main()
