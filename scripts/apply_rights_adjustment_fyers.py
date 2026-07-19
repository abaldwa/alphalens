"""
scripts/apply_rights_adjustment_fyers.py

On-demand job (2026-07-19 user decision — NOT wired into the automatic
ingestion pipeline) that finds RIGHTS corporate actions with no price
adjustment applied yet and computes the correction factor live from
Fyers' own (already-adjusted) price series — see
ingestion/adjust/rights_adjuster.py for the "why Fyers, not a local
formula" rationale.

Two-phase, matching this codebase's existing align_remaining_to_fyers.py
pattern:
    1. --dry-run (default): report the Fyers-derived factor for every
       pending RIGHTS action, applying nothing. Review before touching data.
    2. --apply: actually rescale ohlcv_adjusted (open/high/low/close,
       adj_factor) for rows before ex_date, and record the correction in
       corporate_actions_validation so it isn't re-flagged as pending next
       run — same idempotency mechanism price_adjuster.py's audit table
       uses elsewhere in this codebase.

A RIGHTS action is considered "pending" if it has no corresponding
'confirmed'/'mismatch' row in corporate_actions_validation yet — i.e. it
has never been through this (or the older manual) correction process.

Usage:
    python3 scripts/apply_rights_adjustment_fyers.py [--dry-run] [--apply]
        [--window-days N] [--max-calls N] [--ticker TICKER]
"""

import argparse
import logging
import sys

import duckdb

sys.path.insert(0, ".")
from config.settings import DUCKDB_PATH
from ingestion.adjust.rights_adjuster import compute_rights_adjustment_factor
from ingestion.scrapers.fyers_backfill import FYERSBackfill

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _pending_rights_actions(conn, ticker_filter=None):
    query = """
        SELECT c.ticker, c.ex_date, c.details
        FROM corporate_actions c
        LEFT JOIN corporate_actions_validation v
               ON v.ticker = c.ticker AND v.ex_date = c.ex_date AND v.action_type = c.action_type
        WHERE c.action_type = 'RIGHTS'
          AND v.ticker IS NULL
    """
    params = []
    if ticker_filter:
        query += " AND c.ticker = ?"
        params.append(ticker_filter)
    query += " ORDER BY c.ticker, c.ex_date"
    return conn.execute(query, params).fetchdf()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Actually rescale ohlcv_adjusted (default: dry-run report only)")
    ap.add_argument("--window-days", type=int, default=10)
    ap.add_argument("--max-calls", type=int, default=150, help="Fyers call budget for this run")
    ap.add_argument("--ticker", type=str, default=None, help="Limit to one ticker (debugging)")
    args = ap.parse_args()

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=not args.apply)
    pending = _pending_rights_actions(conn, args.ticker)
    logger.info("Pending RIGHTS actions with no adjustment record: %d", len(pending))
    if pending.empty:
        conn.close()
        return

    fy = FYERSBackfill()
    calls_used = 0
    applied, skipped = [], []

    for row in pending.itertuples():
        if calls_used >= args.max_calls:
            logger.info("Reached --max-calls=%d cap; stopping cleanly for this run.", args.max_calls)
            break

        try:
            result = compute_rights_adjustment_factor(
                conn, fy, row.ticker, str(row.ex_date), window_days=args.window_days
            )
            calls_used += 1
        except Exception as exc:
            logger.warning("Fyers fetch failed for %s @ %s: %s", row.ticker, row.ex_date, exc)
            skipped.append((row.ticker, row.ex_date, str(exc)))
            continue

        if result is None:
            logger.warning("%s @ %s: could not compute a factor (insufficient data)", row.ticker, row.ex_date)
            skipped.append((row.ticker, row.ex_date, "insufficient_data"))
            continue

        logger.info(
            "%s @ %s: ratio_pre=%.4f ratio_post=%.4f price_factor=%.4f (n_pre=%d, n_post=%d)",
            result.ticker, result.ex_date, result.ratio_pre, result.ratio_post,
            result.price_factor, result.n_pre, result.n_post,
        )

        if not args.apply:
            continue

        conn.execute(
            """UPDATE ohlcv_adjusted
               SET open=open*?, high=high*?, low=low*?, close=close*?,
                   adj_factor=adj_factor*?
               WHERE ticker=? AND date < ?""",
            [result.price_factor, result.price_factor, result.price_factor,
             result.price_factor, result.price_factor, row.ticker, row.ex_date],
        )
        conn.execute(
            """INSERT INTO corporate_actions_validation
               (ticker, ex_date, action_type, expected_price_factor, observed_price_factor,
                validation_status, fyers_validated_at)
               VALUES (?, ?, 'RIGHTS', ?, ?, 'confirmed', now())
               ON CONFLICT (ticker, ex_date, action_type) DO UPDATE SET
                   expected_price_factor=excluded.expected_price_factor,
                   observed_price_factor=excluded.observed_price_factor,
                   validation_status='confirmed',
                   fyers_validated_at=now()""",
            [row.ticker, row.ex_date, result.price_factor, result.price_factor],
        )
        applied.append((row.ticker, row.ex_date, result.price_factor))

    conn.close()
    logger.info(
        "Run complete. Calls used: %d/%d. Applied: %d. Skipped: %d.%s",
        calls_used, args.max_calls, len(applied), len(skipped),
        "" if args.apply else " (dry-run — nothing written; pass --apply to rescale ohlcv_adjusted)",
    )


if __name__ == "__main__":
    main()
