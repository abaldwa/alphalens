"""
Validate SPLIT/BONUS/RIGHTS rows in `corporate_actions` against real Fyers
history, marking each row in `corporate_actions_validation` confirmed or
mismatched. Resumable (skips rows already validated) and budget-capped
(stops cleanly when the per-run call cap is hit, leaving the rest
'unchecked' for a later run) since Fyers self-imposes a 1000-calls/day
soft limit shared with other jobs.

Methodology note: Fyers' `history` endpoint returns split/bonus-ADJUSTED
continuous prices (confirmed during the 2026-07-05 full-universe
comparison), so a raw pre/post price jump around the ex_date in Fyers data
is NOT a valid signal — the whole point of adjustment is that Fyers'
series has no jump there. The valid check is: our own adj_close divided by
Fyers' close should be a roughly constant ratio across the window if our
adjustment is correct. If our adjuster missed the action (or used the
wrong ratio), that ratio will itself jump across the ex_date, since ours
is not smoothing what Fyers already smoothed.

Usage:
    python3 scripts/validate_corporate_actions_fyers.py [--max-calls N] [--window-days N]
"""
import argparse
import logging
import sys
from datetime import timedelta

import duckdb
import pandas as pd

sys.path.insert(0, ".")
from config.settings import DUCKDB_PATH
from ingestion.scrapers.fyers_backfill import FYERSBackfill

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MISMATCH_THRESHOLD_PCT = 5.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-calls", type=int, default=150)
    ap.add_argument("--window-days", type=int, default=10)
    args = ap.parse_args()

    conn = duckdb.connect(str(DUCKDB_PATH))
    pending = conn.execute(
        """
        SELECT v.ticker, v.ex_date, v.action_type, v.ratio, c.details
        FROM corporate_actions_validation v
        JOIN corporate_actions c
          ON v.ticker = c.ticker AND v.ex_date = c.ex_date AND v.action_type = c.action_type
        WHERE v.validation_status = 'unchecked'
        ORDER BY v.ticker, v.ex_date
        """
    ).fetchdf()

    logger.info("Pending validation rows: %d", len(pending))
    if pending.empty:
        conn.close()
        return

    fy = FYERSBackfill()
    calls_used = 0

    for row in pending.itertuples():
        if calls_used >= args.max_calls:
            logger.info("Reached --max-calls=%d cap; stopping cleanly for this run.", args.max_calls)
            break

        ex_date = pd.Timestamp(row.ex_date)
        win_from = (ex_date - timedelta(days=args.window_days)).strftime("%Y-%m-%d")
        win_to = (ex_date + timedelta(days=args.window_days)).strftime("%Y-%m-%d")

        try:
            hist = fy.download_history(row.ticker, win_from, win_to)
            calls_used += 1
        except RuntimeError as e:
            if "Invalid symbol" in str(e) or "-300" in str(e):
                logger.warning("Invalid Fyers symbol for %s, skipping: %s", row.ticker, e)
                conn.execute(
                    """UPDATE corporate_actions_validation
                       SET validation_status='no_fyers_data', notes=?, fyers_validated_at=now()
                       WHERE ticker=? AND ex_date=? AND action_type=?""",
                    [str(e)[:500], row.ticker, row.ex_date, row.action_type],
                )
                continue
            logger.warning("Budget exhausted or error for %s: %s", row.ticker, e)
            break
        except Exception as e:
            logger.warning("Fyers fetch failed for %s @ %s: %s", row.ticker, row.ex_date, e)
            conn.execute(
                """UPDATE corporate_actions_validation
                   SET validation_status='error', notes=?, fyers_validated_at=now()
                   WHERE ticker=? AND ex_date=? AND action_type=?""",
                [str(e)[:500], row.ticker, row.ex_date, row.action_type],
            )
            continue

        if hist.empty or len(hist) < 2:
            conn.execute(
                """UPDATE corporate_actions_validation
                   SET validation_status='no_fyers_data', fyers_validated_at=now()
                   WHERE ticker=? AND ex_date=? AND action_type=?""",
                [row.ticker, row.ex_date, row.action_type],
            )
            continue

        hist = hist.sort_values("date")
        hist["date"] = pd.to_datetime(hist["date"])

        ours = conn.execute(
            """SELECT date, close FROM ohlcv_adjusted
               WHERE ticker = ? AND date BETWEEN ? AND ? ORDER BY date""",
            [row.ticker, win_from, win_to],
        ).fetchdf()
        ours["date"] = pd.to_datetime(ours["date"])

        merged = pd.merge(ours, hist[["date", "close"]], on="date", suffixes=("_ours", "_fyers"))
        merged = merged[merged["close_fyers"] > 0]
        if merged.empty:
            conn.execute(
                """UPDATE corporate_actions_validation
                   SET validation_status='insufficient_window', fyers_validated_at=now()
                   WHERE ticker=? AND ex_date=? AND action_type=?""",
                [row.ticker, row.ex_date, row.action_type],
            )
            continue

        merged["ratio"] = merged["close_ours"] / merged["close_fyers"]
        before = merged[merged["date"] < ex_date]
        after = merged[merged["date"] >= ex_date]

        if before.empty or after.empty:
            conn.execute(
                """UPDATE corporate_actions_validation
                   SET validation_status='insufficient_window', fyers_validated_at=now()
                   WHERE ticker=? AND ex_date=? AND action_type=?""",
                [row.ticker, row.ex_date, row.action_type],
            )
            continue

        ratio_pre = before["ratio"].median()
        ratio_post = after["ratio"].median()
        pct_diff = abs(ratio_post - ratio_pre) / ratio_pre * 100 if ratio_pre else None

        if pct_diff is None:
            status = "error"
        else:
            status = "confirmed" if pct_diff <= MISMATCH_THRESHOLD_PCT else "mismatch"

        needs_retrain = status == "mismatch"

        conn.execute(
            """UPDATE corporate_actions_validation
               SET expected_price_factor=?, observed_price_factor=?, pct_diff=?,
                   validation_status=?, needs_retrain=?, fyers_validated_at=now()
               WHERE ticker=? AND ex_date=? AND action_type=?""",
            [ratio_pre, ratio_post, pct_diff, status, needs_retrain,
             row.ticker, row.ex_date, row.action_type],
        )
        logger.info(
            "%s %s %s: ratio_pre=%.4f ratio_post=%.4f pct_diff=%s -> %s",
            row.ticker, row.ex_date, row.action_type, ratio_pre, ratio_post,
            f"{pct_diff:.1f}" if pct_diff is not None else "n/a", status,
        )

    conn.close()
    logger.info("Run complete. Calls used: %d/%d", calls_used, args.max_calls)


if __name__ == "__main__":
    main()
