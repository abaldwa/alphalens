"""
scripts/run_price_adjuster.py

Runs the price adjuster for every ticker in ohlcv_adjusted, processing in
small batches to stay within memory limits (the full universe in one pass
exhausts RAM on a 16 GB laptop).

Usage
-----
    # All tickers in DB (default)
    .venv/bin/python3 scripts/run_price_adjuster.py

    # Universe-only (~2492 active tickers)
    .venv/bin/python3 scripts/run_price_adjuster.py --universe-only

    # Only tickers not yet in the audit table (skip already-adjusted)
    .venv/bin/python3 scripts/run_price_adjuster.py --skip-adjusted

    # Background overnight run
    nohup .venv/bin/python3 scripts/run_price_adjuster.py \\
        > logs/price_adjuster.log 2>&1 &
    tail -f logs/price_adjuster.log
"""

import argparse
import gc
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 30  # tickers per DB connection; keeps peak memory under ~1 GB


def main() -> None:
    parser = argparse.ArgumentParser(description="Run price adjuster for all DB tickers")
    parser.add_argument("--universe-only", action="store_true",
                        help="Only adjust the current ~2492-ticker active universe (default: all tickers in DB)")
    parser.add_argument("--skip-adjusted", action="store_true",
                        help="Skip tickers that already have rows in ohlcv_ca_audit")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"Tickers per DB connection (default: {BATCH_SIZE})")
    args = parser.parse_args()

    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection
    from ingestion.adjust.price_adjuster import adjust_for_corporate_actions

    # ── Resolve ticker list ───────────────────────────────────────────────
    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        if args.universe_only:
            from config.universe import get_tickers
            all_tickers = get_tickers()
        else:
            rows = conn.execute(
                "SELECT DISTINCT ticker FROM ohlcv_adjusted ORDER BY ticker"
            ).fetchall()
            all_tickers = [r[0] for r in rows]

        # scripts/fyers_correct_split_bonus_windows.py (2026-07-30) writes
        # Fyers' own already-adjusted OHLCV directly for these tickers —
        # never layer our own multiplicative adj_factor on top of that.
        try:
            fyers_corrected = {
                r[0] for r in conn.execute("SELECT DISTINCT ticker FROM fyers_ca_corrected").fetchall()
            }
        except Exception:
            fyers_corrected = set()
        if fyers_corrected:
            before = len(all_tickers)
            all_tickers = [t for t in all_tickers if t not in fyers_corrected]
            logger.info(
                "Excluding %d ticker(s) already Fyers-corrected (fyers_ca_corrected): %d -> %d",
                len(fyers_corrected), before, len(all_tickers),
            )

        if args.skip_adjusted:
            already = {r[0] for r in conn.execute(
                "SELECT DISTINCT ticker FROM ohlcv_ca_audit"
            ).fetchall()}
            tickers = [t for t in all_tickers if t not in already]
            logger.info(
                "Ticker pool: %d total, %d already adjusted, %d remaining",
                len(all_tickers), len(already), len(tickers),
            )
        else:
            tickers = all_tickers
            logger.info("Ticker pool: %d tickers", len(tickers))

    if not tickers:
        logger.info("Nothing to do.")
        return

    # ── Batch loop ────────────────────────────────────────────────────────
    ok = err = 0
    t_start = time.monotonic()

    for batch_start in range(0, len(tickers), args.batch_size):
        batch = tickers[batch_start: batch_start + args.batch_size]

        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            for ticker in batch:
                try:
                    adjust_for_corporate_actions(conn, ticker)
                    ok += 1
                except Exception as exc:
                    logger.error("FAILED %s: %s", ticker, exc)
                    err += 1

        gc.collect()
        done = min(batch_start + args.batch_size, len(tickers))
        elapsed = time.monotonic() - t_start
        rate = done / elapsed if elapsed > 0 else 0
        eta_min = (len(tickers) - done) / rate / 60 if rate > 0 else 0
        logger.info(
            "[%d/%d] ok=%d err=%d  %.1f t/s  ETA ~%.0f min",
            done, len(tickers), ok, err, rate, eta_min,
        )

    # ── Summary ──────────────────────────────────────────────────────────
    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        n_audit  = conn.execute("SELECT COUNT(*), COUNT(DISTINCT ticker) FROM ohlcv_ca_audit").fetchone()
        n_adj    = conn.execute(
            "SELECT COUNT(DISTINCT ticker) FROM ohlcv_adjusted WHERE adj_factor != 1.0"
        ).fetchone()[0]
        sample   = conn.execute("""
            SELECT a.ticker, COUNT(*) AS modified_rows,
                   MIN(a.adj_factor) AS min_factor, MAX(a.adj_factor) AS max_factor
            FROM ohlcv_ca_audit a
            GROUP BY a.ticker
            ORDER BY modified_rows DESC
            LIMIT 10
        """).fetchall()

    total_min = (time.monotonic() - t_start) / 60
    logger.info("─" * 60)
    logger.info("Price adjuster complete in %.1f min", total_min)
    logger.info("  Tickers processed : %d ok, %d errors", ok, err)
    logger.info("  ohlcv_ca_audit    : %d rows across %d tickers", n_audit[0], n_audit[1])
    logger.info("  adj_factor != 1.0 : %d tickers", n_adj)
    logger.info("Top 10 most-adjusted tickers:")
    for r in sample:
        logger.info("  %-15s  %5d rows  factor range [%.4f – %.4f]", r[0], r[1], r[2], r[3])


if __name__ == "__main__":
    main()
