#!/usr/bin/env python3
"""
scripts/migrate_backtest_trades_add_channel.py

Adds channel + backtest-date columns to an existing backtest_trades table and
backfills them from backtest_runs.

[2026-08-11] backtest_trades recorded WHAT was traded (ticker, buy/sale, qty,
P&L) and WHICH strategy, but not WHICH ENGINE produced it, nor WHEN the
backtest itself ran. So "show me every Technical trade" or "only trades from
today's run" required joining backtest_runs — and that join is exactly what the
table was designed to avoid, since a trade whose parent run row is missing or
purged silently drops out of the answer.

Adds, matching scripts/load_trade_books_to_db.py:
    channel              'technical' | 'momentum' | 'fundamental' | 'ml'
    backtest_run_at      when the backtest EXECUTED (runs.created_at)
    backtest_start_date  first date the backtest COVERED
    backtest_end_date    last date the backtest COVERED

Three distinct date questions get asked of this table and they are not the same:
when the trade happened (buy_date/sale_date), when the backtest ran
(backtest_run_at), and what period it covered (backtest_start/end_date).

Idempotent: skips columns that already exist, and re-backfills only NULLs.

Usage:
    python scripts/migrate_backtest_trades_add_channel.py            # dry run
    python scripts/migrate_backtest_trades_add_channel.py --apply
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb  # noqa: E402

from config.settings import BACKTEST_DUCKDB_PATH  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

NEW_COLUMNS = {
    "channel": "VARCHAR",
    "backtest_run_at": "TIMESTAMP",
    "backtest_start_date": "DATE",
    "backtest_end_date": "DATE",
}

_VIEW = """
CREATE OR REPLACE VIEW backtest_trades_enriched AS
SELECT t.*, r.mode, r.initial_capital
FROM backtest_trades t
LEFT JOIN backtest_runs r USING (run_id)
"""


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-path", default=str(BACKTEST_DUCKDB_PATH))
    p.add_argument("--apply", action="store_true", help="Apply (default is a dry run)")
    args = p.parse_args()

    conn = duckdb.connect(args.db_path, read_only=not args.apply)
    try:
        existing = {r[0] for r in conn.execute("DESCRIBE backtest_trades").fetchall()}
        missing = {c: t for c, t in NEW_COLUMNS.items() if c not in existing}
        total = conn.execute("SELECT COUNT(*) FROM backtest_trades").fetchone()[0]

        logger.info("backtest_trades: %d rows; columns to add: %s", total, list(missing) or "none")
        if not args.apply:
            logger.info("dry run - nothing changed. Re-run with --apply.")
            return

        for col, coltype in missing.items():
            conn.execute(f"ALTER TABLE backtest_trades ADD COLUMN {col} {coltype}")
            logger.info("  added %s %s", col, coltype)

        # Backfill from the parent run. Trades whose run row was purged keep
        # NULLs rather than being dropped or guessed at.
        conn.execute(
            """
            UPDATE backtest_trades t SET
                channel             = r.channel,
                backtest_run_at     = r.created_at,
                backtest_start_date = r.start_date,
                backtest_end_date   = r.end_date
            FROM backtest_runs r
            WHERE t.run_id = r.run_id
              AND (t.channel IS NULL OR t.backtest_run_at IS NULL)
            """
        )
        conn.execute(_VIEW)
        conn.execute("CHECKPOINT")

        logger.info("backfill by channel:")
        for ch, n in conn.execute(
            "SELECT COALESCE(channel,'(unmatched)'), COUNT(*) FROM backtest_trades GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall():
            logger.info("  %-14s %d", ch, n)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
