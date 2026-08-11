#!/usr/bin/env python3
"""
scripts/load_trade_books_to_db.py

Loads per-run trade logs (backtest/reports/trade_log_<run_id>.csv) into a
queryable `backtest_trades` table in BACKTEST_DUCKDB_PATH.

[2026-08-10] Written because individual trades were NOT queryable. Runs were
persisted to `backtest_runs`, and `backtest_feature_log` held per-ticker
decisions, but the trades themselves — entry, exit, P&L, exit reason — existed
only as ~3,800 loose CSV files. `export_trade_book()` opens the DB read-only
(purely to enrich its CSV output), so nothing ever wrote them to a table.
Answering "show me every trade across all strategies" therefore meant globbing
CSVs instead of running a query.

The table joins to backtest_runs on run_id, which is what carries strategy_id,
channel, template and the run window — so per-strategy, per-style and
per-period slicing all work from SQL.

Idempotent: a run's rows are deleted before re-insert, so re-running after more
jobs finish is safe (the autopilot calls this after the queue).

Usage:
    python scripts/load_trade_books_to_db.py                    # all runs
    python scripts/load_trade_books_to_db.py --suffix ta_full_2007_2026
    python scripts/load_trade_books_to_db.py --since 2026-08-10
"""

import argparse
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb  # noqa: E402

from config.settings import BACKTEST_DUCKDB_PATH  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path("backtest/reports")
_RUN_ID_RE = re.compile(r"trade_log_(.+)\.csv$")

_CREATE = """
CREATE TABLE IF NOT EXISTS backtest_trades (
    run_id        VARCHAR NOT NULL,
    -- Strategy identity is DENORMALISED onto every trade row on purpose.
    -- Deriving it by joining backtest_runs makes the strategy NULL whenever
    -- that row is missing or written later, which is exactly what happened in
    -- testing (4 runs collapsed to 3 strategy groups). A trade must be
    -- self-describing: strategy, stock, buy, sale — all on the row.
    strategy_id   VARCHAR,
    template_name VARCHAR,
    exit_variant  VARCHAR,
    -- [2026-08-11] Which engine produced this trade — 'technical',
    -- 'momentum', 'fundamental' or 'ml'. Same denormalisation rationale:
    -- "show me every Technical trade" must not depend on backtest_runs
    -- still holding the parent row.
    channel       VARCHAR,
    -- WHEN the backtest was executed (backtest_runs.created_at), as distinct
    -- from when the trade happened (buy_date/sale_date) and from the window
    -- the backtest covered (backtest_start_date/backtest_end_date). All three
    -- are different questions and all three get asked; keeping only buy/sale
    -- made "which run produced this?" unanswerable without a join.
    backtest_run_at     TIMESTAMP,
    backtest_start_date DATE,
    backtest_end_date   DATE,
    ticker        VARCHAR NOT NULL,
    qty           DOUBLE,
    buy_date      DATE,
    buy_price     DOUBLE,
    sale_date     DATE,
    sale_price    DOUBLE,
    stock_rank    INTEGER,
    pnl_inr       DOUBLE,
    pnl_pct       DOUBLE,
    exit_reason   VARCHAR,
    -- Derived once here rather than in every query.
    holding_days  INTEGER,
    buy_value     DOUBLE,
    sale_value    DOUBLE,
    -- Indian financial year of the EXIT (Apr 1 - Mar 31), e.g. 'FY2007-08',
    -- because realised P&L is taxed in the year the trade closes.
    financial_year VARCHAR
)
"""

# An open-at-end position has no sale_date and no realised P&L; it must not be
# loaded as a zero-return trade (same rule as
# ta_comprehensive_metrics.load_trade_book).
#
# Columns are named EXPLICITLY rather than relying on SELECT order. The
# migration that introduced channel/backtest_* used ALTER TABLE ADD COLUMN,
# which appends to the END of the table, so a positional INSERT built in the
# logical column order silently lined backtest_run_at up against qty and failed
# with "Unimplemented type for cast (TIMESTAMP -> DOUBLE)". A named list is
# order-independent and cannot drift as columns are added.
_INSERT = """
INSERT INTO backtest_trades (
    run_id, strategy_id, template_name, exit_variant, channel,
    backtest_run_at, backtest_start_date, backtest_end_date,
    ticker, qty, buy_date, buy_price, sale_date, sale_price, stock_rank,
    pnl_inr, pnl_pct, exit_reason, holding_days, buy_value, sale_value,
    financial_year
)
SELECT
    ? AS run_id,
    ? AS strategy_id,
    ? AS template_name,
    ? AS exit_variant,
    ? AS channel,
    TRY_CAST(? AS TIMESTAMP) AS backtest_run_at,
    TRY_CAST(? AS DATE) AS backtest_start_date,
    TRY_CAST(? AS DATE) AS backtest_end_date,
    ticker,
    TRY_CAST(qty AS DOUBLE),
    TRY_CAST(buy_date AS DATE),
    TRY_CAST(buy_price AS DOUBLE),
    TRY_CAST(sale_date AS DATE),
    TRY_CAST(sale_price AS DOUBLE),
    TRY_CAST(stock_rank AS INTEGER),
    TRY_CAST(pnl_inr AS DOUBLE),
    TRY_CAST(pnl_pct AS DOUBLE),
    exit_reason,
    DATE_DIFF('day', TRY_CAST(buy_date AS DATE), TRY_CAST(sale_date AS DATE)),
    TRY_CAST(qty AS DOUBLE) * TRY_CAST(buy_price AS DOUBLE),
    TRY_CAST(qty AS DOUBLE) * TRY_CAST(sale_price AS DOUBLE),
    'FY' || CAST(
        CASE WHEN MONTH(TRY_CAST(sale_date AS DATE)) >= 4
             THEN YEAR(TRY_CAST(sale_date AS DATE))
             ELSE YEAR(TRY_CAST(sale_date AS DATE)) - 1 END AS VARCHAR)
        || '-' || RIGHT(CAST(
        CASE WHEN MONTH(TRY_CAST(sale_date AS DATE)) >= 4
             THEN YEAR(TRY_CAST(sale_date AS DATE)) + 1
             ELSE YEAR(TRY_CAST(sale_date AS DATE)) END AS VARCHAR), 2)
FROM read_csv_auto(?, union_by_name=true, all_varchar=true)
WHERE TRY_CAST(sale_date AS DATE) IS NOT NULL
  AND TRY_CAST(buy_date AS DATE) IS NOT NULL
"""

# Convenience view: trades joined to their run's identity, so slicing by
# strategy/channel/window needs no manual join.
# channel / start_date / end_date / created_at are NOT re-selected from r:
# they now live on backtest_trades itself, and selecting both sides would
# produce duplicate column names in the view.
_CREATE_VIEW = """
CREATE OR REPLACE VIEW backtest_trades_enriched AS
SELECT t.*, r.mode, r.initial_capital
FROM backtest_trades t
LEFT JOIN backtest_runs r USING (run_id)
"""


def load(paths, db_path: Path) -> int:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(_CREATE)
        total = 0
        for i, path in enumerate(paths, 1):
            m = _RUN_ID_RE.search(path.name)
            if not m:
                continue
            run_id = m.group(1)
            meta = conn.execute(
                """SELECT strategy_id,
                          json_extract_string(config_json, '$.template_name'),
                          exit_policy_variant,
                          channel, created_at, start_date, end_date
                   FROM backtest_runs WHERE run_id = ?""",
                [run_id],
            ).fetchone() or (None,) * 7
            conn.execute("DELETE FROM backtest_trades WHERE run_id = ?", [run_id])
            try:
                conn.execute(_INSERT, [run_id, *meta, str(path)])
            except duckdb.Error as exc:
                # One malformed CSV must not sink the whole load.
                logger.warning("skipping %s — %s", path.name, exc)
                continue
            if i % 250 == 0:
                total = conn.execute("SELECT COUNT(*) FROM backtest_trades").fetchone()[0]
                logger.info("  %d/%d files, %d trades loaded", i, len(paths), total)
        conn.execute(_CREATE_VIEW)
        total = conn.execute("SELECT COUNT(*) FROM backtest_trades").fetchone()[0]
        runs = conn.execute("SELECT COUNT(DISTINCT run_id) FROM backtest_trades").fetchone()[0]
        logger.info("backtest_trades: %d trades across %d runs", total, runs)
        return total
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports-dir", default=str(REPORTS_DIR))
    parser.add_argument("--suffix", help="Only runs whose report suffix matches (substring of run_id)")
    parser.add_argument("--db-path", default=str(BACKTEST_DUCKDB_PATH))
    args = parser.parse_args()

    paths = sorted(Path(args.reports_dir).glob("trade_log_*.csv"))
    if args.suffix:
        # The queue names reports by suffix; map suffix -> run_ids via
        # backtest_runs so a filtered load still finds the right files.
        conn = duckdb.connect(str(args.db_path), read_only=True)
        try:
            ids = {
                r[0] for r in conn.execute(
                    "SELECT run_id FROM backtest_runs WHERE run_id LIKE ? OR strategy_id LIKE ?",
                    [f"%{args.suffix}%", f"%{args.suffix}%"],
                ).fetchall()
            }
        finally:
            conn.close()
        if ids:
            paths = [p for p in paths if _RUN_ID_RE.search(p.name).group(1) in ids]

    logger.info("loading %d trade-log files into %s", len(paths), args.db_path)
    load(paths, Path(args.db_path))


if __name__ == "__main__":
    main()
