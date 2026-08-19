"""
strategies/migrations/clean_stale_band_catalog.py

Owner: Platform / Architecture
Run: PYTHONPATH=$PWD .venv/bin/python -m strategies.migrations.clean_stale_band_catalog [--dry-run]

Companion to the 2026-08-19 RANK_BANDS renumbering. Clears the rank-band rows
from `strategy_catalog`, whose descriptors were written by research sweeps
against band tables that no longer exist.

Why delete rather than rename
-----------------------------
Only three of the eight descriptor families could be renamed truthfully:

    band1_1-50      -> unchanged
    band2_51-100    -> unchanged
    band7_501-800   -> unchanged (same range, same id)

    band3_100-150   -> today's band 3 is 101-150 (one rank narrower)
    band4_150-200   -> today's band 4 is 151-200 (one rank narrower)
    band5_100-200   -> retired outright; overlapped bands 3 and 4
    band8_201-250   -> no current band is 201-250 (today's band 5 is 201-300)
    band6_251-500   -> no current band is 251-500 (today's band 6 is 301-500)

Renaming the last five would file a result under a universe it was never
measured against. And the rows are not results in any case: all 480 are
ORPHANS -- none has a `latest_run_id` that still resolves in `backtest_runs`,
and none has trades. They claim 1,380 runs that no longer exist, which is
worse than having no row at all, because the frontend lists them as though
they were real.

The user confirmed on 2026-08-19 that historical band data is not referred
to, so these are dropped and will repopulate correctly the next time the band
sweep runs against the corrected seven-band partition.

Rollback
--------
The deleted rows are written to a CSV beside the database before the DELETE
runs (a full DB copy would be ~7GB for a 480-row change).
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

from typing import Any, List, Optional

from config.settings import BACKTEST_DUCKDB_PATH

logger = logging.getLogger(__name__)

def _scalar(conn: duckdb.DuckDBPyConnection, sql: str, params: Optional[List[Any]] = None) -> Any:
    """The single value of a one-row query.

    `fetchone()` is typed Optional because a SELECT can return no rows -- but
    an aggregate always returns exactly one, so None here means the query was
    not the aggregate it looks like. Failing loudly beats indexing None.
    """
    row = conn.execute(sql, params or []).fetchone()
    if row is None:
        raise RuntimeError(f"expected one row, got none: {sql}")
    return row[0]


#: Rows this script owns: momentum rank-band catalog entries.
WHERE_CLAUSE = "channel = 'momentum' AND descriptor LIKE 'band%'"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=Path(BACKTEST_DUCKDB_PATH).parent,
        help="where the pre-delete CSV of the affected rows is written",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    conn = duckdb.connect(str(BACKTEST_DUCKDB_PATH), read_only=args.dry_run)
    try:
        row = conn.execute(
            f"""
            SELECT count(*),
                   count(*) FILTER (
                       WHERE NOT EXISTS (
                           SELECT 1 FROM backtest_runs r WHERE r.run_id = s.latest_run_id
                       )
                   )
            FROM strategy_catalog s WHERE {WHERE_CLAUSE}
            """
        ).fetchone()
        if row is None:
            raise RuntimeError("count query returned no row")
        n_total, n_orphaned = row
        logger.info("band catalog rows: %d total, %d orphaned", n_total, n_orphaned)

        if n_orphaned != n_total:
            # A row whose run still resolves is a real result, not stale
            # bookkeeping -- refuse rather than delete it as collateral.
            raise SystemExit(
                f"refusing to delete: {n_total - n_orphaned} of {n_total} band rows "
                "still resolve to a live backtest_runs row. Re-check before dropping."
            )

        if args.dry_run:
            logger.info("[dry-run] would delete %d rows", n_total)
            return

        args.backup_dir.mkdir(parents=True, exist_ok=True)
        csv_path = args.backup_dir / "strategy_catalog_bands_predelete_20260819.csv"
        conn.execute(
            f"COPY (SELECT * FROM strategy_catalog WHERE {WHERE_CLAUSE}) "
            f"TO '{csv_path}' (HEADER, DELIMITER ',')"
        )
        logger.info("wrote rollback CSV to %s", csv_path)

        conn.execute(f"DELETE FROM strategy_catalog WHERE {WHERE_CLAUSE}")
        remaining = _scalar(
            conn, f"SELECT count(*) FROM strategy_catalog WHERE {WHERE_CLAUSE}"
        )
        logger.info("deleted=%d remaining=%d", n_total, remaining)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
