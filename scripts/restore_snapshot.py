"""
scripts/restore_snapshot.py

Phase: A25 (Write-Audit-Publish Architecture)
Owner: Platform / DataStore

CLI to restore one or more production tables from a rollback snapshot
(datastore/staging/snapshot.py). Destructive — replaces the current
production table content — so this prompts for confirmation before
executing (matches this project's caution norms around destructive
operations), and always takes a fresh "pre-restore" safety snapshot of
current state first, so a bad restore is itself reversible.

Usage:
    python scripts/restore_snapshot.py --date 2026-07-08
    python scripts/restore_snapshot.py --date 2026-07-08 --table fno_data
    python scripts/restore_snapshot.py --date 2026-07-08 --yes   # skip prompt
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

from config.settings import DUCKDB_PATH, SNAPSHOT_DIR  # noqa: E402
from datastore.api.db import get_duckdb_connection  # noqa: E402
from datastore.staging.publish import publish_run_lock  # noqa: E402
from datastore.staging.snapshot import (  # noqa: E402
    list_snapshot_dates,
    restore_snapshot,
    take_snapshot,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Restore table(s) from an A25 rollback snapshot")
    parser.add_argument("--date", required=True, metavar="YYYY-MM-DD", help="Snapshot date to restore")
    parser.add_argument("--table", action="append", dest="tables", default=None,
                         help="Table to restore (repeatable). Default: every table in the snapshot.")
    parser.add_argument("--yes", action="store_true", help="Skip the confirmation prompt")
    args = parser.parse_args()

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        available_dates = list_snapshot_dates(SNAPSHOT_DIR)
        if args.date not in available_dates:
            logger.error("No snapshot for '%s'. Available dates: %s", args.date, available_dates)
            sys.exit(1)

        target_tables = args.tables or [
            p.stem for p in (SNAPSHOT_DIR / args.date).glob("*.parquet")
        ]
        logger.warning(
            "This will REPLACE current production content of %s with the '%s' snapshot.",
            target_tables, args.date,
        )
        if not args.yes:
            resp = input("Type 'yes' to proceed: ").strip().lower()
            if resp != "yes":
                logger.info("Aborted.")
                sys.exit(1)

        with publish_run_lock() as acquired:
            if not acquired:
                logger.error("Another publish/restore is already in progress. Try again shortly.")
                sys.exit(1)

            pre_restore_date = f"pre_restore_{datetime.now().strftime('%Y%m%dT%H%M%S')}"
            take_snapshot(conn, target_tables, SNAPSHOT_DIR, snapshot_date=pre_restore_date)
            logger.info("Pre-restore safety snapshot taken: %s", pre_restore_date)

            restored = restore_snapshot(conn, SNAPSHOT_DIR, args.date, tables=target_tables)
            logger.info("Restored: %s", restored)


if __name__ == "__main__":
    main()
