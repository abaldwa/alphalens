"""
scripts/backfill_mf_holdings.py

[2026-09-10] Ad-hoc multi-month MF-holdings catch-up, split into the same
fetch-cache vs persist pattern as the other backfills. The underlying
ingestion.scrapers.amfi_holdings module already writes to local parquet
(datastore/normalised/mf_holdings/YYYY-MM.parquet) with zero DB touch via
run_monthly_ingestion() — this script just exposes that for an arbitrary
list of months plus a --persist step for sync_duckdb_table(), which is
otherwise only wired into the weekly scheduler job.

Usage
-----
    # Fetch-only: cache July + August 2026 to local parquet, no DB lock
    .venv/bin/python3 scripts/backfill_mf_holdings.py --months 2026-07 2026-08 --mode fetch

    # Persist-only: publish those cached parquet files to DuckDB (needs lock)
    .venv/bin/python3 scripts/backfill_mf_holdings.py --months 2026-07 2026-08 --mode persist
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)


def _parse_months(values: "list[str]") -> "list[tuple[int, int]]":
    out = []
    for v in values:
        year_s, month_s = v.split("-")
        out.append((int(year_s), int(month_s)))
    return out


def fetch_months(months: "list[tuple[int, int]]") -> None:
    from ingestion.scrapers.amfi_holdings import run_monthly_ingestion
    from ingestion.scrapers.groww_mf_holdings import register_all_amcs

    n = register_all_amcs()
    logger.info(f"Registered {n} Groww-backed AMCs")
    for year, month in months:
        try:
            path = run_monthly_ingestion(year, month)
            logger.info(f"[fetch] {year:04d}-{month:02d} -> {path} (no DB touch)")
        except RuntimeError as exc:
            # SPEC-PIPE-003: disclosure not yet public for this month — expected
            # for the most recent month if run before the ~5th of next month.
            logger.warning(f"[fetch] {year:04d}-{month:02d}: {exc}")


def persist_months(months: "list[tuple[int, int]]") -> None:
    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection
    from ingestion.scrapers.amfi_holdings import sync_duckdb_table

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        for year, month in months:
            n = sync_duckdb_table(conn, year, month)
            logger.info(f"[persist] {year:04d}-{month:02d}: {n} rows synced to mf_holdings")


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-month MF holdings catch-up (fetch/persist split)")
    parser.add_argument("--months", nargs="+", required=True, help="YYYY-MM, one or more, e.g. 2026-07 2026-08")
    parser.add_argument("--mode", choices=["fetch", "persist", "both"], default="both")
    args = parser.parse_args()

    months = _parse_months(args.months)

    if args.mode in ("fetch", "both"):
        fetch_months(months)
    if args.mode in ("persist", "both"):
        persist_months(months)


if __name__ == "__main__":
    main()
