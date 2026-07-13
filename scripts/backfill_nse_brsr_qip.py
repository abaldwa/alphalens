"""
scripts/backfill_nse_brsr_qip.py

CA6 (2026-07-10): backfill/refresh of qip_details + brsr_filings from NSE's
real corporate-further-issues-qip / corporate-bussiness-sustainabilitiy
endpoints — see ingestion/scrapers/nse_brsr_qip.py's module docstring for
the live-verification detail.

SPEC-DS-002 exception (documented precedent — see
ingestion/backfill_runner.py's module docstring for the same reasoning):
bulk backfill scripts write directly to DuckDB, not through the
DataStoreClient API, same as the other NSE-sourced backfill scripts.

Usage:
    .venv/bin/python3 scripts/backfill_nse_brsr_qip.py [--limit N] [--dry-run] [--publish-mode staged|direct]
"""

import argparse
import logging

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from ingestion.scrapers.nse_brsr_qip import download_brsr_filings, download_qip_issues

logger = logging.getLogger(__name__)

_QIP_COLUMNS = [
    "ticker", "app_id", "board_resolution_date", "allotment_date", "listing_date",
    "issue_price", "min_issue_price", "final_issue_size", "no_of_allottees",
    "no_of_shares_allotted", "no_of_equity_shares_listed", "dilution_pct",
]
_BRSR_COLUMNS = ["ticker", "fy_from", "fy_to", "submission_date", "xbrl_file_url", "attachment_file_url"]


def _upsert_direct(conn, table: str, columns: "list[str]", key_cols: "list[str]", rows: "list[dict]") -> None:
    if not rows:
        return
    col_list_sql = ", ".join(columns)
    update_clause = ", ".join(f"{c} = excluded.{c}" for c in columns if c not in key_cols)
    placeholders = ", ".join("?" for _ in columns)
    conn.executemany(
        f"""
        INSERT INTO {table} ({col_list_sql}) VALUES ({placeholders})
        ON CONFLICT ({", ".join(key_cols)}) DO UPDATE SET {update_clause}
        """,
        [tuple(r.get(c) for c in columns) for r in rows],
    )


def _upsert_staged(conn, table: str, columns: "list[str]", key_cols: "list[str]", rows: "list[dict]") -> None:
    import pandas as pd

    from datastore.staging.gate import stage_dataframe
    from datastore.staging.merge import coalesce_merge
    from datastore.staging.publish import publish_run_lock, publish_table

    if not rows:
        return
    new_df = pd.DataFrame(rows, columns=columns)
    existing_df = conn.execute(f"SELECT * FROM {table}").df()
    merged_df = coalesce_merge(existing_df, new_df, key_cols=key_cols, new_wins=True)
    with publish_run_lock() as acquired:
        if not acquired:
            logger.error(f"Another publish is in progress — staged {table} backfill NOT published.")
            return
        result = stage_dataframe(conn, table, merged_df, validators=[])
        if not result.ok:
            logger.error(f"Staging gate rejected the {table} batch — nothing published.")
            return
        published_rows = publish_table(conn, table)
        logger.info(f"Staged publish: {table} now has {published_rows} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill qip_details/brsr_filings from real NSE endpoints")
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N tickers")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and report, write nothing")
    parser.add_argument("--publish-mode", choices=["direct", "staged"], default="staged",
                        help="'staged' (default): atomic publish via datastore/staging, gains an N=7 "
                             "rollback point (A25). 'direct': plain upsert, no rollback.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        tickers = [r[0] for r in conn.execute("SELECT DISTINCT ticker FROM fundamentals ORDER BY ticker").fetchall()]
    if args.limit:
        tickers = tickers[: args.limit]

    logger.info(f"Scanning {len(tickers)} tickers for real QIP/BRSR filings")

    qip_rows: "list[dict]" = []
    brsr_rows: "list[dict]" = []
    for i, ticker in enumerate(tickers):
        if i % 100 == 0:
            logger.info(f"  {i}/{len(tickers)} tickers scanned ({len(qip_rows)} QIP, {len(brsr_rows)} BRSR rows so far)")
        try:
            qip_rows.extend(download_qip_issues(ticker))
        except ConnectionError as exc:
            logger.warning(f"QIP fetch failed for {ticker}: {exc}")
        try:
            brsr_rows.extend(download_brsr_filings(ticker))
        except ConnectionError as exc:
            logger.warning(f"BRSR fetch failed for {ticker}: {exc}")

    logger.info(f"Scan complete: {len(qip_rows)} QIP rows, {len(brsr_rows)} BRSR filing rows staged")

    if args.dry_run:
        for r in qip_rows[:10]:
            logger.info(f"  [dry-run QIP] {r['ticker']} appId={r['app_id']}: dilution_pct={r['dilution_pct']}")
        for r in brsr_rows[:10]:
            logger.info(f"  [dry-run BRSR] {r['ticker']} FY{r['fy_from']}-{r['fy_to']}: {r['submission_date']}")
        return

    if not qip_rows and not brsr_rows:
        logger.info("Nothing new to write.")
        return

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        if args.publish_mode == "staged":
            _upsert_staged(conn, "qip_details", _QIP_COLUMNS, ["ticker", "app_id"], qip_rows)
            _upsert_staged(conn, "brsr_filings", _BRSR_COLUMNS, ["ticker", "fy_to"], brsr_rows)
        else:
            _upsert_direct(conn, "qip_details", _QIP_COLUMNS, ["ticker", "app_id"], qip_rows)
            _upsert_direct(conn, "brsr_filings", _BRSR_COLUMNS, ["ticker", "fy_to"], brsr_rows)

    logger.info(f"Done: {len(qip_rows)} qip_details rows, {len(brsr_rows)} brsr_filings rows upserted")


if __name__ == "__main__":
    main()
