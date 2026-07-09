"""
scripts/backfill_fundamentals_nse_xbrl.py

Backfill/refresh of fundamentals.{goodwill, inventories, ..., shares_outstanding}
from NSE's real regulatory Integrated Filing — IndAS disclosures. See
ingestion/scrapers/nse_xbrl_financials.py's module docstring for the
source and why this is the PREFERRED/primary fundamentals source per
explicit operator instruction (2026-07-07); Screener/Trendlyne remain the
fallback.

[REVISED 2026-07-08, per explicit operator instruction] Redesigned around
three real-data assumptions: (1) NSE's filing list call is cheap (one
HTTP call per ticker) and must always run, to discover genuinely new
filings; (2) a published filing's reported figures never change, so once
downloaded+parsed, a filing is never re-fetched or re-parsed on a later
run — raw HTML is cached locally
(config.settings.NSE_XBRL_RAW_CACHE_DIR/{seq_id}.html) and a SQLite state
table (nse_xbrl_ingested_filings, see ingestion/scrapers/
nse_xbrl_financials.py) tracks which seq_ids have already been processed
into `fundamentals`, so a re-run only downloads/parses the DELTA; (3) the
delta is staged in a DuckDB TEMP TABLE and moved into `fundamentals` in
ONE bulk upsert, not one HTTP POST per row — this also fixes a real
lock-contention problem: the old per-row-POST design held/reacquired the
DuckDB write lock potentially thousands of times over a multi-hour run,
colliding constantly with the scheduler's own DB access; a single bulk
transaction holds the lock only briefly.

SPEC-DS-002 exception (documented precedent — see
ingestion/backfill_runner.py's module docstring for the same reasoning):
bulk backfill scripts write directly to DuckDB, not through the
DataStoreClient API, same as bhavcopy.py/macro.py/price_adjuster.py.

Usage:
    .venv/bin/python3 scripts/backfill_fundamentals_nse_xbrl.py [--limit N] [--dry-run]
"""

import argparse
import logging
import sqlite3
from datetime import datetime, timedelta

from config.settings import DUCKDB_PATH, NSE_XBRL_RAW_CACHE_DIR, PIPELINE_LOG_DB_PATH
from datastore.api.db import get_duckdb_connection
from ingestion.scrapers.nse_xbrl_financials import (
    download_indas_filing,
    ensure_ingested_filings_table,
    get_ingested_seq_ids,
    list_integrated_filings,
    mark_filings_ingested,
)

logger = logging.getLogger(__name__)

# SPEC-PIPE-003 (CRITICAL): announcement_date is the PIT key. list_integrated_filings'
# raw rows carry a real broadcast_Date (live-verified, e.g. "24-Apr-2026 22:57:12")
# — the actual regulatory disclosure timestamp, used directly as announcement_date
# (the most authentic "record date" available for this data). The fixed-delay
# fallback below is kept only for the rare case a filing's broadcast_Date is
# missing/unparseable, so a write is never silently dropped over one bad field.
_ANNOUNCEMENT_DELAY_DAYS = 45

_FY_QUARTER_MAP = {3: 4, 6: 1, 9: 2, 12: 3}

# All fundamentals columns this pipeline can ever populate (superset —
# individual filings may only fill a subset; the temp-table upsert below
# COALESCEs, so an absent field never blanks an existing value from
# Screener/Trendlyne or a prior NSE XBRL run).
_TARGET_COLUMNS = [
    "goodwill", "inventories", "trade_receivables_current", "trade_payables_current",
    "total_liabilities", "audit_qualified_flag",
    "property_plant_equipment", "intangible_assets", "non_current_investments",
    "non_current_trade_receivables", "deferred_tax_assets", "current_investments",
    "current_tax_assets", "borrowings_current", "borrowings_noncurrent",
    "deferred_tax_liabilities", "provisions_current", "provisions_noncurrent",
    "equity_share_capital", "other_equity", "non_controlling_interest", "non_current_liabilities",
    "current_assets", "current_liabilities", "total_assets", "cwip", "shares_outstanding",
]


def _fiscal_year_quarter(quarter_end: "datetime.date") -> "tuple[int, int]":
    quarter = _FY_QUARTER_MAP[quarter_end.month]
    fiscal_year = quarter_end.year if quarter_end.month == 3 else quarter_end.year + 1
    return fiscal_year, quarter


def _resolve_announcement_date(broadcast_str, quarter_end) -> "datetime.date":
    if broadcast_str:
        try:
            announcement_date = datetime.strptime(broadcast_str, "%d-%b-%Y %H:%M:%S").date()
            if announcement_date > quarter_end:
                return announcement_date
        except ValueError:
            pass
    return quarter_end + timedelta(days=_ANNOUNCEMENT_DELAY_DAYS)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh fundamentals from NSE's real Integrated Filing IndAS data (delta-only, cached)"
    )
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N tickers")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and report, write nothing")
    parser.add_argument("--publish-mode", choices=["direct", "staged"], default="direct",
                        help="'direct' (default): unchanged legacy TEMP-TABLE bulk upsert. "
                             "'staged' (A25): merge the delta against production with the same "
                             "new-wins COALESCE policy (datastore/staging/merge.py) and publish "
                             "atomically via datastore/staging.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        tickers = [r[0] for r in conn.execute("SELECT DISTINCT ticker FROM fundamentals ORDER BY ticker").fetchall()]
    if args.limit:
        tickers = tickers[: args.limit]

    state_conn = sqlite3.connect(PIPELINE_LOG_DB_PATH)
    ensure_ingested_filings_table(state_conn)
    already_ingested = get_ingested_seq_ids(state_conn)
    logger.info(
        f"NSE XBRL fundamentals refresh: {len(tickers)} tickers, "
        f"{len(already_ingested)} filings already ingested (will be skipped)"
    )

    delta_records = []  # each: dict with ticker/fiscal_year/quarter/quarter_end_date/announcement_date/**fields
    newly_ingested = []  # each: dict with seq_id/ticker/fiscal_year/quarter
    tickers_with_new_filings = 0
    total_filings_seen = 0

    for i, ticker in enumerate(tickers):
        if i % 100 == 0:
            logger.info(f"  {i}/{len(tickers)} tickers scanned ({len(delta_records)} new filings staged so far)")
        try:
            filings = list_integrated_filings(ticker)
        except ConnectionError as exc:
            logger.warning(f"nse_xbrl: {ticker} filing list unavailable ({exc}), skipping")
            continue
        total_filings_seen += len(filings)
        new_filings = [f for f in filings if str(f.get("seq_Id")) not in already_ingested]
        if not new_filings:
            continue
        tickers_with_new_filings += 1

        # Prefer consolidated over standalone for the same real quarter (this
        # codebase's established convention — see fundamentals schema comments
        # on total_debt/total_equity) so a duplicate-quarter pair never both
        # land in the delta and race each other in the upsert.
        new_filings.sort(key=lambda f: not f.get("consolidated", False))
        written_quarters: set = set()

        for filing in new_filings:
            seq_id = str(filing.get("seq_Id"))
            # Every seq_id scanned this run gets marked ingested no matter the
            # outcome below (real bug found in verification: a filing whose
            # date this script can't parse would otherwise never be marked,
            # so it'd be re-downloaded and re-parsed on every single future
            # run forever — "we do not expect reported numbers to change"
            # means a genuinely bad/unparseable filing needs a code fix to
            # ever succeed, not endless retries against the same real data).
            newly_ingested.append(
                {"seq_id": seq_id, "ticker": ticker, "fiscal_year": None, "quarter": None}
            )
            try:
                parsed = download_indas_filing(
                    filing["ixbrl"], seq_id=seq_id, cache_dir=NSE_XBRL_RAW_CACHE_DIR
                )
            except ConnectionError as exc:
                logger.warning(f"nse_xbrl: {ticker} filing {seq_id} unavailable ({exc}), skipping")
                continue

            qe_str = parsed.pop("quarter_end_date", None)
            parsed.pop("consolidated", None)
            quarter_end = None
            if qe_str:
                # Real filings use two different date-text formats depending
                # on template/era (live-verified: "31-03-2026" on recent
                # filings, "31-Mar-2025" on some older ones) — try both
                # rather than silently dropping the older-format rows.
                for fmt in ("%d-%m-%Y", "%d-%b-%Y"):
                    try:
                        quarter_end = datetime.strptime(qe_str, fmt).date()
                        break
                    except ValueError:
                        continue
            if quarter_end is None:
                logger.warning(f"nse_xbrl: {ticker} filing {seq_id} has no parseable quarter_end_date ('{qe_str}'), skipping")
                continue
            fiscal_year, quarter = _fiscal_year_quarter(quarter_end)
            dedup_key = (fiscal_year, quarter)
            if dedup_key in written_quarters:
                # Already staged the preferred (consolidated) filing for this
                # quarter this run — already marked ingested at the top of
                # this loop, just not staged for write.
                continue
            written_quarters.add(dedup_key)

            announcement_date = _resolve_announcement_date(filing.get("broadcast_Date"), quarter_end)

            record = {
                "ticker": ticker,
                "fiscal_year": fiscal_year,
                "quarter": quarter,
                "quarter_end_date": quarter_end,
                "announcement_date": announcement_date,
                **{col: parsed.get(col) for col in _TARGET_COLUMNS},
            }
            delta_records.append(record)
            newly_ingested.append(
                {"seq_id": seq_id, "ticker": ticker, "fiscal_year": fiscal_year, "quarter": quarter}
            )

    logger.info(
        f"Scan complete: {total_filings_seen} filings seen across {len(tickers)} tickers, "
        f"{tickers_with_new_filings} tickers had new filings, {len(delta_records)} rows staged for upsert"
    )

    if args.dry_run:
        for r in delta_records[:20]:
            logger.info(f"  [dry-run] {r['ticker']} FY{r['fiscal_year']}Q{r['quarter']}: {r['quarter_end_date']}")
        state_conn.close()
        return

    if not delta_records:
        logger.info("Nothing new to write.")
        state_conn.close()
        return

    # Single bulk transaction: stage the delta in a TEMP TABLE, then upsert
    # into fundamentals in one statement — holds the DuckDB write lock only
    # briefly, instead of the old design's thousands of individual API
    # round-trips spread over hours (a real, observed lock-contention
    # problem against the concurrently-running scheduler).
    all_cols = ["ticker", "fiscal_year", "quarter", "quarter_end_date", "announcement_date"] + _TARGET_COLUMNS
    col_list_sql = ", ".join(all_cols)
    update_cols = [c for c in _TARGET_COLUMNS]
    update_clause = ", ".join(f"{c} = COALESCE(excluded.{c}, fundamentals.{c})" for c in update_cols)

    if args.publish_mode == "staged":
        import pandas as pd

        from datastore.staging.gate import stage_dataframe
        from datastore.staging.merge import coalesce_merge
        from datastore.staging.publish import publish_run_lock, publish_table

        new_df = pd.DataFrame([{c: r.get(c) for c in all_cols} for r in delta_records])
        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            existing_df = conn.execute("SELECT * FROM fundamentals").df()
            merged_df = coalesce_merge(
                existing_df, new_df, key_cols=["ticker", "fiscal_year", "quarter"],
                new_wins=True,  # nse_xbrl: freshly-parsed filing value always overwrites
            )
            with publish_run_lock() as acquired:
                if not acquired:
                    logger.error("Another publish is in progress — staged xbrl backfill NOT published.")
                else:
                    result = stage_dataframe(conn, "fundamentals", merged_df, validators=[])
                    if not result.ok:
                        logger.error("Staging gate rejected the entire batch — nothing published.")
                    else:
                        published_rows = publish_table(conn, "fundamentals")
                        logger.info(
                            "Staged publish: %d delta rows merged, %d now in fundamentals",
                            len(delta_records), published_rows,
                        )
    else:
        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            # "SELECT * FROM fundamentals WHERE FALSE" copies the real table's
            # exact column types (INTEGER/DOUBLE/BOOLEAN/DATE/...) without a
            # separate, drift-prone hardcoded type list — a plain `CREATE TABLE
            # (col ANY, ...)` isn't valid DuckDB DDL (ANY is a function-signature
            # type, not a storable column type — confirmed live).
            conn.execute("CREATE TEMP TABLE nse_xbrl_delta AS SELECT * FROM fundamentals WHERE FALSE")
            rows = [tuple(r[c] for c in all_cols) for r in delta_records]
            placeholders = ", ".join("?" for _ in all_cols)
            conn.executemany(f"INSERT INTO nse_xbrl_delta ({col_list_sql}) VALUES ({placeholders})", rows)
            conn.execute(
                f"""
                INSERT INTO fundamentals ({col_list_sql})
                SELECT {col_list_sql} FROM nse_xbrl_delta
                ON CONFLICT (ticker, fiscal_year, quarter) DO UPDATE SET {update_clause}
                """
            )
            conn.execute("DROP TABLE nse_xbrl_delta")

    mark_filings_ingested(state_conn, newly_ingested)
    state_conn.close()

    logger.info(f"Done: {len(delta_records)} fundamentals rows upserted, {len(newly_ingested)} filings marked ingested")


if __name__ == "__main__":
    main()
