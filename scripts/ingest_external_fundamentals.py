#!/usr/bin/env python3
"""
scripts/ingest_external_fundamentals.py

Ingestion script for a generic external CSV fundamentals source
(ingestion/fundamentals/sources.py::CsvFundamentalSourceAdapter) — a
long/EAV-shaped file (ticker,metric,as_of_date,value,source,confidence),
one row per metric per date, NOT the wide per-quarter shape
`datastore.api.schemas.FundamentalsWrite` expects.

F5 (2026-07-10): this script previously only logged what it "would write"
(scripts/ingest_external_fundamentals.py's own comment admitted
"DataStoreClient.write_fundamentals would need to be implemented") —
nothing was ever actually persisted, regardless of --dry-run.
DataStoreClient.write_fundamentals/write_fundamentals_batch ARE real and
working (datastore/client.py), but that API endpoint hardcodes
fundamentals_source="screener" server-side (it was built exclusively for
screener.py's batch_export) — using it here would mislabel every row from
this genuinely different source. This script instead writes directly to
DuckDB, same SPEC-DS-002 exception already documented and used by
scripts/backfill_fundamentals_nse_xbrl.py / backfill_fundamentals_trendlyne.py
for bulk backfill scripts.

Pivot logic: groups the CSV's long rows by (ticker, inferred quarter_end_date)
into FundamentalsWrite's wide shape. The CSV has no fiscal_year/quarter/
quarter_end_date of its own — only a per-metric as_of_date (when that value
became known) — so quarter_end_date is inferred as the most recent standard
fiscal quarter-end (Mar 31/Jun 30/Sep 30/Dec 31) strictly before as_of_date,
and as_of_date itself is used as announcement_date (SPEC-PIPE-003 requires
announcement_date > quarter_end_date, which this construction guarantees).
Only metric names matching a real FundamentalsWrite field are written — an
unrecognized metric name is logged and skipped, never silently coerced into
some other column.

Usage:
    .venv/bin/python3 scripts/ingest_external_fundamentals.py --csv path/to/file.csv [--tickers T1 T2] [--as-of YYYY-MM-DD] [--dry-run]
"""

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from features.fundamental_quality_gate import validate_and_annotate
from features.fundamental_source_priority import SOURCE_PRIORITY, build_priority_update_clause
from ingestion.fundamentals.sources import CsvFundamentalSourceAdapter, merge_fundamental_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

_QUARTER_ENDS = [(3, 31), (6, 30), (9, 30), (12, 31)]

# Only these may be written — a CSV `metric` value outside this set is
# logged and dropped, never silently mapped onto some other column.
_VALID_METRICS = {
    "revenue", "ebitda", "pat", "eps", "operating_margin", "ebitda_margin", "net_margin",
    "roe", "roce", "debt_to_equity", "interest_coverage", "fcf", "asset_turnover",
    "inventory_days", "receivable_days", "payable_days", "book_value_per_share",
    "shares_outstanding", "gross_profit", "capex", "current_assets", "current_liabilities",
    "total_debt", "cash_and_equivalents", "depreciation",
    "total_equity", "retained_earnings", "total_assets", "cwip",
}


def _infer_quarter_end(as_of: date) -> date:
    """Most recent standard fiscal quarter-end strictly before `as_of`."""
    candidates = []
    for year in (as_of.year, as_of.year - 1):
        for month, day in _QUARTER_ENDS:
            candidates.append(date(year, month, day))
    earlier = [d for d in candidates if d < as_of]
    return max(earlier)


def _fiscal_year_quarter(quarter_end: date) -> "tuple[int, int]":
    quarter_map = {3: 4, 6: 1, 9: 2, 12: 3}
    quarter = quarter_map[quarter_end.month]
    fiscal_year = quarter_end.year if quarter_end.month == 3 else quarter_end.year + 1
    return fiscal_year, quarter


def _pivot_to_fundamentals_rows(merged_rows: "list[dict]") -> "list[dict]":
    """
    Group long/EAV rows (ticker, metric, as_of_date, value, ...) into
    FundamentalsWrite-shaped wide rows keyed by (ticker, quarter_end_date).
    """
    grouped: "dict[tuple, dict]" = {}
    skipped_metrics: set = set()

    for row in merged_rows:
        metric = row.get("metric")
        if metric not in _VALID_METRICS:
            skipped_metrics.add(metric)
            continue
        try:
            as_of = datetime.strptime(row["as_of_date"], "%Y-%m-%d").date()
        except (ValueError, KeyError, TypeError):
            logger.warning(f"Unparseable as_of_date on row {row!r}, skipping")
            continue

        quarter_end = _infer_quarter_end(as_of)
        fiscal_year, quarter = _fiscal_year_quarter(quarter_end)
        key = (row["ticker"], fiscal_year, quarter)

        if key not in grouped:
            grouped[key] = {
                "ticker": row["ticker"],
                "fiscal_year": fiscal_year,
                "quarter": quarter,
                "quarter_end_date": quarter_end,
                "announcement_date": as_of,
            }
        else:
            # A later as_of_date for the same inferred quarter is a more
            # recent restatement/disclosure — keep the latest announcement_date.
            if as_of > grouped[key]["announcement_date"]:
                grouped[key]["announcement_date"] = as_of

        grouped[key][metric] = row["value"]

    if skipped_metrics:
        logger.warning(f"Skipped {len(skipped_metrics)} unrecognized metric name(s): {sorted(skipped_metrics)}")

    return list(grouped.values())


def main() -> None:
    parser = argparse.ArgumentParser(description="Load fundamentals from an external CSV into the datastore")
    parser.add_argument("--csv", type=Path, required=True,
                        help="Path to CSV file with columns: ticker,metric,as_of_date,value,source,confidence")
    parser.add_argument("--tickers", nargs="+", help="Specific tickers to process (default: all in CSV)")
    parser.add_argument("--as-of", type=str, default=datetime.now().date().isoformat(),
                        help="As-of date for PIT filtering (YYYY-MM-DD)")
    parser.add_argument("--lookback-years", type=int, default=4, help="How many years of history to consider")
    parser.add_argument("--dry-run", action="store_true", help="Parse and pivot, write nothing")
    args = parser.parse_args()

    if not args.csv.exists():
        logger.error("CSV file not found: %s", args.csv)
        sys.exit(1)

    logger.info("Loading fundamentals from %s", args.csv)
    adapter = CsvFundamentalSourceAdapter(args.csv)

    if args.tickers:
        tickers = args.tickers
    else:
        tickers_set = set()
        with args.csv.open("r", encoding="utf-8") as handle:
            next(handle)
            for line in handle:
                parts = line.strip().split(",")
                if len(parts) >= 1 and parts[0]:
                    tickers_set.add(parts[0])
        tickers = sorted(tickers_set)

    logger.info("Processing %d tickers: %s", len(tickers), ", ".join(tickers[:10]) + ("..." if len(tickers) > 10 else ""))

    all_merged_rows = []
    for ticker in tickers:
        raw_rows = adapter.fetch_ticker_history(ticker=ticker, as_of=args.as_of, lookback_years=args.lookback_years)
        if not raw_rows:
            logger.debug("No rows found for %s", ticker)
            continue
        merged = merge_fundamental_rows(raw_rows)
        logger.debug("Merged %d raw rows to %d final rows for %s", len(raw_rows), len(merged), ticker)
        all_merged_rows.extend(merged)

    fundamentals_rows = _pivot_to_fundamentals_rows(all_merged_rows)
    logger.info("Pivoted %d long rows into %d wide (ticker, fiscal_year, quarter) rows", len(all_merged_rows), len(fundamentals_rows))

    if args.dry_run:
        for row in fundamentals_rows[:10]:
            logger.info("[dry-run] would write: %s", {k: v for k, v in row.items() if v is not None})
        if len(fundamentals_rows) > 10:
            logger.info("[dry-run] ... and %d more rows", len(fundamentals_rows) - 10)
        logger.info("DRY RUN complete. No data was written.")
        return

    if not fundamentals_rows:
        logger.info("Nothing to write.")
        return

    for row in fundamentals_rows:
        validate_and_annotate(row)
        row["fundamentals_source"] = "external_csv"
        row["fundamentals_source_priority"] = SOURCE_PRIORITY["external_csv"]

    data_cols = sorted(_VALID_METRICS)
    all_cols = (
        ["ticker", "fiscal_year", "quarter", "quarter_end_date", "announcement_date"]
        + data_cols
        + ["quality_flag", "quality_flag_reason", "fundamentals_source", "fundamentals_source_priority"]
    )
    col_list_sql = ", ".join(all_cols)
    update_cols = data_cols + ["quality_flag", "quality_flag_reason"]
    update_clause = build_priority_update_clause(update_cols)

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        conn.execute("CREATE TEMP TABLE external_csv_delta AS SELECT * FROM fundamentals WHERE FALSE")
        rows = [tuple(r.get(c) for c in all_cols) for r in fundamentals_rows]
        placeholders = ", ".join("?" for _ in all_cols)
        conn.executemany(f"INSERT INTO external_csv_delta ({col_list_sql}) VALUES ({placeholders})", rows)
        conn.execute(
            f"""
            INSERT INTO fundamentals ({col_list_sql})
            SELECT {col_list_sql} FROM external_csv_delta
            ON CONFLICT (ticker, fiscal_year, quarter) DO UPDATE SET {update_clause}
            """
        )
        conn.execute("DROP TABLE external_csv_delta")

    logger.info("Finished. Wrote %d fundamentals rows to datastore.", len(fundamentals_rows))


if __name__ == "__main__":
    main()
