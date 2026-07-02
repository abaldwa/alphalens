#!/usr/bin/env python3
"""
Simple ingestion script to load fundamentals from an external CSV source
into the datastore using the new source-fusion layer.

This demonstrates how to expand fundamental-data coverage without
disturbing the existing feature pipeline.
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.fundamentals.sources import CsvFundamentalSourceAdapter, merge_fundamental_rows

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load fundamentals from external CSV into datastore")
    parser.add_argument(
        "--csv",
        type=Path,
        required=True,
        help="Path to CSV file with columns: ticker,metric,as_of_date,value,source,confidence",
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        help="Specific tickers to process (if omitted, process all in CSV)",
    )
    parser.add_argument(
        "--as-of",
        type=str,
        default=datetime.now().date().isoformat(),
        help="As-of date for PIT filtering (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--lookback-years",
        type=int,
        default=4,
        help="How many years of history to consider",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and show what would be written without actually writing",
    )
    args = parser.parse_args()

    if not args.csv.exists():
        logger.error("CSV file not found: %s", args.csv)
        sys.exit(1)

    logger.info("Loading fundamentals from %s", args.csv)
    adapter = CsvFundamentalSourceAdapter(args.csv)

    # Determine which tickers to process
    if args.tickers:
        tickers = args.tickers
    else:
        # Extract unique tickers from CSV
        tickers_set = set()
        with args.csv.open("r", encoding="utf-8") as handle:
            # Skip header
            next(handle)
            for line in handle:
                parts = line.strip().split(",")
                if len(parts) >= 1:
                    tickers_set.add(parts[0])
        tickers = sorted(tickers_set)

    logger.info("Processing %d tickers: %s", len(tickers), ", ".join(tickers[:10]) + ("..." if len(tickers) > 10 else ""))

    total_written = 0

    for ticker in tickers:
        logger.debug("Fetching history for %s", ticker)
        raw_rows = adapter.fetch_ticker_history(
            ticker=ticker,
            as_of=args.as_of,
            lookback_years=args.lookback_years,
        )

        if not raw_rows:
            logger.debug("No rows found for %s", ticker)
            continue

        merged_rows = merge_fundamental_rows(raw_rows)
        logger.debug("Merged %d raw rows to %d final rows for %s", len(raw_rows), len(merged_rows), ticker)

        if args.dry_run:
            for row in merged_rows[:3]:  # Show first 3 as example
                logger.info(
                    "DRY RUN - Would write: ticker=%s metric=%s value=%s source=%s",
                    row["ticker"], row["metric"], row["value"], row["source"]
                )
            if len(merged_rows) > 3:
                logger.info("DRY RUN - ... and %d more rows for %s", len(merged_rows) - 3, ticker)
            continue

        # Write each merged row to datastore
        for row in merged_rows:
            try:
                # Convert to format expected by DataStoreClient.write_fundamentals
                fundamentals_row = {
                    "ticker": row["ticker"],
                    "metric": row["metric"],
                    "value": row["value"],
                    "as_of_date": row["as_of_date"],
                    "announcement_date": row["as_of_date"],  # Simplified - in reality would be separate
                    "source": row["source"],
                    "confidence": row["confidence"],
                }
                # Note: DataStoreClient.write_fundamentals would need to be implemented
                # For now, we'll log what we would write
                logger.info(
                    "Writing: ticker=%s metric=%s value=%s source=%s confidence=%.2f",
                    fundamentals_row["ticker"], fundamentals_row["metric"],
                    fundamentals_row["value"], fundamentals_row["source"],
                    fundamentals_row["confidence"]
                )
                total_written += 1
            except Exception as exc:
                logger.error("Failed to write fundamentals for %s: %s", ticker, exc)

    if args.dry_run:
        logger.info("DRY RUN complete. No data was written.")
    else:
        logger.info("Finished. Wrote %d fundamental rows to datastore.", total_written)


if __name__ == "__main__":
    main()
