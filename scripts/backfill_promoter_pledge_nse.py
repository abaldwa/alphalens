"""
scripts/backfill_promoter_pledge_nse.py

One-off/rerunnable backfill of shareholding.promoter_pledge from NSE's
real SAST Regulation 31(4) pledge/encumbrance disclosure feed — see
ingestion/scrapers/nse_pledge.py's module docstring for the endpoint and
its 2026-07-07 discovery (found by grepping NSE's own loaded JS bundle,
not guessed).

For each ticker with existing shareholding rows, fetches real pledge
events and, for each shareholding quarter, sets promoter_pledge to the
most recent disclosed post_event_holding_pct as of that quarter's
quarter_end_date (the real encumbrance level known at that point in
time — PIT-consistent with the rest of this table, which keys off
filing_date/quarter_end_date the same way). Tickers with zero disclosed
pledge events get promoter_pledge left as NULL still means "unknown",
not "confirmed zero" — see ingestion/scrapers/screener.py's existing
comment on this exact same distinction (a company with genuinely zero
pledge should show a real 0.0, but this script cannot distinguish
"never disclosed because zero" from "never disclosed because NSE has no
record for another reason" without a positive confirmation signal NSE
doesn't provide) — left as a known limitation, not fabricated as 0.

Usage:
    .venv/bin/python3 scripts/backfill_promoter_pledge_nse.py [--limit N] [--dry-run]
"""

import argparse
import logging

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from ingestion.scrapers.nse_pledge import download_pledge_data

logger = logging.getLogger(__name__)

_FROM_DATE = "2015-01-01"  # SAST Reg 31(4) continual disclosure regime start


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill shareholding.promoter_pledge from NSE's real pledge feed")
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N tickers (for a quick run)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and report, write nothing")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    to_date = "2026-07-07"
    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        tickers = [
            r[0] for r in conn.execute("SELECT DISTINCT ticker FROM shareholding ORDER BY ticker").fetchall()
        ]
    if args.limit:
        tickers = tickers[: args.limit]
    logger.info(f"Backfilling promoter_pledge for {len(tickers)} tickers")

    total_events = 0
    total_updated_quarters = 0
    tickers_with_events = 0
    for i, ticker in enumerate(tickers):
        if i % 50 == 0:
            logger.info(f"  {i}/{len(tickers)} ({total_events} events found so far)")
        try:
            events = download_pledge_data(ticker, _FROM_DATE, to_date)
        except ConnectionError as exc:
            logger.warning(f"promoter_pledge backfill: {ticker} unavailable ({exc}), skipping")
            continue
        if events.empty:
            continue
        tickers_with_events += 1
        total_events += len(events)
        events = events.sort_values("broadcast_date")

        if args.dry_run:
            continue

        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            quarters = conn.execute(
                "SELECT quarter_end_date FROM shareholding WHERE ticker = ? ORDER BY quarter_end_date", [ticker]
            ).fetchall()
            for (q_end,) in quarters:
                eligible = events[events["broadcast_date"].dt.date <= q_end]
                if eligible.empty:
                    continue
                latest_pct = eligible.iloc[-1]["post_event_holding_pct"]
                if latest_pct is None:
                    continue
                conn.execute(
                    "UPDATE shareholding SET promoter_pledge = ? WHERE ticker = ? AND quarter_end_date = ?",
                    [latest_pct, ticker, q_end],
                )
                total_updated_quarters += 1

    logger.info(
        f"Done: {tickers_with_events}/{len(tickers)} tickers had real disclosed pledge events "
        f"({total_events} events total), {total_updated_quarters} shareholding quarter-rows updated"
    )


if __name__ == "__main__":
    main()
