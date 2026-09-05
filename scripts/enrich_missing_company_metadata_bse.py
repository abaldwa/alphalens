"""
scripts/enrich_missing_company_metadata_bse.py

[2026-09-05] BSE fallback pass for tickers scripts/enrich_missing_company_
metadata.py's screener.in-only pass couldn't resolve (config/
tickers_missing_company_name.csv, ~691 remaining as of this writing).
Uses ingestion.scrapers.screener_sector_lookup.resolve_company_metadata_bse
(exact-symbol BSE search + ComHeadernew) — verified live 2026-09-05 to
match screener.in's own taxonomy/values (e.g. ALKYLAMINE -> "Chemicals"
via BSE, identical to the existing manually-researched value already in
the universe CSV for that ticker).

Resumable/checkpointed the same way as the screener-only script: writes to
config/company_metadata_enrichment_bse_progress.csv incrementally, and
still-unresolved tickers to config/company_metadata_enrichment_bse_unresolved.csv.
Does NOT modify config/nifty500_universe.csv directly — run
scripts/apply_company_metadata_enrichment.py afterward (it already merges
from company_metadata_enrichment_progress.csv; point it at this file, or
merge both progress files, before applying).
"""

import csv
import sys
import time
from pathlib import Path
from typing import Optional

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ingestion.scrapers.screener_sector_lookup import resolve_company_metadata_bse  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MISSING_CSV = PROJECT_ROOT / "config" / "tickers_missing_company_name.csv"
PROGRESS_CSV = PROJECT_ROOT / "config" / "company_metadata_enrichment_bse_progress.csv"
UNRESOLVED_CSV = PROJECT_ROOT / "config" / "company_metadata_enrichment_bse_unresolved.csv"

REQUEST_DELAY_SECONDS = 0.6
PROGRESS_FIELDS = ["ticker", "company_name", "sector", "isin"]


def _load_done_tickers() -> set[str]:
    done: set[str] = set()
    for path in (PROGRESS_CSV, UNRESOLVED_CSV):
        if path.exists():
            with open(path, newline="") as f:
                done.update(row["ticker"] for row in csv.DictReader(f) if row.get("ticker"))
    return done


def main(limit: Optional[int] = None) -> None:
    with open(MISSING_CSV, newline="") as f:
        tickers = [row["ticker"] for row in csv.DictReader(f)]

    done = _load_done_tickers()
    todo = [t for t in tickers if t not in done]
    if limit:
        todo = todo[:limit]

    print(f"{len(tickers)} total blank tickers, {len(done)} already processed, {len(todo)} to process this run")

    progress_is_new = not PROGRESS_CSV.exists()
    unresolved_is_new = not UNRESOLVED_CSV.exists()
    resolved_count = 0
    unresolved_count = 0

    with open(PROGRESS_CSV, "a", newline="") as pf, open(UNRESOLVED_CSV, "a", newline="") as uf:
        pwriter = csv.DictWriter(pf, fieldnames=PROGRESS_FIELDS)
        uwriter = csv.DictWriter(uf, fieldnames=["ticker"])
        if progress_is_new:
            pwriter.writeheader()
        if unresolved_is_new:
            uwriter.writeheader()

        for i, ticker in enumerate(todo):
            try:
                metadata = resolve_company_metadata_bse(ticker)
                if metadata is None:
                    uwriter.writerow({"ticker": ticker})
                    uf.flush()
                    unresolved_count += 1
                else:
                    pwriter.writerow({
                        "ticker": ticker,
                        "company_name": metadata["company_name"],
                        "sector": metadata["sector"],
                        "isin": metadata["isin"],
                    })
                    pf.flush()
                    resolved_count += 1
            except requests.RequestException as exc:
                print(f"  [{ticker}] network error, marking unresolved this run: {exc}", file=sys.stderr)
                uwriter.writerow({"ticker": ticker})
                uf.flush()
                unresolved_count += 1

            time.sleep(REQUEST_DELAY_SECONDS)
            if (i + 1) % 50 == 0:
                print(f"  ...{i + 1}/{len(todo)} processed ({resolved_count} resolved, {unresolved_count} unresolved so far)")

    print(f"Done this run: {resolved_count} resolved, {unresolved_count} unresolved.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Process at most N tickers this run (for testing)")
    args = parser.parse_args()
    main(limit=args.limit)
