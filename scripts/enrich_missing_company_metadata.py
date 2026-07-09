"""
scripts/enrich_missing_company_metadata.py

Phase: Backlog #31 follow-up (2026-07-04)
Owner: Platform / QA

Resolves company_name/sector for the 1,817 blank-name tickers in
config/nifty500_universe.csv via screener.in's public company-search API
(no login required) — confirmed live against RELIANCE/TCS/HDFCBANK that
screener's "Peer comparison" breadcrumb's 2nd-level link matches this
project's existing `sector` taxonomy convention exactly (e.g.
"Oil Gas & Consumable Fuels", "Information Technology", "Financial Services").

Resumable/checkpointed: writes resolved rows to
config/company_metadata_enrichment_progress.csv incrementally (one line per
resolved ticker, flushed immediately) so an interrupted run loses no
progress — re-running only processes tickers not already in that file.

Does NOT modify config/nifty500_universe.csv directly; run
scripts/apply_company_metadata_enrichment.py afterward to merge resolved
rows in (kept separate so a bad run can be inspected/discarded before it
touches the real universe file).

Tickers screener.in has no match for are logged to
config/company_metadata_enrichment_unresolved.csv for a follow-up pass
against Tijori/Trendlyne (both require login; not attempted here since
screener alone resolves the large majority — see this script's summary
output for the actual unresolved count).
"""

import csv
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MISSING_CSV = PROJECT_ROOT / "config" / "tickers_missing_company_name.csv"
PROGRESS_CSV = PROJECT_ROOT / "config" / "company_metadata_enrichment_progress.csv"
UNRESOLVED_CSV = PROJECT_ROOT / "config" / "company_metadata_enrichment_unresolved.csv"

SEARCH_URL = "https://www.screener.in/api/company/search/"
HEADERS = {"User-Agent": "Mozilla/5.0 (AlphaLens research; contact via repo owner)"}
REQUEST_DELAY_SECONDS = 0.6
PROGRESS_FIELDS = ["ticker", "company_name", "sector", "screener_url"]


def _load_done_tickers() -> set:
    done = set()
    for path in (PROGRESS_CSV, UNRESOLVED_CSV):
        if path.exists():
            with open(path, newline="") as f:
                done.update(row["ticker"] for row in csv.DictReader(f) if row.get("ticker"))
    return done


def _search_screener(ticker: str) -> dict | None:
    resp = requests.get(SEARCH_URL, params={"q": ticker}, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    results = resp.json()
    for r in results:
        url = r.get("url", "")
        slug = url.strip("/").split("/")[-2] if url.count("/") >= 2 else ""
        if slug.upper() == ticker.upper():
            return r
    # No exact slug match — screener's search can still be a same-company
    # rename/merge; don't guess, treat as unresolved rather than fabricate.
    return None


def _fetch_sector(company_url_path: str) -> str | None:
    resp = requests.get(f"https://www.screener.in{company_url_path}", headers=HEADERS, timeout=20)
    if resp.status_code != 200:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    peers = soup.find(id="peers")
    if not peers:
        return None
    heading = peers.find(["h2", "h3"])
    if not heading:
        return None
    breadcrumb = heading.find_next_sibling("p", class_="sub")
    if not breadcrumb:
        return None
    links = breadcrumb.find_all("a")
    if len(links) < 2:
        return None
    return links[1].get_text(strip=True)


def main(limit: int | None = None) -> None:
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
                match = _search_screener(ticker)
                if match is None:
                    uwriter.writerow({"ticker": ticker})
                    uf.flush()
                    unresolved_count += 1
                else:
                    sector = _fetch_sector(match["url"])
                    time.sleep(REQUEST_DELAY_SECONDS)
                    pwriter.writerow({
                        "ticker": ticker,
                        "company_name": match["name"],
                        "sector": sector or "",
                        "screener_url": match["url"],
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
    print(f"Progress file: {PROGRESS_CSV} ({sum(1 for _ in open(PROGRESS_CSV)) - 1} total resolved rows)")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Process at most N tickers this run (for testing)")
    args = parser.parse_args()
    main(limit=args.limit)
