"""
scripts/enrich_missing_company_metadata_trendlyne.py

Phase: Backlog #31 follow-up, Big Investor Activity Phase D session (2026-07-05)
Owner: Platform / QA

Resolves company_name (and, where confidently mappable, sector) for the
691 tickers screener.in's public search couldn't resolve
(config/company_metadata_enrichment_unresolved.csv, produced by
scripts/enrich_missing_company_metadata.py) via Trendlyne's authenticated
autocomplete search API — confirmed live 2026-07-05 against a real
account (TRENDLYNE_USERNAME/PASSWORD).

Real API found live: GET https://trendlyne.com/member/api/ac_snames/stock/
?term={ticker}, headers {"X-Requested-With": "XMLHttpRequest",
"Referer": "https://trendlyne.com/"} (plain GET with no such header
returns the literal string "fail", not JSON or an error status — found by
trial). Returns a ranked list of candidate stocks; each has NSEcode/id/
value fields — only an EXACT (case-insensitive) match against the
requested ticker is accepted, same "don't guess, don't fabricate"
discipline as enrich_missing_company_metadata.py's _search_screener() —
confirmed necessary live: searching "GUJGASLTD" returns "GUJENERGY" as its
top (fuzzy, not exact) result, a different company.

SECTOR TAXONOMY MISMATCH (real finding, not assumed): Trendlyne's
sectorName field uses ITS OWN taxonomy, confirmed different from this
project's existing ~17-value sector column (populated by screener.in's
"Peers" breadcrumb, matching the NSE-sector-index convention other code
depends on for sector-relative scoring) — e.g. Trendlyne's "Oil & Gas" vs
this project's "Oil Gas & Consumable Fuels", "Banking and Finance" vs
"Financial Services". Writing Trendlyne's raw sectorName into the
existing `sector` column would silently introduce a second, incompatible
taxonomy. _SECTOR_MAP below is a best-effort, ONLY-WHERE-CONFIDENT mapping
built from a handful of live-verified samples (TCS, HDFCBANK, SUNPHARMA,
ITC, TATASTEEL, ULTRACEMCO, BHARTIARTL, MARUTI, RELIANCE) plus reasonable
direct-name inference for this project's remaining sector buckets — NOT
exhaustively verified against every Trendlyne sector label that exists.
Any raw sectorName not in _SECTOR_MAP is recorded as-is in the progress
CSV's trendlyne_sector_raw column but left OUT of the `sector` field
merged into the universe, rather than guessed.

Resumable/checkpointed, same convention as enrich_missing_company_metadata.py.
"""

import csv
import re
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
UNRESOLVED_CSV = PROJECT_ROOT / "config" / "company_metadata_enrichment_unresolved.csv"
PROGRESS_CSV = PROJECT_ROOT / "config" / "company_metadata_enrichment_trendlyne_progress.csv"
STILL_UNRESOLVED_CSV = PROJECT_ROOT / "config" / "company_metadata_enrichment_trendlyne_unresolved.csv"

SEARCH_URL = "https://trendlyne.com/member/api/ac_snames/stock/"
SEARCH_HEADERS = {"X-Requested-With": "XMLHttpRequest", "Referer": "https://trendlyne.com/"}
REQUEST_DELAY_SECONDS = 1.0
PROGRESS_FIELDS = ["ticker", "company_name", "sector", "trendlyne_sector_raw", "trendlyne_url"]

_TICKER_SUFFIX_PATTERN = re.compile(r"\s*\([^)]*\)\s*$")

# See module docstring's "SECTOR TAXONOMY MISMATCH" note — only confident
# mappings, everything else stays unmapped (blank sector, raw value kept
# in the progress CSV for a future manual pass).
_SECTOR_MAP = {
    "software & services": "Information Technology",
    "information technology": "Information Technology",
    "banking and finance": "Financial Services",
    "pharmaceuticals & biotechnology": "Healthcare",
    "food, beverages & tobacco": "Fast Moving Consumer Goods",
    "fmcg": "Fast Moving Consumer Goods",
    "metals & mining": "Metals & Mining",
    "cement and construction": "Construction Materials",
    "telecom services": "Telecommunication",
    "automobiles & auto components": "Automobile and Auto Components",
    "oil & gas": "Oil Gas & Consumable Fuels",
    "utilities": "Utilities",
    "power": "Power",
    "realty": "Realty",
    "textiles": "Textiles",
    "chemicals": "Chemicals",
    "consumer durables": "Consumer Durables",
    "diversified": "Diversified",
    "capital goods": "Capital Goods",
    "services": "Services",
    "construction": "Construction",
    "forest materials": "Forest Materials",
    "media entertainment & publication": "Media Entertainment & Publication",
    # Added after observing these raw values in a real run, 2026-07-05:
    "healthcare": "Healthcare",
    "chemicals & petrochemicals": "Chemicals",
    "general industrials": "Capital Goods",
    "commercial services & supplies": "Services",
    "retailing": "Consumer Services",
    "diversified consumer services": "Consumer Services",
    "hotels restaurants & tourism": "Consumer Services",
    "media": "Media Entertainment & Publication",
    "textiles apparels & accessories": "Textiles",
    # Deliberately left unmapped (not confident): "transportation" — could
    # plausibly be Services or its own bucket; no equivalent seen yet in
    # this project's existing sector taxonomy to map to with confidence.
    # "telecommunications equipment" — could be Telecommunication (operator
    # bucket) or Capital Goods (hardware manufacturing); genuinely
    # ambiguous without inspecting the specific company.
}


def _load_done_tickers() -> set:
    done = set()
    for path in (PROGRESS_CSV, STILL_UNRESOLVED_CSV):
        if path.exists():
            with open(path, newline="") as f:
                done.update(row["ticker"] for row in csv.DictReader(f) if row.get("ticker"))
    return done


def _search_trendlyne(session, ticker: str) -> dict | None:
    resp = session.get(SEARCH_URL, params={"term": ticker}, headers=SEARCH_HEADERS, timeout=20)
    resp.raise_for_status()
    if resp.text.strip() == "fail":
        return None
    results = resp.json()
    for r in results:
        candidate = (r.get("NSEcode") or r.get("id") or r.get("value") or "").upper()
        if candidate == ticker.upper():
            return r
    # No exact NSEcode/id/value match — Trendlyne's search ranks fuzzy
    # matches first when there's no real hit (confirmed live:
    # "GUJGASLTD" -> top result "GUJENERGY", a different company). Treat
    # as unresolved rather than fabricate.
    return None


def _company_name_from_label(label: str) -> str:
    """'Reliance Industries Ltd.(RELIANCE)' -> 'Reliance Industries Ltd.'"""
    return _TICKER_SUFFIX_PATTERN.sub("", label).strip()


def main(limit: int | None = None) -> None:
    from ingestion.scrapers.trendlyne import TrendlyneScraper

    with open(UNRESOLVED_CSV, newline="") as f:
        tickers = [row["ticker"] for row in csv.DictReader(f)]

    done = _load_done_tickers()
    todo = [t for t in tickers if t not in done]
    if limit:
        todo = todo[:limit]

    print(f"{len(tickers)} total screener-unresolved tickers, {len(done)} already processed, {len(todo)} to process this run")

    scraper = TrendlyneScraper()
    scraper.login()
    session = scraper._session

    progress_is_new = not PROGRESS_CSV.exists()
    unresolved_is_new = not STILL_UNRESOLVED_CSV.exists()
    resolved_count = 0
    unresolved_count = 0
    unmapped_sector_count = 0
    consecutive_errors = 0

    with open(PROGRESS_CSV, "a", newline="") as pf, open(STILL_UNRESOLVED_CSV, "a", newline="") as uf:
        pwriter = csv.DictWriter(pf, fieldnames=PROGRESS_FIELDS)
        uwriter = csv.DictWriter(uf, fieldnames=["ticker"])
        if progress_is_new:
            pwriter.writeheader()
        if unresolved_is_new:
            uwriter.writeheader()

        for i, ticker in enumerate(todo):
            try:
                match = _search_trendlyne(session, ticker)
                consecutive_errors = 0
                if match is None:
                    # A genuine "no exact match" result — this ticker really
                    # was searched and really came back empty/fuzzy-only.
                    uwriter.writerow({"ticker": ticker})
                    uf.flush()
                    unresolved_count += 1
                else:
                    raw_sector = match.get("sectorName", "")
                    mapped_sector = _SECTOR_MAP.get(raw_sector.strip().lower(), "")
                    if raw_sector and not mapped_sector:
                        unmapped_sector_count += 1
                    company_name = _company_name_from_label(match.get("label", ""))
                    trendlyne_url = match["urls"][0][1] if match.get("urls") else ""
                    pwriter.writerow({
                        "ticker": ticker,
                        "company_name": company_name,
                        "sector": mapped_sector,
                        "trendlyne_sector_raw": raw_sector,
                        "trendlyne_url": trendlyne_url,
                    })
                    pf.flush()
                    resolved_count += 1
            except Exception as exc:  # noqa: BLE001
                # [AS BUILT, 2026-07-05] REAL INCIDENT: an earlier version of
                # this except-block wrote every request-level failure
                # (broken session, rate-limit block, transient network
                # error) straight into STILL_UNRESOLVED_CSV alongside
                # genuine "no match" results — indistinguishable from a
                # real miss. A live run's session broke partway through
                # (a concurrent login attempt got HTTP 405, strongly
                # suggesting Trendlyne throttling/blocking after this
                # session's cumulative request volume) and the run raced
                # through the remaining ~400 tickers in seconds, writing
                # every one of them as "unresolved" — corrupting that file
                # with false negatives that would never have been retried.
                # Recovered by hand (cross-referencing against
                # progress.csv's last real success in original ticker
                # order) — see BuildLog/session notes, 2026-07-05.
                #
                # Fix: NEVER write an exception-caused failure to
                # STILL_UNRESOLVED_CSV. After 3 consecutive request-level
                # exceptions, stop the run entirely (something is actually
                # broken — auth, network, or a block) rather than
                # continuing to iterate a dead session.
                consecutive_errors += 1
                print(f"  [{ticker}] request error ({consecutive_errors} in a row): {exc}", file=sys.stderr)
                if consecutive_errors >= 3:
                    print(
                        f"  Aborting: {consecutive_errors} consecutive request failures in a row — likely a "
                        "broken session, network outage, or Trendlyne throttling, not genuine misses. "
                        "Re-run once the underlying issue (check login(), rate limiting) is resolved; "
                        "already-processed tickers are skipped automatically on retry.",
                        file=sys.stderr,
                    )
                    break

            time.sleep(REQUEST_DELAY_SECONDS)
            if (i + 1) % 50 == 0:
                print(f"  ...{i + 1}/{len(todo)} processed ({resolved_count} resolved, {unresolved_count} unresolved so far)")

    print(f"Done this run: {resolved_count} resolved ({unmapped_sector_count} with an unmapped/blank sector), {unresolved_count} confirmed still unresolved.")
    print(f"Progress file: {PROGRESS_CSV} ({sum(1 for _ in open(PROGRESS_CSV)) - 1} total resolved rows)")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Process at most N tickers this run (for testing)")
    args = parser.parse_args()
    main(limit=args.limit)
