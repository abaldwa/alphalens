"""
ingestion/scrapers/screener_sector_lookup.py

[2026-09-05] Extracted from scripts/enrich_missing_company_metadata.py
(2026-07-04) so the same proven sector-resolution logic can be reused by
config/build_universe.py to fill `sector` for any NEWLY added ticker (e.g.
a fresh IPO landing in ohlcv_adjusted), not just the one-time historical
backlog that script was written for.

Resolves a ticker's sector via screener.in's public company-search API (no
login required). Confirmed live against RELIANCE/TCS/HDFCBANK (2026-07-04)
and re-verified 2026-09-05: screener's "Peer comparison" breadcrumb's 2nd
link matches this project's existing `sector` taxonomy convention exactly
(e.g. "Oil, Gas & Consumable Fuels", "Information Technology", "Financial
Services" — see config/sector_index_map.py for the known punctuation
variants this taxonomy already tolerates).

Never fabricates: returns None (not a guess) whenever screener has no
exact-slug match for the ticker, or the expected page structure isn't
found — callers must treat None as "still needs manual resolution",
same convention as config/company_metadata_enrichment_unresolved.csv.
"""

import csv
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

SEARCH_URL = "https://www.screener.in/api/company/search/"
HEADERS = {"User-Agent": "Mozilla/5.0 (AlphaLens research; contact via repo owner)"}
_TIMEOUT_S = 20


def search_screener(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Look up `ticker` on screener.in and return its search result dict
    (has 'name', 'url') if the result's URL slug matches the ticker
    exactly. Returns None (not a best-guess) on no exact match — screener's
    fuzzy search can surface a same-company rename/merge that isn't
    actually this ticker.
    """
    resp = requests.get(SEARCH_URL, params={"q": ticker}, headers=HEADERS, timeout=_TIMEOUT_S)
    resp.raise_for_status()
    results = resp.json()
    for r in results:
        url = r.get("url", "")
        slug = url.strip("/").split("/")[-2] if url.count("/") >= 2 else ""
        if slug.upper() == ticker.upper():
            return dict(r)
    return None


def fetch_sector(company_url_path: str) -> Optional[str]:
    """
    Fetch a screener.in company page and extract its Sector (the 2nd link
    in the "Peer comparison" section's breadcrumb — Broad Sector / Sector /
    Broad Industry / Industry). Returns None if the page, the peers
    section, or the breadcrumb isn't found in the expected shape (screener
    changing its page layout, or a company with no sector classification)
    rather than guessing.
    """
    from bs4 import BeautifulSoup

    resp = requests.get(f"https://www.screener.in{company_url_path}", headers=HEADERS, timeout=_TIMEOUT_S)
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
    return str(links[1].get_text(strip=True))


def resolve_sector(ticker: str) -> Optional[str]:
    """
    One-call convenience: search_screener + fetch_sector. Returns None on
    any failure (no match, network error, unexpected page shape) — never
    raises for a "just couldn't resolve this one" case, so a caller
    processing many tickers can treat this as skip-and-continue. A real
    requests.RequestException still propagates (caller's problem: NSE/
    screener unreachable at all, not this one ticker being unresolvable).
    """
    match = search_screener(ticker)
    if match is None:
        return None
    return fetch_sector(match["url"])


# --- BSE fallback ------------------------------------------------------
# [2026-09-05] www.nseindia.com is fully blocked from this environment
# (403 Access Denied on even the homepage, Akamai edge-level bot block —
# NOT a rate limit that clears with retries) so NSE's quote-equity API
# (the only per-ticker industry source NSE itself publishes) is not usable
# here. api.bseindia.com IS reachable and its ComHeadernew endpoint's
# "IndustryNew" field was verified live (2026-09-05) to match screener.in's
# taxonomy exactly for RELIANCE ("Oil, Gas & Consumable Fuels" both), so
# it's a legitimate second real source, not a fabrication risk — used only
# when screener.in has no match.
_BSE_SEARCH_URL = "https://api.bseindia.com/BseIndiaAPI/api/PeerSmartSearch/w"
_BSE_HEADER_URL = "https://api.bseindia.com/BseIndiaAPI/api/ComHeadernew/w"
_BSE_HEADERS = {"User-Agent": HEADERS["User-Agent"], "Referer": "https://www.bseindia.com/", "Accept": "application/json, text/plain, */*"}


_NSE_BSE_MAP_PATH = Path(__file__).resolve().parents[2] / "config" / "nse_bse_ticker_map.csv"
_NSE_BSE_MAP_FIELDS = ["ticker", "bse_scripcode", "company_name", "isin", "resolved_at"]
_nse_bse_map_cache: Optional[Dict[str, Dict[str, str]]] = None


def _load_nse_bse_map() -> Dict[str, Dict[str, str]]:
    """Lazy-loaded, process-lifetime cache of config/nse_bse_ticker_map.csv."""
    global _nse_bse_map_cache
    if _nse_bse_map_cache is None:
        _nse_bse_map_cache = {}
        if _NSE_BSE_MAP_PATH.exists():
            with open(_NSE_BSE_MAP_PATH, newline="") as f:
                for row in csv.DictReader(f):
                    if row.get("ticker"):
                        _nse_bse_map_cache[row["ticker"]] = row
    return _nse_bse_map_cache


def _save_to_nse_bse_map(ticker: str, scripcode: str, company_name: str, isin: str) -> None:
    """
    Append one resolved ticker -> BSE-scrip mapping to config/
    nse_bse_ticker_map.csv (created with a header if it doesn't exist yet)
    and update the in-memory cache, so this exact ticker never needs a
    fresh BSE search again — only the (cheap) ComHeadernew call to refresh
    sector/name if ever needed.
    """
    is_new = not _NSE_BSE_MAP_PATH.exists()
    _NSE_BSE_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_NSE_BSE_MAP_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_NSE_BSE_MAP_FIELDS)
        if is_new:
            writer.writeheader()
        row = {
            "ticker": ticker, "bse_scripcode": scripcode, "company_name": company_name,
            "isin": isin, "resolved_at": date.today().isoformat(),
        }
        writer.writerow(row)
    _load_nse_bse_map()[ticker] = row


def bse_find_scrip(ticker: str) -> Optional[Dict[str, str]]:
    """
    BSE's search-by-text endpoint returns HTML `<li>` fragments, not JSON,
    and is a fuzzy/substring search that can return multiple unrelated
    companies for one query (verified live: searching "CUPID" returns both
    the real NSE-listed CUPID Ltd AND an unrelated "Cupid Breweries and
    Distilleries Ltd"). Only accepts a result whose embedded exchange
    symbol is an EXACT match for `ticker` (not "starts with") — same
    no-guessing discipline as search_screener's exact-slug requirement.

    Checks config/nse_bse_ticker_map.csv first (persistent cache built by
    every successful resolution here and by scripts/
    enrich_missing_company_metadata_bse.py) before hitting BSE's live
    search — so a ticker resolved once never needs the fuzzy-search step
    again, only ever a fresh ComHeadernew call if sector/name is needed
    again later. A static map alone would go stale for brand-new tickers,
    so this cache is additive to (never a replacement for) the live
    fallback below on a cache miss.

    Returns {'scripcode', 'company_name', 'isin'} on an exact match
    (from cache or live), else None.
    """
    import re

    cached = _load_nse_bse_map().get(ticker)
    if cached:
        return {"scripcode": cached["bse_scripcode"], "company_name": cached["company_name"], "isin": cached["isin"]}

    resp = requests.get(_BSE_SEARCH_URL, params={"Type": "SS", "text": ticker}, headers=_BSE_HEADERS, timeout=_TIMEOUT_S)
    resp.raise_for_status()
    for m in re.finditer(r"liclick\('(\d+)','([^']+)'\).*?<span>(.*?)</span>", resp.text, re.S):
        scripcode, company_name, span = m.groups()
        span_clean = re.sub(r"&nbsp;|<[^>]+>", " ", span).strip()
        isin_match = re.search(r"[A-Z]{2}[A-Z0-9]{9}[0-9]", span_clean)
        symbol_part = span_clean[: isin_match.start()].strip() if isin_match else span_clean.split()[0]
        if symbol_part.upper() == ticker.upper():
            company_name = company_name.strip()
            isin = isin_match.group(0) if isin_match else ""
            _save_to_nse_bse_map(ticker, scripcode, company_name, isin)
            return {"scripcode": scripcode, "company_name": company_name, "isin": isin}
    return None


def resolve_sector_bse(ticker: str) -> Optional[str]:
    """BSE fallback for resolve_sector — see resolve_company_metadata_bse for the full lookup this wraps."""
    metadata = resolve_company_metadata_bse(ticker)
    return metadata["sector"] if metadata else None


def resolve_company_metadata_bse(ticker: str) -> Optional[Dict[str, str]]:
    """
    Resolve `ticker` on BSE (exact-symbol match) and return
    {'company_name', 'sector', 'isin'} from the combined search +
    ComHeadernew lookup — one call gets both the company name and sector
    a blank-company_name ticker needs (see
    scripts/sync_stock_master_from_universe.py's docstring: it skips any
    row with a blank company_name, which is exactly why sector data for
    such tickers never reaches stock_master even once resolved elsewhere).
    Returns None if BSE has no exact-symbol match, or 'sector' is empty
    in the result (still returns company_name/isin in that case — a
    caller wanting only fully-complete rows should check `sector`).
    """
    scrip = bse_find_scrip(ticker)
    if scrip is None:
        return None
    resp = requests.get(
        _BSE_HEADER_URL, params={"quotetype": "EQ", "scripcode": scrip["scripcode"], "seriesid": ""},
        headers=_BSE_HEADERS, timeout=_TIMEOUT_S,
    )
    sector = None
    if resp.status_code == 200:
        industry = resp.json().get("IndustryNew")
        sector = industry.strip() if industry and industry.strip() else None
    return {"company_name": scrip["company_name"], "sector": sector or "", "isin": scrip["isin"]}


def resolve_sector_any_source(ticker: str) -> Optional[str]:
    """
    Try screener.in first (already the project's primary, longer-verified
    source), then fall back to BSE if screener has no match. Returns None
    only if BOTH sources fail to resolve — a genuine "needs manual
    research" case (same bucket the Aug 2026 B-028 49-stock manual pass
    handled). A requests.RequestException from screener does NOT
    short-circuit to BSE (a real network outage should surface, not be
    silently masked by trying a second host) — only an explicit "no match"
    (None) triggers the fallback.
    """
    sector = resolve_sector(ticker)
    if sector:
        return sector
    return resolve_sector_bse(ticker)
