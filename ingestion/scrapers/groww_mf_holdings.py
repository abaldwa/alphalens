"""
ingestion/scrapers/groww_mf_holdings.py

Phase: 2.2 (AMFI MF Holdings + Corporate Action Features)
Specs: SPEC-PIPE-001, SPEC-PIPE-003 (CRITICAL), SPEC-MFHOLD-001
Owner: Platform / Ingestion
Consumers: ingestion/scrapers/amfi_holdings.py (registry),
           ingestion/scheduler/pipeline_scheduler.py, features/mf_holdings.py

Groww (groww.in) — the PRIMARY mutual-fund-holdings source for all AMCs,
per SPEC-MFHOLD-001 (alphalens_docs/specs/08_specifications.md). Replaces
the original "scrape all ~44 AMC websites individually" plan after a
direct, live investigation: Groww's mutual-fund pages embed complete,
real per-scheme holdings (company_name, sector_name, nature_name,
corpus_per = % of AUM) directly in server-rendered HTML (`__NEXT_DATA__`,
a Next.js SSR JSON blob) — reachable with a plain `requests.get()`, no
login, no JavaScript execution, no bot-blocking. Verified specifically
against HDFC and ICICI Prudential — the two AMCs whose own sites blocked
every other approach tried (see ingestion/scrapers/sbi_mf_holdings.py's
docstring for the per-AMC-website approach this replaced).

A second real endpoint enumerates every scheme for a given fund house in
one call (`GROWW_SEARCH_API`, `size=500` returns every scheme for every
AMC checked in a single page). The AMC directory itself (49 AMCs — a
superset of the ~44 the original build prompt estimated) is embedded the
same way on any AMC's own page — `discover_groww_amc_directory()` reads
it directly, never a hardcoded list.

Precision tradeoffs vs. SBI's direct Excel source (sbi_mf_holdings.py),
documented not hidden:
- No per-holding ISIN — Groww exposes company_name + a slug only. `isin`
  is populated via a ticker-keyed cross-reference against
  `config.universe`'s real ISIN column (added in P2.1) instead.
- No share quantity — only `corpus_per` (% of AUM). `quantity` is left
  NaN, never fabricated (a fabricated value would corrupt features/
  mf_holdings.py's mf_sip_inflow_proxy, which depends on real deltas).
- No historical archive — only the CURRENT live snapshot. Confirmed live
  (twice, on different days): every holding across a full AMC's schemes
  shares exactly one `portfolio_date`, and that date does not change
  until the AMC publishes its next disclosure. `make_amc_fetcher()`'s
  `fetch_fn` validates the live snapshot's own `portfolio_date` actually
  falls within the requested (year, month) and raises rather than
  silently mislabeling stale data — Groww-sourced ingestion can only
  capture "now," never backfill a past month. (Confirmed live: April
  2026 data is not retrievable through Groww — see BuildLog.md "P2.2
  continued — pivot to Groww".)
- Ticker resolution only covers this project's current investable
  universe (Nifty 500). Measured live against HDFC Mutual Fund: of 4,876
  real equity holdings, 180 were derivative (Futures/Options) positions
  (explicitly excluded — not share ownership) and 960 were genuine
  companies outside the Nifty 500 (e.g. "Metro Brands Ltd") — an honest
  scope limit, not a matching bug. The remaining 76.6% resolved correctly.

PIT Assumptions
----------------
SPEC-PIPE-003 (CRITICAL): see `make_amc_fetcher()`'s live-snapshot-date
validation above — this is the PIT enforcement point for this source.
"""

import json
import logging
import re
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd
import requests

from config.settings import AMFI_RAW_DIR, GROWW_RATE_LIMIT_SLEEP_SECONDS
from config.universe import load_universe_raw
from ingestion.scrapers.amfi_holdings import register_amc
from ingestion.scrapers.browser import DEFAULT_USER_AGENT

logger = logging.getLogger(__name__)

SEARCH_API = "https://groww.in/v1/api/search/v3/query/filter_derived_data/st_filter"
FUND_BASE_URL = "https://groww.in/mutual-funds"
AMC_LISTING_URL = "https://groww.in/mutual-funds/amc/sbi-mutual-funds"  # any AMC page embeds the full directory

_NEXT_DATA_PATTERN = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL)
_NAME_NOISE_PATTERN = re.compile(r"\b(ltd|limited|the)\b\.?", re.IGNORECASE)


def discover_amc_directory() -> List[Dict[str, str]]:
    """
    Fetch Groww's own full AMC directory (verified live: 49 AMCs at time
    of writing — a superset of the ~44 this build prompt estimated).

    Returns
    -------
    list of dict
        Each: {'name': 'SBI Mutual Fund', 'search_id': 'sbi-mutual-funds'}.

    Spec References
    ----------------
    SPEC-PIPE-001, SPEC-MFHOLD-001.

    Raises
    ------
    requests.RequestException
        If the fetch fails.
    ValueError
        If the page's embedded __NEXT_DATA__ JSON doesn't have the expected shape.
    """
    response = requests.get(AMC_LISTING_URL, timeout=30, headers={"User-Agent": DEFAULT_USER_AGENT})
    response.raise_for_status()
    match = _NEXT_DATA_PATTERN.search(response.text)
    if not match:
        raise ValueError("Groww AMC listing page: __NEXT_DATA__ not found — page structure may have changed")
    data = json.loads(match.group(1))
    amcs = data["props"]["pageProps"]["amcMainPageData"][0]["amcs"]
    return [{"name": a["name"], "search_id": a["search_id"]} for a in amcs]


def _list_scheme_ids(fund_house: str) -> List[str]:
    """All Direct-Growth scheme id slugs for one fund house, in one call (size=500 covers every AMC checked)."""
    params = {
        "available_for_investment": "true",
        "doc_type": "scheme",
        "fund_house": fund_house,
        "index": "false",
        "page": 0,
        "plan_type": "Direct",
        "scheme_type": "Growth",
        "size": 500,
        "sort_by": 3,
    }
    response = requests.get(SEARCH_API, params=params, timeout=30, headers={"User-Agent": DEFAULT_USER_AGENT})
    response.raise_for_status()
    content = response.json().get("content", [])
    return [item["id"] for item in content if item.get("id")]


def _fetch_scheme_detail(scheme_id: str) -> Optional[Dict[str, Any]]:
    """One scheme's full mfServerSideData (holdings, aum, scheme_name, isin, ...), or None if unreachable."""
    response = requests.get(f"{FUND_BASE_URL}/{scheme_id}", timeout=30, headers={"User-Agent": DEFAULT_USER_AGENT})
    if response.status_code != 200:
        logger.warning(f"Groww scheme fetch failed for {scheme_id}: HTTP {response.status_code}")
        return None
    match = _NEXT_DATA_PATTERN.search(response.text)
    if not match:
        return None
    data = json.loads(match.group(1))
    return data.get("props", {}).get("pageProps", {}).get("mfServerSideData")


def _normalize_company_name(name: Optional[str]) -> str:
    """'HDFC Bank Ltd' / 'Hdfc Bank Limited' -> 'hdfcbank' — robust enough for exact-set matching, not fuzzy.

    [AS BUILT, 2026-07-05] `not name` alone does not catch a NaN float —
    `bool(float("nan"))` is True, not False — so a company_name that is
    NaN (stock_master has ~691 still-unresolved blank rows, see
    FutureDevelopment.md's #31) would crash re.sub with a TypeError
    instead of normalizing to "" like a real empty name would. Found via
    trendlyne.py's identical helper crashing on a real run against the
    same universe data; fixed here too since both duplicate this logic.
    """
    if not isinstance(name, str) or not name:
        return ""
    cleaned = _NAME_NOISE_PATTERN.sub("", name)
    return re.sub(r"[^a-z0-9]+", "", cleaned.lower())


def _build_company_name_to_ticker_isin_map() -> Dict[str, tuple]:
    """
    Normalized company_name -> (ticker, isin), from the real universe
    (config.universe.load_universe_raw). The same universe table P2.1's
    fundamentals scraper added a real `isin` column to — reused here so
    Groww-sourced holdings (which expose no per-holding ISIN themselves)
    still get a real ISIN via this ticker-keyed cross-reference.
    """
    df = load_universe_raw()
    return {_normalize_company_name(row.company_name): (row.ticker, row.isin) for row in df.itertuples()}


def make_amc_fetcher(fund_house: str) -> Callable[[int, int], bytes]:
    """
    Build a fetch_fn for one AMC: enumerate its schemes, fetch each one's
    live holdings snapshot, validate the snapshot's own portfolio_date
    actually falls in the requested (year, month) — Groww has no
    historical archive, only "now" — and return the raw scheme-detail
    list as JSON bytes.

    Parameters
    ----------
    fund_house : str
        Exact Groww fund_house name (e.g. "HDFC Mutual Fund").

    Returns
    -------
    callable
        (year, month) -> bytes, conforming to AMCSource.fetch_fn.
    """

    def fetch_fn(year: int, month: int) -> bytes:
        scheme_ids = _list_scheme_ids(fund_house)
        if not scheme_ids:
            raise ConnectionError(f"Groww: no schemes found for fund house '{fund_house}'")

        schemes = []
        for scheme_id in scheme_ids:
            detail = _fetch_scheme_detail(scheme_id)
            if detail:
                schemes.append(detail)
            time.sleep(GROWW_RATE_LIMIT_SLEEP_SECONDS)

        if not schemes:
            raise ConnectionError(f"Groww: every scheme fetch failed for fund house '{fund_house}'")

        # SPEC-PIPE-003 (CRITICAL): Groww only ever exposes the current
        # live snapshot. Confirm it actually represents the requested
        # (year, month) rather than silently mislabeling stale/future data.
        sample_holdings = schemes[0].get("holdings") or []
        if sample_holdings:
            portfolio_date = sample_holdings[0].get("portfolio_date")
            if portfolio_date:
                snapshot_dt = datetime.fromisoformat(portfolio_date.replace("Z", "+00:00"))
                if not (snapshot_dt.year == year and snapshot_dt.month == month):
                    raise ConnectionError(
                        f"Groww's live snapshot for {fund_house} is dated {snapshot_dt.date()}, "
                        f"not {year}-{month:02d} — Groww has no historical archive; this fetch_fn "
                        "can only capture the current month, not backfill a past one."
                    )

        raw_dir = AMFI_RAW_DIR / "groww" / re.sub(r"[^a-zA-Z0-9]+", "_", fund_house)
        raw_dir.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(schemes).encode()
        (raw_dir / f"{year:04d}-{month:02d}.json").write_bytes(payload)
        return payload

    return fetch_fn


def parse_amc(raw: bytes) -> pd.DataFrame:
    """
    Parse one AMC's Groww scheme-detail list into the standard holdings
    shape (scheme_name, isin, ticker, quantity, value_inr). See this
    module's docstring for the documented precision tradeoffs.

    Parameters
    ----------
    raw : bytes
        JSON-encoded list of scheme-detail dicts, as returned by a
        make_amc_fetcher()-built fetch_fn.

    Returns
    -------
    pd.DataFrame

    Raises
    ------
    None — unresolvable holdings are skipped, not raised.
    """
    schemes = json.loads(raw)
    name_to_ticker_isin = _build_company_name_to_ticker_isin_map()

    records = []
    for scheme in schemes:
        scheme_name = scheme.get("scheme_name")
        aum_cr = scheme.get("aum") or 0
        for holding in scheme.get("holdings") or []:
            if holding.get("nature_name") != "EQUITY":
                continue
            instrument_name = holding.get("company_name") or ""
            if instrument_name.endswith(("Futures", "Options")):
                continue
            match = name_to_ticker_isin.get(_normalize_company_name(instrument_name))
            if not match:
                continue
            ticker, isin = match
            corpus_per = holding.get("corpus_per") or 0
            records.append(
                {
                    "scheme_name": scheme_name,
                    "isin": isin,
                    "ticker": ticker,
                    "quantity": np.nan,
                    "value_inr": (corpus_per / 100.0) * aum_cr * 1e7,
                }
            )

    return pd.DataFrame(records, columns=["scheme_name", "isin", "ticker", "quantity", "value_inr"])


def register_all_amcs() -> int:
    """
    Discover Groww's live AMC directory and register every one as a
    primary holdings source. A real network call — deliberately NOT
    invoked at module import time (so importing this module in a test
    never hits the network); call explicitly (CLI `--all-groww`, or the
    scheduled job) when actually ingesting. Safe to call multiple times
    (re-registering is just an AMC_REGISTRY dict overwrite).

    Returns
    -------
    int
        Number of AMCs registered.

    Spec References
    ----------------
    SPEC-SOLID-002 (Open/Closed), SPEC-MFHOLD-001.

    Raises
    ------
    requests.RequestException, ValueError
        Propagated from discover_amc_directory().
    """
    amcs = discover_amc_directory()
    for amc in amcs:
        register_amc(amc["name"], make_amc_fetcher(amc["name"]), parse_amc)
    logger.info(f"Registered {len(amcs)} Groww-backed AMCs")
    return len(amcs)
