"""
ingestion/scrapers/sebi_enforcement_orders.py

Phase: full-codebase-review Fix A5 (2026-07-19)
Owner: Platform / Ingestion
Consumers: systems/ml_signal_engine/models/pnd/pnd_detector.py
           (load_pnd_training_data_from_db)

Why this exists
----------------
pnd_detector.py's KNOWN_PND_TICKERS is a hardcoded, undated list of 20
tickers — its own module docstring documents this as a [KNOWN GAP]:
positives are scored on their MOST RECENT 180 days of trading (post-
enforcement, already-scrutinized), not the actual manipulation-era
window, because no per-ticker event-date metadata existed anywhere in
this codebase. This module is the real data source that closes that gap.

Live-verified structure (2026-07-19, from this environment):
    https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=2&ssid=9&smid=6
"Orders of AO" (Adjudicating Officer) — the SEBI Enforcement > Orders
category most likely to contain price-manipulation/pump-and-dump cases
against listed companies. Confirmed live: a server-rendered HTML table
(`<table id='sample_1'>`, `<tbody><tr role='row' class='odd'>` rows) with
two columns — Date ("Jul 17, 2026" format) and Title (an `<a>` linking to
the order's detail page, title attribute holds the full case description,
e.g. "Adjudication Order in the matter of Citrus Check Inns Limited").
Confirmed the `cur_pg` query param does NOT paginate further back — this
endpoint appears to return only the most recent ~25 orders; there is no
confirmed mechanism in this session to reach older orders (a real,
disclosed limitation, not silently ignored — see fetch_ao_orders below).

What this scraper does NOT do (explicitly, per 2026-07-19 user decision
to build against verified real structure but flag anything unverified):
    - It does NOT parse individual order PDFs/detail pages for the exact
      manipulation-period date range some orders state in their text.
      That would require per-order HTML/PDF text extraction not yet
      built or verified against a real sample. manipulation_start_date/
      manipulation_end_date are left NULL for every scraped row — a
      documented gap (SPEC-QUALITY-003: NULL, not fabricated), not a
      silently wrong value. load_pnd_training_data_from_db's caller-side
      fallback (a window ending at order_date) is expected to be the
      primary path for the foreseeable future.
    - Many "Orders of AO" titles are individual-person options-trading
      cases ("Illiquid Stock Options at BSE" against a named individual),
      not company-level equity price manipulation — _looks_like_pnd_case
      keyword-filters these out, but is a coarse heuristic, not a
      guaranteed-accurate classifier. Titles that DO pass the filter
      still only give a company NAME, not an NSE ticker — resolve_ticker
      fuzzy-matches against config.universe's real company_name column
      and returns None (never a guessed/wrong ticker) on no confident
      match.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from difflib import get_close_matches
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SEBI_HOMEPAGE_URL = "https://www.sebi.gov.in"
# sid=2 (Enforcement), ssid=9 (Orders), smid=6 (Orders of AO) — verified
# live 2026-07-19 (see module docstring). Other smid values under the same
# sid/ssid (Orders of SAT=1, Chairperson/Members=2, Settlement=3, ED/CGM
# quasi-judicial=133, etc.) were seen in the same page's category list but
# not individually fetched/verified this session.
SEBI_AO_ORDERS_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do"
SEBI_AO_ORDERS_PARAMS = {"doListing": "yes", "sid": "2", "ssid": "9", "smid": "6"}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
_TIMEOUT_S = 20

# Coarse keyword filter (2026-07-19, unverified against a large labeled
# sample — best-effort, not a guaranteed-accurate classifier): excludes
# individual-trader options-manipulation cases, keeps company-level
# matters that plausibly involve price/volume manipulation of the equity
# itself.
_EXCLUDE_TITLE_PATTERNS = re.compile(
    r"illiquid stock options|thematic inspection|debenture trustee|"
    r"proprietor|research analyst",
    re.IGNORECASE,
)
_COMPANY_MATTER_PATTERN = re.compile(
    r"in the matter of (?:dealings? in )?(.+?)(?:\s+in the matter of|\.?\s*$)",
    re.IGNORECASE,
)


def _sebi_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def fetch_ao_orders_html(session: Optional[requests.Session] = None) -> str:
    """
    Fetch the real "Orders of AO" listing page (live-verified structure,
    see module docstring). Returns the raw HTML; parse with
    parse_ao_orders_html().

    Known limitation: this endpoint has only been confirmed to return the
    most recent ~25 orders in this session — no working pagination/older-
    archive mechanism was found. Each scraper run re-fetches the same
    "latest N" window; genuinely historical orders (older than whatever
    is currently listed) are not reachable via this function as verified.

    Raises
    ------
    requests.RequestException
        On network failure — never silently returns empty/fabricated HTML.
    """
    session = session or _sebi_session()
    response = session.get(
        SEBI_AO_ORDERS_URL, params=SEBI_AO_ORDERS_PARAMS, timeout=_TIMEOUT_S
    )
    response.raise_for_status()
    return response.text


def parse_ao_orders_html(html: str) -> List[Dict[str, Any]]:
    """
    Parse the real table structure verified live 2026-07-19: a
    `<table id='sample_1'>` with `<tbody><tr role='row'>` rows, each
    containing a Date `<td>` ("Jul 17, 2026" format) and a Title `<td>`
    with an `<a href=...>` to the order's detail page.

    Returns
    -------
    list of dict
        {order_date: date, title: str, detail_url: str} per row, in the
        order they appear on the page (most recent first, per observed
        behavior). Rows whose date can't be parsed are skipped (logged,
        never fabricated).
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="sample_1")
    if table is None:
        logger.warning("sebi_enforcement_orders: no table#sample_1 found — page structure may have changed")
        return []

    rows: List[Dict[str, Any]] = []
    tbody = table.find("tbody")
    if tbody is None:
        return []

    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        date_text = tds[0].get_text(strip=True)
        link = tds[1].find("a")
        if link is None:
            continue
        title = link.get("title") or link.get_text(strip=True)
        detail_url = link.get("href", "")

        try:
            order_date = datetime.strptime(date_text, "%b %d, %Y").date()
        except ValueError:
            logger.debug(f"sebi_enforcement_orders: unparseable date {date_text!r}, skipping row")
            continue

        rows.append({"order_date": order_date, "title": title.strip(), "detail_url": detail_url})

    return rows


def _looks_like_pnd_case(title: str) -> bool:
    """Coarse keyword filter — see module docstring's disclosed
    limitation. Excludes individual-trader options-manipulation and
    unrelated administrative-order categories; does not positively
    confirm a title IS a pump-and-dump case, only that it isn't an
    obviously-excluded category."""
    return not _EXCLUDE_TITLE_PATTERNS.search(title)


def _extract_company_name(title: str) -> Optional[str]:
    """Best-effort company-name extraction from a title's free text
    (e.g. "Adjudication Order in the matter of Citrus Check Inns
    Limited" -> "Citrus Check Inns Limited"). Returns None if the title
    doesn't match the expected "in the matter of X" pattern — never
    guesses a name from an unrecognized format."""
    match = _COMPANY_MATTER_PATTERN.search(title)
    if not match:
        return None
    name = match.group(1).strip().rstrip(".")
    return name or None


def resolve_ticker(company_name: str, universe_df: pd.DataFrame, cutoff: float = 0.85) -> Optional[str]:
    """
    Fuzzy-match `company_name` (free text from an order title) against
    `universe_df`'s real `company_name` column (config/nifty500_universe.csv)
    to find the corresponding NSE ticker.

    Parameters
    ----------
    company_name : str
        Extracted company name from an order title.
    universe_df : pd.DataFrame
        Must have `ticker` and `company_name` columns (config.universe.load_universe_raw()).
    cutoff : float
        difflib.get_close_matches similarity cutoff (default 0.85 — high,
        deliberately conservative: a wrong ticker match on safety-relevant
        PnD training data is worse than no match at all).

    Returns
    -------
    str or None
        The matched ticker, or None if no sufficiently-close match exists
        — never returns a low-confidence guess.
    """
    if "company_name" not in universe_df.columns or "ticker" not in universe_df.columns:
        return None
    names = universe_df["company_name"].dropna().tolist()
    matches = get_close_matches(company_name, names, n=1, cutoff=cutoff)
    if not matches:
        return None
    matched_row = universe_df[universe_df["company_name"] == matches[0]]
    if matched_row.empty:
        return None
    return str(matched_row.iloc[0]["ticker"])


def build_enforcement_order_rows(
    orders: List[Dict[str, Any]], universe_df: pd.DataFrame
) -> List[Dict[str, Any]]:
    """
    Convert parsed order rows into sebi_enforcement_orders table rows,
    applying the PnD-case keyword filter and ticker resolution.

    Returns
    -------
    list of dict
        {ticker, company_name, order_date, order_type, source_url,
        manipulation_start_date, manipulation_end_date} — the latter two
        always None (see module docstring: not yet extracted from order
        detail pages). Only rows that both look like a company-level
        matter (_looks_like_pnd_case) AND resolve to a real ticker are
        included — a title that doesn't resolve is logged and dropped,
        never inserted with a fabricated/guessed ticker.
    """
    result = []
    for order in orders:
        title = order["title"]
        if not _looks_like_pnd_case(title):
            continue
        company_name = _extract_company_name(title)
        if company_name is None:
            logger.debug(f"sebi_enforcement_orders: no company name extracted from {title!r}")
            continue
        ticker = resolve_ticker(company_name, universe_df)
        if ticker is None:
            logger.debug(f"sebi_enforcement_orders: no ticker match for {company_name!r}")
            continue
        result.append({
            "ticker": ticker,
            "company_name": company_name,
            "order_date": order["order_date"],
            "order_type": "AO",
            "source_url": order["detail_url"],
            "manipulation_start_date": None,
            "manipulation_end_date": None,
        })
    return result


def write_enforcement_orders(conn, rows: List[Dict[str, Any]]) -> int:
    """Upsert rows into the sebi_enforcement_orders table (see
    datastore/schema/create_normalised.py for schema). Returns count written."""
    if not rows:
        return 0
    for row in rows:
        conn.execute(
            """
            INSERT INTO sebi_enforcement_orders
                (ticker, company_name, order_date, order_type, source_url,
                 manipulation_start_date, manipulation_end_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (ticker, order_date, source_url) DO NOTHING
            """,
            [
                row["ticker"], row["company_name"], row["order_date"], row["order_type"],
                row["source_url"], row["manipulation_start_date"], row["manipulation_end_date"],
            ],
        )
    return len(rows)
