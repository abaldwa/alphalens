"""
ingestion/scrapers/nse_ipo.py

Phase: follow-up (deep-forensic/corp-action gap fix)
Specs: SPEC-FEAT-001, SPEC-PIPE-006
Owner: Platform / Ingestion
Consumers: scripts/backfill_listing_dates_nse.py

Real, free, structured NSE endpoint for historical IPO listing dates —
live-discovered and verified 2026-07-07:

    https://www.nseindia.com/api/public-past-issues?index=Equity

Returns real JSON (1,377 rows verified live), each with `symbol` and
`listingDate` ("DD-MMM-YYYY", or "-" for an IPO that hasn't listed yet).
This resolves stock_master.listing_date, which was 0/1626 populated
before this — not because NSE doesn't publish it, but because nothing in
this codebase had ever fetched it. ipo_lockin_expiry_proximity/
ipo_listing_age_months (features/corporate_action_features.py) were
permanently NaN as a direct result.

Same NSE session-priming pattern as ingestion/scrapers/corporate_actions.py
(NSE requires a homepage GET first to set cookies before its /api/* JSON
endpoints will respond) — see that module's `_nse_session()` for the
precedent this mirrors.
"""

import logging
from datetime import datetime, date
from typing import Dict

import requests

logger = logging.getLogger(__name__)

NSE_HOMEPAGE_URL = "https://www.nseindia.com"
NSE_PAST_ISSUES_URL = "https://www.nseindia.com/api/public-past-issues?index=Equity"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
_TIMEOUT_S = 20
_MAX_RETRIES = 3


def _nse_session() -> requests.Session:
    """Create an NSE session with browser headers and homepage cookies (same pattern as corporate_actions.py)."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    session.get(NSE_HOMEPAGE_URL, timeout=_TIMEOUT_S)
    return session


def download_past_issues() -> Dict[str, date]:
    """
    Fetch NSE's full historical past-issues (IPO) list and return a
    {ticker: listing_date} dict for every real, already-listed issue.

    Returns
    -------
    dict
        {ticker: date} — only rows with a real, parseable listingDate
        (excludes upcoming/not-yet-listed issues, whose listingDate is "-").

    Spec References
    ----------------
    SPEC-PIPE-006: retry up to _MAX_RETRIES times.
    SPEC-FEAT-001: no fabricated fallback — real data or an exception.

    Raises
    ------
    ConnectionError
        If the fetch fails after retries.
    """
    last_exc = None
    for attempt in range(_MAX_RETRIES):
        try:
            session = _nse_session()
            resp = session.get(NSE_PAST_ISSUES_URL, timeout=_TIMEOUT_S)
            resp.raise_for_status()
            rows = resp.json()
            result: Dict[str, date] = {}
            for row in rows:
                ticker = row.get("symbol")
                listing_date_str = row.get("listingDate")
                if not ticker or not listing_date_str or listing_date_str == "-":
                    continue
                try:
                    listing_date = datetime.strptime(listing_date_str, "%d-%b-%Y").date()
                except ValueError:
                    logger.debug(f"nse_ipo: unparseable listingDate '{listing_date_str}' for {ticker}, skipping")
                    continue
                # A ticker can appear more than once (re-listing, symbol reuse) —
                # keep the earliest real listing, consistent with "IPO listing date" semantics.
                if ticker not in result or listing_date < result[ticker]:
                    result[ticker] = listing_date
            return result
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            logger.warning(f"download_past_issues attempt {attempt + 1}/{_MAX_RETRIES} failed: {exc}")
    raise ConnectionError(f"Failed to download NSE past issues after {_MAX_RETRIES} attempts: {last_exc}")
