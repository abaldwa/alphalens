"""
ingestion/scrapers/nse_brsr_qip.py

CA6 (2026-07-10): two real, live-verified NSE endpoints identified in an
earlier session (2026-07-08, see FeatureBacklog.md's CA6 entry) but never
built into a scraper — built here for the two that are actually buildable
without a missing secondary lookup param (RPT/governance need a `seqNum`/
`recId` from an undiscovered master-list endpoint — see FeatureBacklog.md
CA6, deliberately NOT attempted here, same "don't guess at an unconfirmed
endpoint shape" discipline this codebase uses elsewhere).

Live-verified 2026-07-10 (session):
    https://www.nseindia.com/api/corporate-further-issues-qip?symbol=X
        -> fully structured JSON, real QIP issue data (issue price, dates,
           allottee counts, dilution-relevant share counts). Confirmed
           against IDFCFIRSTB (2 real QIPs) and ZOMATO (1 real QIP).
    https://www.nseindia.com/api/corporate-bussiness-sustainabilitiy?symbol=X
        -> real BRSR (Business Responsibility and Sustainability Report)
           filing index — submission date + a linked XBRL XML file per
           fiscal year. Confirmed against RELIANCE (2 real filings).
           Scope here is deliberately limited to the filing INDEX, not
           parsing the linked XBRL for individual ESG metrics — that's a
           much larger, separately-scoped effort (hundreds of BRSR-specific
           XBRL tags), not attempted this session.

Session pattern (headers/cookie warm-up) copied from
ingestion/scrapers/nse_xbrl_financials.py's _nse_session, which itself
matches corporate_actions.py's established convention.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

NSE_HOMEPAGE_URL = "https://www.nseindia.com"
NSE_QIP_URL = "https://www.nseindia.com/api/corporate-further-issues-qip"
NSE_BRSR_URL = "https://www.nseindia.com/api/corporate-bussiness-sustainabilitiy"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
_TIMEOUT_S = 25
_MAX_RETRIES = 3


def _nse_session() -> requests.Session:
    """NSE session with browser headers and homepage cookies (same pattern as nse_xbrl_financials.py)."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    session.get(NSE_HOMEPAGE_URL, timeout=_TIMEOUT_S)
    return session


def _get_json(url: str, ticker: str) -> List[Dict[str, Any]]:
    last_exc = None
    for attempt in range(_MAX_RETRIES):
        try:
            session = _nse_session()
            resp = session.get(url, params={"symbol": ticker}, timeout=_TIMEOUT_S)
            resp.raise_for_status()
            payload = resp.json()
            return payload.get("data", [])
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            logger.warning(f"{url} ({ticker}) attempt {attempt + 1}/{_MAX_RETRIES} failed: {exc}")
    raise ConnectionError(f"Failed to fetch {url} for {ticker} after {_MAX_RETRIES} attempts: {last_exc}")


def _parse_nse_date(value: Optional[str]):
    """NSE dates render as '09-OCT-2023' — returns a date, or None if blank/unparseable."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%d-%b-%Y").date()
    except ValueError:
        return None


def _parse_number(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def download_qip_issues(ticker: str) -> List[Dict[str, Any]]:
    """
    Fetch and parse real QIP issue records for one ticker.

    Returns
    -------
    list of dict
        One row per real QIP `appId`, shaped for qip_details:
        ticker, app_id, board_resolution_date, allotment_date,
        listing_date, issue_price, min_issue_price, final_issue_size,
        no_of_allottees, no_of_shares_allotted, no_of_equity_shares_listed,
        dilution_pct (shares allotted / shares listed post-issue, when both
        are known and listed > 0 — the real dilution NSE's own
        `distPerShrsAvailed` field approximates but doesn't expose as a
        clean ratio).

    Raises
    ------
    ConnectionError
        If the fetch fails after retries.
    """
    rows = _get_json(NSE_QIP_URL, ticker)
    out = []
    for r in rows:
        if r.get("issue_type") != "QIP":
            continue  # this endpoint can carry other further-issue types; only QIP is in scope here
        shares_allotted = _parse_number(r.get("noOfSharesAllotted"))
        shares_listed = _parse_number(r.get("noOfEquitySharesListed"))
        dilution_pct = (
            shares_allotted / shares_listed
            if shares_allotted is not None and shares_listed else None
        )
        out.append({
            "ticker": ticker,
            "app_id": str(r.get("appId")),
            "board_resolution_date": _parse_nse_date(r.get("boardResolutionDate")),
            "allotment_date": _parse_nse_date(r.get("dtOfAllotmentOfShares")),
            "listing_date": _parse_nse_date(r.get("dateOfListing")),
            "issue_price": _parse_number(r.get("issPricePerUnit")),
            "min_issue_price": _parse_number(r.get("minIssPricePerUnit")),
            "final_issue_size": _parse_number(r.get("finalAmountOfIssueSize")),
            "no_of_allottees": int(_parse_number(r.get("noOfAllottees")) or 0) or None,
            "no_of_shares_allotted": int(shares_allotted) if shares_allotted is not None else None,
            "no_of_equity_shares_listed": int(shares_listed) if shares_listed is not None else None,
            "dilution_pct": dilution_pct,
        })
    return out


def download_brsr_filings(ticker: str) -> List[Dict[str, Any]]:
    """
    Fetch the real BRSR filing index for one ticker.

    Returns
    -------
    list of dict
        One row per real filing, shaped for brsr_filings: ticker, fy_from,
        fy_to, submission_date, xbrl_file_url, attachment_file_url.
        Does NOT parse the linked XBRL for individual ESG metrics — see
        module docstring.

    Raises
    ------
    ConnectionError
        If the fetch fails after retries.
    """
    rows = _get_json(NSE_BRSR_URL, ticker)
    out = []
    for r in rows:
        out.append({
            "ticker": ticker,
            "fy_from": int(r["fyFrom"]) if r.get("fyFrom") is not None else None,
            "fy_to": int(r["fyTo"]) if r.get("fyTo") is not None else None,
            "submission_date": _parse_nse_date(r.get("submissionDate")),
            "xbrl_file_url": r.get("xbrlFile"),
            "attachment_file_url": r.get("attachmentFile"),
        })
    return [r for r in out if r["fy_to"] is not None]  # fy_to is the PRIMARY KEY column, never write a NULL
