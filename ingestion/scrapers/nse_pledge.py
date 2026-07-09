"""
ingestion/scrapers/nse_pledge.py

Phase: follow-up (deep-forensic/shareholding gap fix, 2026-07-07)
Specs: SPEC-FEAT-001, SPEC-PIPE-006
Owner: Platform / Ingestion
Consumers: scripts/backfill_promoter_pledge_nse.py

Real, free, structured NSE promoter-pledge/encumbrance endpoint — live-
discovered 2026-07-07 by grepping NSE's own corporate-filings.js bundle
for the real API path behind
https://www.nseindia.com/companies-listing/corporate-filings-pledged-data
(a client-rendered SPA whose HTML source has no visible API hints, but
whose loaded JS bundle does):

    https://www.nseindia.com/api/corporate-pledgedata-sast3132?symbol=X
        &from_date=DD-MM-YYYY&to_date=DD-MM-YYYY

Live-verified against VERTOZ (a real company with disclosed promoter
encumbrance): real per-event rows with `postEventHoldingPerc` (the
promoter's pledge/encumbrance % of their own holding immediately after
the disclosed event — this is the "promoter_pledge" value this feature
needs, not `encumbPerc`, which is a per-event delta, not a running
level). Corrects the earlier same-day research finding in
ingestion/scrapers/screener.py's docstring, which tried several guessed
corpType values against a *different* endpoint
(/api/CorpInfo?corpType=sast) and found only empty results — the real
endpoint uses a distinct path with a required `symbol` query param, not
discoverable without inspecting the loaded JS.

This is a per-ticker, on-demand endpoint (no bulk "all tickers" variant
found) — same one-call-per-ticker shape as
ingestion/scrapers/corporate_actions.py's per-ticker corporate actions
fetch, not a daily-scheduler-friendly bulk call. Backfilled via
scripts/backfill_promoter_pledge_nse.py rather than wired into the daily
morning step (SEBI SAST Reg 31(4) disclosures are event-driven, not
scheduled — there is no "today's pledge data" to poll for; a periodic
backfill re-run picks up new disclosures as they occur).
"""

import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

NSE_HOMEPAGE_URL = "https://www.nseindia.com"
NSE_PLEDGE_URL = "https://www.nseindia.com/api/corporate-pledgedata-sast3132"
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


def _parse_broadcast_date(value: Optional[str]) -> Optional[datetime]:
    """'DD-Mon-YYYY HH:MM:SS' -> datetime, or None if unparseable."""
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%d-%b-%Y %H:%M:%S")
    except ValueError:
        return None


def _parse_pct(value) -> Optional[float]:
    """NSE returns percentages as whitespace-padded strings ('    25.77') or '0' — parse to float, None if blank."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def download_pledge_data(ticker: str, from_date: str, to_date: str) -> pd.DataFrame:
    """
    Fetch real promoter pledge/encumbrance disclosure events for one ticker.

    Parameters
    ----------
    ticker : str
    from_date, to_date : str
        "YYYY-MM-DD".

    Returns
    -------
    pd.DataFrame
        Columns: ticker, broadcast_date (datetime, real disclosure date —
        the PIT-correct field), post_event_holding_pct (promoter's
        encumbrance % of their own holding immediately after this event —
        the running "promoter_pledge" level, not a delta), attachment_url.
        Empty DataFrame if the ticker has no disclosed pledge events in
        range (the common case — most companies have zero promoter
        pledge, which is a real "0", not missing data).

    Spec References
    ----------------
    SPEC-PIPE-006: retry up to _MAX_RETRIES times.

    Raises
    ------
    ConnectionError
        If the fetch fails after retries.
    """
    from_dt = datetime.strptime(from_date, "%Y-%m-%d")
    to_dt = datetime.strptime(to_date, "%Y-%m-%d")
    params = {
        "symbol": ticker,
        "from_date": from_dt.strftime("%d-%m-%Y"),
        "to_date": to_dt.strftime("%d-%m-%Y"),
    }

    last_exc = None
    for attempt in range(_MAX_RETRIES):
        try:
            session = _nse_session()
            resp = session.get(NSE_PLEDGE_URL, params=params, timeout=_TIMEOUT_S)
            resp.raise_for_status()
            payload = resp.json()
            rows: List[dict] = []
            for row in payload.get("data", []):
                broadcast_date = _parse_broadcast_date(row.get("broadcastdate"))
                if broadcast_date is None:
                    continue
                rows.append(
                    {
                        "ticker": ticker,
                        "broadcast_date": broadcast_date,
                        "post_event_holding_pct": _parse_pct(row.get("postEventHoldingPerc")),
                        "attachment_url": row.get("attachment"),
                    }
                )
            return pd.DataFrame(rows)
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            logger.warning(f"download_pledge_data({ticker}) attempt {attempt + 1}/{_MAX_RETRIES} failed: {exc}")
    raise ConnectionError(f"Failed to download NSE pledge data for {ticker} after {_MAX_RETRIES} attempts: {last_exc}")
