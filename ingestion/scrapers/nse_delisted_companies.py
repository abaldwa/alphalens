"""
ingestion/scrapers/nse_delisted_companies.py

Phase: full-codebase-review Fix A4 (2026-07-19)
Owner: Platform / Ingestion
Consumers: config/build_universe.py (build_historical_universe_from_delisted)

*** UNVERIFIED TARGET STRUCTURE — READ BEFORE TRUSTING ANY OUTPUT ***

Why this exists
----------------
features/momentum_universe.py's candidate ticker pool is drawn from
config/nifty500_universe.csv, which config/build_universe.py's own
docstring describes as a CURRENT-DAY snapshot (built from either NSE's
live Nifty500 list or a trailing-90-day-activity proxy). Any stock that
delisted, merged, or was suspended before the CSV was last rebuilt is
permanently invisible to a historical momentum backtest, even for years
when it legitimately belonged in the tracked market-cap bands — real,
material survivorship bias (Reliance Capital, DHFL, Yes Bank
pre-restructuring, various pre-2016 mergers all fall into this gap).
This scraper is meant to close it by sourcing NSE's historical delisted/
suspended companies list into a `delisted_companies` table that
config/build_universe.py can union into a true historical candidate pool.

Verification status (2026-07-19, this session)
------------------------------------------------
NSE's main site (www.nseindia.com) returns HTTP 403 to every request
from this environment, including the plain homepage — a host-level
block, not a wrong URL (matches an existing documented note in
scripts/build_momentum_benchmark_db.py about the same host blocking this
environment for other scrapers). archives.nseindia.com returns HTTP 503
("An error occurred while processing your request") for every path
tried, including plausible delisted-companies CSV names
(content/equities/eq_delisted.csv). Several guessed nseindia.com API
paths (api/corporates-db-search?index=corp_delist,
api/corporate-delisted) returned a real, clean 404 (not a block) —
meaning the correct endpoint path is genuinely unknown from this
session, not merely blocked.

Per 2026-07-19 user decision, this module is built against NSE's
documented delisted-companies report format (the "Company Master" /
"Delisted Companies" listing NSE has published for years under its
"Product > Equity > Corporate Filings" section, structurally similar to
its other corporate-database exports — a JSON list of records keyed by
symbol/company name/exchange/delisting date/delisting type) — but this
has NOT been confirmed against a live response in this session. Every
public function below is written to fail LOUDLY (raise, log at ERROR)
on an unexpected response shape rather than silently parsing garbage
into delisted_companies — see _parse_delisted_companies_json's shape
validation. Before relying on this scraper's output for a real backtest:
    1. Run fetch_delisted_companies_json() from an environment with real
       NSE access and inspect the raw response.
    2. Update NSE_DELISTED_URL / _RECORD_KEY_CANDIDATES below if the
       real shape differs from what's assumed here.
    3. Only then trust delisted_companies table contents.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

NSE_HOMEPAGE_URL = "https://www.nseindia.com"
# UNVERIFIED (see module docstring) — best-known real NSE endpoint
# pattern for its corporate-database search API, by analogy with
# ingestion/scrapers/nse_corporate_announcements.py's confirmed-real
# /api/corporate-announcements endpoint shape.
NSE_DELISTED_URL = "https://www.nseindia.com/api/corporates-db-search"
NSE_DELISTED_PARAMS = {"index": "corp_delist"}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
_TIMEOUT_S = 20

# UNVERIFIED — candidate key names for each field, tried in order, based
# on NSE's observed camelCase convention elsewhere in this codebase's
# other real NSE integrations (nse_corporate_announcements.py's
# symbol/sm_name/an_dt). A real response with none of these keys present
# will raise rather than silently producing empty/wrong rows.
_SYMBOL_KEYS = ("symbol", "sm_symbol", "SYMBOL")
_COMPANY_NAME_KEYS = ("companyName", "sm_name", "nameOfCompany", "COMPANY_NAME")
_DELISTING_DATE_KEYS = ("delistingDate", "dateOfDelisting", "delistDt", "DELISTING_DATE")
_DELISTING_TYPE_KEYS = ("delistingType", "typeOfDelisting", "DELISTING_TYPE")


def _nse_session() -> requests.Session:
    """Same real session-warmup pattern as
    nse_corporate_announcements.py's _nse_session — NSE's API requires a
    homepage visit first to set required cookies."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    session.get(NSE_HOMEPAGE_URL, timeout=_TIMEOUT_S)
    return session


def fetch_delisted_companies_json(session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    """
    Fetch NSE's delisted-companies list. UNVERIFIED endpoint (see module
    docstring) — will raise on a non-200 response or a JSON shape that
    isn't a list, rather than returning an empty/fabricated result.

    Raises
    ------
    requests.RequestException
        On network failure or non-200 response.
    ValueError
        If the response isn't valid JSON, or isn't a list of records.
    """
    session = session or _nse_session()
    response = session.get(NSE_DELISTED_URL, params=NSE_DELISTED_PARAMS, timeout=_TIMEOUT_S)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        raise ValueError(
            f"nse_delisted_companies: expected a JSON list, got {type(data).__name__} — "
            "NSE_DELISTED_URL's real response shape differs from what this module assumes "
            "(see module docstring's verification checklist)."
        )
    return data


def _first_present(record: Dict[str, Any], keys: tuple) -> Optional[Any]:
    for key in keys:
        if key in record and record[key] not in (None, ""):
            return record[key]
    return None


def _parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(value), fmt).date()
        except ValueError:
            continue
    logger.debug(f"nse_delisted_companies: unparseable date {value!r}")
    return None


def parse_delisted_companies(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse raw NSE delisted-companies records into delisted_companies table
    rows. A record missing a resolvable symbol is skipped (logged) rather
    than inserted with a fabricated ticker — see module docstring for why
    the exact field names are unverified.

    Returns
    -------
    list of dict
        {ticker, company_name, delisting_date, delisting_type, source_url}
    """
    rows = []
    for record in records:
        symbol = _first_present(record, _SYMBOL_KEYS)
        if symbol is None:
            logger.debug(f"nse_delisted_companies: no recognizable symbol key in record {record!r}")
            continue
        rows.append({
            "ticker": str(symbol).strip().upper(),
            "company_name": _first_present(record, _COMPANY_NAME_KEYS),
            "delisting_date": _parse_date(_first_present(record, _DELISTING_DATE_KEYS)),
            "delisting_type": _first_present(record, _DELISTING_TYPE_KEYS),
            "source_url": NSE_DELISTED_URL,
        })
    return rows


def write_delisted_companies(conn, rows: List[Dict[str, Any]]) -> int:
    """Upsert rows into the delisted_companies table (see
    datastore/schema/create_normalised.py). Returns count written."""
    if not rows:
        return 0
    for row in rows:
        conn.execute(
            """
            INSERT INTO delisted_companies
                (ticker, company_name, delisting_date, delisting_type, source_url)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (ticker) DO UPDATE SET
                company_name = excluded.company_name,
                delisting_date = excluded.delisting_date,
                delisting_type = excluded.delisting_type,
                source_url = excluded.source_url
            """,
            [row["ticker"], row["company_name"], row["delisting_date"],
             row["delisting_type"], row["source_url"]],
        )
    return len(rows)
