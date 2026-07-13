"""
ingestion/scrapers/etf_list.py

Phase: 0.4 (Data Ingestion Scrapers)
Owner: Platform / Ingestion
Consumers: ingestion/scrapers/bhavcopy.py

Downloads NSE's official daily list of ETF-segment symbols
(nseindia.com/api/etf — the JSON feed behind the "Exchange Traded Funds"
market-watch page). Used to exclude ETFs from the bhavcopy before it is
written into ohlcv_adjusted: ETFs trade under the same EQ series as
equities, so series alone cannot separate them, but they carry no
fundamentals/shareholding/corporate-actions and must never enter the
equity universe or participate in strategies.

Raw response retained under datastore/raw/etf_list/ for audit, same
convention as bhavcopy.py's datastore/raw/bhavcopy/.
"""

import json
import logging
from datetime import datetime
from typing import Optional

import requests

from config.settings import RAW_DIR
from ingestion.scrapers._retry import RETRY_DELAY_SECONDS, retry_call

logger = logging.getLogger(__name__)

NSE_HOMEPAGE_URL = "https://www.nseindia.com"
NSE_ETF_LIST_URL = "https://www.nseindia.com/api/etf"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

MAX_RETRIES = 3


def _nse_session() -> requests.Session:
    """Browser-like session with NSE homepage cookies primed (required by nseindia.com APIs)."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Referer": "https://www.nseindia.com/market-data/exchange-traded-funds-etf",
    })
    session.get(NSE_HOMEPAGE_URL, timeout=10)
    return session


def _save_raw(trade_date: datetime, payload: dict) -> None:
    """Persist the unmodified raw JSON response to datastore/raw/etf_list/ (audit trail)."""
    raw_dir = RAW_DIR / "etf_list"
    raw_dir.mkdir(parents=True, exist_ok=True)
    with open(raw_dir / f"{trade_date.date().isoformat()}.json", "w") as f:
        json.dump(payload, f)


def download_etf_list(date: str) -> set[str]:
    """
    Download NSE's current ETF-segment symbol list.

    Parameters
    ----------
    date : str
        "YYYY-MM-DD" — used only to name the raw-audit file; NSE's /api/etf
        feed is a live snapshot, not a per-date archive (there is no
        historical endpoint for past ETF-list snapshots).

    Returns
    -------
    set[str]
        Upper-cased, stripped ETF ticker symbols currently listed on NSE.

    Raises
    ------
    ConnectionError
        If the fetch fails after MAX_RETRIES attempts, or the response
        doesn't contain the expected "data" list.
    """
    trade_date = datetime.strptime(date, "%Y-%m-%d")

    def _fetch() -> set[str]:
        session = _nse_session()
        response = session.get(NSE_ETF_LIST_URL, timeout=15)
        response.raise_for_status()
        payload = response.json()
        rows = payload["data"]
        tickers = {
            str(row["symbol"]).strip().upper()
            for row in rows
            if row.get("symbol")
        }
        if not tickers:
            raise ValueError("ETF list response contained no symbols")

        _save_raw(trade_date, payload)
        logger.info(f"ETF list downloaded for {date}: {len(tickers)} symbols")
        return tickers

    try:
        return retry_call(
            _fetch,
            retries=MAX_RETRIES,
            label=f"ETF list fetch for {date}",
            wait_seconds=RETRY_DELAY_SECONDS,
            exceptions=(requests.RequestException, ValueError, KeyError),
        )
    except ConnectionError as exc:
        raise ConnectionError(
            f"Failed to download ETF list for {date} after {MAX_RETRIES} attempts: {exc}"
        ) from exc


def load_last_cached_etf_list() -> Optional[set[str]]:
    """
    Load the most recent previously-saved raw ETF list from
    datastore/raw/etf_list/, for use when today's live download fails.

    Returns
    -------
    set[str] or None
        None if no raw ETF-list file has ever been saved.
    """
    raw_dir = RAW_DIR / "etf_list"
    if not raw_dir.exists():
        return None

    files = sorted(raw_dir.glob("*.json"), reverse=True)
    if not files:
        return None

    with open(files[0]) as f:
        payload = json.load(f)

    tickers = {
        str(row["symbol"]).strip().upper()
        for row in payload.get("data", [])
        if row.get("symbol")
    }
    return tickers or None
