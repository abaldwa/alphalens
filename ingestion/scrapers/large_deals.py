"""
ingestion/scrapers/large_deals.py

Phase: 3 (Large Deals Ingestion)
Owner: Platform / Ingestion
Consumers: ingestion/scheduler/daily_pipeline.py

Downloads Bulk Deals and Block Deals from NSE and BSE for a given trading
date, and persists them into the `large_deals` DuckDB table.

Definitions:
    Bulk Deal  — a single transaction in which ≥ 0.5% of the total shares
                 listed of a company are traded (SEBI circular).
    Block Deal — a single transaction of ≥ 500,000 shares or ≥ Rs. 10 crore
                 executed in the Block Deal window (9:15–9:30 AM).

NSE endpoints used (JSON API, requires session cookies):
    Bulk deals:
        https://www.nseindia.com/api/snapshot-capital-market-bulk-deals
    Block deals:
        https://www.nseindia.com/api/snapshot-capital-market-block-deals
    Historical (date-specific):
        https://www.nseindia.com/api/historical/bulk-deals?from=DD-MM-YYYY&to=DD-MM-YYYY
        https://www.nseindia.com/api/historical/block-deals?from=DD-MM-YYYY&to=DD-MM-YYYY

BSE endpoint used (open JSON API, no session cookies needed):
    Bulk deals:
        https://api.bseindia.com/BseIndiaAPI/api/BulkDeals/w?strdate=DDMMYYYY&enddate=DDMMYYYY
    Block deals:
        https://api.bseindia.com/BseIndiaAPI/api/BlockDeals/w?strdate=DDMMYYYY&enddate=DDMMYYYY

NSE response structure (snapshot endpoint, as of 2026):
    {
      "data": [
        {
          "name":      "COMPANY NAME",
          "no":        "RELIANCE",     // NSE symbol
          "dt":        "25-JUN-2024",  // trade date
          "pd":        "CLIENT NAME",  // participant / client
          "bs":        "BUY",          // or "SELL"
          "qt":        1234567,        // quantity
          "vl":        2850.50,        // trade price
          "remarks":   ""
        }
      ]
    }
    Historical endpoint returns the same structure nested under data.bulkDealData
    or data.blockDealData — both patterns are handled.

BSE response structure:
    {
      "Table": [
        {
          "DT_DATE":   "20240625",   // YYYYMMDD
          "SC_CODE":   "500325",     // BSE scrip code
          "SC_NAME":   "RELIANCE",   // company name (not necessarily NSE symbol)
          "SCRIP_ID":  "RELIANCE",   // often matches NSE symbol
          "CLNT_NAME": "CLIENT",
          "BUY_SELL":  "B",          // "B" or "S"
          "DEAL_QTY":  1000000,
          "DEAL_PRICE": 2850.50
        }
      ]
    }

The `ticker` column in large_deals is populated from:
    NSE: `no` field (NSE equity symbol, same as ohlcv_adjusted.ticker)
    BSE: `SCRIP_ID` field (usually matches NSE symbol; may differ for some scrips)

Raw JSON responses are saved to datastore/raw/large_deals/{date}_{exchange}_{type}.json
for audit.
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests

from config.settings import LARGE_DEALS_RATE_LIMIT_SLEEP_SECONDS, LARGE_DEALS_RAW_DIR
from ingestion.scrapers.bhavcopy import NSE_HOMEPAGE_URL, USER_AGENT

logger = logging.getLogger(__name__)

NSE_BULK_DEALS_SNAPSHOT = "https://www.nseindia.com/api/snapshot-capital-market-bulk-deals"
NSE_BLOCK_DEALS_SNAPSHOT = "https://www.nseindia.com/api/snapshot-capital-market-block-deals"
NSE_BULK_DEALS_HISTORY = "https://www.nseindia.com/api/historical/bulk-deals"
NSE_BLOCK_DEALS_HISTORY = "https://www.nseindia.com/api/historical/block-deals"

BSE_BULK_DEALS_URL = "https://api.bseindia.com/BseIndiaAPI/api/BulkDeals/w"
BSE_BLOCK_DEALS_URL = "https://api.bseindia.com/BseIndiaAPI/api/BlockDeals/w"

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2

EXCHANGE_NSE = "NSE"
EXCHANGE_BSE = "BSE"
DEAL_TYPE_BULK = "BULK"
DEAL_TYPE_BLOCK = "BLOCK"

_REQUIRED_COLUMNS = [
    "trade_date", "exchange", "deal_type", "ticker",
    "client_name", "transaction_type", "quantity", "price", "remarks",
]


def _nse_session() -> requests.Session:
    """Create an NSE session with browser headers and homepage cookies."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    session.get(NSE_HOMEPAGE_URL, timeout=10)
    return session


def _bse_session() -> requests.Session:
    """Create a BSE session with browser headers (no cookie priming required)."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Referer": "https://www.bseindia.com/",
        "Origin": "https://www.bseindia.com",
    })
    return session


def _save_raw(target_date: str, exchange: str, deal_type: str, payload: object) -> None:
    """Save raw response JSON for audit."""
    raw_dir: Path = LARGE_DEALS_RAW_DIR
    raw_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{target_date}_{exchange}_{deal_type}.json"
    with open(raw_dir / fname, "w") as f:
        json.dump(payload, f, indent=2)


def _parse_nse_date(date_str: str) -> Optional[str]:
    """Parse NSE date string to YYYY-MM-DD. Handles DD-MMM-YYYY, DD-MM-YYYY, YYYY-MM-DD."""
    if not date_str or str(date_str).strip() in ("-", ""):
        return None
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(date_str).strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _normalise_transaction_type(raw: str) -> str:
    """Normalise buy/sell flag to 'B' or 'S'."""
    v = str(raw).strip().upper()
    if v in ("B", "BUY", "PURCHASE"):
        return "B"
    if v in ("S", "SELL", "SALE"):
        return "S"
    return v[:1] if v else ""


# ---------------------------------------------------------------------------
# NSE fetchers
# ---------------------------------------------------------------------------

def _fetch_nse_deals(target_date: str, deal_type: str) -> List[dict]:
    """
    Fetch bulk or block deals from NSE for target_date.

    Tries the historical endpoint first (date-specific). Falls back to the
    snapshot endpoint (today's live data) if the historical call fails — the
    snapshot only carries today's data, so the fallback is only useful when
    target_date == today.

    Parameters
    ----------
    target_date : str
        "YYYY-MM-DD"
    deal_type : str
        "BULK" or "BLOCK"

    Returns
    -------
    list of dict
        Raw NSE records.

    Raises
    ------
    ConnectionError
        If both endpoints fail after MAX_RETRIES.
    """
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    nse_date = dt.strftime("%d-%m-%Y")

    history_url = NSE_BULK_DEALS_HISTORY if deal_type == DEAL_TYPE_BULK else NSE_BLOCK_DEALS_HISTORY
    snapshot_url = NSE_BULK_DEALS_SNAPSHOT if deal_type == DEAL_TYPE_BULK else NSE_BLOCK_DEALS_SNAPSHOT

    last_exc: Optional[Exception] = None

    # --- Try historical endpoint first ---
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = _nse_session()
            resp = session.get(
                history_url,
                params={"from": nse_date, "to": nse_date},
                timeout=15,
            )
            resp.raise_for_status()
            payload = resp.json()
            # Historical endpoint wraps results under data.bulkDealData / data.blockDealData
            if isinstance(payload, list):
                return payload
            if isinstance(payload, dict):
                data = payload.get("data", payload)
                if isinstance(data, list):
                    return data
                # Try nested keys
                for key in ("bulkDealData", "blockDealData", "bulkDeals", "blockDeals"):
                    if key in data and isinstance(data[key], list):
                        return data[key]
                return list(data.values())[0] if data else []
            return []
        except Exception as exc:
            last_exc = exc
            logger.debug(
                f"NSE {deal_type} historical attempt {attempt}/{MAX_RETRIES} "
                f"for {target_date}: {exc}"
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)

    # --- Fall back to snapshot (useful only if target_date == today) ---
    logger.warning(
        f"NSE {deal_type} historical endpoint failed for {target_date} "
        f"({last_exc}) — trying snapshot endpoint"
    )
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = _nse_session()
            resp = session.get(snapshot_url, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
            if isinstance(payload, list):
                return payload
            if isinstance(payload, dict):
                data = payload.get("data", payload)
                if isinstance(data, list):
                    return data
                for key in ("bulkDealData", "blockDealData", "bulkDeals", "blockDeals"):
                    if key in data and isinstance(data[key], list):
                        return data[key]
                return list(data.values())[0] if data else []
            return []
        except Exception as exc:
            last_exc = exc
            logger.warning(
                f"NSE {deal_type} snapshot attempt {attempt}/{MAX_RETRIES} "
                f"for {target_date}: {exc}"
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)

    raise ConnectionError(
        f"NSE {deal_type} deals unavailable for {target_date} "
        f"after {MAX_RETRIES * 2} total attempts: {last_exc}"
    )


def _parse_nse_records(records: List[dict], target_date: str, deal_type: str) -> pd.DataFrame:
    """
    Parse raw NSE bulk/block deal records into the large_deals schema.

    NSE field mapping (multiple naming conventions observed):
        ticker          : no, SCRIP_CD, scrip_code, symbol
        trade_date      : dt, TRADE_DT, date
        client_name     : pd, CLIENT_NAME, client
        transaction_type: bs, BUY_SELL, buySell
        quantity        : qt, QTY_TRD, qty
        price           : vl, TRADE_PRICE, price
        remarks         : remarks, REMARKS
    """
    rows = []
    for rec in records:
        def g(*keys):
            for k in keys:
                v = rec.get(k) or rec.get(k.upper()) or rec.get(k.lower())
                if v is not None and str(v).strip() not in ("", "-", "null", "None"):
                    return v
            return None

        ticker = g("no", "scrip_code", "symbol", "SCRIP_CD", "SYMBOL_CODE")
        dt_raw = g("dt", "TRADE_DT", "date", "DATE")
        client = g("pd", "CLIENT_NAME", "client", "CLIENT")
        bs = g("bs", "BUY_SELL", "buySell", "BUY_SELL_FLAG")
        qty = g("qt", "QTY_TRD", "qty", "QUANTITY", "QTY")
        price = g("vl", "TRADE_PRICE", "price", "PRICE", "VALUE")
        remarks = g("remarks", "REMARKS")

        if not ticker:
            continue

        parsed_date = _parse_nse_date(dt_raw) if dt_raw else target_date

        rows.append({
            "trade_date": parsed_date or target_date,
            "exchange": EXCHANGE_NSE,
            "deal_type": deal_type,
            "ticker": str(ticker).strip().upper(),
            "client_name": str(client).strip() if client else None,
            "transaction_type": _normalise_transaction_type(str(bs)) if bs else None,
            "quantity": int(float(str(qty).replace(",", ""))) if qty else None,
            "price": float(str(price).replace(",", "")) if price else None,
            "remarks": str(remarks).strip() if remarks else None,
        })

    return pd.DataFrame(rows, columns=_REQUIRED_COLUMNS) if rows else pd.DataFrame(columns=_REQUIRED_COLUMNS)


# ---------------------------------------------------------------------------
# BSE fetchers
# ---------------------------------------------------------------------------

def _fetch_bse_deals(target_date: str, deal_type: str) -> List[dict]:
    """
    Fetch bulk or block deals from BSE open API.

    BSE date format for the query params: DDMMYYYY (no separators).

    Returns
    -------
    list of dict
        Raw BSE records from the "Table" key of the JSON response.

    Raises
    ------
    ConnectionError
        After MAX_RETRIES failures.
    """
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    bse_date = dt.strftime("%d%m%Y")

    url = BSE_BULK_DEALS_URL if deal_type == DEAL_TYPE_BULK else BSE_BLOCK_DEALS_URL
    params = {"strdate": bse_date, "enddate": bse_date}

    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = _bse_session()
            resp = session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
            # BSE wraps the list under "Table"
            if isinstance(payload, list):
                return payload
            if isinstance(payload, dict):
                for key in ("Table", "table", "data", "Data"):
                    if key in payload and isinstance(payload[key], list):
                        return payload[key]
            return []
        except Exception as exc:
            last_exc = exc
            logger.warning(
                f"BSE {deal_type} attempt {attempt}/{MAX_RETRIES} "
                f"for {target_date}: {exc}"
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)

    raise ConnectionError(
        f"BSE {deal_type} deals unavailable for {target_date} "
        f"after {MAX_RETRIES} attempts: {last_exc}"
    )


def _parse_bse_date(raw: str) -> Optional[str]:
    """Parse BSE date formats: YYYYMMDD, DD-MM-YYYY, DD/MM/YYYY, DD-MMM-YYYY."""
    if not raw or str(raw).strip() in ("-", ""):
        return None
    raw = str(raw).strip()
    for fmt in ("%Y%m%d", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_bse_records(records: List[dict], target_date: str, deal_type: str) -> pd.DataFrame:
    """
    Parse raw BSE bulk/block deal records into the large_deals schema.

    BSE field mapping:
        ticker          : SCRIP_ID, SC_NAME (best-effort NSE-symbol approximation)
        trade_date      : DT_DATE
        client_name     : CLNT_NAME
        transaction_type: BUY_SELL ("B" or "S")
        quantity        : DEAL_QTY
        price           : DEAL_PRICE
        remarks         : REMARKS (if present)

    Note: BSE's SCRIP_ID usually matches the NSE symbol but may differ for
    some securities. SC_NAME is the company name (never an NSE symbol).
    """
    rows = []
    for rec in records:
        dt_raw = rec.get("DT_DATE") or rec.get("dt_date") or rec.get("DATE") or ""
        ticker = (rec.get("SCRIP_ID") or rec.get("scrip_id") or
                  rec.get("SC_CODE") or rec.get("SC_NAME") or "")
        client = rec.get("CLNT_NAME") or rec.get("clnt_name") or rec.get("CLIENT_NAME") or ""
        bs = rec.get("BUY_SELL") or rec.get("buy_sell") or ""
        qty = rec.get("DEAL_QTY") or rec.get("deal_qty") or rec.get("QTY") or None
        price = rec.get("DEAL_PRICE") or rec.get("deal_price") or rec.get("PRICE") or None
        remarks = rec.get("REMARKS") or rec.get("remarks") or None

        if not ticker:
            continue

        parsed_date = _parse_bse_date(str(dt_raw)) if dt_raw else target_date

        rows.append({
            "trade_date": parsed_date or target_date,
            "exchange": EXCHANGE_BSE,
            "deal_type": deal_type,
            "ticker": str(ticker).strip().upper(),
            "client_name": str(client).strip() if client else None,
            "transaction_type": _normalise_transaction_type(str(bs)) if bs else None,
            "quantity": int(float(str(qty).replace(",", ""))) if qty else None,
            "price": float(str(price).replace(",", "")) if price else None,
            "remarks": str(remarks).strip() if remarks else None,
        })

    return pd.DataFrame(rows, columns=_REQUIRED_COLUMNS) if rows else pd.DataFrame(columns=_REQUIRED_COLUMNS)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def download_large_deals(date: str) -> pd.DataFrame:
    """
    Download bulk and block deals from both NSE and BSE for one trading date.

    Each of the four sources (NSE bulk, NSE block, BSE bulk, BSE block) is
    fetched independently. A failure from any single source is caught, logged,
    and skipped — the others still contribute rows. This mirrors the
    SPEC-PIPE-006 "mark unavailable, non-critical" philosophy applied to macro
    indicators: a BSE outage must not block NSE deals, and vice versa.

    Parameters
    ----------
    date : str
        Trading date, "YYYY-MM-DD".

    Returns
    -------
    pd.DataFrame
        Combined large_deals rows (all four sources) with columns:
        trade_date, exchange, deal_type, ticker, client_name,
        transaction_type, quantity, price, remarks.
        Empty DataFrame if all four sources fail.

    Raises
    ------
    None — failures per source are caught and logged.
    """
    frames = []

    sources = [
        (EXCHANGE_NSE, DEAL_TYPE_BULK, _fetch_nse_deals, _parse_nse_records),
        (EXCHANGE_NSE, DEAL_TYPE_BLOCK, _fetch_nse_deals, _parse_nse_records),
        (EXCHANGE_BSE, DEAL_TYPE_BULK, _fetch_bse_deals, _parse_bse_records),
        (EXCHANGE_BSE, DEAL_TYPE_BLOCK, _fetch_bse_deals, _parse_bse_records),
    ]

    for exchange, deal_type, fetcher, parser in sources:
        try:
            raw = fetcher(date, deal_type)
            _save_raw(date, exchange, deal_type, raw)
            df = parser(raw, date, deal_type)
            frames.append(df)
            logger.info(f"large_deals {exchange} {deal_type}: {len(df)} rows for {date}")
            time.sleep(LARGE_DEALS_RATE_LIMIT_SLEEP_SECONDS)
        except Exception as exc:
            logger.warning(
                f"large_deals {exchange} {deal_type}: unavailable for {date} "
                f"({exc}) — non-critical, skipping"
            )

    if not frames:
        logger.warning(f"large_deals: all four sources failed for {date}")
        return pd.DataFrame(columns=_REQUIRED_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"large_deals: {len(combined)} total rows for {date} (NSE+BSE bulk+block)")
    return combined


def persist_large_deals(conn, df: pd.DataFrame, trade_date: str) -> int:
    """
    Delete existing large_deals rows for trade_date and insert the new set.

    Uses the same delete-then-insert pattern as fno_data (no PRIMARY KEY,
    daily snapshot replaces the day's records atomically).

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
    df : pd.DataFrame
        Output of download_large_deals().
    trade_date : str
        "YYYY-MM-DD" — the day being replaced.

    Returns
    -------
    int
        Number of rows inserted.
    """
    conn.execute("DELETE FROM large_deals WHERE trade_date = ?", [trade_date])

    if df.empty:
        logger.info(f"persist_large_deals: no rows to insert for {trade_date}")
        return 0

    conn.register("_large_deals_staging", df)
    try:
        conn.execute(
            """
            INSERT INTO large_deals
                (trade_date, exchange, deal_type, ticker,
                 client_name, transaction_type, quantity, price, remarks)
            SELECT
                CAST(trade_date AS DATE), exchange, deal_type, ticker,
                client_name, transaction_type,
                CAST(quantity AS BIGINT), CAST(price AS DOUBLE), remarks
            FROM _large_deals_staging
            """
        )
    finally:
        conn.unregister("_large_deals_staging")

    logger.info(f"persist_large_deals: {len(df)} rows written for {trade_date}")
    return len(df)
