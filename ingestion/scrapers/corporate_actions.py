"""
ingestion/scrapers/corporate_actions.py

Phase: 3 (Corporate Actions Ingestion)
Specs: SPEC-PIPE-002
Owner: Platform / Ingestion
Consumers: ingestion/scheduler/daily_pipeline.py, ingestion/adjust/price_adjuster.py,
    features/corporate_action_features.py

Downloads corporate actions from NSE's JSON API and persists them into the
corporate_actions DuckDB table. Covers all action types that NSE publishes:
SPLIT, BONUS, DIVIDEND, RIGHTS, BUYBACK, QIP, AGM, and anything else
bucketed as OTHER. Raw JSON is retained under datastore/raw/corporate_actions/
for audit.

IMPORTANT — price adjuster integration:
    ingestion/adjust/price_adjuster.py currently handles only SPLIT and BONUS.
    DIVIDEND rows are stored here so features/corporate_action_features.py can
    compute dividend yield, but they do NOT affect adj_factor until the price
    adjuster's logic for dividends is deliberated and agreed upon
    (PRICE_ADJUSTMENT_ENABLED=False in config/settings.py).

NSE API endpoint:
    https://www.nseindia.com/api/corporates-corporateActions?index=equities
    Optional date-range params: &from_date=DD-MM-YYYY&to_date=DD-MM-YYYY
    (NSE may or may not honour the date filter; response is filtered client-side
    by ex_date regardless.)

Response structure (as of 2026):
    {
      "data": [
        {
          "symbol":    "RELIANCE",
          "series":    "EQ",
          "subject":   "Interim Dividend",
          "exDate":    "25-JUN-2024",
          "recDate":   "25-JUN-2024",
          "bcStartDate": "",
          "purpose":   "INTERIM DIVIDEND - RS 10 PER SHARE"
        }, ...
      ]
    }

Date format from NSE: "DD-MMM-YYYY" (e.g. "25-JUN-2024"). Falls back to
"DD-MM-YYYY" and "YYYY-MM-DD" if the first parse fails.

ratio semantics (see also datastore/schema/create_normalised.py):
    SPLIT    — new shares per old share.  e.g. FV 10→2: ratio=5
    BONUS    — bonus shares per held share.  e.g. 1:1 bonus: ratio=1
    DIVIDEND — INR per share.  e.g. Rs.10 dividend: ratio=10.0
    RIGHTS   — rights shares per held share.  e.g. 1:5 rights: ratio=0.2
    Others   — 0.0 (no price-adjustment relevance)
"""

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import requests

from config.settings import NSE_CA_RATE_LIMIT_SLEEP_SECONDS, NSE_CA_RAW_DIR
from ingestion.scrapers.bhavcopy import NSE_HOMEPAGE_URL, USER_AGENT

logger = logging.getLogger(__name__)

NSE_CA_URL = "https://www.nseindia.com/api/corporates-corporateActions"

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2

# action_type values stored in corporate_actions
_ACTION_DIVIDEND = "DIVIDEND"
_ACTION_SPLIT = "SPLIT"
_ACTION_BONUS = "BONUS"
_ACTION_RIGHTS = "RIGHTS"
_ACTION_BUYBACK = "BUYBACK"
_ACTION_QIP = "QIP"
_ACTION_AGM = "AGM"
_ACTION_OTHER = "OTHER"

# EQ series only (matches bhavcopy.py EQ_SERIES filter)
_EQ_SERIES = {"EQ"}


def _nse_session() -> requests.Session:
    """Create an NSE session with browser headers and homepage cookies."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    session.get(NSE_HOMEPAGE_URL, timeout=10)
    return session


def _parse_nse_date(date_str: str) -> Optional[str]:
    """
    Parse an NSE date string to "YYYY-MM-DD".

    Handles: "25-JUN-2024" (DD-MMM-YYYY), "25-06-2024" (DD-MM-YYYY),
    "2024-06-25" (ISO). Returns None for blank / "-" / unparseable values.
    """
    if not date_str or date_str.strip() in ("-", ""):
        return None
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    logger.debug(f"Could not parse NSE date '{date_str}'")
    return None


def _parse_purpose(purpose: str, ticker: str) -> Tuple[str, float]:
    """
    Extract (action_type, ratio) from an NSE corporate-action purpose string.

    The purpose string is free text. Parsing is best-effort; unrecognised
    patterns fall back to ("OTHER", 0.0). The raw purpose is always stored
    in the `details` column for audit and re-parsing.

    Examples handled:
        "INTERIM DIVIDEND - RS 10 PER SHARE"          -> (DIVIDEND, 10.0)
        "FINAL DIVIDEND - RE 0.50 PER SHARE"          -> (DIVIDEND, 0.5)
        "STOCK SPLIT FROM RS.10/- TO RS.2/-"          -> (SPLIT, 5.0)
        "SUBDIVISION OF FACE VALUE FROM RS 10 TO RS 1" -> (SPLIT, 10.0)
        "BONUS 1:1"                                    -> (BONUS, 1.0)
        "BONUS ISSUE 1:2"                              -> (BONUS, 0.5)
        "RIGHTS ISSUE 1:5"                             -> (RIGHTS, 0.2)
        "BUY BACK OF SHARES"                           -> (BUYBACK, 0.0)
        "QIP"                                          -> (QIP, 0.0)
        "ANNUAL GENERAL MEETING"                       -> (AGM, 0.0)
    """
    p = purpose.upper().strip()

    # ----- AGM / EGM (check early — must not match as DIVIDEND/BONUS/etc.) -----
    if "GENERAL MEETING" in p or p.startswith("AGM") or p.startswith("EGM"):
        return _ACTION_AGM, 0.0

    # ----- BUYBACK -----
    if "BUY BACK" in p or "BUYBACK" in p:
        return _ACTION_BUYBACK, 0.0

    # ----- QIP -----
    if "QUALIFIED INSTITUTIONAL PLACEMENT" in p or re.search(r'\bQIP\b', p):
        return _ACTION_QIP, 0.0

    # ----- DIVIDEND -----
    if "DIVIDEND" in p:
        # e.g. "INTERIM DIVIDEND - RS 10 PER SHARE" / "DIVIDEND - RE 0.50/SHARE"
        m = re.search(r'(?:RS|RE|INR|₹)\s*\.?\s*(\d[\d,]*(?:\.\d+)?)', p)
        if m:
            raw_val = m.group(1).replace(",", "")
            try:
                return _ACTION_DIVIDEND, float(raw_val)
            except ValueError:
                pass
        logger.debug(f"{ticker}: DIVIDEND purpose but no parseable amount: '{purpose}'")
        return _ACTION_DIVIDEND, 0.0

    # ----- SPLIT (face-value subdivision) -----
    # Pattern A: "FROM RS.10/- TO RS.2/-" or "FROM RS 10 TO RS 2"
    fv_match = re.search(
        r'FROM\s+(?:RS|RE|INR)\.?\s*(\d+(?:\.\d+)?)/?\s*(?:/-\s*)?TO\s+(?:RS|RE|INR)\.?\s*(\d+(?:\.\d+)?)',
        p,
    )
    if fv_match and ("SPLIT" in p or "SUBDIVIS" in p or "FACE VALUE" in p or "F.?V.?" in p):
        old_fv = float(fv_match.group(1))
        new_fv = float(fv_match.group(2))
        if new_fv > 0 and old_fv > new_fv:
            return _ACTION_SPLIT, old_fv / new_fv

    # Pattern B: explicit ratio "SPLIT 1:10" or "STOCK SPLIT RATIO 1:10"
    if "SPLIT" in p:
        m = re.search(r'(\d+)\s*:\s*(\d+)', p)
        if m:
            # In Indian CA language "SPLIT 1:10" can mean face value changes
            # from Rs.10 to Rs.1 (ratio = 10 new shares per old). We store the
            # larger / smaller depending on which direction makes sense.
            a, b = int(m.group(1)), int(m.group(2))
            # If first < second, treat second as new shares per old (more common)
            ratio = max(a, b) / min(a, b) if min(a, b) > 0 else float(a)
            return _ACTION_SPLIT, ratio
        logger.debug(f"{ticker}: SPLIT purpose but no ratio found: '{purpose}'")
        return _ACTION_SPLIT, 0.0

    # ----- BONUS -----
    if "BONUS" in p:
        m = re.search(r'(\d+)\s*:\s*(\d+)', p)
        if m:
            bonus = int(m.group(1))
            held = int(m.group(2))
            ratio = bonus / held if held > 0 else 0.0
            return _ACTION_BONUS, ratio
        logger.debug(f"{ticker}: BONUS purpose but no ratio found: '{purpose}'")
        return _ACTION_BONUS, 0.0

    # ----- RIGHTS -----
    if "RIGHT" in p:
        m = re.search(r'(\d+)\s*:\s*(\d+)', p)
        if m:
            rights = int(m.group(1))
            held = int(m.group(2))
            ratio = rights / held if held > 0 else 0.0
            return _ACTION_RIGHTS, ratio
        logger.debug(f"{ticker}: RIGHTS purpose but no ratio found: '{purpose}'")
        return _ACTION_RIGHTS, 0.0

    return _ACTION_OTHER, 0.0


def _fetch_corporate_actions_json(target_date: str) -> List[dict]:
    """
    Fetch corporate actions from NSE's JSON API.

    Parameters
    ----------
    target_date : str
        "YYYY-MM-DD" — used as both the from/to date filter sent to NSE
        (best-effort; NSE may not honour it) and for client-side ex_date
        filtering in download_corporate_actions().

    Returns
    -------
    list of dict
        Raw NSE corporate-action records for all equity-series events.

    Raises
    ------
    ConnectionError
        If the fetch fails after MAX_RETRIES attempts.
    """
    # NSE expects DD-MM-YYYY for its date filter
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    nse_date = dt.strftime("%d-%m-%Y")

    params = {
        "index": "equities",
        "from_date": nse_date,
        "to_date": nse_date,
    }

    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = _nse_session()
            response = session.get(NSE_CA_URL, params=params, timeout=15)
            response.raise_for_status()
            payload = response.json()
            # NSE may return a list directly or wrap it in {"data": [...]}
            if isinstance(payload, list):
                return payload
            if isinstance(payload, dict):
                return payload.get("data", [])
            return []
        except Exception as exc:
            last_exc = exc
            logger.warning(
                f"NSE corporate actions fetch attempt {attempt}/{MAX_RETRIES} "
                f"failed for {target_date}: {exc}"
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)

    raise ConnectionError(
        f"Failed to download NSE corporate actions for {target_date} "
        f"after {MAX_RETRIES} attempts: {last_exc}"
    )


def _save_raw(target_date: str, records: List[dict]) -> None:
    """Persist raw NSE JSON to datastore/raw/corporate_actions/ for audit."""
    raw_dir: Path = NSE_CA_RAW_DIR
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"{target_date}.json"
    with open(path, "w") as f:
        json.dump(records, f, indent=2)


def download_corporate_actions(date: str, filter_by_date: bool = True) -> pd.DataFrame:
    """
    Download and parse NSE corporate actions for one trading date.

    Parameters
    ----------
    date : str
        Trading date, "YYYY-MM-DD".
    filter_by_date : bool
        If True (default), only return rows whose ex_date == date. If False,
        return all records the API returns (useful for backfill / inspection).

    Returns
    -------
    pd.DataFrame
        Columns: ticker, ex_date, action_type, ratio, announcement_date,
        record_date, details. EQ series only; AGM/EGM rows are included
        (stored as action_type=AGM) so the complete corporate event history
        is preserved even though they have no price-adjustment effect.

    Spec References
    ----------------
    SPEC-PIPE-002: corporate actions ledger.

    PIT Assumptions
    ----------------
    ex_date is the operative date for price adjustments. announcement_date
    (not available directly from the CA endpoint — set to None here) is the
    PIT key for features/corporate_action_features.py.

    Raises
    ------
    ConnectionError
        If the NSE API is unreachable after MAX_RETRIES.
    """
    records = _fetch_corporate_actions_json(date)
    _save_raw(date, records)

    rows = []
    for rec in records:
        # Normalise field names: NSE may use camelCase or UPPER_CASE
        symbol = (rec.get("symbol") or rec.get("SYMBOL") or "").strip()
        series = (rec.get("series") or rec.get("SERIES") or "").strip().upper()
        ex_date_raw = rec.get("exDate") or rec.get("EX_DATE") or ""
        rec_date_raw = rec.get("recDate") or rec.get("REC_DATE") or ""
        purpose = (rec.get("purpose") or rec.get("PURPOSE") or rec.get("subject") or "").strip()

        if not symbol or series not in _EQ_SERIES:
            continue

        ex_date = _parse_nse_date(ex_date_raw)
        if not ex_date:
            continue

        if filter_by_date and ex_date != date:
            continue

        record_date = _parse_nse_date(rec_date_raw)
        action_type, ratio = _parse_purpose(purpose, symbol)

        rows.append({
            "ticker": symbol,
            "ex_date": ex_date,
            "action_type": action_type,
            "ratio": ratio,
            "announcement_date": None,  # not exposed by the CA endpoint
            "record_date": record_date,
            "details": purpose or None,
        })

    df = pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["ticker", "ex_date", "action_type", "ratio",
                 "announcement_date", "record_date", "details"]
    )

    logger.info(
        f"download_corporate_actions: {len(df)} EQ-series records "
        f"(ex_date={date}, filter_by_date={filter_by_date}) "
        f"from {len(records)} total NSE records"
    )
    time.sleep(NSE_CA_RATE_LIMIT_SLEEP_SECONDS)
    return df


def upsert_corporate_actions(conn, df: pd.DataFrame) -> int:
    """
    Upsert corporate actions rows into the DuckDB corporate_actions table.

    Uses INSERT … ON CONFLICT DO NOTHING so that re-running for the same
    date is a no-op for existing rows (SPEC-PIPE-002: idempotent). The
    primary key is (ticker, ex_date, action_type) — if the same action
    needs to be corrected, delete the old row first.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
    df : pd.DataFrame
        Output of download_corporate_actions().

    Returns
    -------
    int
        Number of rows inserted (0 if all already existed).
    """
    if df.empty:
        return 0

    conn.register("_ca_upsert_staging", df)
    try:
        result = conn.execute(
            """
            INSERT INTO corporate_actions
                (ticker, ex_date, action_type, ratio,
                 announcement_date, record_date, details)
            SELECT ticker, CAST(ex_date AS DATE), action_type, ratio,
                   CAST(announcement_date AS DATE),
                   CAST(record_date AS DATE), details
            FROM _ca_upsert_staging
            ON CONFLICT (ticker, ex_date, action_type) DO NOTHING
            """
        )
        inserted = result.fetchone()
        count = inserted[0] if inserted else 0
    finally:
        conn.unregister("_ca_upsert_staging")

    logger.info(f"upsert_corporate_actions: {count} new rows inserted (of {len(df)} staged)")
    return count
