"""
ingestion/scrapers/fno.py

Phase: 0.4 (Data Ingestion Scrapers)
Specs: SPEC-PIPE-001
Owner: Platform / Ingestion
Consumers: ingestion/scheduler, features/fno_features, datastore/raw

Downloads the daily NSE F&O (futures and options) bhavcopy. Stores open
interest, volume, and settlement price keyed by ticker/expiry/strike/
option_type, for every instrument type (futures and options, index and
stock). Raw response retained under datastore/raw/fno/ for audit
(SPEC-PIPE-001).
"""

import io
import logging
import time
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from config.settings import RAW_DIR
from ingestion.scrapers.bhavcopy import NSE_HOMEPAGE_URL, USER_AGENT

logger = logging.getLogger(__name__)

NSE_FNO_BHAVCOPY_URL_TEMPLATE = (
    "https://archives.nseindia.com/content/historical/DERIVATIVES/"
    "{year}/{mon}/fo{ddmmyyyy}bhav.csv.zip"
)

REQUIRED_COLUMNS = [
    "ticker", "instrument", "expiry", "strike", "option_type",
    "oi", "volume", "settle_price",
]

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2


def _fno_session() -> requests.Session:
    """Build a requests.Session with NSE's required headers/cookies."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    session.get(NSE_HOMEPAGE_URL, timeout=10)
    return session


def _fetch_fno_bhavcopy_csv(trade_date: datetime) -> pd.DataFrame:
    """
    Fetch the raw F&O bhavcopy CSV (from inside the NSE zip) for one date.

    Parameters
    ----------
    trade_date : datetime

    Returns
    -------
    pd.DataFrame
        Raw NSE columns, unmodified.

    Spec References
    ----------------
    SPEC-PIPE-001

    PIT Assumptions
    ----------------
    None — same-day archive data.

    Raises
    ------
    ConnectionError
        After MAX_RETRIES failed attempts.
    """
    import zipfile

    url = NSE_FNO_BHAVCOPY_URL_TEMPLATE.format(
        year=trade_date.year,
        mon=trade_date.strftime("%b").upper(),
        ddmmyyyy=trade_date.strftime("%d%m%Y"),
    )

    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = _fno_session()
            response = session.get(url, timeout=15)
            response.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
                with zf.open(csv_name) as fh:
                    return pd.read_csv(fh)
        except (requests.RequestException, zipfile.BadZipFile, pd.errors.ParserError) as exc:
            last_exc = exc
            logger.warning(
                f"F&O bhavcopy fetch attempt {attempt}/{MAX_RETRIES} failed "
                f"for {trade_date.date()}: {exc}"
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)

    raise ConnectionError(
        f"Failed to download F&O bhavcopy for {trade_date.date()} "
        f"after {MAX_RETRIES} attempts: {last_exc}"
    )


def _save_raw(trade_date: datetime, raw: pd.DataFrame) -> None:
    """Persist the unmodified raw fetch to datastore/raw/fno/ (SPEC-PIPE-001)."""
    raw_dir = RAW_DIR / "fno"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw.to_csv(raw_dir / f"{trade_date.date().isoformat()}.csv", index=False)


def download_fno_bhavcopy(date: str) -> pd.DataFrame:
    """
    Download and parse the NSE F&O bhavcopy for one trading date.

    Parameters
    ----------
    date : str
        Trading date, "YYYY-MM-DD".

    Returns
    -------
    pd.DataFrame
        Columns: ticker, instrument, expiry, strike, option_type, oi,
        volume, settle_price. One row per ticker/expiry/strike/option_type
        combination (futures rows have strike=NaN, option_type=None).

    Spec References
    ----------------
    SPEC-PIPE-001

    PIT Assumptions
    ----------------
    None — same-day archive data.

    Raises
    ------
    ConnectionError
        If the download fails after MAX_RETRIES attempts.
    """
    trade_date = datetime.strptime(date, "%Y-%m-%d")
    raw = _fetch_fno_bhavcopy_csv(trade_date)
    _save_raw(trade_date, raw)

    raw.columns = [c.strip().upper() for c in raw.columns]
    for col in ("SYMBOL", "INSTRUMENT", "OPTION_TYP"):
        if col in raw.columns and raw[col].dtype == object:
            raw[col] = raw[col].str.strip()

    option_type = raw["OPTION_TYP"].replace({"XX": None})
    strike = pd.to_numeric(raw["STRIKE_PR"], errors="coerce")
    strike = strike.where(option_type.notna())  # futures: no strike

    df = pd.DataFrame(
        {
            "ticker": raw["SYMBOL"],
            "instrument": raw["INSTRUMENT"],
            "expiry": pd.to_datetime(raw["EXPIRY_DT"], errors="coerce", dayfirst=True),
            "strike": strike,
            "option_type": option_type,
            "oi": pd.to_numeric(raw["OPEN_INT"], errors="coerce"),
            "volume": pd.to_numeric(raw["CONTRACTS"], errors="coerce"),
            "settle_price": pd.to_numeric(raw["SETTLE_PR"], errors="coerce"),
        }
    )

    logger.info(f"F&O bhavcopy downloaded for {date}: {len(df)} contracts")
    return df[REQUIRED_COLUMNS]
