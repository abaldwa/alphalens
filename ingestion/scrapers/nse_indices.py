"""
ingestion/scrapers/nse_indices.py

Phase: FutureDevelopment #25/#30 (sector rotation + real backtest benchmark)
Owner: Platform / Ingestion
Consumers: ingestion/scheduler/daily_pipeline.py, scripts/backfill_index_ohlcv.py

Downloads NSE's daily indices-close archive ("ind_close_all"), the same
data NSE Indices Ltd. itself uses to publish index OHLC. Raw response bytes
are retained under datastore/raw/nse_indices/ for audit, mirroring
ingestion/scrapers/bhavcopy.py's raw-retention convention. Only a fixed
allowlist of tracked indices (Nifty 50/500 plus the sector indices this
project can map to its own sector taxonomy, see config/sector_index_map.py)
is kept in the parsed output; the rest of NSE's ~80-index CSV is discarded.
"""

import io
import logging
import time
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from config.settings import RAW_DIR

logger = logging.getLogger(__name__)

NSE_HOMEPAGE_URL = "https://www.nseindia.com"
NSE_INDICES_URL_TEMPLATE = (
    "https://archives.nseindia.com/content/indices/ind_close_all_{ddmmyyyy}.csv"
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2

# Indices this project can put to use: the two benchmark-level indices
# (#30) plus every sector index that config/sector_index_map.py can map to
# a sector value in config/nifty500_universe.csv (#25). Index names must
# match NSE's "Index Name" column exactly (case/spacing as NSE publishes
# them).
TRACKED_INDICES = [
    "Nifty 50",
    "Nifty 500",
    "Nifty Auto",
    "Nifty Bank",
    "Nifty IT",
    "Nifty FMCG",
    "Nifty Healthcare Index",
    "Nifty Metal",
    "Nifty Realty",
    "Nifty Energy",
    "Nifty PSE",
    "Nifty Financial Services",
    "Nifty Pharma",
    "Nifty Oil & Gas",
    "Nifty Media",
]

REQUIRED_COLUMNS = ["date", "index_name", "open", "high", "low", "close", "volume"]


def _nse_session() -> requests.Session:
    """Build a requests.Session carrying NSE's required browser-like headers
    and homepage cookies (NSE archives reject requests without them)."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    session.get(NSE_HOMEPAGE_URL, timeout=10)
    return session


def _fetch_indices_csv(trade_date: datetime) -> pd.DataFrame:
    """Fetch the raw ind_close_all CSV for one date, retrying on failure."""
    url = NSE_INDICES_URL_TEMPLATE.format(ddmmyyyy=trade_date.strftime("%d%m%Y"))

    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = _nse_session()
            response = session.get(url, timeout=15)
            response.raise_for_status()
            return pd.read_csv(io.StringIO(response.text))
        except (requests.RequestException, pd.errors.ParserError) as exc:
            last_exc = exc
            logger.warning(
                f"Indices-close fetch attempt {attempt}/{MAX_RETRIES} failed "
                f"for {trade_date.date()}: {exc}"
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)

    raise ConnectionError(
        f"Failed to download indices-close CSV for {trade_date.date()} "
        f"after {MAX_RETRIES} attempts: {last_exc}"
    )


def _save_raw(trade_date: datetime, raw: pd.DataFrame) -> None:
    """Persist the unmodified raw fetch to datastore/raw/nse_indices/ for audit."""
    raw_dir = RAW_DIR / "nse_indices"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw.to_csv(raw_dir / f"{trade_date.date().isoformat()}.csv", index=False)


def download_index_ohlcv(date: str) -> pd.DataFrame:
    """
    Download and parse NSE's indices-close CSV for one trading date,
    filtered to TRACKED_INDICES.

    Parameters
    ----------
    date : str
        Trading date, "YYYY-MM-DD".

    Returns
    -------
    pd.DataFrame
        Columns: date, index_name, open, high, low, close, volume.
        One row per index in TRACKED_INDICES that NSE published for this
        date (missing indices are simply absent, not raised as errors).

    Raises
    ------
    ConnectionError
        If the download fails after MAX_RETRIES attempts.
    """
    trade_date = datetime.strptime(date, "%Y-%m-%d")
    raw = _fetch_indices_csv(trade_date)
    _save_raw(trade_date, raw)

    raw.columns = [c.strip() for c in raw.columns]
    raw["Index Name"] = raw["Index Name"].str.strip()
    raw = raw[raw["Index Name"].isin(TRACKED_INDICES)].reset_index(drop=True)

    volume = raw["Volume"].replace("-", pd.NA) if "Volume" in raw.columns else pd.NA
    df = pd.DataFrame(
        {
            "date": trade_date.date().isoformat(),
            "index_name": raw["Index Name"],
            "open": pd.to_numeric(raw["Open Index Value"], errors="coerce"),
            "high": pd.to_numeric(raw["High Index Value"], errors="coerce"),
            "low": pd.to_numeric(raw["Low Index Value"], errors="coerce"),
            "close": pd.to_numeric(raw["Closing Index Value"], errors="coerce"),
            "volume": pd.to_numeric(volume, errors="coerce"),
        }
    )

    return df[REQUIRED_COLUMNS]
