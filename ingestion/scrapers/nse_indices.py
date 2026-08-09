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
from datetime import datetime

import pandas as pd
import requests

from config.settings import RAW_DIR
from ingestion.scrapers._retry import RETRY_DELAY_SECONDS, retry_call

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

# Indices this project can put to use: the two benchmark-level indices
# (#30) plus every sector index that config/sector_index_map.py can map to
# a sector value in config/nifty500_universe.csv (#25). Index names must
# match NSE's "Index Name" column exactly (case/spacing as NSE publishes
# them).
TRACKED_INDICES = [
    "Nifty 50",
    # [2026-08-09] "Nifty 100" was missing from this list, so the daily
    # ind_close_all filter silently dropped it every single day and
    # index_ohlcv had ZERO Nifty 100 rows. features/technical.py's
    # rs_vs_nifty100_21d therefore fell back to the NIF100BEES ETF proxy,
    # which only lists from 2015-01-01 — leaving that feature empty for
    # 2006-2015 with no error anywhere. Adding it here is what keeps
    # FUTURE dates populated; the 2006-2026 history itself came from
    # NSE's own historical PR CSVs (scripts/ingest_index_csv.py).
    "Nifty 100",
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

# 2026-07-20: NSE renamed every CNX-prefixed index to its current Nifty-
# prefixed name around 2015-11-06 (confirmed live against the real
# ind_close_all archive: "CNX Nifty"/"CNX Bank"/etc. still present
# 2015-11-03, fully replaced by "Nifty 50"/"Nifty Bank"/etc. by
# 2016-01-04); "Nifty 50" itself carried an even earlier name, "S&P CNX
# Nifty", back to the start of the archive (~2012-03-12). Without mapping
# these, download_index_ohlcv's TRACKED_INDICES filter silently dropped
# every pre-rename row — real historical data was reachable but never
# actually stored under a name any consumer queries for. Each entry here
# was verified against a real historical CSV, not guessed — see
# BacktestUmbrellaPlan.md's 2026-07-20 index-history entry.
#
# "Nifty Healthcare Index" and "Nifty Oil & Gas" have no entry: neither
# appeared under any CNX-era name in the real 2012-2015 archives checked
# (both indices launched later) — left unmapped rather than guessed, so
# they simply have no data before their real launch date, same as today.
HISTORICAL_INDEX_ALIASES = {
    "Nifty 50": ["CNX Nifty", "S&P CNX Nifty"],
    # Same CNX-era rename pattern as its siblings (see the note above);
    # included so the archive's pre-2016 rows are captured under the
    # current name rather than silently dropped.
    "Nifty 100": ["CNX 100", "S&P CNX 100"],
    "Nifty 500": ["CNX 500", "S&P CNX 500"],
    "Nifty Auto": ["CNX Auto"],
    "Nifty Bank": ["CNX Bank"],
    "Nifty IT": ["CNX IT"],
    "Nifty FMCG": ["CNX FMCG"],
    "Nifty Metal": ["CNX Metal"],
    "Nifty Realty": ["CNX Realty"],
    "Nifty Energy": ["CNX Energy"],
    "Nifty PSE": ["CNX PSE"],
    "Nifty Financial Services": ["CNX Finance"],
    "Nifty Pharma": ["CNX Pharma"],
    "Nifty Media": ["CNX Media"],
}
_ALIAS_TO_CANONICAL = {
    alias: canonical for canonical, aliases in HISTORICAL_INDEX_ALIASES.items() for alias in aliases
}

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

    def _fetch() -> pd.DataFrame:
        session = _nse_session()
        response = session.get(url, timeout=15)
        response.raise_for_status()
        return pd.read_csv(io.StringIO(response.text))

    try:
        return retry_call(
            _fetch,
            retries=MAX_RETRIES,
            label=f"Indices-close fetch for {trade_date.date()}",
            wait_seconds=RETRY_DELAY_SECONDS,
            exceptions=(requests.RequestException, pd.errors.ParserError),
        )
    except ConnectionError as exc:
        raise ConnectionError(
            f"Failed to download indices-close CSV for {trade_date.date()} "
            f"after {MAX_RETRIES} attempts: {exc}"
        ) from exc


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

    # Same RCA as ingestion/scrapers/bhavcopy.py's DATE1 check: NSE's
    # archives.nseindia.com host has been observed to return HTTP 200 with
    # the last available file (not a 404) when queried for a date it has no
    # data for (e.g. an undeclared holiday). ind_close_all carries its own
    # "Index Date" column (e.g. "15-Jan-2025") — verify it matches the
    # requested date instead of trusting the response blindly, so a holiday
    # gap doesn't silently duplicate the prior trading day's index OHLCV
    # under today's date.
    if "Index Date" in raw.columns:
        index_date_str = raw["Index Date"].astype(str).str.strip()
        # 2026-07-20: historical archive files (pre-~2016) stamp this column
        # "DD-MM-YYYY" (e.g. "05-01-2015"), not the "DD-Mon-YYYY" format
        # ("15-Jan-2025") current files use — parsing only the current
        # format left this staleness check silently inert (every row
        # parsed to NaT, so `mismatched` was always empty) for any
        # historical backfill date. Try both, current format first.
        returned_dates = pd.to_datetime(index_date_str, format="%d-%b-%Y", errors="coerce")
        still_unparsed = returned_dates.isna()
        if still_unparsed.any():
            returned_dates = returned_dates.where(
                ~still_unparsed, pd.to_datetime(index_date_str, format="%d-%m-%Y", errors="coerce")
            )
        mismatched = returned_dates.dropna().dt.date.astype(str)
        mismatched = mismatched[mismatched != date]
        if not mismatched.empty:
            raise ValueError(
                f"Indices-close fetch for {date} returned data stamped {mismatched.iloc[0]} "
                f"(NSE archive likely served a stale/cached file — the requested date "
                f"may be an undeclared holiday; check config/nse_holidays.py)"
            )

    # 2026-07-20 fix: canonicalize known historical NSE index names (see
    # HISTORICAL_INDEX_ALIASES) to their current name BEFORE filtering to
    # TRACKED_INDICES — otherwise every pre-rename row (e.g. "CNX Nifty"
    # before NSE's ~2015-11-06 rename to "Nifty 50") was silently dropped
    # by the isin() filter below, even though the real data was right
    # there in the fetched CSV.
    raw["Index Name"] = raw["Index Name"].replace(_ALIAS_TO_CANONICAL)

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
