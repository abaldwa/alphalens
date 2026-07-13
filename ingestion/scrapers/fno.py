"""
ingestion/scrapers/fno.py

Phase: 0.4 (Data Ingestion Scrapers); URL fixed + persistence wired in 2.3
Specs: SPEC-PIPE-001
Owner: Platform / Ingestion
Consumers: ingestion/scheduler/daily_pipeline.py, features/fno_features.py, datastore/raw

Downloads the daily NSE F&O (futures and options) bhavcopy. Stores open
interest, volume, settlement price, and underlying spot price keyed by
ticker/expiry/strike/option_type, for every instrument type (futures and
options, index and stock). Raw response retained under datastore/raw/fno/
for audit (SPEC-PIPE-001).

[AS BUILT, P2.3] The original NSE_FNO_BHAVCOPY_URL_TEMPLATE
(archives.nseindia.com/content/historical/DERIVATIVES/...) 404s against
NSE's current archive — confirmed live (every recent trading day tried).
NSE migrated to a unified "UDiFF" bhavcopy format; the real, working
endpoint and column set (verified live against 2026-06-22's actual file)
are used here instead. The new format is strictly richer than the old
one: `UndrlygPric` (NSE's own reported underlying/spot price for that
contract) and `ChngInOpnIntrst` (day-over-day OI change, pre-computed by
NSE rather than requiring a separate lag-join) are both new real columns
this module now captures — features/fno_features.py uses both directly.
"""

import io
import logging
from datetime import datetime

import pandas as pd
import requests

from config.settings import RAW_DIR
from ingestion.scrapers._retry import RETRY_DELAY_SECONDS, retry_call
from ingestion.scrapers.bhavcopy import NSE_HOMEPAGE_URL, USER_AGENT

logger = logging.getLogger(__name__)

# [AS BUILT, P2.3] NSE's current UDiFF unified bhavcopy endpoint — verified
# live (HTTP 200, real ~1.4MB zip) against 2026-06-22. Replaces the
# pre-existing broken archives.nseindia.com/content/historical/DERIVATIVES/
# path (404 on every date tried).
NSE_FNO_BHAVCOPY_URL_TEMPLATE = (
    "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip"
)

REQUIRED_COLUMNS = [
    "ticker", "instrument", "expiry", "strike", "option_type",
    "oi", "oi_change", "volume", "settle_price", "close_price", "underlying_price",
]

MAX_RETRIES = 3


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

    url = NSE_FNO_BHAVCOPY_URL_TEMPLATE.format(yyyymmdd=trade_date.strftime("%Y%m%d"))

    def _fetch() -> pd.DataFrame:
        session = _fno_session()
        response = session.get(url, timeout=15)
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
            with zf.open(csv_name) as fh:
                return pd.read_csv(fh)

    try:
        return retry_call(
            _fetch,
            retries=MAX_RETRIES,
            label=f"F&O bhavcopy fetch for {trade_date.date()}",
            wait_seconds=RETRY_DELAY_SECONDS,
            exceptions=(requests.RequestException, zipfile.BadZipFile, pd.errors.ParserError),
        )
    except ConnectionError as exc:
        raise ConnectionError(
            f"Failed to download F&O bhavcopy for {trade_date.date()} "
            f"after {MAX_RETRIES} attempts: {exc}"
        ) from exc


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
        oi_change, volume, settle_price, close_price, underlying_price.
        One row per ticker/expiry/strike/option_type combination (futures
        rows have strike=NaN, option_type=None). `instrument` values:
        STF (stock future), STO (stock option), IDF (index future), IDO
        (index option) — NSE's own UDiFF FinInstrmTp codes, kept as-is
        rather than relabeled, since features/fno_features.py and any
        other consumer can filter on these directly.

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

    for col in ("TckrSymb", "FinInstrmTp", "OptnTp"):
        if col in raw.columns and raw[col].dtype == object:
            raw[col] = raw[col].str.strip()

    option_type = raw["OptnTp"]
    strike = pd.to_numeric(raw["StrkPric"], errors="coerce")

    df = pd.DataFrame(
        {
            "ticker": raw["TckrSymb"],
            "instrument": raw["FinInstrmTp"],
            "expiry": pd.to_datetime(raw["XpryDt"], errors="coerce"),
            "strike": strike,
            "option_type": option_type,
            "oi": pd.to_numeric(raw["OpnIntrst"], errors="coerce"),
            "oi_change": pd.to_numeric(raw["ChngInOpnIntrst"], errors="coerce"),
            "volume": pd.to_numeric(raw["TtlTradgVol"], errors="coerce"),
            "settle_price": pd.to_numeric(raw["SttlmPric"], errors="coerce"),
            "close_price": pd.to_numeric(raw["ClsPric"], errors="coerce"),
            "underlying_price": pd.to_numeric(raw["UndrlygPric"], errors="coerce"),
        }
    )

    logger.info(f"F&O bhavcopy downloaded for {date}: {len(df)} contracts")
    return df[REQUIRED_COLUMNS]
