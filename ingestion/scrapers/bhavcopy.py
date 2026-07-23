"""
ingestion/scrapers/bhavcopy.py

Phase: 0.4 (Data Ingestion Scrapers)
Specs: SPEC-PIPE-001, SPEC-PIPE-005
Owner: Platform / Ingestion
Consumers: ingestion/scheduler, ingestion/adjust/price_adjuster, datastore/raw

Downloads the daily NSE equity bhavcopy (OHLCV + delivery, "sec_bhavdata_full")
from NSE archives. Raw response bytes are retained under datastore/raw/bhavcopy/
for audit (SPEC-PIPE-001: "Raw bhavcopy retained in DataStore raw layer").
This module does not write to the normalised store itself — adjustment and
DuckDB insertion happen downstream in ingestion/adjust/price_adjuster.py.

API_SPEC.md contract for this module: download_bhavcopy(date) -> DataFrame,
validate_bhavcopy(df, expected_tickers) -> dict. download_bhavcopy is
implemented below; validate_bhavcopy (SPEC-PIPE-005) now lives in
ingestion/quality/validator.py and is re-exported here unchanged so no
caller's import needs to change (SOLID-S: quality logic has one home).
"""

import io
import logging
from datetime import datetime

import pandas as pd
import requests

from config.etf_exclusions import ETF_TICKERS as _FALLBACK_ETF_TICKERS
from config.settings import MIN_STOCKS_FOR_INFERENCE, RAW_DIR
from features.technical import BENCHMARK_TICKERS
from ingestion.quality.validator import ANOMALY_PCT_CHANGE_THRESHOLD, validate_bhavcopy  # noqa: F401
from ingestion.scrapers._retry import RETRY_DELAY_SECONDS, retry_call

logger = logging.getLogger(__name__)

# NIFTYBEES/NIF100BEES/MONIFTY500 are genuine NSE ETFs and would otherwise be
# excluded by _resolve_etf_exclusions below — but features/technical.py's
# BENCHMARK_TICKERS relies on their OHLCV being present in ohlcv_adjusted as
# the only available Nifty-index-level price series (no raw NSE index is
# ingested). They must never be dropped here even though they're correctly
# excluded from config/nifty500_universe.csv (they aren't tradeable/investable,
# they're used purely as an external benchmark series).
_BENCHMARK_EXEMPT_TICKERS = frozenset(BENCHMARK_TICKERS.values())

NSE_HOMEPAGE_URL = "https://www.nseindia.com"
# "sec_bhavdata_full" is NSE's current combined OHLCV + delivery report —
# it superseded the older split bhavcopy-CSV + separate MTO delivery file.
NSE_BHAVCOPY_URL_TEMPLATE = (
    "https://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv"
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

# SPEC-PIPE-001: required output columns, in order.
REQUIRED_COLUMNS = [
    "ticker", "open", "high", "low", "close", "volume",
    "traded_qty", "delivery_qty", "series",
]

EQ_SERIES = {"EQ"}
EXCLUDED_SERIES = {"BE", "BL", "SM", "ST"}
MAX_RETRIES = 3


def _nse_session() -> requests.Session:
    """
    Build a requests.Session carrying NSE's required browser-like headers
    and homepage cookies (NSE archives reject requests without them).

    Returns
    -------
    requests.Session

    Spec References
    ----------------
    SPEC-PIPE-001

    Raises
    ------
    requests.RequestException
        If the homepage cookie-priming request fails. Caught by callers'
        retry loop — never raised directly to download_bhavcopy callers.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    session.get(NSE_HOMEPAGE_URL, timeout=10)
    return session


def _fetch_bhavcopy_csv(trade_date: datetime) -> pd.DataFrame:
    """
    Fetch the raw sec_bhavdata_full CSV for one date, retrying on failure.

    Parameters
    ----------
    trade_date : datetime

    Returns
    -------
    pd.DataFrame
        Raw NSE columns, unmodified (SYMBOL, SERIES, OPEN_PRICE, ...).

    Spec References
    ----------------
    SPEC-PIPE-001: "Raises: ConnectionError if download fails after 3 retries."

    PIT Assumptions
    ----------------
    None — this is same-day NSE archive data, no PIT lag.

    Raises
    ------
    ConnectionError
        After MAX_RETRIES failed attempts.
    """
    url = NSE_BHAVCOPY_URL_TEMPLATE.format(ddmmyyyy=trade_date.strftime("%d%m%Y"))

    def _fetch() -> pd.DataFrame:
        session = _nse_session()
        response = session.get(url, timeout=15)
        response.raise_for_status()
        return pd.read_csv(io.StringIO(response.text))

    try:
        return retry_call(
            _fetch,
            retries=MAX_RETRIES,
            label=f"Bhavcopy fetch for {trade_date.date()}",
            wait_seconds=RETRY_DELAY_SECONDS,
            exceptions=(requests.RequestException, pd.errors.ParserError),
        )
    except ConnectionError as exc:
        raise ConnectionError(
            f"Failed to download bhavcopy for {trade_date.date()} "
            f"after {MAX_RETRIES} attempts: {exc}"
        ) from exc


def _save_raw(trade_date: datetime, raw: pd.DataFrame) -> None:
    """Persist the unmodified raw fetch to datastore/raw/bhavcopy/ (SPEC-PIPE-001)."""
    raw_dir = RAW_DIR / "bhavcopy"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw.to_csv(raw_dir / f"{trade_date.date().isoformat()}.csv", index=False)


def _resolve_etf_exclusions(date: str) -> set:
    """
    Get today's ETF ticker set to exclude from the bhavcopy, freshest first.

    Tries, in order: (1) a live download of NSE's current ETF-segment list,
    (2) the most recently cached raw ETF-list response, (3) the hardcoded
    bootstrap set in config/etf_exclusions.py. Never raises — an ETF-list
    outage must not block bhavcopy ingestion (SPEC-PIPE-006 "mark
    unavailable, non-critical" pattern), it just risks one day of stale
    exclusions.

    _BENCHMARK_EXEMPT_TICKERS is always subtracted from whichever source
    wins — those 3 ETFs are genuinely NSE ETFs (so any live/cached NSE list
    would legitimately include them) but their OHLCV is a load-bearing
    input elsewhere (features/technical.py's BENCHMARK_TICKERS), so they
    must never be dropped from ohlcv_adjusted regardless of source.
    """
    from ingestion.scrapers import etf_list

    try:
        return etf_list.download_etf_list(date) - _BENCHMARK_EXEMPT_TICKERS
    except ConnectionError as exc:
        logger.warning(f"ETF list download failed for {date}, falling back: {exc}")

    cached = etf_list.load_last_cached_etf_list()
    if cached:
        logger.info(f"Using last cached ETF list for {date} ({len(cached)} tickers)")
        return cached - _BENCHMARK_EXEMPT_TICKERS

    logger.warning(f"No cached ETF list available for {date}; using bootstrap fallback list")
    return set(_FALLBACK_ETF_TICKERS) - _BENCHMARK_EXEMPT_TICKERS


def download_bhavcopy(date: str) -> pd.DataFrame:
    """
    Download and parse the NSE equity bhavcopy for one trading date.

    Parameters
    ----------
    date : str
        Trading date, "YYYY-MM-DD".

    Returns
    -------
    pd.DataFrame
        Columns: ticker, open, high, low, close, volume, traded_qty,
        delivery_qty, series. EQ series only — BE/BL/SM/ST and all other
        series are filtered out. ETFs are also filtered out — they trade
        under EQ but are not equities — using a fresh daily download of
        NSE's ETF-segment list (see _resolve_etf_exclusions).

    Spec References
    ----------------
    SPEC-PIPE-001: NSE bhavcopy ingestion; raw retained for audit.
    SPEC-PIPE-005: delivery_pct in [0, 100]; completeness gate (>= 450 stocks).

    PIT Assumptions
    ----------------
    None — bhavcopy is same-day, publicly available data with no
    announcement-date lag.

    Raises
    ------
    ConnectionError
        If the download fails after MAX_RETRIES attempts.
    ValueError
        If fewer than MIN_STOCKS_FOR_INFERENCE stocks remain after
        filtering to EQ series, if any ticker appears more than once, if
        any price is <= 0, or if a present delivery_pct falls outside
        [0, 100].
    """
    trade_date = datetime.strptime(date, "%Y-%m-%d")
    raw = _fetch_bhavcopy_csv(trade_date)
    _save_raw(trade_date, raw)

    raw.columns = [c.strip().upper() for c in raw.columns]
    for col in ("SYMBOL", "SERIES"):
        if raw[col].dtype == object:
            raw[col] = raw[col].str.strip()

    # RCA 2026-07-05: NSE's archive returns HTTP 200 with the last available
    # bhavcopy file (not a 404) when queried for a date it has no data for
    # (observed on real holidays missing from config/nse_holidays.py at the
    # time). This silently duplicated the prior trading day's OHLCV+delivery
    # data under the holiday's date across ~2,100 tickers. The raw CSV
    # carries its own trade-date column (DATE1, e.g. "15-Jan-2025") — verify
    # it matches the requested date instead of trusting the response blindly.
    if "DATE1" in raw.columns:
        returned_dates = pd.to_datetime(raw["DATE1"].astype(str).str.strip(), format="%d-%b-%Y", errors="coerce")
        mismatched = returned_dates.dropna().dt.date.astype(str)
        mismatched = mismatched[mismatched != date]
        if not mismatched.empty:
            raise ValueError(
                f"Bhavcopy fetch for {date} returned data stamped {mismatched.iloc[0]} "
                f"(NSE archive likely served a stale/cached file — the requested date "
                f"may be an undeclared holiday; check config/nse_holidays.py)"
            )

    traded_qty = pd.to_numeric(raw["TTL_TRD_QNTY"], errors="coerce")
    delivery_qty = pd.to_numeric(raw["DELIV_QTY"], errors="coerce")

    df = pd.DataFrame(
        {
            "ticker": raw["SYMBOL"],
            "series": raw["SERIES"],
            "open": pd.to_numeric(raw["OPEN_PRICE"], errors="coerce"),
            "high": pd.to_numeric(raw["HIGH_PRICE"], errors="coerce"),
            "low": pd.to_numeric(raw["LOW_PRICE"], errors="coerce"),
            "close": pd.to_numeric(raw["CLOSE_PRICE"], errors="coerce"),
            "volume": traded_qty,
            "traded_qty": traded_qty,
            "delivery_qty": delivery_qty,
        }
    )

    df = df[df["series"].isin(EQ_SERIES)]
    etf_tickers = _resolve_etf_exclusions(date)
    df = df[~df["ticker"].isin(etf_tickers)].reset_index(drop=True)

    duplicated = df["ticker"].duplicated()
    if duplicated.any():
        dupes = sorted(df.loc[duplicated, "ticker"].unique().tolist())
        raise ValueError(f"Duplicate tickers in bhavcopy for {date}: {dupes}")

    price_cols = ["open", "high", "low", "close"]
    if (df[price_cols] <= 0).any().any():
        bad = df.loc[(df[price_cols] <= 0).any(axis=1), "ticker"].tolist()
        raise ValueError(f"Non-positive prices in bhavcopy for {date}: {bad}")

    delivery_pct = (df["delivery_qty"] / df["traded_qty"] * 100).where(df["traded_qty"] > 0)
    out_of_range = delivery_pct.notna() & ~delivery_pct.between(0, 100)
    if out_of_range.any():
        bad = df.loc[out_of_range, "ticker"].tolist()
        raise ValueError(f"delivery_pct out of [0, 100] for {date}: {bad}")

    if len(df) < MIN_STOCKS_FOR_INFERENCE:
        raise ValueError(
            f"Only {len(df)} EQ-series stocks found for {date}; "
            f"minimum required is {MIN_STOCKS_FOR_INFERENCE}"
        )

    logger.info(f"Bhavcopy downloaded for {date}: {len(df)} EQ stocks")
    return df[REQUIRED_COLUMNS]
