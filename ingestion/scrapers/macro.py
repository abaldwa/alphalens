"""
ingestion/scrapers/macro.py

Phase: 0.4 (Data Ingestion Scrapers)
Specs: SPEC-PIPE-006
Owner: Platform / Ingestion
Consumers: ingestion/scheduler, features/macro_features, datastore/normalised

Downloads daily macro indicators: India VIX (NSE), FII/DII cash activity
(NSE), USD/INR + Brent crude + gold (Yahoo Finance), and India 10yr/3mo
bond yields (FRED, sourced from RBI/OECD upstream — see download_bond_
yields). All retry up to 3 times on failure, then fall back to the most
recent previously-stored value in macro_indicators (DuckDB, Store 2)
rather than failing the pipeline run (SPEC-PIPE-006). FII/DII additionally
carries an explicit `is_stale` flag on fallback, honoring SPEC-PIPE-006's
"mark unavailable, non-critical" language for that source specifically,
while still satisfying the uniform retry+fallback contract requested for
all three original functions (download_vix/download_fiidii/download_fx).

download_crude_oil/download_gold were added in P1.2 (see BuildLog.md) —
download_fx's docstring had already flagged "Crude/Gold follow the
identical pattern in later phases"; this is that phase. download_bond_
yields is a new pattern (FRED CSV, not Yahoo JSON) because RBI's own site
publishes yields as PDF circulars, not a scrapeable JSON/CSV endpoint, and
NSE/CCIL's G-Sec pages either don't expose daily yield data publicly or
return 403 to non-browser clients — see BuildLog.md "P1.2" for the sources
tried and rejected.
"""

import io
import logging
import time
from datetime import date as date_type
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

NSE_HOMEPAGE_URL = "https://www.nseindia.com"
NSE_VIX_URL = "https://www.nseindia.com/api/historicalOR/vixhistory"
NSE_FIIDII_URL = "https://www.nseindia.com/api/fiidiiTradeReact"
YAHOO_FX_URL = "https://query1.finance.yahoo.com/v8/finance/chart/INR=X"
# Brent (not WTI): 01_features.md's crude_oil_change_21d names "Brent crude"
# specifically, and Indian crude imports are priced off Brent, not WTI.
YAHOO_CRUDE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/BZ=F"
YAHOO_GOLD_URL = "https://query1.finance.yahoo.com/v8/finance/chart/GC=F"
# Global index snapshots (2026-07, backlog #1/#2/#3 "Morning Catch-Up
# redesign", Sub-task B): same Yahoo Finance chart JSON endpoint already
# used above for USD/INR/Crude/Gold — no new dependency needed (yfinance
# is not in requirements/*.txt; this project already has its own
# direct-HTTP Yahoo chart client here, so that's the "existing precedent"
# to follow rather than adding a package). Captured once daily at 07:30
# IST alongside VIX/FII-DII/USD-INR — see
# ingestion/scheduler/daily_pipeline.py's step_download_macro_morning.
YAHOO_NASDAQ_URL = "https://query1.finance.yahoo.com/v8/finance/chart/%5EIXIC"
YAHOO_DOW_URL = "https://query1.finance.yahoo.com/v8/finance/chart/%5EDJI"
YAHOO_SP500_URL = "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC"
YAHOO_NIKKEI_URL = "https://query1.finance.yahoo.com/v8/finance/chart/%5EN225"
YAHOO_HANGSENG_URL = "https://query1.finance.yahoo.com/v8/finance/chart/%5EHSI"
# [backlog #2, 2026-07-04] ICE US Dollar Index futures continuous — same
# Yahoo chart endpoint pattern, live-verified (DX-Y.NYB returned a real
# price during design review). Captured alongside the other 5 indices above.
YAHOO_DXY_URL = "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB"
# FRED series IDs (free, no API key, plain CSV) for India bond yields —
# both monthly, not daily; download_bond_yields forward-fills via "most
# recent value <= as_of", same convention as the rest of this module's
# fallback logic. INDIR3TIB01STM (3-month interbank/T-bill rate) is used
# as the short end of the curve for yield_spread_10yr_2yr: a true daily
# India 2-year G-Sec series is not available from a free, scrapeable
# source as of this writing (RBI/CCIL both blocked direct access — see
# module docstring) — documented as an approximation, not the literal 2yr.
FRED_YIELD_10YR_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=INDIRLTLT01STM"
FRED_YIELD_3M_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=INDIR3TIB01STM"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2


def _retry(fetch_fn, *, label: str):
    """
    Call fetch_fn() up to MAX_RETRIES times, sleeping between attempts.

    Parameters
    ----------
    fetch_fn : Callable[[], Any]
    label : str
        Used only for log messages.

    Returns
    -------
    Any
        fetch_fn()'s return value on the first successful attempt.

    Spec References
    ----------------
    SPEC-PIPE-006: "retry 3 times on failure".

    Raises
    ------
    ConnectionError
        After MAX_RETRIES failed attempts.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fetch_fn()
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning(f"{label} fetch attempt {attempt}/{MAX_RETRIES} failed: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
    raise ConnectionError(f"Failed to fetch {label} after {MAX_RETRIES} attempts: {last_exc}")


def _get_previous_value(
    indicator: str,
    before_date: date_type,
    db_path: Optional[Path] = None,
    in_memory: bool = False,
) -> Optional[float]:
    """
    Look up the most recent macro_indicators value strictly before a date.

    Parameters
    ----------
    indicator : str
        e.g. 'INDIA_VIX', 'USD_INR'.
    before_date : date
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH when in_memory is False.
    in_memory : bool
        If True, look up against an in-memory DuckDB (db_path is ignored)
        — matches the in_memory convention used throughout
        datastore/schema/*.py, so this helper is testable the same way.

    Returns
    -------
    float or None
        None if no prior value exists or the table/file isn't there yet.

    Spec References
    ----------------
    SPEC-PIPE-006: fallback-to-previous-day source of truth.

    PIT Assumptions
    ----------------
    Only ever looks strictly backward (date < before_date) — never reads a
    value that wasn't yet known as of before_date.

    Raises
    ------
    None
    """
    from datastore.api.db import get_duckdb_connection

    if in_memory:
        db_path = None
    elif db_path is None:
        from config.settings import DUCKDB_PATH

        db_path = DUCKDB_PATH

    try:
        with get_duckdb_connection(db_path) as conn:
            row = conn.execute(
                "SELECT value FROM macro_indicators "
                "WHERE indicator = ? AND date < ? ORDER BY date DESC LIMIT 1",
                [indicator, before_date.isoformat()],
            ).fetchone()
    except Exception as exc:
        logger.warning(f"Could not look up previous {indicator} value: {exc}")
        return None

    return row[0] if row else None


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    session.get(NSE_HOMEPAGE_URL, timeout=10)
    return session


def download_vix(date: str, db_path: Optional[Path] = None, in_memory: bool = False) -> float:
    """
    Fetch India VIX for one date from NSE; fall back to the previous
    available value if the live fetch fails after retries.

    Parameters
    ----------
    date : str
        "YYYY-MM-DD".
    db_path : Path, optional
        macro_indicators DuckDB path, used only for the fallback lookup.

    Returns
    -------
    float
        India VIX closing value.

    Spec References
    ----------------
    SPEC-PIPE-006: "India VIX from NSE daily; fallback to previous day if
    unavailable."

    PIT Assumptions
    ----------------
    None — VIX is same-day, publicly available data.

    Raises
    ------
    ConnectionError
        If the live fetch fails after MAX_RETRIES and no previous value
        exists in macro_indicators to fall back to.
    """
    trade_date = datetime.strptime(date, "%Y-%m-%d").date()

    def _fetch() -> float:
        session = _session()
        response = session.get(
            NSE_VIX_URL,
            params={"from": trade_date.strftime("%d-%m-%Y"), "to": trade_date.strftime("%d-%m-%Y")},
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") or []
        if not data:
            # A non-trading day (or a date NSE hasn't published yet) returns an
            # empty 'data' list, not an HTTP error -- raising RequestException
            # here routes it through _retry's existing retry+ConnectionError
            # path, which then triggers the documented previous-value fallback
            # (SPEC-PIPE-006), instead of an unhandled IndexError escaping
            # past every caller's exception handling (e.g. crashing the daily
            # pipeline's download_macro step, which only catches ConnectionError).
            raise requests.RequestException(f"Empty VIX 'data' in NSE response for {date}")
        # NSE's historicalOR/vixhistory endpoint returns 'EOD_CLOSE_INDEX_VAL',
        # not a plain 'CLOSE' key -- verified against the live endpoint.
        return float(data[0]["EOD_CLOSE_INDEX_VAL"])

    try:
        return _retry(_fetch, label="India VIX")
    except ConnectionError:
        previous = _get_previous_value("INDIA_VIX", trade_date, db_path, in_memory)
        if previous is None:
            raise
        logger.warning(f"India VIX unavailable for {date}; using previous value {previous}")
        return previous


def download_fiidii(date: str, db_path: Optional[Path] = None, in_memory: bool = False) -> Dict:
    """
    Fetch FII/DII cash buy/sell activity for one date from NSE.

    Parameters
    ----------
    date : str
        "YYYY-MM-DD".
    db_path : Path, optional
        macro_indicators DuckDB path, used only for the fallback lookup.

    Returns
    -------
    dict
        {'fii_buy_cr': float, 'fii_sell_cr': float, 'fii_net_cr': float,
         'dii_buy_cr': float, 'dii_sell_cr': float, 'dii_net_cr': float,
         'is_stale': bool}
        `is_stale` is True when this is a previous-day fallback value
        rather than a fresh fetch (SPEC-PIPE-006: "mark unavailable,
        non-critical").

    Spec References
    ----------------
    SPEC-PIPE-006: "FII/DII from NSE; mark unavailable if scrape fails
    (non-critical)."

    PIT Assumptions
    ----------------
    None — same-day published data.

    Raises
    ------
    ConnectionError
        If the live fetch fails after MAX_RETRIES and no previous value
        exists to fall back to.
    """
    trade_date = datetime.strptime(date, "%Y-%m-%d").date()

    def _fetch() -> Dict:
        session = _session()
        response = session.get(NSE_FIIDII_URL, timeout=15)
        response.raise_for_status()
        payload = response.json()
        fii = next(r for r in payload if r["category"] == "FII/FPI")
        dii = next(r for r in payload if r["category"] == "DII")
        return {
            "fii_buy_cr": float(fii["buyValue"]),
            "fii_sell_cr": float(fii["sellValue"]),
            "fii_net_cr": float(fii["buyValue"]) - float(fii["sellValue"]),
            "dii_buy_cr": float(dii["buyValue"]),
            "dii_sell_cr": float(dii["sellValue"]),
            "dii_net_cr": float(dii["buyValue"]) - float(dii["sellValue"]),
            "is_stale": False,
        }

    try:
        return _retry(_fetch, label="FII/DII")
    except ConnectionError:
        previous = _get_previous_value("FII_DII_NET", trade_date, db_path, in_memory)
        if previous is None:
            raise
        logger.warning(f"FII/DII unavailable for {date}; marking stale, using previous net {previous}")
        return {
            "fii_buy_cr": None,
            "fii_sell_cr": None,
            "fii_net_cr": previous,
            "dii_buy_cr": None,
            "dii_sell_cr": None,
            "dii_net_cr": None,
            "is_stale": True,
        }


def download_fx(date: str, db_path: Optional[Path] = None, in_memory: bool = False) -> Dict:
    """
    Fetch USD/INR for one date from Yahoo Finance; fall back to the
    previous available value if the live fetch fails after retries.

    Parameters
    ----------
    date : str
        "YYYY-MM-DD".
    db_path : Path, optional
        macro_indicators DuckDB path, used only for the fallback lookup.

    Returns
    -------
    dict
        {'usd_inr': float}

    Spec References
    ----------------
    SPEC-PIPE-006: "USD/INR, Crude, Gold from Yahoo Finance; retry 3 times
    on failure." (Crude/Gold follow the identical pattern in later phases;
    USD/INR is implemented here as the representative case.)

    PIT Assumptions
    ----------------
    None — same-day market data.

    Raises
    ------
    ConnectionError
        If the live fetch fails after MAX_RETRIES and no previous value
        exists to fall back to.
    """
    trade_date = datetime.strptime(date, "%Y-%m-%d").date()

    def _fetch() -> Dict:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        response = session.get(YAHOO_FX_URL, timeout=15)
        response.raise_for_status()
        payload = response.json()
        result = payload.get("chart", {}).get("result") or []
        if not result:
            # Same reasoning as download_vix's empty-data guard above: an
            # empty 'result' list must route through _retry's
            # ConnectionError + previous-value fallback path, not raise a
            # raw IndexError that escapes step_download_macro's
            # except ConnectionError and crashes the whole daily pipeline step.
            raise requests.RequestException(f"Empty Yahoo Finance 'result' for {date}")
        close = result[0]["meta"]["regularMarketPrice"]
        return {"usd_inr": float(close)}

    try:
        return _retry(_fetch, label="USD/INR")
    except ConnectionError:
        previous = _get_previous_value("USD_INR", trade_date, db_path, in_memory)
        if previous is None:
            raise
        logger.warning(f"USD/INR unavailable for {date}; using previous value {previous}")
        return {"usd_inr": previous}


def download_crude_oil(date: str, db_path: Optional[Path] = None, in_memory: bool = False) -> Dict:
    """
    Fetch Brent crude (USD/barrel) for one date from Yahoo Finance; fall
    back to the previous available value if the live fetch fails after
    retries. Same pattern as download_fx (download_fx's docstring flagged
    this as a later-phase follow-up; this is that follow-up).

    Parameters
    ----------
    date : str
        "YYYY-MM-DD".
    db_path : Path, optional
        macro_indicators DuckDB path, used only for the fallback lookup.

    Returns
    -------
    dict
        {'crude_oil_price': float}

    Spec References
    ----------------
    SPEC-PIPE-006: "USD/INR, Crude, Gold from Yahoo Finance; retry 3 times
    on failure."

    PIT Assumptions
    ----------------
    None — same-day market data.

    Raises
    ------
    ConnectionError
        If the live fetch fails after MAX_RETRIES and no previous value
        exists to fall back to.
    """
    trade_date = datetime.strptime(date, "%Y-%m-%d").date()

    def _fetch() -> Dict:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        response = session.get(YAHOO_CRUDE_URL, timeout=15)
        response.raise_for_status()
        payload = response.json()
        result = payload.get("chart", {}).get("result") or []
        if not result:
            raise requests.RequestException(f"Empty Yahoo Finance 'result' for Brent crude {date}")
        close = result[0]["meta"]["regularMarketPrice"]
        return {"crude_oil_price": float(close)}

    try:
        return _retry(_fetch, label="Brent Crude")
    except ConnectionError:
        previous = _get_previous_value("CRUDE_OIL", trade_date, db_path, in_memory)
        if previous is None:
            raise
        logger.warning(f"Crude oil unavailable for {date}; using previous value {previous}")
        return {"crude_oil_price": previous}


def download_gold(date: str, db_path: Optional[Path] = None, in_memory: bool = False) -> Dict:
    """
    Fetch COMEX gold (USD/oz) for one date from Yahoo Finance; fall back
    to the previous available value if the live fetch fails after retries.

    Parameters
    ----------
    date : str
        "YYYY-MM-DD".
    db_path : Path, optional
        macro_indicators DuckDB path, used only for the fallback lookup.

    Returns
    -------
    dict
        {'gold_price': float}

    Spec References
    ----------------
    SPEC-PIPE-006: "USD/INR, Crude, Gold from Yahoo Finance; retry 3 times
    on failure."

    PIT Assumptions
    ----------------
    None — same-day market data.

    Raises
    ------
    ConnectionError
        If the live fetch fails after MAX_RETRIES and no previous value
        exists to fall back to.
    """
    trade_date = datetime.strptime(date, "%Y-%m-%d").date()

    def _fetch() -> Dict:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        response = session.get(YAHOO_GOLD_URL, timeout=15)
        response.raise_for_status()
        payload = response.json()
        result = payload.get("chart", {}).get("result") or []
        if not result:
            raise requests.RequestException(f"Empty Yahoo Finance 'result' for Gold {date}")
        close = result[0]["meta"]["regularMarketPrice"]
        return {"gold_price": float(close)}

    try:
        return _retry(_fetch, label="Gold")
    except ConnectionError:
        previous = _get_previous_value("GOLD", trade_date, db_path, in_memory)
        if previous is None:
            raise
        logger.warning(f"Gold unavailable for {date}; using previous value {previous}")
        return {"gold_price": previous}


def _download_yahoo_index(
    date: str,
    url: str,
    indicator: str,
    label: str,
    result_key: str,
    db_path: Optional[Path] = None,
    in_memory: bool = False,
) -> Dict:
    """
    Shared fetch for one global index snapshot via Yahoo Finance's chart
    JSON endpoint — same request/parse/fallback shape as download_fx/
    download_crude_oil/download_gold above, factored out so the five new
    indices (Nasdaq/Dow/S&P 500/Nikkei/Hang Seng, 2026-07 backlog #1/#2/#3
    Sub-task B) don't each re-duplicate it.

    Parameters
    ----------
    date : str
        "YYYY-MM-DD".
    url : str
        Yahoo Finance chart endpoint for this index's ticker.
    indicator : str
        macro_indicators `indicator` name used for the previous-value
        fallback lookup, e.g. 'NASDAQ_COMPOSITE'.
    label : str
        Used only for log/error messages.
    result_key : str
        Key under which the fetched value is returned, e.g. 'nasdaq_composite'.
    db_path : Path, optional
        macro_indicators DuckDB path, used only for the fallback lookup.
    in_memory : bool
        See download_vix's parameter of the same name.

    Returns
    -------
    dict
        {result_key: float}

    Spec References
    ----------------
    2026-07 backlog #1/#2/#3 (Morning Catch-Up redesign), Sub-task B.

    PIT Assumptions
    ----------------
    None — same-day market data when fetched after that market's close;
    when fetched pre-market IST (07:30, well before US/Japan/HK market
    hours), Yahoo's `regularMarketPrice` reflects the most recent prior
    close, same as every other same-day-snapshot indicator in this module
    — see step_download_macro_morning's docstring for why that is the
    intended PIT behavior here, not a bug.

    Raises
    ------
    ConnectionError
        If the live fetch fails after MAX_RETRIES and no previous value
        exists to fall back to.
    """
    trade_date = datetime.strptime(date, "%Y-%m-%d").date()

    def _fetch() -> Dict:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        response = session.get(url, timeout=15)
        response.raise_for_status()
        payload = response.json()
        result = payload.get("chart", {}).get("result") or []
        if not result:
            raise requests.RequestException(f"Empty Yahoo Finance 'result' for {label} {date}")
        close = result[0]["meta"]["regularMarketPrice"]
        return {result_key: float(close)}

    try:
        return _retry(_fetch, label=label)
    except ConnectionError:
        previous = _get_previous_value(indicator, trade_date, db_path, in_memory)
        if previous is None:
            raise
        logger.warning(f"{label} unavailable for {date}; using previous value {previous}")
        return {result_key: previous}


def download_nasdaq(date: str, db_path: Optional[Path] = None, in_memory: bool = False) -> Dict:
    """Fetch Nasdaq Composite (^IXIC) via Yahoo Finance. See _download_yahoo_index."""
    return _download_yahoo_index(
        date, YAHOO_NASDAQ_URL, "NASDAQ_COMPOSITE", "Nasdaq Composite", "nasdaq_composite",
        db_path, in_memory,
    )


def download_dow(date: str, db_path: Optional[Path] = None, in_memory: bool = False) -> Dict:
    """Fetch Dow Jones Industrial Average (^DJI) via Yahoo Finance. See _download_yahoo_index."""
    return _download_yahoo_index(
        date, YAHOO_DOW_URL, "DOW_JONES", "Dow Jones", "dow_jones", db_path, in_memory,
    )


def download_sp500(date: str, db_path: Optional[Path] = None, in_memory: bool = False) -> Dict:
    """Fetch S&P 500 (^GSPC) via Yahoo Finance. See _download_yahoo_index."""
    return _download_yahoo_index(
        date, YAHOO_SP500_URL, "SP500", "S&P 500", "sp500", db_path, in_memory,
    )


def download_nikkei(date: str, db_path: Optional[Path] = None, in_memory: bool = False) -> Dict:
    """Fetch Nikkei 225 (^N225) via Yahoo Finance. See _download_yahoo_index."""
    return _download_yahoo_index(
        date, YAHOO_NIKKEI_URL, "NIKKEI_225", "Nikkei 225", "nikkei_225", db_path, in_memory,
    )


def download_hangseng(date: str, db_path: Optional[Path] = None, in_memory: bool = False) -> Dict:
    """Fetch Hang Seng (^HSI) via Yahoo Finance. See _download_yahoo_index."""
    return _download_yahoo_index(
        date, YAHOO_HANGSENG_URL, "HANG_SENG", "Hang Seng", "hang_seng", db_path, in_memory,
    )


def download_dxy(date: str, db_path: Optional[Path] = None, in_memory: bool = False) -> Dict:
    """Fetch ICE US Dollar Index futures continuous (DX-Y.NYB) via Yahoo Finance.

    [backlog #2, 2026-07-04] See _download_yahoo_index for the shared
    fetch/mark-unavailable pattern.
    """
    return _download_yahoo_index(
        date, YAHOO_DXY_URL, "DXY", "US Dollar Index", "dxy", db_path, in_memory,
    )


def _fetch_fred_series(url: str, label: str) -> pd.Series:
    """
    Fetch a full FRED CSV series and return it as a date-indexed pd.Series.

    Parameters
    ----------
    url : str
        FRED `fredgraph.csv` URL for one series ID.
    label : str
        Used only for log/error messages.

    Returns
    -------
    pd.Series
        Index: observation_date (datetime64). Values: float, NaN rows from
        FRED's "." missing-value marker dropped.

    Raises
    ------
    requests.RequestException
        On a non-2xx response or an unparseable/empty CSV body.

    Notes
    -----
    Deliberately does NOT send the shared browser-spoofing `USER_AGENT`
    header NSE/Yahoo calls use elsewhere in this module — empirically,
    FRED's edge consistently hangs to a read-timeout (not a fast 4xx) when
    that specific header is present, while an unheadered request returns
    in well under a second. Likely bot-mitigation behavior on FRED's side;
    not worth fighting since FRED has no auth/rate-limit need for a public
    CSV endpoint.
    """
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    try:
        df = pd.read_csv(io.StringIO(response.text))
    except Exception as exc:
        raise requests.RequestException(f"Could not parse FRED CSV for {label}: {exc}")

    if df.empty or df.shape[1] < 2:
        raise requests.RequestException(f"Empty/malformed FRED CSV for {label}")

    df.columns = ["observation_date", "value"]
    df["observation_date"] = pd.to_datetime(df["observation_date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"]).sort_values("observation_date")
    return df.set_index("observation_date")["value"]


def download_bond_yields(date: str, db_path: Optional[Path] = None, in_memory: bool = False) -> Dict:
    """
    Fetch India 10yr and 3-month bond yields as of `date` from FRED.

    FRED's India series (sourced from OECD's Main Economic Indicators,
    ultimately RBI data) are monthly, not daily — this looks up the most
    recent published observation with date <= `date` (a PIT-safe
    forward-fill: never reads a value published after `date`), rather than
    requiring an exact-date match the series can't provide.

    Parameters
    ----------
    date : str
        "YYYY-MM-DD".
    db_path : Path, optional
        macro_indicators DuckDB path, used only for the fallback lookup.

    Returns
    -------
    dict
        {'yield_10yr': float, 'yield_3m': float}. `yield_3m` is a 3-month
        interbank/T-bill rate, used elsewhere as the short-end proxy for
        yield_spread_10yr_2yr — see module docstring for why a true daily
        2-year G-Sec series isn't available from a free source.

    Spec References
    ----------------
    SPEC-PIPE-006 (extended in P1.2 — see BuildLog.md — to cover bond
    yields under the same "Yahoo/RBI sources, retry, fallback" framing,
    even though the concrete source ended up being FRED, not RBI directly).

    PIT Assumptions
    ----------------
    Only ever reads observations with observation_date <= `date` — never a
    later-published value, even though FRED itself has no PIT awareness.

    Raises
    ------
    ConnectionError
        If the live fetch fails after MAX_RETRIES and no previous value
        exists to fall back to (checked independently per series).
    """
    trade_date = datetime.strptime(date, "%Y-%m-%d").date()

    def _latest_as_of(series_url: str, label: str) -> float:
        def _fetch() -> float:
            series = _fetch_fred_series(series_url, label)
            eligible = series[series.index.date <= trade_date]  # type: ignore[union-attr]
            if eligible.empty:
                raise requests.RequestException(f"No FRED observation <= {date} for {label}")
            return float(eligible.iloc[-1])

        return _retry(_fetch, label=label)

    result = {}
    for key, url, indicator, label in (
        ("yield_10yr", FRED_YIELD_10YR_URL, "YIELD_10YR", "India 10yr yield"),
        ("yield_3m", FRED_YIELD_3M_URL, "YIELD_3M", "India 3mo yield"),
    ):
        try:
            result[key] = _latest_as_of(url, label)
        except ConnectionError:
            previous = _get_previous_value(indicator, trade_date, db_path, in_memory)
            if previous is None:
                raise
            logger.warning(f"{label} unavailable for {date}; using previous value {previous}")
            result[key] = previous

    return result
