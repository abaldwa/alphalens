"""
scripts/backfill_macro.py

Phase: 3 (Macro Indicator Backfill)
Specs: SPEC-PIPE-006
Owner: Platform / Ingestion
Consumers: features/macro_features.py

Backfills macro_indicators table for historical dates using batch API calls:
  - India VIX          : NSE historicalOR/vixhistory (1-year windows)
  - FII/DII net flows  : NSE historicalOR/fiidiiTradeReact (1-year windows)
  - USD/INR            : yfinance (INR=X) — full range in one call
  - Brent crude        : yfinance (BZ=F)
  - Gold               : yfinance (GC=F)
  - 10yr/3mo yields    : FRED CSV (full history in one download)

No credentials required — all sources are public.

Usage
-----
    # Default: 2006-01-01 → today
    .venv/bin/python3 scripts/backfill_macro.py

    # Custom range
    .venv/bin/python3 scripts/backfill_macro.py --from-date 2015-01-01
"""

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)

NSE_HOMEPAGE = "https://www.nseindia.com"
NSE_VIX_HIST = "https://www.nseindia.com/api/historicalOR/vixhistory"
NSE_FIIDII_HIST = "https://www.nseindia.com/api/historicalOR/fiidiiTradeReact"
FRED_10YR = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=INDIRLTLT01STM"
FRED_3M = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=INDIR3TIB01STM"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
SLEEP = 1.0


def _nse_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    s.get(NSE_HOMEPAGE, timeout=15)
    return s


def _yearly_windows(from_dt: date, to_dt: date) -> List[tuple]:
    windows = []
    start = from_dt
    while start <= to_dt:
        end = min(date(start.year, 12, 31), to_dt)
        windows.append((start, end))
        start = date(start.year + 1, 1, 1)
    return windows


# ---------------------------------------------------------------------------
# VIX
# ---------------------------------------------------------------------------

def _fetch_vix_window(session: requests.Session, from_dt: date, to_dt: date) -> pd.DataFrame:
    params = {"from": from_dt.strftime("%d-%m-%Y"), "to": to_dt.strftime("%d-%m-%Y")}
    for attempt in range(1, 4):
        try:
            r = session.get(NSE_VIX_HIST, params=params, timeout=20)
            r.raise_for_status()
            data = r.json().get("data", [])
            if not data:
                return pd.DataFrame()
            rows = []
            for rec in data:
                dt_str = rec.get("EOD_TIMESTAMP") or rec.get("TIMESTAMP", "")
                val = rec.get("EOD_CLOSE_INDEX_VAL") or rec.get("VIX_CLOSE")
                if not dt_str or val is None:
                    continue
                try:
                    dt = datetime.strptime(dt_str, "%d-%b-%Y").date()
                except ValueError:
                    try:
                        dt = datetime.strptime(dt_str, "%Y-%m-%d").date()
                    except ValueError:
                        continue
                rows.append({"date": dt.isoformat(), "indicator": "INDIA_VIX", "value": float(val)})
            return pd.DataFrame(rows)
        except Exception as exc:
            logger.warning("VIX attempt %d/3 failed (%s→%s): %s", attempt, from_dt, to_dt, exc)
            if attempt < 3:
                time.sleep(5)
    return pd.DataFrame()


def backfill_vix(session: requests.Session, from_dt: date, to_dt: date) -> pd.DataFrame:
    frames = []
    for w_start, w_end in _yearly_windows(from_dt, to_dt):
        df = _fetch_vix_window(session, w_start, w_end)
        if not df.empty:
            frames.append(df)
            logger.info("VIX %s → %s: %d rows", w_start, w_end, len(df))
        time.sleep(SLEEP)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["date", "indicator", "value"]
    )


# ---------------------------------------------------------------------------
# FII / DII
# ---------------------------------------------------------------------------

def _fetch_fiidii_window(session: requests.Session, from_dt: date, to_dt: date) -> pd.DataFrame:
    params = {"from": from_dt.strftime("%d-%m-%Y"), "to": to_dt.strftime("%d-%m-%Y")}
    for attempt in range(1, 4):
        try:
            r = session.get(NSE_FIIDII_HIST, params=params, timeout=20)
            r.raise_for_status()
            payload = r.json()
            # NSE may return list or {"data": [...]}
            data = payload if isinstance(payload, list) else payload.get("data", [])
            if not data:
                return pd.DataFrame()
            rows = []
            for rec in data:
                dt_str = rec.get("date") or rec.get("DATE") or ""
                try:
                    dt = datetime.strptime(dt_str, "%d-%b-%Y").date()
                except ValueError:
                    try:
                        dt = datetime.strptime(dt_str, "%Y-%m-%d").date()
                    except ValueError:
                        continue
                date_iso = dt.isoformat()
                try:
                    fii_buy = float(rec.get("fiiBuy") or rec.get("FII_BUY_VALUE") or 0)
                    fii_sell = float(rec.get("fiiSell") or rec.get("FII_SELL_VALUE") or 0)
                    dii_buy = float(rec.get("diiBuy") or rec.get("DII_BUY_VALUE") or 0)
                    dii_sell = float(rec.get("diiSell") or rec.get("DII_SELL_VALUE") or 0)
                except (TypeError, ValueError):
                    continue
                rows.append({"date": date_iso, "indicator": "FII_NET_CR", "value": fii_buy - fii_sell})
                rows.append({"date": date_iso, "indicator": "DII_NET_CR", "value": dii_buy - dii_sell})
            return pd.DataFrame(rows)
        except Exception as exc:
            logger.warning("FII/DII attempt %d/3 failed (%s→%s): %s", attempt, from_dt, to_dt, exc)
            if attempt < 3:
                time.sleep(5)
    return pd.DataFrame()


def backfill_fiidii(session: requests.Session, from_dt: date, to_dt: date) -> pd.DataFrame:
    frames = []
    for w_start, w_end in _yearly_windows(from_dt, to_dt):
        df = _fetch_fiidii_window(session, w_start, w_end)
        if not df.empty:
            frames.append(df)
            logger.info("FII/DII %s → %s: %d rows", w_start, w_end, len(df))
        time.sleep(SLEEP)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["date", "indicator", "value"]
    )


# ---------------------------------------------------------------------------
# Yahoo Finance via yfinance (handles rate limiting internally)
# ---------------------------------------------------------------------------

def _fetch_yahoo(symbol: str, indicator: str, from_dt: date, to_dt: date) -> pd.DataFrame:
    import yfinance as yf
    for attempt in range(1, 4):
        try:
            df = yf.download(
                symbol,
                start=from_dt.isoformat(),
                end=(to_dt + timedelta(days=1)).isoformat(),
                auto_adjust=True,
                progress=False,
            )
            if df.empty:
                logger.warning("yfinance %s: empty result", symbol)
                return pd.DataFrame(columns=["date", "indicator", "value"])
            close = df["Close"]
            if hasattr(close, "squeeze"):
                close = close.squeeze()
            rows = [
                {"date": idx.date().isoformat(), "indicator": indicator, "value": float(v)}
                for idx, v in close.items()
                if v is not None and not (hasattr(v, "__float__") and str(v) == "nan")
            ]
            result = pd.DataFrame(rows)
            logger.info("yfinance %s (%s): %d rows", symbol, indicator, len(result))
            return result
        except Exception as exc:
            logger.warning("yfinance %s attempt %d/3 failed: %s", symbol, attempt, exc)
            if attempt < 3:
                time.sleep(10)
    return pd.DataFrame(columns=["date", "indicator", "value"])


# ---------------------------------------------------------------------------
# FRED bond yields
# ---------------------------------------------------------------------------

def _fetch_fred(url: str, indicator: str) -> pd.DataFrame:
    for attempt in range(1, 4):
        try:
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=90)
            r.raise_for_status()
            df = pd.read_csv(pd.io.common.StringIO(r.text))
            df.columns = ["observation_date", "value"]
            df = df[df["value"] != "."].copy()
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna(subset=["value"])
            df["date"] = pd.to_datetime(df["observation_date"]).dt.date.apply(lambda d: d.isoformat())
            df["indicator"] = indicator
            logger.info("FRED %s: %d rows", indicator, len(df))
            return df[["date", "indicator", "value"]]
        except Exception as exc:
            logger.warning("FRED %s attempt %d/3 failed: %s", indicator, attempt, exc)
            if attempt < 3:
                time.sleep(5)
    return pd.DataFrame(columns=["date", "indicator", "value"])


# ---------------------------------------------------------------------------
# DB write
# ---------------------------------------------------------------------------

def _upsert(conn, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    conn.register("_macro_batch", df)
    conn.execute("""
        INSERT INTO macro_indicators (date, indicator, value)
        SELECT CAST(date AS DATE), indicator, value FROM _macro_batch
        ON CONFLICT (date, indicator) DO UPDATE SET value = excluded.value
    """)
    conn.unregister("_macro_batch")
    return len(df)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill macro_indicators from NSE/Yahoo/FRED")
    parser.add_argument("--from-date", default="2006-01-01", metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--skip-vix", action="store_true")
    parser.add_argument("--skip-fiidii", action="store_true")
    parser.add_argument("--skip-yahoo", action="store_true")
    parser.add_argument("--skip-bonds", action="store_true")
    args = parser.parse_args()

    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection

    from_dt = date.fromisoformat(args.from_date)
    to_dt = date.fromisoformat(args.to_date) if args.to_date else date.today()
    logger.info("Macro backfill %s → %s", from_dt, to_dt)

    session = _nse_session()
    all_frames: List[pd.DataFrame] = []

    if not args.skip_vix:
        logger.info("--- India VIX ---")
        all_frames.append(backfill_vix(session, from_dt, to_dt))

    if not args.skip_fiidii:
        logger.info("--- FII/DII ---")
        all_frames.append(backfill_fiidii(session, from_dt, to_dt))

    if not args.skip_yahoo:
        logger.info("--- Yahoo Finance (FX / Crude / Gold) ---")
        all_frames.append(_fetch_yahoo("INR=X", "USD_INR", from_dt, to_dt))
        time.sleep(SLEEP)
        all_frames.append(_fetch_yahoo("BZ=F", "CRUDE_OIL", from_dt, to_dt))
        time.sleep(SLEEP)
        all_frames.append(_fetch_yahoo("GC=F", "GOLD", from_dt, to_dt))

    if not args.skip_bonds:
        logger.info("--- FRED Bond Yields ---")
        all_frames.append(_fetch_fred(FRED_10YR, "YIELD_10YR"))
        time.sleep(SLEEP)
        all_frames.append(_fetch_fred(FRED_3M, "YIELD_3M"))

    combined = pd.concat([f for f in all_frames if not f.empty], ignore_index=True)
    combined = combined.drop_duplicates(subset=["date", "indicator"])
    logger.info("Total rows to upsert: %d", len(combined))

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        n = _upsert(conn, combined)
        summary = conn.execute(
            "SELECT indicator, MIN(date), MAX(date), COUNT(*) FROM macro_indicators "
            "GROUP BY indicator ORDER BY indicator"
        ).fetchall()

    logger.info("Upserted %d rows. Final macro_indicators summary:", n)
    for r in summary:
        logger.info("  %-20s  %s → %s  (%d rows)", r[0], r[1], r[2], r[3])


if __name__ == "__main__":
    main()
