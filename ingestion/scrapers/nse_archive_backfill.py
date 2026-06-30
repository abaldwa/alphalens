"""
ingestion/scrapers/nse_archive_backfill.py

Phase: 3.4 (NSE Historical Archive Backfill)
Specs: SPEC-PIPE-001, SPEC-PIPE-002, SPEC-DS-007
Owner: Platform / Ingestion
Consumers: ingestion/backfill_runner, operator (one-time run)

Downloads historical NSE equity bhavcopy data going back to 2006 and
writes it to the ohlcv_adjusted DuckDB table (adj_factor=1.0; corporate
action adjustment is applied afterwards by price_adjuster.py).

Two NSE archive URL formats are used automatically based on date:

  2006-01-02 → 2024-07-31   cm-bhav format (ZIP per trading day):
    https://nsearchives.nseindia.com/content/historical/EQUITIES/
    {YYYY}/{MMM}/cm{DD}{MMM}{YYYY}bhav.csv.zip
    Columns: SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, TOTTRDQTY, ISIN, …
    No delivery data. ISIN available.

  2024-08-01 → today        sec_bhavdata_full format (CSV per trading day):
    https://archives.nseindia.com/products/content/
    sec_bhavdata_full_{DDMMYYYY}.csv
    Columns: SYMBOL, SERIES, OPEN_PRICE, HIGH_PRICE, LOW_PRICE,
             CLOSE_PRICE, TTL_TRD_QNTY, DELIV_QTY, DELIV_PER, …
    Delivery data available.

Both return unadjusted (raw) prices. Filter: EQ series only.

Prices are NOT adjusted here; run ingestion/adjust/price_adjuster.py
(or step_adjust_prices in daily_pipeline.py) afterwards.

Progress & Resume
-----------------
A checkpoint file (NSE_ARCHIVE_CHECKPOINT_PATH from config.settings) stores
the last successfully written date (YYYY-MM-DD). Re-running will skip all
dates already in DuckDB (>= MIN_STOCKS_PER_DATE EQ stocks written) and
resume from the next missing date, making the process fully idempotent.

Non-trading days (weekends, NSE holidays) return HTTP 404 or an empty/tiny
file; they are silently skipped and never written to the checkpoint.

Estimated runtime (full 2006-present, ~4,900 trading days)
-----------------------------------------------------------
~1.5 hours at default 1.1s/request (polite rate limit, single-threaded).

Usage
-----
  # Full backfill from 2006 (default)
  nohup .venv/bin/python3 -m ingestion.scrapers.nse_archive_backfill \\
      > logs/nse_archive.log 2>&1 &

  # Custom date range
  .venv/bin/python3 -m ingestion.scrapers.nse_archive_backfill \\
      --from-date 2015-01-01 --to-date 2020-12-31

  # Check progress without writing (dry run)
  .venv/bin/python3 -m ingestion.scrapers.nse_archive_backfill --dry-run

  # Resume after interruption (auto — checkpoint is read automatically)
  .venv/bin/python3 -m ingestion.scrapers.nse_archive_backfill
"""

import argparse
import io
import logging
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import requests

from config.settings import DUCKDB_PATH, RAW_DIR
from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

# Archive URL switch date: cm-bhav works up to this date, sec_bhavdata_full from next day
_CM_BHAV_LAST_DATE = date(2024, 7, 31)

# NSE archive base URLs
_CM_BHAV_URL = (
    "https://nsearchives.nseindia.com/content/historical/EQUITIES"
    "/{year}/{month}/cm{day}{month}{year}bhav.csv.zip"
)
_SEC_BHAV_URL = (
    "https://archives.nseindia.com/products/content"
    "/sec_bhavdata_full_{ddmmyyyy}.csv"
)

# NSE warm-up URL (required to get session cookies)
_NSE_HOMEPAGE = "https://www.nseindia.com"

# Backfill defaults
DEFAULT_FROM_DATE = date(2006, 1, 2)   # earliest confirmed available date

# Checkpoint
NSE_ARCHIVE_CHECKPOINT_PATH = RAW_DIR / "nse_archive" / "backfill_checkpoint.txt"

# DB size limit — stop and warn if file exceeds this many bytes
DB_SIZE_CHECK_INTERVAL = 100            # log DB size every N dates downloaded

# Skip dates already loaded with at least this many EQ stocks
MIN_STOCKS_PER_DATE = 50

# Only keep EQ series (same filter as daily bhavcopy pipeline)
EQ_SERIES = {"EQ"}

# Polite inter-request sleep (seconds) — NSE archive is public but rate-sensitive
REQUEST_SLEEP = 1.1

# Minimum file size to be considered a real trading-day file (bytes)
MIN_FILE_SIZE = 5_000

# Column mappings
_CM_BHAV_COLS = {
    "SYMBOL": "ticker",
    "OPEN":   "open",
    "HIGH":   "high",
    "LOW":    "low",
    "CLOSE":  "close",
    "TOTTRDQTY": "volume",
}
_SEC_BHAV_COLS = {
    "SYMBOL":      "ticker",
    "OPEN_PRICE":  "open",
    "HIGH_PRICE":  "high",
    "LOW_PRICE":   "low",
    "CLOSE_PRICE": "close",
    "TTL_TRD_QNTY": "volume",
    "DELIV_QTY":   "delivery_qty",
    "DELIV_PER":   "delivery_pct",
}

_UPSERT_SQL = """
    INSERT INTO ohlcv_adjusted
        (date, ticker, open, high, low, close, volume, delivery_qty, delivery_pct, adj_factor)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1.0)
    ON CONFLICT (date, ticker) DO UPDATE SET
        open         = excluded.open,
        high         = excluded.high,
        low          = excluded.low,
        close        = excluded.close,
        volume       = excluded.volume,
        delivery_qty = excluded.delivery_qty,
        delivery_pct = excluded.delivery_pct
"""


# ── Session management ─────────────────────────────────────────────────────────


def _make_session() -> requests.Session:
    """Create a requests session with NSE homepage cookies."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    try:
        session.get(_NSE_HOMEPAGE, timeout=15)
        time.sleep(1.0)
    except Exception as exc:
        logger.warning("Could not warm NSE session: %s", exc)
    return session


def _refresh_session_if_needed(session: requests.Session, requests_made: int) -> requests.Session:
    """Re-warm NSE session every 200 requests to avoid cookie expiry."""
    if requests_made > 0 and requests_made % 200 == 0:
        logger.info("Re-warming NSE session (request #%d)", requests_made)
        try:
            session.get(_NSE_HOMEPAGE, timeout=15)
            time.sleep(1.0)
        except Exception as exc:
            logger.warning("Session re-warm failed: %s — continuing", exc)
    return session


# ── URL builders ───────────────────────────────────────────────────────────────


def _cm_bhav_url(d: date) -> str:
    month_abbr = d.strftime("%b").upper()   # JAN, FEB, …
    return _CM_BHAV_URL.format(
        year=d.strftime("%Y"),
        month=month_abbr,
        day=d.strftime("%d"),
    )


def _sec_bhav_url(d: date) -> str:
    return _SEC_BHAV_URL.format(ddmmyyyy=d.strftime("%d%m%Y"))


def _url_for_date(d: date) -> Tuple[str, str]:
    """Return (url, format_name) for the given date."""
    if d <= _CM_BHAV_LAST_DATE:
        return _cm_bhav_url(d), "cm_bhav"
    return _sec_bhav_url(d), "sec_bhav"


# ── Download & parse ───────────────────────────────────────────────────────────


def _download_cm_bhav(session: requests.Session, d: date) -> Optional[pd.DataFrame]:
    """
    Download and parse the cm-bhav ZIP for date d.

    Returns None if the date is a non-trading day (404, empty file, no EQ rows).
    """
    url = _cm_bhav_url(d)
    try:
        r = session.get(url, timeout=20)
    except requests.RequestException as exc:
        logger.warning("%s: download error — %s", d, exc)
        return None

    if r.status_code == 404 or len(r.content) < MIN_FILE_SIZE:
        return None   # holiday / weekend
    if r.status_code != 200:
        logger.warning("%s cm_bhav: HTTP %d", d, r.status_code)
        return None
    if r.content[:2] != b"PK":
        return None   # BSE redirect HTML

    try:
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        raw = pd.read_csv(zf.open(zf.namelist()[0]))
    except Exception as exc:
        logger.warning("%s cm_bhav: parse error — %s", d, exc)
        return None

    raw.columns = [c.strip().upper() for c in raw.columns]
    if "SYMBOL" not in raw.columns or "SERIES" not in raw.columns:
        return None

    raw["SYMBOL"] = raw["SYMBOL"].astype(str).str.strip()
    raw["SERIES"] = raw["SERIES"].astype(str).str.strip()
    df = raw[raw["SERIES"].isin(EQ_SERIES)].copy()
    if df.empty:
        return None

    # Rename to standard columns
    df = df.rename(columns=_CM_BHAV_COLS)
    keep = ["ticker", "open", "high", "low", "close", "volume"]
    df = df[[c for c in keep if c in df.columns]].copy()

    # Delivery data not available in cm-bhav
    df["delivery_qty"] = None
    df["delivery_pct"] = None

    # Coerce numeric
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"])
    df = df[df["close"] > 0]
    return df if not df.empty else None


def _download_sec_bhav(session: requests.Session, d: date) -> Optional[pd.DataFrame]:
    """
    Download and parse the sec_bhavdata_full CSV for date d.

    Returns None for non-trading days.
    """
    url = _sec_bhav_url(d)
    try:
        r = session.get(url, timeout=20)
    except requests.RequestException as exc:
        logger.warning("%s: download error — %s", d, exc)
        return None

    if r.status_code == 404 or len(r.content) < MIN_FILE_SIZE:
        return None
    if r.status_code != 200:
        logger.warning("%s sec_bhav: HTTP %d", d, r.status_code)
        return None

    try:
        raw = pd.read_csv(io.StringIO(r.text))
    except Exception as exc:
        logger.warning("%s sec_bhav: parse error — %s", d, exc)
        return None

    raw.columns = [c.strip().upper() for c in raw.columns]
    if "SYMBOL" not in raw.columns:
        return None

    raw["SYMBOL"] = raw["SYMBOL"].astype(str).str.strip()
    raw["SERIES"] = raw["SERIES"].astype(str).str.strip() if "SERIES" in raw.columns else "EQ"
    df = raw[raw["SERIES"].isin(EQ_SERIES)].copy()
    if df.empty:
        return None

    df = df.rename(columns=_SEC_BHAV_COLS)
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ("delivery_qty", "delivery_pct"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = None

    df = df.dropna(subset=["open", "high", "low", "close"])
    df = df[df["close"] > 0]
    keep = ["ticker", "open", "high", "low", "close", "volume", "delivery_qty", "delivery_pct"]
    df = df[[c for c in keep if c in df.columns]].copy()
    return df if not df.empty else None


def download_for_date(session: requests.Session, d: date) -> Optional[pd.DataFrame]:
    """Download and parse bhavcopy for a single date. Returns None for non-trading days."""
    if d <= _CM_BHAV_LAST_DATE:
        return _download_cm_bhav(session, d)
    return _download_sec_bhav(session, d)


# ── DuckDB helpers ─────────────────────────────────────────────────────────────


def _date_already_loaded(conn, d: date) -> bool:
    """Return True if ohlcv_adjusted already has >= MIN_STOCKS_PER_DATE rows for date d."""
    row = conn.execute(
        "SELECT COUNT(*) FROM ohlcv_adjusted WHERE date = ?", [d.isoformat()]
    ).fetchone()
    return (row[0] if row else 0) >= MIN_STOCKS_PER_DATE


def _write_to_duckdb(conn, d: date, df: pd.DataFrame) -> int:
    """Upsert one day's EQ rows into ohlcv_adjusted. Returns rows written."""
    date_str = d.isoformat()
    rows = [
        (
            date_str,
            row.ticker,
            float(row.open),
            float(row.high),
            float(row.low),
            float(row.close),
            int(row.volume) if pd.notna(row.volume) else None,
            int(row.delivery_qty) if pd.notna(getattr(row, "delivery_qty", None)) else None,
            float(row.delivery_pct) if pd.notna(getattr(row, "delivery_pct", None)) else None,
        )
        for row in df.itertuples(index=False)
    ]
    conn.executemany(_UPSERT_SQL, rows)
    return len(rows)


# ── Checkpoint helpers ─────────────────────────────────────────────────────────


def _read_checkpoint() -> Optional[date]:
    if NSE_ARCHIVE_CHECKPOINT_PATH.exists():
        txt = NSE_ARCHIVE_CHECKPOINT_PATH.read_text().strip()
        if txt:
            try:
                return date.fromisoformat(txt)
            except ValueError:
                pass
    return None


def _write_checkpoint(d: date) -> None:
    NSE_ARCHIVE_CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    NSE_ARCHIVE_CHECKPOINT_PATH.write_text(d.isoformat())


def _db_size_bytes(db_path: Path) -> int:
    """Return the DuckDB file size in bytes (WAL file included if present)."""
    total = db_path.stat().st_size if db_path.exists() else 0
    wal = db_path.with_suffix(".duckdb.wal")
    if wal.exists():
        total += wal.stat().st_size
    return total


# ── Date range helpers ─────────────────────────────────────────────────────────


def _trading_dates(from_date: date, to_date: date, reverse: bool = False):
    """
    Yield weekday dates between from_date and to_date.
    reverse=True yields newest→oldest (today first, 2006 last).
    Holidays are skipped at download time (404 → None).
    """
    current = from_date
    dates = []
    while current <= to_date:
        if current.weekday() < 5:
            dates.append(current)
        current += timedelta(days=1)
    if reverse:
        dates.reverse()
    yield from dates


# ── Main backfill function ─────────────────────────────────────────────────────


def run_nse_archive_backfill(
    from_date: date = DEFAULT_FROM_DATE,
    to_date: Optional[date] = None,
    db_path: Optional[Path] = None,
    dry_run: bool = False,
    force: bool = False,
    reverse: bool = True,
) -> dict:
    """
    Download NSE bhavcopy archive from from_date to to_date and write to DuckDB.

    Parameters
    ----------
    from_date : date
        Earliest date in range (default: 2006-01-02).
    to_date : date, optional
        Latest date in range (default: today).
    db_path : Path, optional
        DuckDB path (default: config.settings.DUCKDB_PATH).
    dry_run : bool
        Print stats only — no downloads or DB writes.
    force : bool
        Re-download even dates already in DuckDB.
    reverse : bool
        If True (default), download newest → oldest so recent data is
        available first. Checkpoint tracks the *oldest* date reached.

    Returns
    -------
    dict with keys: dates_processed, dates_skipped, dates_failed,
                    rows_written, elapsed_seconds
    """
    import time as _time

    to_date = to_date or date.today()
    db_path = db_path or DUCKDB_PATH
    t0 = _time.monotonic()

    # Checkpoint: in reverse mode it stores the *oldest* date reached so far.
    # On resume we skip anything newer (already loaded) via DuckDB check.
    checkpoint = _read_checkpoint()
    if checkpoint and not force:
        if reverse:
            # Already went back this far — start from one day earlier
            effective_to = checkpoint - timedelta(days=1)
            effective_from = from_date
            logger.info(
                "Resuming reverse backfill from %s → %s (checkpoint %s already done).",
                effective_to, effective_from, checkpoint,
            )
        else:
            effective_from = checkpoint + timedelta(days=1)
            effective_to = to_date
            logger.info(
                "Resuming forward backfill from %s (checkpoint %s already done).",
                effective_from, checkpoint,
            )
    else:
        effective_from = from_date
        effective_to = to_date

    all_dates = list(_trading_dates(effective_from, effective_to, reverse=reverse))
    total = len(all_dates)
    direction = "newest→oldest" if reverse else "oldest→newest"
    logger.info(
        "NSE archive backfill (%s): %s → %s  (%d weekdays to check)  dry_run=%s",
        direction,
        all_dates[0] if all_dates else effective_from,
        all_dates[-1] if all_dates else effective_to,
        total, dry_run,
    )

    if dry_run:
        logger.info("DRY RUN — no downloads or writes.")
        return {"dates_to_process": total, "from": str(effective_from), "to": str(effective_to)}

    stats = {"dates_processed": 0, "dates_skipped": 0, "dates_failed": 0, "rows_written": 0}

    session = _make_session()
    requests_made = 0

    with get_duckdb_connection(db_path, persist=False) as conn:
        for i, d in enumerate(all_dates, 1):

            # Skip if already loaded
            if not force and _date_already_loaded(conn, d):
                stats["dates_skipped"] += 1
                if i % 500 == 0:
                    logger.info(
                        "Progress: %d/%d  skipped=%d  written=%d rows",
                        i, total, stats["dates_skipped"], stats["rows_written"],
                    )
                continue

            session = _refresh_session_if_needed(session, requests_made)

            df = download_for_date(session, d)
            requests_made += 1
            time.sleep(REQUEST_SLEEP)

            if df is None:
                # Non-trading day (404 / holiday / weekend)
                stats["dates_skipped"] += 1
                continue

            try:
                n = _write_to_duckdb(conn, d, df)
                stats["rows_written"] += n
                stats["dates_processed"] += 1
                _write_checkpoint(d)
            except Exception as exc:
                logger.error("%s: DB write failed — %s", d, exc)
                stats["dates_failed"] += 1
                continue

            if i % 50 == 0 or i == total:
                elapsed = _time.monotonic() - t0
                rate = requests_made / elapsed * 3600
                eta_h = (total - i) / max(rate, 1)
                db_gb = _db_size_bytes(db_path) / 1024 ** 3
                logger.info(
                    "Progress %d/%d  date=%s  written=%d rows  skipped=%d  "
                    "failed=%d  rate=%.0f req/h  ETA=%.1fh  DB=%.2fGB",
                    i, total, d,
                    stats["rows_written"], stats["dates_skipped"],
                    stats["dates_failed"], rate, eta_h, db_gb,
                )

            # Log DB size periodically
            if requests_made % DB_SIZE_CHECK_INTERVAL == 0:
                db_gb = _db_size_bytes(db_path) / 1024 ** 3
                logger.info("DB size: %.2fGB  (date=%s)", db_gb, d)

    stats["elapsed_seconds"] = round(_time.monotonic() - t0, 1)
    db_gb = _db_size_bytes(db_path) / 1024 ** 3
    logger.info(
        "Backfill complete: %d dates processed, %d skipped, %d failed, "
        "%d rows written in %.0fs  DB=%.2fGB",
        stats["dates_processed"], stats["dates_skipped"],
        stats["dates_failed"], stats["rows_written"],
        stats["elapsed_seconds"], db_gb,
    )
    return stats


# ── CLI ────────────────────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(
        description=(
            "NSE archive historical backfill — downloads bhavcopy from "
            "nsearchives.nseindia.com (2006-Jul 2024) and "
            "archives.nseindia.com (Aug 2024-present) into DuckDB."
        )
    )
    parser.add_argument(
        "--from-date",
        default=DEFAULT_FROM_DATE.isoformat(),
        help=f"Start date YYYY-MM-DD (default: {DEFAULT_FROM_DATE})",
    )
    parser.add_argument(
        "--to-date",
        default=date.today().isoformat(),
        help="End date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print stats only — no downloads or DB writes.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even dates already in DuckDB.",
    )
    parser.add_argument(
        "--no-reverse",
        action="store_true",
        help="Download oldest→newest instead of default newest→oldest.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Override DuckDB path (default: config.settings.DUCKDB_PATH)",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path) if args.db_path else None
    stats = run_nse_archive_backfill(
        from_date=date.fromisoformat(args.from_date),
        to_date=date.fromisoformat(args.to_date),
        db_path=db_path,
        dry_run=args.dry_run,
        force=args.force,
        reverse=not args.no_reverse,
    )
    if not args.dry_run:
        print("\nSummary:")
        for k, v in stats.items():
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
